"""Pre-processing and aggregation microservice (Spark Structured Streaming).

energy.raw (Kafka)
  -> parse JSON, cast, validate
      -> invalid  -> energy.invalid (Kafka, dead-letter topic)
      -> valid    -> energy.readings (TimescaleDB, idempotent insert)
                  -> watermark 10 min
                       -> tumbling 5 min   -> energy.agg_5min  + energy.aggregated (Kafka)
                       -> sliding 15/5 min -> energy.agg_15min
                       -> tumbling 24 h    -> energy.agg_daily
  -> valid/invalid counts per 5 min        -> energy.quality_5min

Every query has its own checkpoint, and all database writes are upserts, so a restart
continues from the last committed Kafka offsets without double counting.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

import sinks

KAFKA = os.environ["KAFKA_BOOTSTRAP"]
RAW_TOPIC = os.environ["RAW_TOPIC"]
AGG_TOPIC = os.environ["AGG_TOPIC"]
DLQ_TOPIC = os.environ["DLQ_TOPIC"]
CHECKPOINTS = os.environ.get("CHECKPOINT_DIR", "/checkpoints")
TRIGGER = f"{int(os.environ.get('TRIGGER_SECONDS', '5'))} seconds"
WATERMARK = os.environ.get("WATERMARK", "10 minutes")
MAX_OFFSETS = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "20000")
V_MIN = float(os.environ.get("VOLTAGE_MIN", "200"))
V_MAX = float(os.environ.get("VOLTAGE_MAX", "260"))

MEASURES = [
    "global_active_power", "global_reactive_power", "voltage", "global_intensity",
    "sub_metering_1", "sub_metering_2", "sub_metering_3",
]

EVENT_SCHEMA = T.StructType(
    [T.StructField("schema_version", T.IntegerType()),
     T.StructField("meter_id", T.StringType()),
     T.StructField("ts", T.StringType())]
    + [T.StructField(m, T.DoubleType()) for m in MEASURES]
)


def build_session() -> SparkSession:
    spark = (SparkSession.builder
             .appName("energy-stream-aggregation")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw(spark: SparkSession) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("subscribe", RAW_TOPIC)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", MAX_OFFSETS)
            .option("failOnDataLoss", "false")
            .load())


def parse_and_validate(raw: DataFrame) -> DataFrame:
    """Adds typed columns, ``is_valid`` and an ``invalid_reason``."""
    parsed = (raw
              .select(F.col("value").cast("string").alias("payload"),
                      F.col("timestamp").alias("kafka_ts"))
              .withColumn("e", F.from_json("payload", EVENT_SCHEMA))
              .select("payload", "kafka_ts", "e.*")
              .withColumn("event_time", F.to_timestamp("ts")))

    reason = (F.when(F.col("e_null"), F.lit("payload is not valid JSON"))
              .when(F.col("meter_id").isNull(), F.lit("missing meter_id"))
              .when(F.col("event_time").isNull(), F.lit("missing or bad timestamp")))
    for m in MEASURES:
        reason = reason.when(F.col(m).isNull(), F.lit(f"missing {m}"))
    reason = (reason
              .when(~F.col("voltage").between(V_MIN, V_MAX), F.lit(f"voltage outside {V_MIN:g}-{V_MAX:g} V"))
              .when(F.col("global_active_power") < 0, F.lit("negative active power"))
              .when(F.col("global_intensity") < 0, F.lit("negative intensity"))
              .when((F.col("sub_metering_1") < 0) | (F.col("sub_metering_2") < 0) | (F.col("sub_metering_3") < 0),
                    F.lit("negative sub-meter value")))

    return (parsed
            .withColumn("e_null", F.col("schema_version").isNull() & F.col("meter_id").isNull())
            .withColumn("invalid_reason", reason)
            .withColumn("is_valid", F.col("invalid_reason").isNull())
            .drop("e_null"))


def flatten_window(df: DataFrame) -> DataFrame:
    return (df.withColumn("window_start", F.col("window.start"))
              .withColumn("window_end", F.col("window.end"))
              .drop("window"))


def main() -> None:
    spark = build_session()
    events = parse_and_validate(read_raw(spark))

    valid = events.filter("is_valid").select("meter_id", "event_time", *MEASURES)
    invalid = events.filter("NOT is_valid")

    def start(df: DataFrame, name: str, writer):
        return (writer(df.writeStream.queryName(name))
                .option("checkpointLocation", f"{CHECKPOINTS}/{name}")
                .trigger(processingTime=TRIGGER)
                .start())

    # 1) dead-letter topic: rejected events with the reason (governance: nothing disappears silently)
    dlq = invalid.select(
        F.coalesce("meter_id", F.lit("unknown")).alias("key"),
        F.to_json(F.struct(
            F.lit("spark").alias("stage"),
            F.col("invalid_reason").alias("reason"),
            F.col("payload").alias("raw"),
            F.current_timestamp().alias("detected_at"))).alias("value"))
    start(dlq, "dlq", lambda w: w.format("kafka")
          .option("kafka.bootstrap.servers", KAFKA).option("topic", DLQ_TOPIC).outputMode("append"))

    # 2) cleaned readings
    readings = valid.withColumnRenamed("event_time", "ts")
    start(readings, "readings", lambda w: w.outputMode("append")
          .foreachBatch(sinks.upsert_batch("energy.readings", ["meter_id", "ts"], do_nothing=True)))

    watermarked = valid.withWatermark("event_time", WATERMARK)

    # 3) tumbling 5-minute windows -> database and Kafka topic for further consumers
    agg_5 = flatten_window(watermarked
        .groupBy("meter_id", F.window("event_time", "5 minutes"))
        .agg(F.avg("global_active_power").alias("avg_active_power"),
             F.min("global_active_power").alias("min_active_power"),
             F.max("global_active_power").alias("max_active_power"),
             F.avg("voltage").alias("avg_voltage"),
             F.min("voltage").alias("min_voltage"),
             F.max("voltage").alias("max_voltage"),
             F.sum("sub_metering_1").alias("sum_sub_1_wh"),
             F.sum("sub_metering_2").alias("sum_sub_2_wh"),
             F.sum("sub_metering_3").alias("sum_sub_3_wh"),
             F.count(F.lit(1)).alias("reading_count")))
    start(agg_5, "agg_5min", lambda w: w.outputMode("update")
          .foreachBatch(sinks.upsert_batch("energy.agg_5min", ["meter_id", "window_start"],
                                           kafka_topic=AGG_TOPIC, kafka_bootstrap=KAFKA)))

    # 4) sliding 15-minute windows, every 5 minutes: smoother trend and peak load
    agg_15 = flatten_window(watermarked
        .groupBy("meter_id", F.window("event_time", "15 minutes", "5 minutes"))
        .agg(F.avg("global_active_power").alias("avg_active_power"),
             F.max("global_intensity").alias("max_intensity"),
             F.count(F.lit(1)).alias("reading_count")))
    start(agg_15, "agg_15min", lambda w: w.outputMode("update")
          .foreachBatch(sinks.upsert_batch("energy.agg_15min", ["meter_id", "window_start"])))

    # 5) daily energy: kW averaged over one minute -> kWh = kW / 60; sub-meters are Wh per minute
    daily = flatten_window(watermarked
        .groupBy("meter_id", F.window("event_time", "1 day"))
        .agg((F.sum("global_active_power") / 60).alias("energy_kwh"),
             (F.sum("sub_metering_1") / 1000).alias("sub_1_kwh"),
             (F.sum("sub_metering_2") / 1000).alias("sub_2_kwh"),
             (F.sum("sub_metering_3") / 1000).alias("sub_3_kwh"),
             F.count(F.lit(1)).alias("reading_count")))
    for i in (1, 2, 3):
        daily = daily.withColumn(f"sub_{i}_share",
                                 F.when(F.col("energy_kwh") > 0, F.col(f"sub_{i}_kwh") / F.col("energy_kwh")))
    start(daily, "agg_daily", lambda w: w.outputMode("update")
          .foreachBatch(sinks.upsert_batch("energy.agg_daily", ["meter_id", "window_start"])))

    # 6) data quality: valid vs. invalid readings per 5-minute window
    quality = flatten_window(events
        .filter(F.col("event_time").isNotNull() & F.col("meter_id").isNotNull())
        .withWatermark("event_time", WATERMARK)
        .groupBy("meter_id", F.window("event_time", "5 minutes"))
        .agg(F.sum(F.col("is_valid").cast("long")).alias("valid_count"),
             F.sum((~F.col("is_valid")).cast("long")).alias("invalid_count")))
    start(quality, "quality_5min", lambda w: w.outputMode("update")
          .foreachBatch(sinks.upsert_batch("energy.quality_5min", ["meter_id", "window_start"])))

    print(f"started {len(spark.streams.active)} streaming queries "
          f"(trigger {TRIGGER}, watermark {WATERMARK})", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
