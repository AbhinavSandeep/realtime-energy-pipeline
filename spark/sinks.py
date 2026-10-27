"""Idempotent sinks used from foreachBatch (they run on the driver).

Spark's JDBC writer can only append, which would create duplicates after a restart or
when a window is updated by late events. Therefore each micro-batch is written with
INSERT ... ON CONFLICT (upsert), which makes re-processing a batch harmless.
"""
from __future__ import annotations

import os
import time
from typing import Callable, Iterable

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CHUNK = 2000


def connect():
    for attempt in range(10):
        try:
            return psycopg2.connect(
                host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
                dbname=os.environ["DB_NAME"], user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"], options="-c timezone=UTC",
                connect_timeout=10, application_name="spark-stream-job")
        except psycopg2.OperationalError as exc:
            print(f"database not reachable ({exc}), retry {attempt + 1}", flush=True)
            time.sleep(3)
    raise RuntimeError("database not reachable")


def upsert_sql(table: str, columns: list[str], keys: list[str], do_nothing: bool) -> str:
    col_list = ", ".join(columns)
    conflict = ", ".join(keys)
    if do_nothing:
        return f"INSERT INTO {table} ({col_list}) VALUES %s ON CONFLICT ({conflict}) DO NOTHING"
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in keys)
    return (f"INSERT INTO {table} ({col_list}) VALUES %s "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}, updated_at = now()")


def chunks(rows: Iterable, size: int):
    buf = []
    for r in rows:
        buf.append(tuple(r))
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf


def upsert_batch(table: str, keys: list[str], do_nothing: bool = False,
                 kafka_topic: str | None = None, kafka_bootstrap: str | None = None
                 ) -> Callable[[DataFrame, int], None]:
    def write(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        batch = batch.persist()
        try:
            if kafka_topic:  # publish results for further consumers (e.g. alerting)
                (batch.select(F.col("meter_id").alias("key"),
                              F.to_json(F.struct(*batch.columns)).alias("value"))
                      .write.format("kafka")
                      .option("kafka.bootstrap.servers", kafka_bootstrap)
                      .option("topic", kafka_topic)
                      .save())

            columns = batch.columns
            sql = upsert_sql(table, columns, keys, do_nothing)
            n = 0
            with connect() as conn, conn.cursor() as cur:
                for part in chunks(batch.toLocalIterator(), CHUNK):
                    execute_values(cur, sql, part, page_size=CHUNK)
                    n += len(part)
            conn.close()
            print(f"[{table}] batch {batch_id}: {n} rows upserted", flush=True)
        finally:
            batch.unpersist()

    return write
