#!/bin/bash
# Runs once, when the database volume is created for the first time.
# Creates the schema, the TimescaleDB hypertables and two least-privilege roles:
#   WRITER_USER  -> used by the Spark job (insert/update only)
#   READER_USER  -> used by the API and Grafana (read only)
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v writer="$WRITER_USER" -v writer_pw="$WRITER_PASSWORD" \
     -v reader="$READER_USER" -v reader_pw="$READER_PASSWORD" \
     -v dbname="$POSTGRES_DB" <<'EOSQL'

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ------------------------------------------------------------------ roles
CREATE ROLE :"writer" LOGIN PASSWORD :'writer_pw';
CREATE ROLE :"reader" LOGIN PASSWORD :'reader_pw';
REVOKE ALL ON DATABASE :"dbname" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"dbname" TO :"writer", :"reader";

CREATE SCHEMA energy;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA energy TO :"writer", :"reader";

-- ------------------------------------------------------------------ tables
-- Cleaned readings (one row per meter and minute). Primary key makes
-- inserts idempotent: a replayed event is ignored (ON CONFLICT DO NOTHING).
CREATE TABLE energy.readings (
    meter_id               TEXT             NOT NULL,
    ts                     TIMESTAMPTZ      NOT NULL,
    global_active_power    DOUBLE PRECISION NOT NULL,  -- kW
    global_reactive_power  DOUBLE PRECISION NOT NULL,  -- kW
    voltage                DOUBLE PRECISION NOT NULL,  -- V
    global_intensity       DOUBLE PRECISION NOT NULL,  -- A
    sub_metering_1         DOUBLE PRECISION NOT NULL,  -- Wh, kitchen
    sub_metering_2         DOUBLE PRECISION NOT NULL,  -- Wh, laundry
    sub_metering_3         DOUBLE PRECISION NOT NULL,  -- Wh, water heater + AC
    ingested_at            TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (meter_id, ts)
);
SELECT create_hypertable('energy.readings', by_range('ts', INTERVAL '7 days'));

-- Tumbling 5-minute windows
CREATE TABLE energy.agg_5min (
    meter_id          TEXT             NOT NULL,
    window_start      TIMESTAMPTZ      NOT NULL,
    window_end        TIMESTAMPTZ      NOT NULL,
    avg_active_power  DOUBLE PRECISION,
    min_active_power  DOUBLE PRECISION,
    max_active_power  DOUBLE PRECISION,
    avg_voltage       DOUBLE PRECISION,
    min_voltage       DOUBLE PRECISION,
    max_voltage       DOUBLE PRECISION,
    sum_sub_1_wh      DOUBLE PRECISION,
    sum_sub_2_wh      DOUBLE PRECISION,
    sum_sub_3_wh      DOUBLE PRECISION,
    reading_count     BIGINT,
    updated_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (meter_id, window_start)
);
SELECT create_hypertable('energy.agg_5min', by_range('window_start', INTERVAL '30 days'));

-- Sliding 15-minute windows, slide 5 minutes
CREATE TABLE energy.agg_15min (
    meter_id          TEXT             NOT NULL,
    window_start      TIMESTAMPTZ      NOT NULL,
    window_end        TIMESTAMPTZ      NOT NULL,
    avg_active_power  DOUBLE PRECISION,
    max_intensity     DOUBLE PRECISION,
    reading_count     BIGINT,
    updated_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (meter_id, window_start)
);
SELECT create_hypertable('energy.agg_15min', by_range('window_start', INTERVAL '30 days'));

-- Tumbling 24-hour windows
CREATE TABLE energy.agg_daily (
    meter_id          TEXT             NOT NULL,
    window_start      TIMESTAMPTZ      NOT NULL,
    window_end        TIMESTAMPTZ      NOT NULL,
    energy_kwh        DOUBLE PRECISION,
    sub_1_kwh         DOUBLE PRECISION,
    sub_2_kwh         DOUBLE PRECISION,
    sub_3_kwh         DOUBLE PRECISION,
    sub_1_share       DOUBLE PRECISION,
    sub_2_share       DOUBLE PRECISION,
    sub_3_share       DOUBLE PRECISION,
    reading_count     BIGINT,
    updated_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (meter_id, window_start)
);
SELECT create_hypertable('energy.agg_daily', by_range('window_start', INTERVAL '365 days'));

-- Data quality per 5-minute window
CREATE TABLE energy.quality_5min (
    meter_id          TEXT        NOT NULL,
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,
    valid_count       BIGINT,
    invalid_count     BIGINT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (meter_id, window_start)
);
SELECT create_hypertable('energy.quality_5min', by_range('window_start', INTERVAL '30 days'));

-- Latest 5-minute window per meter (used by the API /latest endpoint)
CREATE VIEW energy.latest_5min AS
SELECT DISTINCT ON (meter_id) *
FROM energy.agg_5min
ORDER BY meter_id, window_start DESC;

-- ------------------------------------------------------------------ grants
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA energy TO :"writer";
GRANT SELECT ON ALL TABLES IN SCHEMA energy TO :"reader";

-- ------------------------------------------------------------------ governance
COMMENT ON SCHEMA energy IS 'Real-time energy pipeline. Owner: data engineering project. Source: UCI household power consumption (pseudonymous meter IDs).';
COMMENT ON TABLE energy.readings  IS 'Validated readings from topic energy.raw (lineage: producer -> Kafka -> Spark).';
COMMENT ON TABLE energy.agg_5min  IS 'Tumbling 5-min windows, event time, watermark 10 min.';
COMMENT ON TABLE energy.agg_15min IS 'Sliding 15-min windows (slide 5 min), event time, watermark 10 min.';
COMMENT ON TABLE energy.agg_daily IS 'Tumbling 24-h windows: energy in kWh and sub-meter shares.';
COMMENT ON TABLE energy.quality_5min IS 'Valid vs. invalid readings per 5-min window.';

-- Retention: the data set is historical (2006-2010), so a time-based
-- retention policy relative to now() would delete everything at once.
-- With live data the following policies would be enabled:
--   SELECT add_retention_policy('energy.readings', INTERVAL '90 days');
--   SELECT add_retention_policy('energy.agg_5min', INTERVAL '365 days');
EOSQL

echo "energy schema, hypertables and roles created."
