"""Delivery microservice: REST API for the real-time energy report.

Reads the aggregates from TimescaleDB with a read-only database role.
Interactive documentation: http://localhost:8000/docs
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

API_KEY = os.environ.get("API_KEY", "")

CONNINFO = (
    f"host={os.environ.get('DB_HOST', 'localhost')} port={os.environ.get('DB_PORT', '5432')} "
    f"dbname={os.environ.get('DB_NAME', 'energy')} user={os.environ.get('DB_USER', 'report_reader')} "
    f"password={os.environ.get('DB_PASSWORD', '')} options='-c timezone=UTC -c default_transaction_read_only=on' "
    f"application_name=energy-api"
)

pool = ConnectionPool(CONNINFO, min_size=1, max_size=5, open=False, kwargs={"row_factory": dict_row})


@asynccontextmanager
async def lifespan(_app: FastAPI):
    pool.open(wait=False)
    yield
    pool.close()


app = FastAPI(
    title="Energy Stream API",
    version="1.0.0",
    description="Real-time aggregates of smart-meter readings (Kafka -> Spark -> TimescaleDB).",
    lifespan=lifespan,
)


def check_key(x_api_key: str | None = Header(default=None)) -> None:
    """Optional API key (set API_KEY in .env to enable)."""
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")


def query(sql: str, params: dict | None = None) -> list[dict]:
    with pool.connection(timeout=5) as conn:
        return conn.execute(sql, params or {}).fetchall()


class WindowSize(str, Enum):
    five = "5min"
    fifteen = "15min"
    daily = "daily"


TABLES = {
    WindowSize.five: "energy.agg_5min",
    WindowSize.fifteen: "energy.agg_15min",
    WindowSize.daily: "energy.agg_daily",
}


@app.get("/health", tags=["system"])
def health() -> dict:
    try:
        query("SELECT 1 AS ok")
        return {"status": "ok", "database": "reachable"}
    except Exception as exc:  # noqa: BLE001 - report any DB problem
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc.__class__.__name__}") from None


@app.get("/meters", tags=["report"], dependencies=[Depends(check_key)])
def meters() -> list[dict]:
    """All meters with the time range already aggregated."""
    return query("""
        SELECT meter_id, min(window_start) AS first_window, max(window_end) AS last_window,
               sum(reading_count)::bigint AS readings
        FROM energy.agg_5min GROUP BY meter_id ORDER BY meter_id""")


@app.get("/latest", tags=["report"], dependencies=[Depends(check_key)])
def latest(meter_id: str | None = None) -> list[dict]:
    """Most recent 5-minute window per meter (or for one meter)."""
    if meter_id:
        return query("SELECT * FROM energy.latest_5min WHERE meter_id = %(m)s", {"m": meter_id})
    return query("SELECT * FROM energy.latest_5min ORDER BY meter_id")


@app.get("/windows", tags=["report"], dependencies=[Depends(check_key)])
def windows(
    meter_id: str,
    size: WindowSize = WindowSize.five,
    start: datetime | None = Query(default=None, description="window_start >= start (ISO time)"),
    end: datetime | None = Query(default=None, description="window_start < end (ISO time)"),
    limit: int = Query(default=288, ge=1, le=5000),
) -> list[dict]:
    """Window results of one meter in a time range, newest first."""
    table = TABLES[size]  # whitelisted table name, never user input in SQL
    return query(f"""
        SELECT * FROM {table}
        WHERE meter_id = %(m)s
          AND (%(s)s::timestamptz IS NULL OR window_start >= %(s)s)
          AND (%(e)s::timestamptz IS NULL OR window_start <  %(e)s)
        ORDER BY window_start DESC LIMIT %(l)s""",
        {"m": meter_id, "s": start, "e": end, "l": limit})


@app.get("/quality", tags=["report"], dependencies=[Depends(check_key)])
def quality(meter_id: str | None = None, limit: int = Query(default=100, ge=1, le=5000)) -> list[dict]:
    """Valid and invalid readings per 5-minute window."""
    return query("""
        SELECT * FROM energy.quality_5min
        WHERE (%(m)s::text IS NULL OR meter_id = %(m)s)
        ORDER BY window_start DESC LIMIT %(l)s""", {"m": meter_id, "l": limit})


@app.get("/stats", tags=["system"], dependencies=[Depends(check_key)])
def stats() -> dict:
    """Row counts and processing latency (time between window end and last update)."""
    rows = query("""
        SELECT (SELECT count(*) FROM energy.readings)  AS readings,
               (SELECT count(*) FROM energy.agg_5min)  AS windows_5min,
               (SELECT count(*) FROM energy.agg_15min) AS windows_15min,
               (SELECT count(*) FROM energy.agg_daily) AS windows_daily,
               (SELECT coalesce(sum(invalid_count), 0)::bigint FROM energy.quality_5min) AS invalid_readings,
               (SELECT max(ingested_at) FROM energy.readings) AS last_ingest""")
    return rows[0]
