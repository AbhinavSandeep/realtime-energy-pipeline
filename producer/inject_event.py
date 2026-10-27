"""Sends one manual reading to Kafka, to show that changes reach the aggregates in real time.

Without --ts the event gets the newest event time currently in the stream, so it falls into a
window that is still open (an older time stamp would be behind the watermark and be dropped).
The default meter "m-manual" keeps the manual readings apart from the replayed meters.

Usage (inside the running producer container):
    docker compose exec producer python inject_event.py --power 9.5
    docker compose exec producer python inject_event.py --voltage 999          # invalid -> DLQ
    curl "http://localhost:8000/latest?meter_id=m-manual"                      # a few seconds later
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from confluent_kafka import Producer

from events import SCHEMA_VERSION
from kafka_utils import latest_event_time


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ts", help="event time (ISO, e.g. 2006-12-16T18:02:00); default: newest time in the stream")
    ap.add_argument("--meter-id", default="m-manual")
    ap.add_argument("--power", type=float, default=2.0, help="global active power in kW")
    ap.add_argument("--reactive", type=float, default=0.1)
    ap.add_argument("--voltage", type=float, default=240.0)
    ap.add_argument("--intensity", type=float, default=8.0)
    ap.add_argument("--sub", type=float, nargs=3, default=[0.0, 1.0, 17.0], metavar=("S1", "S2", "S3"))
    args = ap.parse_args()

    bootstrap, topic = os.environ["KAFKA_BOOTSTRAP"], os.environ["RAW_TOPIC"]
    ts = datetime.fromisoformat(args.ts) if args.ts else latest_event_time(bootstrap, topic)
    if ts is None:
        raise SystemExit(f"topic {topic} is empty: pass --ts explicitly")

    event = {
        "schema_version": SCHEMA_VERSION,
        "meter_id": args.meter_id,
        "ts": ts.isoformat(),
        "global_active_power": args.power,
        "global_reactive_power": args.reactive,
        "voltage": args.voltage,
        "global_intensity": args.intensity,
        "sub_metering_1": args.sub[0],
        "sub_metering_2": args.sub[1],
        "sub_metering_3": args.sub[2],
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "manual": True,
    }
    p = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    p.produce(topic, key=args.meter_id, value=json.dumps(event))
    p.flush(10)
    print("sent:", json.dumps(event, indent=2))


if __name__ == "__main__":
    main()
