"""Ingestion microservice: replays the UCI household power file as a live smart-meter stream.

* reads the file line by line and sends one JSON event per minute-reading to Kafka
* paces the events (SPEEDUP event seconds per wall second) to simulate real time
* sends structurally broken lines to the dead-letter topic
* remembers its position in a state file, so a restart does not resend old events
* optionally keeps watching the file for appended rows (FOLLOW=true): manual changes
  to the source therefore become visible in the aggregates
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import KafkaException, Producer

from events import HEADER_PREFIX, InvalidRecord, LateEventInjector, Pacer, event_time, parse_line, pseudonymous_meter_id
from kafka_utils import latest_event_time, stable_instance_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s producer %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        sys.exit(f"missing environment variable {name}")
    return value


BOOTSTRAP = env("KAFKA_BOOTSTRAP")
RAW_TOPIC = env("RAW_TOPIC")
DLQ_TOPIC = env("DLQ_TOPIC")
DATA_FILE = Path(env("DATA_FILE"))
DATA_URL = env("DATA_URL", "")
STATE_DIR = Path(env("STATE_DIR", "/state"))
SPEEDUP = float(env("SPEEDUP", "3600"))
MAX_EVENTS = int(env("MAX_EVENTS", "0"))
LATE_EVENT_RATE = float(env("LATE_EVENT_RATE", "0"))
LATE_DELAY_MINUTES = float(env("LATE_DELAY_MINUTES", "5"))
FOLLOW = env("FOLLOW", "true").lower() == "true"
SALT = env("METER_ID_SALT", "")
EXIT_WHEN_DONE = env("EXIT_WHEN_DONE", "false").lower() == "true"
# A meter that joins a running stream starts at the current event time (like a new device going
# online). Starting at 2006 would put all its events behind the Spark watermark.
JOIN_STREAM_TIME = env("JOIN_STREAM_TIME", "true").lower() == "true"

# Every container gets its own pseudonymous meter (docker compose up --scale producer=3).
# Derived from the stable container name, so a recreated container continues as the same meter.
METER_ID = os.environ.get("METER_ID") or pseudonymous_meter_id(SALT, stable_instance_name())
STATE_FILE = STATE_DIR / f"{METER_ID}.pos"

running = True


def stop(signum, _frame):
    global running
    log.info("signal %s received, finishing", signum)
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


# ------------------------------------------------------------------ data set
def ensure_dataset() -> None:
    if DATA_FILE.exists():
        return
    if not DATA_URL:
        sys.exit(f"{DATA_FILE} not found and DATA_URL is empty")
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    zip_path = DATA_FILE.with_suffix(".zip")
    log.info("downloading data set from %s", DATA_URL)
    urllib.request.urlretrieve(DATA_URL, zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".txt"))
        with zf.open(member) as src, open(DATA_FILE, "wb") as dst:
            dst.write(src.read())
    zip_path.unlink()
    log.info("data set ready: %s (%.0f MB)", DATA_FILE, DATA_FILE.stat().st_size / 1e6)


# ------------------------------------------------------------------ position state
def load_position() -> int:
    try:
        return int(STATE_FILE.read_text().strip())
    except (FileNotFoundError, ValueError):
        return 0


def save_position(line_no: int) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(str(line_no))
    tmp.replace(STATE_FILE)  # atomic


# ------------------------------------------------------------------ kafka
def make_producer() -> Producer:
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "client.id": f"producer-{METER_ID}",
        "acks": "all",                 # wait for the broker to persist the event
        "enable.idempotence": True,    # no duplicates on internal retries
        "linger.ms": 20,
        "compression.type": "lz4",
    }
    for attempt in range(30):
        try:
            p = Producer(conf)
            p.list_topics(timeout=5)
            return p
        except KafkaException as exc:
            log.warning("kafka not ready (%s), retry %d", exc, attempt + 1)
            time.sleep(2)
    sys.exit("kafka not reachable")


def on_delivery(err, msg):
    if err is not None:
        log.error("delivery failed for %s: %s", msg.key(), err)


def send(producer: Producer, topic: str, payload: dict) -> None:
    while True:
        try:
            producer.produce(topic, key=METER_ID, value=json.dumps(payload), on_delivery=on_delivery)
            break
        except BufferError:  # local queue full: let it drain
            producer.poll(0.5)
    producer.poll(0)


def send_invalid(producer: Producer, line: str, reason: str, line_no: int) -> None:
    send(producer, DLQ_TOPIC, {
        "meter_id": METER_ID,
        "stage": "producer",
        "reason": reason,
        "line_no": line_no,
        "raw": line.strip()[:500],
        "detected_at": datetime.now(timezone.utc).isoformat(),
    })


# ------------------------------------------------------------------ main loop
def lines_from(path: Path, start: int):
    """Yields (line_no, line). With FOLLOW it waits for appended lines at EOF (like tail -f)."""
    line_no = 0
    pending = ""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        while running:
            chunk = fh.readline()
            if not chunk:
                if not FOLLOW:
                    return
                time.sleep(1)
                continue
            pending += chunk
            if not pending.endswith("\n"):  # line is still being written
                continue
            line, pending = pending, ""
            line_no += 1
            if line_no > start:
                yield line_no, line


def main() -> None:
    ensure_dataset()
    producer = make_producer()
    pacer = Pacer(SPEEDUP)
    late = LateEventInjector(LATE_EVENT_RATE, LATE_DELAY_MINUTES)
    position = load_position()
    join_at = latest_event_time(BOOTSTRAP, RAW_TOPIC) if position == 0 and JOIN_STREAM_TIME else None
    log.info("meter %s starts at line %d (speed-up %.0fx, follow=%s%s)", METER_ID, position, SPEEDUP, FOLLOW,
             f", joining stream at {join_at.isoformat()}" if join_at else "")

    sent = invalid = 0
    started = time.monotonic()
    line_no = position
    for line_no, line in lines_from(DATA_FILE, position):
        if not running or (MAX_EVENTS and sent >= MAX_EVENTS):
            break
        if line.startswith(HEADER_PREFIX) or not line.strip():
            continue
        try:
            event = parse_line(line, METER_ID)
        except InvalidRecord as exc:
            send_invalid(producer, line, str(exc), line_no)
            invalid += 1
            continue
        if join_at is not None:
            if event_time(event) < join_at:
                continue  # skip history before the current stream time
            join_at = None

        wait = pacer.delay(event_time(event), time.monotonic())
        if wait > 0:
            producer.poll(0)
            time.sleep(wait)

        for out in late.process(event):
            out["sent_at"] = datetime.now(timezone.utc).isoformat()
            send(producer, RAW_TOPIC, out)
            sent += 1

        if line_no % 1000 == 0:
            save_position(line_no)
        if sent and sent % 10000 == 0:
            rate = sent / (time.monotonic() - started)
            log.info("sent %d events (%.0f/s), invalid %d, event time %s", sent, rate, invalid, event["ts"])

    for out in late.flush():
        send(producer, RAW_TOPIC, out)
        sent += 1
    producer.flush(30)
    save_position(line_no)
    log.info("finished: %d events sent, %d invalid lines, position %d", sent, invalid, line_no)
    # keep the container alive (restart policy) without re-sending: idle until stopped
    while running and not EXIT_WHEN_DONE:
        time.sleep(5)


if __name__ == "__main__":
    main()
