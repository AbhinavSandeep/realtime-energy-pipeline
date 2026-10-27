"""Pure functions of the producer: parsing, pseudonymisation, pacing and late events.

Kept free of Kafka so that they can be unit-tested without a broker.
"""
from __future__ import annotations

import hashlib
import random
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator

HEADER_PREFIX = "Date;Time;"

MEASURES = (
    "global_active_power",
    "global_reactive_power",
    "voltage",
    "global_intensity",
    "sub_metering_1",
    "sub_metering_2",
    "sub_metering_3",
)

SCHEMA_VERSION = 1


class InvalidRecord(ValueError):
    """Raised when a line cannot be turned into an event at all."""


def pseudonymous_meter_id(salt: str, source: str) -> str:
    """Stable, non-reversible meter ID (data minimisation: no real identifier leaves the producer)."""
    digest = hashlib.sha256(f"{salt}:{source}".encode()).hexdigest()
    return f"m-{digest[:10]}"


def _to_float(value: str) -> float | None:
    value = value.strip()
    if value in ("", "?"):
        return None  # missing value: forwarded as null, Spark marks the event as invalid
    try:
        return float(value)
    except ValueError as exc:
        raise InvalidRecord(f"not a number: {value!r}") from exc


def parse_line(line: str, meter_id: str) -> dict:
    """Turn one line of the UCI file into an event.

    Format: ``16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000``
    Structural problems (wrong column count, bad time stamp) raise InvalidRecord.
    Missing measurements ("?") are kept as None so that the stream processor can count them.
    """
    parts = line.rstrip("\r\n").split(";")
    if len(parts) != 2 + len(MEASURES):
        raise InvalidRecord(f"expected {2 + len(MEASURES)} columns, got {len(parts)}")
    try:
        ts = datetime.strptime(f"{parts[0]} {parts[1]}", "%d/%m/%Y %H:%M:%S")
    except ValueError as exc:
        raise InvalidRecord(f"bad timestamp: {parts[0]} {parts[1]}") from exc

    event = {"schema_version": SCHEMA_VERSION, "meter_id": meter_id, "ts": ts.isoformat()}
    for name, raw in zip(MEASURES, parts[2:], strict=True):
        event[name] = _to_float(raw)
    return event


def event_time(event: dict) -> datetime:
    return datetime.fromisoformat(event["ts"])


class Pacer:
    """Maps event time to wall-clock time: ``speedup`` event seconds per real second.

    Uses a fixed schedule (start + offset / speedup) so that small delays do not add up.
    """

    def __init__(self, speedup: float):
        self.speedup = speedup
        self._first_event: datetime | None = None
        self._start_wall: float | None = None

    def delay(self, ts: datetime, now: float) -> float:
        """Seconds to wait before sending an event with time stamp ``ts``."""
        if self.speedup <= 0:
            return 0.0
        if self._first_event is None:
            self._first_event, self._start_wall = ts, now
            return 0.0
        target = self._start_wall + (ts - self._first_event).total_seconds() / self.speedup
        return max(0.0, target - now)


@dataclass
class LateEventInjector:
    """Holds back a small share of events and releases them later (out-of-order arrival).

    An event held back ``delay_minutes`` of event time arrives after newer events.
    If that delay is shorter than the Spark watermark it is still aggregated.
    """

    rate: float
    delay_minutes: float
    rng: random.Random = field(default_factory=random.Random)
    _held: deque = field(default_factory=deque)

    def process(self, event: dict) -> Iterator[dict]:
        now_ts = event_time(event)
        # release everything that has waited long enough
        while self._held and (now_ts - event_time(self._held[0])).total_seconds() >= self.delay_minutes * 60:
            yield self._held.popleft()
        if self.rate > 0 and self.rng.random() < self.rate:
            self._held.append(event)
        else:
            yield event

    def flush(self) -> Iterator[dict]:
        while self._held:
            yield self._held.popleft()
