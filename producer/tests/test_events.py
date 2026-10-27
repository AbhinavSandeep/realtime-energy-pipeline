import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from events import (  # noqa: E402
    InvalidRecord,
    LateEventInjector,
    Pacer,
    event_time,
    parse_line,
    pseudonymous_meter_id,
)

GOOD = "16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000\n"
MISSING = "28/4/2007;00:21:00;?;?;?;?;?;?;\n"


def test_parse_good_line():
    e = parse_line(GOOD, "m-1")
    assert e["ts"] == "2006-12-16T17:24:00"
    assert e["meter_id"] == "m-1"
    assert e["global_active_power"] == pytest.approx(4.216)
    assert e["voltage"] == pytest.approx(234.84)
    assert e["sub_metering_3"] == pytest.approx(17.0)


def test_missing_values_become_none():
    e = parse_line(MISSING, "m-1")
    assert e["ts"] == "2007-04-28T00:21:00"  # single-digit month is accepted
    assert all(e[k] is None for k in ("global_active_power", "voltage", "sub_metering_3"))


@pytest.mark.parametrize("line", [
    "16/12/2006;17:24:00;4.216\n",                                   # too few columns
    "32/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000\n",  # bad date
    "16/12/2006;17:24:00;abc;0.418;234.840;18.400;0.000;1.000;17.000\n",    # not a number
])
def test_structurally_invalid_lines_raise(line):
    with pytest.raises(InvalidRecord):
        parse_line(line, "m-1")


def test_meter_id_is_stable_and_pseudonymous():
    a = pseudonymous_meter_id("salt", "host-a")
    assert a == pseudonymous_meter_id("salt", "host-a")
    assert a != pseudonymous_meter_id("salt", "host-b")
    assert a != pseudonymous_meter_id("other", "host-a")
    assert "host-a" not in a and a.startswith("m-")


def test_pacer_follows_event_time():
    p = Pacer(speedup=60)  # 1 minute of data per second
    t0 = datetime(2006, 12, 16, 17, 24)
    assert p.delay(t0, now=100.0) == 0
    assert p.delay(t0 + timedelta(minutes=1), now=100.0) == pytest.approx(1.0)
    assert p.delay(t0 + timedelta(minutes=2), now=101.5) == pytest.approx(0.5)
    assert p.delay(t0 + timedelta(minutes=3), now=110.0) == 0  # behind schedule: no wait


def test_pacer_zero_speedup_means_no_wait():
    p = Pacer(speedup=0)
    t0 = datetime(2006, 12, 16)
    assert p.delay(t0, 0) == 0 and p.delay(t0 + timedelta(hours=1), 0) == 0


def _events(n):
    t0 = datetime(2006, 12, 16, 17, 0)
    return [{"ts": (t0 + timedelta(minutes=i)).isoformat(), "i": i} for i in range(n)]


def test_late_injector_keeps_all_events_and_reorders_some():
    inj = LateEventInjector(rate=0.2, delay_minutes=5, rng=random.Random(1))
    out = [o for e in _events(200) for o in inj.process(e)] + list(inj.flush())
    assert sorted(o["i"] for o in out) == list(range(200))          # nothing lost
    assert [o["i"] for o in out] != list(range(200))               # some out of order
    # a held event arrives at most ~5 minutes (event time) after it was due
    positions = {o["i"]: pos for pos, o in enumerate(out)}
    assert all(positions[i] - i <= 6 for i in range(195))


def test_late_injector_rate_zero_is_passthrough():
    inj = LateEventInjector(rate=0, delay_minutes=5)
    evs = _events(10)
    assert [o for e in evs for o in inj.process(e)] == evs


def test_event_time_roundtrip():
    e = parse_line(GOOD, "m")
    assert event_time(e) == datetime(2006, 12, 16, 17, 24)
