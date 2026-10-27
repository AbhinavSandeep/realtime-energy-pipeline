"""Runs the producer main loop against a fake Kafka client (no broker needed)."""
import importlib
import json
import sys
import types
from datetime import datetime
from pathlib import Path

SAMPLE = """Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3
16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000
16/12/2006;17:25:00;5.360;0.436;233.630;23.000;0.000;1.000;16.000
16/12/2006;17:26:00;?;?;?;?;?;?;
broken line
16/12/2006;17:27:00;5.374;0.498;233.290;23.000;0.000;2.000;17.000
"""


class FakeProducer:
    sent: list = []

    def __init__(self, conf):
        self.conf = conf

    def list_topics(self, timeout=None):
        return {}

    def produce(self, topic, key, value, on_delivery=None):
        FakeProducer.sent.append((topic, key, json.loads(value)))

    def poll(self, _t):
        return 0

    def flush(self, _t=None):
        return 0


def load_producer(monkeypatch, tmp_path, **extra):
    fake = types.ModuleType("confluent_kafka")
    fake.Producer = FakeProducer
    fake.Consumer = object
    fake.TopicPartition = object
    fake.KafkaException = Exception
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake)
    data = tmp_path / "data.txt"
    data.write_text(SAMPLE)
    env = {"KAFKA_BOOTSTRAP": "x", "RAW_TOPIC": "raw", "DLQ_TOPIC": "dlq", "DATA_FILE": str(data),
           "STATE_DIR": str(tmp_path / "state"), "SPEEDUP": "0", "FOLLOW": "false",
           "EXIT_WHEN_DONE": "true", "LATE_EVENT_RATE": "0", "METER_ID": "m-test",
           "JOIN_STREAM_TIME": "false", **extra}
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    sys.modules.pop("producer", None)
    sys.modules.pop("kafka_utils", None)
    return importlib.import_module("producer")


def test_main_sends_valid_and_dlq_and_resumes(monkeypatch, tmp_path):
    FakeProducer.sent = []
    mod = load_producer(monkeypatch, tmp_path)
    mod.main()

    raw = [m for t, _, m in FakeProducer.sent if t == "raw"]
    dlq = [m for t, _, m in FakeProducer.sent if t == "dlq"]
    assert [m["ts"] for m in raw] == ["2006-12-16T17:24:00", "2006-12-16T17:25:00",
                                      "2006-12-16T17:26:00", "2006-12-16T17:27:00"]
    assert raw[2]["voltage"] is None                     # missing values forwarded for Spark to reject
    assert len(dlq) == 1 and "columns" in dlq[0]["reason"]
    assert all(k == "m-test" for _, k, _ in FakeProducer.sent)

    # restart: position file prevents re-sending
    FakeProducer.sent = []
    mod = load_producer(monkeypatch, tmp_path)
    mod.main()
    assert FakeProducer.sent == []


def test_new_meter_joins_running_stream_at_current_event_time(monkeypatch, tmp_path):
    FakeProducer.sent = []
    mod = load_producer(monkeypatch, tmp_path, JOIN_STREAM_TIME="true")
    monkeypatch.setattr(mod, "latest_event_time", lambda *_: datetime(2006, 12, 16, 17, 26))
    mod.main()
    raw = [m["ts"] for t, _, m in FakeProducer.sent if t == "raw"]
    assert raw == ["2006-12-16T17:26:00", "2006-12-16T17:27:00"]   # history before 17:26 skipped
