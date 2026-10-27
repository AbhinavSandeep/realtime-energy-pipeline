"""Small Kafka and container helpers shared by producer.py and inject_event.py."""
from __future__ import annotations

import json
import socket
from datetime import datetime

from confluent_kafka import Consumer, TopicPartition


def latest_event_time(bootstrap: str, topic: str) -> datetime | None:
    """Newest event time in the topic (last message of every partition), None if the topic is empty."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "event-time-probe",
                  "enable.auto.commit": False})
    newest = None
    try:
        partitions = c.list_topics(topic, timeout=10).topics[topic].partitions
        for p in partitions:
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if high <= low:
                continue
            c.assign([TopicPartition(topic, p, high - 1)])
            msg = c.poll(10)
            if msg is None or msg.error():
                continue
            try:
                ts = datetime.fromisoformat(json.loads(msg.value())["ts"])
            except (ValueError, KeyError, TypeError):
                continue
            newest = ts if newest is None or ts > newest else newest
    finally:
        c.close()
    return newest


def stable_instance_name() -> str:
    """Container name such as 'energy-pipeline-producer-2'.

    The hostname of a container is its random ID and changes when the container is recreated.
    Docker's DNS resolves the container IP back to the stable name, which keeps the
    meter ID (and its position file) the same across restarts and rebuilds.
    """
    host = socket.gethostname()
    try:
        name = socket.gethostbyaddr(socket.gethostbyname(host))[0].split(".")[0]
        return name or host
    except OSError:
        return host
