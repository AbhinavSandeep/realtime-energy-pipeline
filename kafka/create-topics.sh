#!/bin/bash
# Creates the Kafka topics of the pipeline. Safe to run repeatedly.
set -euo pipefail

BIN=/opt/kafka/bin
BOOTSTRAP=kafka:9092

create() {
  local topic=$1 partitions=$2
  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 \
    --config retention.ms="$RETENTION_MS" \
    --config cleanup.policy=delete
}

create "$RAW_TOPIC" "$RAW_PARTITIONS"
create "$AGG_TOPIC" "$RAW_PARTITIONS"
create "$DLQ_TOPIC" 1

echo "Topics:"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe \
  --topic "$RAW_TOPIC,$AGG_TOPIC,$DLQ_TOPIC" 2>/dev/null || \
  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list
