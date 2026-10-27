#!/bin/bash
# Reliability test: stop the stream processor and the broker while data is flowing,
# start them again and check that processing continues (checkpoints + Kafka retention).
# Usage: bash scripts/failure_test.sh
set -euo pipefail

count() {
  docker compose exec -T timescaledb sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM energy.readings"'
}

echo "readings before: $(count)"
echo "stopping spark-job for 30 s (producer keeps writing to Kafka) ..."
docker compose stop spark-job
sleep 30
echo "readings while stopped: $(count)"
docker compose start spark-job

echo "restarting kafka ..."
docker compose restart kafka
sleep 60
echo "readings after recovery: $(count)"

echo "duplicate check (must be 0):"
docker compose exec -T timescaledb sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM (SELECT meter_id, ts FROM energy.readings GROUP BY 1,2 HAVING count(*)>1) d"'
