#!/bin/bash
# =============================================================================
# Create Kafka topics for AIS pipeline
# =============================================================================

set -euo pipefail

TOPICS=(
  "weather_history"
  "openaq-hourly"
  "sentinel5p-summary"
  "maiac-summary"
)

echo "=== Create AIS Kafka topics ==="
for topic in "${TOPICS[@]}"; do
  echo "- Create topic: ${topic}"
  docker exec kafka kafka-topics \
    --create \
    --bootstrap-server kafka:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic "${topic}" \
    --if-not-exists
done

echo
echo "=== Current topics ==="
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092

echo
echo "=== Topic details ==="
for topic in "${TOPICS[@]}"; do
  echo "- ${topic}"
  docker exec kafka kafka-topics \
    --describe \
    --bootstrap-server kafka:9092 \
    --topic "${topic}"
done
