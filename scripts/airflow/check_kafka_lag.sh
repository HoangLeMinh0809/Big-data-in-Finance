#!/bin/bash
set -euo pipefail

GROUP_ID="${1:-}"
TOPIC="${2:-}"
MAX_LAG="${3:-50000}"

if [ -z "$GROUP_ID" ] || [ -z "$TOPIC" ]; then
  echo "Usage: $0 <group_id> <topic> [max_lag]" >&2
  exit 1
fi

RAW_OUTPUT="$(docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --group "$GROUP_ID" --describe 2>&1 || true)"

if echo "$RAW_OUTPUT" | grep -qi "does not exist"; then
  echo "[WARN] Kafka consumer group does not exist yet (warm-up): $GROUP_ID"
  echo "$RAW_OUTPUT"
  exit 0
fi

PARTITIONS="$(echo "$RAW_OUTPUT" | awk -v topic="$TOPIC" '$1 == topic && $5 ~ /^[0-9]+$/ {count += 1} END {print count + 0}')"
LAG_SUM="$(echo "$RAW_OUTPUT" | awk -v topic="$TOPIC" '$1 == topic && $5 ~ /^[0-9]+$/ {sum += $5} END {print sum + 0}')"

if [ "$PARTITIONS" -eq 0 ]; then
  echo "[WARN] No lag metrics found for topic=$TOPIC group=$GROUP_ID (warm-up)"
  echo "$RAW_OUTPUT"
  exit 0
fi

echo "[INFO] Kafka lag topic=${TOPIC} group=${GROUP_ID}: lag=${LAG_SUM}, partitions=${PARTITIONS}, max=${MAX_LAG}"

if [ "$LAG_SUM" -gt "$MAX_LAG" ]; then
  echo "[ERROR] Kafka lag above threshold for topic=${TOPIC}: ${LAG_SUM} > ${MAX_LAG}" >&2
  exit 1
fi

echo "[OK] Kafka lag within threshold for topic=${TOPIC}"
