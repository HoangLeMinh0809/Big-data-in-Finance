#!/bin/bash
set -e

echo "=== Create Kafka topic: openaq-hourly ==="

docker exec kafka kafka-topics \
  --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic openaq-hourly \
  --if-not-exists

echo "=== Done ==="
