#!/bin/bash
# =============================================================================
# Tạo Kafka topics cho pipeline
# Chạy bên trong container kafka
# =============================================================================

echo "=== Tạo Kafka topic: weather-history ==="

docker exec kafka kafka-topics \
  --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic weather-history \
  --if-not-exists

echo ""
echo "=== Danh sách topics hiện có ==="
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092

echo ""
echo "=== Chi tiết topic weather-history ==="
docker exec kafka kafka-topics \
  --describe \
  --bootstrap-server kafka:9092 \
  --topic weather-history
