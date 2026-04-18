#!/bin/bash
# =============================================================================
# Tạo Kafka topic cho stock-prices-daily
# Chạy bên trong container kafka
# =============================================================================

echo "=== Tạo Kafka topic: stock-prices-daily ==="

docker exec kafka kafka-topics \
  --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic stock-prices-daily \
  --if-not-exists

echo ""
echo "=== Danh sách topics hiện có ==="
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092

echo ""
echo "=== Chi tiết topic stock-prices-daily ==="
docker exec kafka kafka-topics \
  --describe \
  --bootstrap-server kafka:9092 \
  --topic stock-prices-daily
