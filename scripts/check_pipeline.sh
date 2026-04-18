#!/bin/bash
# =============================================================================
# Kiểm tra pipeline end-to-end
# =============================================================================

echo "============================================"
echo "  PIPELINE HEALTH CHECK"
echo "============================================"

# 1. Kiểm tra Kafka topic
echo ""
echo "[1/4] Kafka — Kiểm tra topic và message count"
echo "--- Topics ---"
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092

echo ""
echo "--- Message count trong stock-prices-daily ---"
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic stock-prices-daily \
  --time -1 2>/dev/null || echo "Topic chưa có hoặc chưa có message"

# 2. Xem sample messages
echo ""
echo "[2/4] Kafka — 3 messages đầu tiên (timeout 5s)"
docker exec kafka timeout 5 kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic stock-prices-daily \
  --from-beginning \
  --max-messages 3 2>/dev/null || echo "Không đọc được message (topic có thể trống)"

# 3. Kiểm tra HDFS
echo ""
echo "[3/4] HDFS — Kiểm tra thư mục output"
docker exec namenode hdfs dfs -ls -R /data/stock_prices_daily/ 2>/dev/null \
  || echo "Chưa có dữ liệu trong HDFS (Spark chưa chạy hoặc chưa xử lý)"

# 4. Kiểm tra HDFS checkpoint
echo ""
echo "[4/4] HDFS — Kiểm tra checkpoint"
docker exec namenode hdfs dfs -ls /checkpoints/stock_prices_daily/ 2>/dev/null \
  || echo "Chưa có checkpoint (Spark chưa chạy)"

echo ""
echo "============================================"
echo "  CHECK HOÀN TẤT"
echo "============================================"
