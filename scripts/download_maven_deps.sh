#!/bin/bash
# =============================================================================
# download_maven_deps.sh
# Pre-download all Maven dependencies to avoid network issues in containers
# Run this on the host machine before starting Spark jobs
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MAVEN_CACHE_DIR="${ROOT_DIR}/maven-cache"

echo "=== Creating Maven cache directory ==="
mkdir -p "$MAVEN_CACHE_DIR"

echo "=== Downloading Kafka packages ==="
docker run --rm \
  -v "$MAVEN_CACHE_DIR:/root/.m2" \
  maven:3.8.1-jdk-11 \
  mvn dependency:get \
    -Dartifact=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

echo "=== Downloading Iceberg packages ==="
docker run --rm \
  -v "$MAVEN_CACHE_DIR:/root/.m2" \
  maven:3.8.1-jdk-11 \
  mvn dependency:get \
    -Dartifact=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2

echo "=== Downloading Cassandra packages ==="
docker run --rm \
  -v "$MAVEN_CACHE_DIR:/root/.m2" \
  maven:3.8.1-jdk-11 \
  mvn dependency:get \
    -Dartifact=com.datastax.spark:spark-cassandra-connector_2.12:3.5.1

echo "=== Maven cache directory created at: $MAVEN_CACHE_DIR ==="
echo "=== This directory will be mounted to Spark containers ==="
