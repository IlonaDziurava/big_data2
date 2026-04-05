#!/bin/bash
set -e

cd /app
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python3)
unset PYSPARK_PYTHON

echo "Starting data preparation..."

if [ ! -f a.parquet ]; then
  echo "ERROR: app/a.parquet not found. Place the parquet file at app/a.parquet"
  exit 1
fi

# Clean old HDFS data
echo "Cleaning old HDFS paths..."
hdfs dfs -rm -r -f /data       2>/dev/null || true
hdfs dfs -rm -r -f /input/data 2>/dev/null || true

# Upload parquet to HDFS
echo "Uploading parquet to HDFS..."
hdfs dfs -put -f a.parquet /

# Run PySpark preparation
echo "Running prepare_data.py..."
spark-submit \
  --master local[2] \
  --driver-memory 1G \
  --conf spark.driver.memoryOverhead=512 \
  /app/prepare_data.py

echo "Waiting for HDFS output..."
for i in $(seq 1 30); do
  if hdfs dfs -test -e /input/data/_SUCCESS 2>/dev/null; then
    echo "Data preparation complete!"
    break
  fi
  echo "  Waiting... ($i/30)"
  sleep 3
done

if ! hdfs dfs -test -e /input/data/_SUCCESS 2>/dev/null; then
  echo "ERROR: /input/data/_SUCCESS not found after waiting"
  exit 1
fi

echo ""
echo "=== Verification ==="
echo "Documents in HDFS /data:"
hdfs dfs -ls /data 2>/dev/null | tail -5
echo ""
echo "MapReduce input lines:"
hdfs dfs -cat /input/data/part-00000 2>/dev/null | wc -l
echo ""
echo "Sample input line (first 200 chars):"
hdfs dfs -cat /input/data/part-00000 2>/dev/null | head -1 | cut -c1-200
echo ""
echo "Data preparation DONE"