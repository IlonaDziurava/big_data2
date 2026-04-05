#!/bin/bash
set -e

cd /app
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python3)
unset PYSPARK_PYTHON

echo "Starting data preparation..."

# If no parquet file, use the committed sample documents in app/data/
if [ ! -f a.parquet ]; then
  echo "No a.parquet found — using committed sample documents from app/data/"

  if [ -z "$(ls -A /app/data/*.txt 2>/dev/null)" ]; then
    echo "ERROR: No .txt files found in app/data/ either. Cannot proceed."
    exit 1
  fi

  DOC_COUNT=$(ls /app/data/*.txt 2>/dev/null | wc -l)
  echo "Found $DOC_COUNT documents in app/data/"

  # Clean old HDFS paths
  hdfs dfs -rm -r -f /data       2>/dev/null || true
  hdfs dfs -rm -r -f /input/data 2>/dev/null || true

  # Upload the committed sample documents to HDFS
  echo "Uploading sample documents to HDFS /data..."
  hdfs dfs -mkdir -p /data
  for f in /app/data/*.txt; do
    hdfs dfs -put -f "$f" /data/ 2>/dev/null || true
  done

  UPLOADED=$(hdfs dfs -ls /data 2>/dev/null | grep -c "\.txt" || echo 0)
  echo "Uploaded $UPLOADED files to HDFS /data"

  # Build MapReduce input using PySpark (reads from HDFS /data)
  echo "Building MapReduce input with PySpark..."
  spark-submit \
    --master local[2] \
    --driver-memory 512m \
    /app/build_input_from_data.py

else
  echo "Found a.parquet — running full data preparation pipeline..."

  # Clean old HDFS paths
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
fi

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
  echo "ERROR: /input/data/_SUCCESS not found"
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