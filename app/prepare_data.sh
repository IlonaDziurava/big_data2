#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

echo "Starting data preparation..."

if [ ! -f a.parquet ]; then
  echo "ERROR: app/a.parquet not found. Add the assignment parquet as app/a.parquet"
  exit 1
fi

echo "Cleaning old HDFS data..."
hdfs dfs -rm -r /data 2>/dev/null || true
hdfs dfs -rm -r /input/data 2>/dev/null || true

# upload parquet
echo "Uploading parquet to HDFS..."
hdfs dfs -put -f a.parquet /

# run spark job
echo "Running prepare_data.py..."
# local: foreach(create_doc) must write files on this host before hdfs dfs -put
spark-submit \
  --master local[1] \
  --driver-memory 1G \
  --conf spark.driver.memoryOverhead=512 \
  prepare_data.py

echo "Waiting for Spark output..."
while ! hdfs dfs -test -e /input/data/_SUCCESS; do
  echo "Still processing..."
  sleep 2
done

echo "Spark job finished!"

# checks
echo "Checking results..."

echo "Documents sample:"
hdfs dfs -ls /data | head -5

echo ""
echo "MapReduce input:"
hdfs dfs -ls /input/data

echo ""
echo "Sample content:"
hdfs dfs -cat /input/data/part-00000 | head -3

echo ""
echo "Data preparation DONE"