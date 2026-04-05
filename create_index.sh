#!/bin/bash
set -e

INPUT_PATH=${1:-/input/data}

echo "Creating index from: $INPUT_PATH"

# Clean up previous index data
echo "Cleaning up previous index..."
hdfs dfs -rm -r -f /indexer 2>/dev/null || true
hdfs dfs -rm -r -f /tmp/indexer 2>/dev/null || true

# Create output directory
hdfs dfs -mkdir -p /indexer

echo "Running MapReduce indexer..."
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.map.memory.mb=512 \
    -D mapreduce.reduce.memory.mb=512 \
    -D mapreduce.job.name=SearchEngineIndexer \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input "$INPUT_PATH" \
    -output /tmp/indexer/output

echo "MapReduce job completed!"

echo "Organizing index data..."

# Extract VOCAB lines -> strip the leading "VOCAB\t" prefix
hdfs dfs -cat /tmp/indexer/output/part-* \
    | grep "^VOCAB" \
    | cut -f2- \
    > /tmp/vocab_local.txt
hdfs dfs -mkdir -p /indexer/vocabulary
hdfs dfs -put -f /tmp/vocab_local.txt /indexer/vocabulary/data.txt
rm /tmp/vocab_local.txt

# Extract INDEX lines -> strip the leading "INDEX\t" prefix
hdfs dfs -cat /tmp/indexer/output/part-* \
    | grep "^INDEX" \
    | cut -f2- \
    > /tmp/index_local.txt
hdfs dfs -mkdir -p /indexer/index
hdfs dfs -put -f /tmp/index_local.txt /indexer/index/data.txt
rm /tmp/index_local.txt

# Extract STATS lines -> strip the leading "STATS\t" prefix
hdfs dfs -cat /tmp/indexer/output/part-* \
    | grep "^STATS" \
    | cut -f2- \
    > /tmp/stats_local.txt
hdfs dfs -mkdir -p /indexer/stats
hdfs dfs -put -f /tmp/stats_local.txt /indexer/stats/data.txt
rm /tmp/stats_local.txt

# Cleanup temp
hdfs dfs -rm -r -f /tmp/indexer

echo ""
echo "Index created successfully!"
echo ""
echo "Sample vocabulary (first 5 terms):"
hdfs dfs -cat /indexer/vocabulary/data.txt 2>/dev/null | head -5 || echo "(empty)"

echo ""
echo "Sample index (first 5 entries):"
hdfs dfs -cat /indexer/index/data.txt 2>/dev/null | head -5 || echo "(empty)"

echo ""
echo "Sample stats (first 5 docs):"
hdfs dfs -cat /indexer/stats/data.txt 2>/dev/null | head -5 || echo "(empty)"