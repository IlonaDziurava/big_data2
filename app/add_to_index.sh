#!/bin/bash
set -e

cd /app
if [ -f .venv/bin/activate ]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi

if [ -z "$1" ]; then
    echo "Usage: $0 <local_file_path>"
    exit 1
fi

LOCAL_FILE=$1

if [ ! -f "$LOCAL_FILE" ]; then
    echo "Error: File not found: $LOCAL_FILE"
    exit 1
fi

echo "Adding document to index: $LOCAL_FILE"

# Generate doc_id from filename
DOC_ID=$(basename "$LOCAL_FILE" .txt | cut -d'_' -f1)
DOC_TITLE=$(basename "$LOCAL_FILE" .txt | cut -d'_' -f2-)

echo "Document ID: $DOC_ID"
echo "Document Title: $DOC_TITLE"

# Read file content
DOC_TEXT=$(cat "$LOCAL_FILE")

# Create temp input file
TEMP_INPUT="/tmp/new_doc_input.txt"
echo -e "${DOC_ID}\t${DOC_TITLE}\t${DOC_TEXT}" > $TEMP_INPUT

# Upload to HDFS
hdfs dfs -put -f $TEMP_INPUT /tmp/new_doc_input.txt

# Run MapReduce on single document
echo "Processing document..."
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.reduces=1 \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -mapper mapper1.py \
    -reducer reducer1.py \
    -input /tmp/new_doc_input.txt \
    -output /tmp/new_doc_output

# Extract and load to Cassandra
echo "Updating Cassandra tables..."
python3 /app/add_doc_to_cassandra.py

# Cleanup
hdfs dfs -rm -r -f /tmp/new_doc_input.txt /tmp/new_doc_output
rm $TEMP_INPUT

echo "Document added to index successfully!"