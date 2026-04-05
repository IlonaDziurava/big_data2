#!/bin/bash
set -e

cd /app
if [ -f .venv/bin/activate ]; then
  source .venv/bin/activate
fi

if [ -z "$1" ]; then
    echo "Usage: $0 <local_file_path>"
    echo "  File must be named: <doc_id>_<doc_title>.txt"
    exit 1
fi

LOCAL_FILE=$1

if [ ! -f "$LOCAL_FILE" ]; then
    echo "Error: File not found: $LOCAL_FILE"
    exit 1
fi

# Parse doc_id and doc_title from filename (format: <id>_<title>.txt)
BASENAME=$(basename "$LOCAL_FILE" .txt)
DOC_ID=$(echo "$BASENAME" | cut -d'_' -f1)
DOC_TITLE=$(echo "$BASENAME" | cut -d'_' -f2- | tr '_' ' ')

if [ -z "$DOC_ID" ] || [ -z "$DOC_TITLE" ]; then
    echo "Error: Filename must follow format <doc_id>_<doc_title>.txt"
    exit 1
fi

echo "Adding document to index:"
echo "  File:  $LOCAL_FILE"
echo "  ID:    $DOC_ID"
echo "  Title: $DOC_TITLE"

# Build the tab-separated input line expected by mapper1.py
DOC_TEXT=$(cat "$LOCAL_FILE" | tr '\t' ' ' | tr '\n' ' ')
TEMP_INPUT="/tmp/new_doc_input_$$.txt"
printf '%s\t%s\t%s\n' "$DOC_ID" "$DOC_TITLE" "$DOC_TEXT" > "$TEMP_INPUT"

# Upload to HDFS
hdfs dfs -put -f "$TEMP_INPUT" /tmp/new_doc_input.txt
rm "$TEMP_INPUT"

# Remove old output if present
hdfs dfs -rm -r -f /tmp/new_doc_output

# Run MapReduce on the single document
echo "Running MapReduce for single document..."
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.job.name=AddDocToIndex \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input /tmp/new_doc_input.txt \
    -output /tmp/new_doc_output

# Update Cassandra with the new document's data
echo "Updating Cassandra tables..."
python3 /app/add_doc_to_cassandra.py

# Cleanup HDFS temp files
hdfs dfs -rm -r -f /tmp/new_doc_input.txt /tmp/new_doc_output

echo "Document '$DOC_TITLE' (id=$DOC_ID) added to index successfully!"