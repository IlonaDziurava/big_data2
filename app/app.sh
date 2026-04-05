#!/bin/bash
set -e

# Start SSH server (required for Hadoop to communicate between nodes)
service ssh restart

# Start Hadoop and YARN services
bash /app/start-services.sh

# Set up Python virtual environment
if [ ! -d /app/.venv ]; then
  python3 -m venv /app/.venv
fi
source /app/.venv/bin/activate

pip install -q --upgrade pip
pip install -q -r /app/requirements.txt

# Pack the venv for distribution to YARN executors
rm -f /app/.venv.tar.gz
venv-pack -o /app/.venv.tar.gz

# Prepare data: read parquet, create documents, upload to HDFS
echo "Preparing data..."
bash /app/prepare_data.sh

# Build the index with MapReduce and load into Cassandra
echo "Creating index..."
bash /app/index.sh

# Run sample queries
echo ""
echo "Running sample searches..."
bash /app/search.sh "computer science"
echo ""
bash /app/search.sh "history of art"

# Keep container running
tail -f /dev/null