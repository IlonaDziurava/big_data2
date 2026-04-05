#!/bin/bash
set -e

# Start SSH
service ssh restart

# Start HDFS, YARN, history server
bash /app/start-services.sh

# Set up Python venv
if [ ! -d /app/.venv ]; then
  python3 -m venv /app/.venv
fi
source /app/.venv/bin/activate

pip install -q --upgrade pip
pip install -q -r /app/requirements.txt

# Pack venv
rm -f /app/.venv.tar.gz
venv-pack -o /app/.venv.tar.gz 2>/dev/null || echo "venv-pack warning (non-fatal)"

echo ""
echo "=== Step 1: Preparing data ==="
bash /app/prepare_data.sh

echo ""
echo "=== Step 2: Building index ==="
bash /app/index.sh

echo ""
echo "=== Step 3: Running sample queries ==="
bash /app/search.sh "computer science"
echo ""
bash /app/search.sh "history"

# Keep container alive so logs are visible
tail -f /dev/null