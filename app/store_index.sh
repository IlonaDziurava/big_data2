#!/bin/bash
set -e

cd /app
if [ -f .venv/bin/activate ]; then
  source .venv/bin/activate
fi

echo "Storing index data in Cassandra..."
echo "Waiting for Cassandra to be ready..."

python3 - <<'PYTHON_WAIT'
import sys
import time
from cassandra.cluster import Cluster

max_retries = 60
retry_count = 0

print("Attempting to connect to Cassandra...")

while retry_count < max_retries:
    try:
        cluster = Cluster(['cassandra-server'])
        session = cluster.connect()
        print("Cassandra is ready!")
        cluster.shutdown()
        sys.exit(0)
    except Exception as e:
        retry_count += 1
        if retry_count % 6 == 0:
            elapsed = retry_count * 5
            print(f"Still waiting... ({elapsed}s elapsed): {e}")
        time.sleep(5)

print("ERROR: Cassandra did not become ready after 5 minutes!")
sys.exit(1)
PYTHON_WAIT

echo ""
echo "Creating Cassandra tables..."

python3 - <<'PYTHON_CREATE'
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-server'])
session = cluster.connect()

print("Reading CQL file...")

with open('/app/create_tables.cql', 'r') as f:
    lines = []
    for line in f:
        s = line.strip()
        if not s or s.startswith('--'):
            continue
        lines.append(line)
    cql_content = ''.join(lines)

statements = [s.strip() for s in cql_content.split(';') if s.strip()]

for statement in statements:
    preview = statement.replace('\n', ' ')[:80]
    print(f"Executing: {preview}...")
    session.execute(statement)

print("Tables created successfully!")
cluster.shutdown()
PYTHON_CREATE

echo ""
echo "Loading index data into Cassandra..."
python3 /app/app.py

echo ""
echo "Index data stored successfully in Cassandra!"