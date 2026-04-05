#!/bin/bash
set -e

echo "===================="
echo "FULL INDEX PIPELINE"
echo "===================="

# Step 1: Create index with MapReduce
echo ""
echo "Step 1: Creating index with MapReduce..."
bash /app/create_index.sh

# Step 2: Store in Cassandra
echo ""
echo "Step 2: Storing index in Cassandra..."
bash /app/store_index.sh

echo ""
echo "===================="
echo "INDEX PIPELINE COMPLETE!"
echo "===================="