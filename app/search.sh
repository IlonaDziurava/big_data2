#!/bin/bash
set -e

cd /app
if [ -z "$1" ]; then
    echo "Usage: bash search.sh 'your query'"
    exit 1
fi

QUERY="$1"

if [ -f .venv/bin/activate ]; then
  source .venv/bin/activate
fi

if [ -x /app/.venv/bin/python3 ]; then
  DRIVER_PY=/app/.venv/bin/python3
elif [ -n "$VIRTUAL_ENV" ] && [ -x "$VIRTUAL_ENV/bin/python3" ]; then
  DRIVER_PY="$VIRTUAL_ENV/bin/python3"
else
  DRIVER_PY="$(command -v python3)"
fi

export PYSPARK_DRIVER_PYTHON="$DRIVER_PY"
export PYSPARK_PYTHON="$DRIVER_PY"
export SEARCH_QUERY="$QUERY"

echo "Query: $QUERY"
echo ""

spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    --executor-cores 1 \
    --num-executors 2 \
    --conf spark.driver.memoryOverhead=256 \
    --conf spark.executor.memoryOverhead=256 \
    --conf spark.yarn.am.memory=512m \
    --conf spark.yarn.am.memoryOverhead=256 \
    --conf spark.yarn.am.waitTime=300s \
    --conf spark.network.timeout=300s \
    --conf spark.executor.heartbeatInterval=60s \
    --conf spark.sql.shuffle.partitions=2 \
    --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$DRIVER_PY" \
    --conf "spark.executorEnv.PYSPARK_PYTHON=$DRIVER_PY" \
    --conf "spark.yarn.appMasterEnv.SEARCH_QUERY=$QUERY" \
    --conf "spark.executorEnv.SEARCH_QUERY=$QUERY" \
    --archives /app/.venv.tar.gz#environment \
    /app/query.py