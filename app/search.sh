#!/bin/bash
set -e

cd /app
if [ -z "$1" ]; then
    echo "Usage: bash search.sh 'your query'"
    exit 1
fi

QUERY="$1"

if [ -f .venv/bin/activate ]; then
  # shellcheck source=/dev/null
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

echo "Query: $QUERY"
echo ""

# YARN driver often does not get stdin; pass via env. query.py also accepts stdin/argv.
export SEARCH_QUERY="$QUERY"

spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 384m \
    --executor-memory 256m \
    --executor-cores 1 \
    --num-executors 1 \
    --conf spark.driver.memoryOverhead=128 \
    --conf spark.executor.memoryOverhead=128 \
    --conf spark.yarn.am.memory=256m \
    --conf spark.yarn.am.memoryOverhead=128 \
    --conf spark.yarn.am.waitTime=600s \
    --conf spark.network.timeout=600s \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="$DRIVER_PY" \
    --conf spark.executorEnv.PYSPARK_PYTHON="$DRIVER_PY" \
    /app/query.py
