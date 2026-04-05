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
    --master local[*] \
    --driver-memory 512m \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=ERROR,console" \
    /app/query.py