#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
source .venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt

rm -f .venv.tar.gz
venv-pack -o .venv.tar.gz

# Collect data
echo "Preparing data..."
bash prepare_data.sh

# Run the indexer
echo "Creating index..."
bash index.sh

# Run a sample query
echo ""
echo "Running sample search..."
bash search.sh "computer science"

tail -f /dev/null
