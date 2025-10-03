#!/bin/bash
# Fintech Enrichment Demo Runner
# Uses already-built Cython extensions (no rebuild needed)
#
# Usage:
#   ./run_demo.sh --securities 500000 --quotes 500000
#   ./run_demo.sh --data-dir /path/to/csvs --securities 1000000 --quotes 1000000
#
# Or set environment variables:
#   export SABOT_DATA_DIR=/path/to/csvs
#   ./run_demo.sh --securities 500000 --quotes 500000

export PYTHONPATH=/Users/bengamble/Sabot:$PYTHONPATH

# Load .env if it exists
if [ -f .env ]; then
    source .env
fi

# Activate venv and run
source .venv/bin/activate

python examples/fintech_enrichment_demo/arrow_optimized_enrichment.py "$@"
