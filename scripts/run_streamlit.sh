#!/usr/bin/env bash
# Usage: scripts/run_streamlit.sh
set -euo pipefail

# load env if present
if [ -f spark_streaming/.env ]; then
  echo "Loading spark_streaming/.env"
  source scripts/load_env.sh spark_streaming/.env
fi

# Run streamlit app
streamlit run dashboards/streamlit_app.py
