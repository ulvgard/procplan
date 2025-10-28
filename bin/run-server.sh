#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

CONFIG_FILE=${1:-config.sample.json}
DB_PATH=${2:-data/procplan.db}

cd "$REPO_DIR"
mkdir -p "$(dirname "$DB_PATH")"

python -m procplan.server \
  --config "$CONFIG_FILE" \
  --database "$DB_PATH" \
  --host 0.0.0.0 \
  --port 8080
