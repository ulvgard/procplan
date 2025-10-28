#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run the container" >&2
  exit 1
fi

docker compose up --build "$@"
