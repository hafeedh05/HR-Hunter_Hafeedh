#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <brief-path> [limit]"
  exit 1
fi

BRIEF_PATH="$1"
LIMIT="${2:-200}"
ROOT="$HOME/hr-hunter-vm"
VENV="$ROOT/.venv"

if [[ ! -d "$ROOT" ]]; then
  echo "Remote project directory missing: $ROOT" >&2
  exit 1
fi

python3 -m venv "$VENV"
. "$VENV/bin/activate"
pip install -q --upgrade pip
pip install -q httpx pyyaml

SCRAPINGBEE_API_KEY="$(sudo grep -m1 '^SCRAPINGBEE_API_KEY=' /etc/reap/reap.env | cut -d= -f2-)"
if [[ -z "$SCRAPINGBEE_API_KEY" ]]; then
  echo "SCRAPINGBEE_API_KEY not found on remote host" >&2
  exit 1
fi

cd "$ROOT"
PYTHONPATH=src SCRAPINGBEE_API_KEY="$SCRAPINGBEE_API_KEY" python -m hr_hunter.cli search \
  --brief "$BRIEF_PATH" \
  --providers scrapingbee_google \
  --limit "$LIMIT"
