#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CODEX_ROOT="${CODEX_HOME:-$HOME/.codex}"
INSTALL_ROOT="$CODEX_ROOT/mcp/hr-hunter-gcp"
VENV_DIR="$INSTALL_ROOT/.venv"

mkdir -p "$INSTALL_ROOT"

python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
"$VENV_DIR/bin/python" -m pip install -e "${REPO_ROOT}[mcp]"

cat <<EOF
Global MCP environment created at:
  $INSTALL_ROOT

Add this block to:
  $CODEX_ROOT/config.toml

[mcp_servers.hrHunterGcp]
command = "$VENV_DIR/bin/python"
args = ["-m", "hr_hunter.gcp_mcp"]
EOF
