#!/usr/bin/env bash
set -euo pipefail

TARGET_RELEASE="${1:?target release path required}"
WORKSPACE_ROOT="${2:-/srv/hr-hunter}"
SERVICE_NAME="${SERVICE_NAME:-hr-hunter}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8765/healthz}"

if [[ ! -d "$TARGET_RELEASE" ]]; then
  echo "missing release path: $TARGET_RELEASE" >&2
  exit 1
fi

ln -sfn "$TARGET_RELEASE" "$WORKSPACE_ROOT/current"
sudo systemctl daemon-reload
sudo systemctl restart "$SERVICE_NAME"
curl --fail --silent --show-error "$HEALTH_URL" >/dev/null

echo "rolled_back_to=$TARGET_RELEASE"
