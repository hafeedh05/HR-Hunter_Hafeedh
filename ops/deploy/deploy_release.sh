#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${1:-$(pwd)}"
WORKSPACE_ROOT="${2:-/srv/hr-hunter}"
SERVICE_NAME="${SERVICE_NAME:-hr-hunter}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8765/healthz}"

commit_sha="$(git -C "$REPO_ROOT" rev-parse --verify HEAD)"
commit_short="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
release_path="$WORKSPACE_ROOT/releases/${timestamp}-${commit_short}"
backup_prefix="$WORKSPACE_ROOT/backups/${timestamp}-pre-${commit_short}"

mkdir -p "$WORKSPACE_ROOT/releases" "$WORKSPACE_ROOT/backups"
cp "/etc/systemd/system/${SERVICE_NAME}.service" "${backup_prefix}.${SERVICE_NAME}.service.before" 2>/dev/null || true
readlink "$WORKSPACE_ROOT/current" > "${backup_prefix}.current_symlink.before" 2>/dev/null || true

mkdir -p "$release_path"
git -C "$REPO_ROOT" archive "$commit_sha" | tar -x -C "$release_path"
ln -sfn "$release_path" "$WORKSPACE_ROOT/current"

sudo systemctl daemon-reload
sudo systemctl restart "$SERVICE_NAME"
curl --fail --silent --show-error "$HEALTH_URL" >/dev/null

echo "deployed_commit=$commit_sha"
echo "release_path=$release_path"
echo "backup_prefix=$backup_prefix"
