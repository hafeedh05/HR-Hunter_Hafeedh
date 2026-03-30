#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <bundle-path> [target-dir] [python-bin] [secret-env-file]" >&2
  exit 1
fi

BUNDLE_PATH="$1"
TARGET_DIR="${2:-$HOME/hr-hunter}"
PYTHON_BIN="${3:-python3}"
SECRET_ENV_FILE="${4:-/etc/reap/reap.env}"

case "$TARGET_DIR" in
  "~")
    TARGET_DIR="$HOME"
    ;;
  "~/"*)
    TARGET_DIR="$HOME/${TARGET_DIR#~/}"
    ;;
esac

if [[ ! -f "$BUNDLE_PATH" ]]; then
  echo "Bundle not found: $BUNDLE_PATH" >&2
  exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python binary not found: $PYTHON_BIN" >&2
  exit 1
fi

"$PYTHON_BIN" - <<'PY'
import sys
if sys.version_info < (3, 10):
    raise SystemExit("Python 3.10 or newer is required for HR Hunter.")
PY

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

mkdir -p "$TARGET_DIR"
rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"

tar -xzf "$BUNDLE_PATH" -C "$TARGET_DIR"

cd "$TARGET_DIR"
"$PYTHON_BIN" -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e .

cat > "$TARGET_DIR/run-hr-hunter.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$TARGET_DIR"
if [[ -f "$SECRET_ENV_FILE" ]]; then
  export HR_HUNTER_SECRET_ENV_FILES="$SECRET_ENV_FILE"
fi
exec "$TARGET_DIR/.venv/bin/hr-hunter" "\$@"
EOF
chmod +x "$TARGET_DIR/run-hr-hunter.sh"

cat <<EOF
Installed HR Hunter at: $TARGET_DIR
CLI wrapper: $TARGET_DIR/run-hr-hunter.sh
Secret env file: $SECRET_ENV_FILE

Example:
  $TARGET_DIR/run-hr-hunter.sh search --brief examples/search_briefs/sr_product_lead_ai_jan26_vm.yaml --providers scrapingbee_google --limit 150
EOF
