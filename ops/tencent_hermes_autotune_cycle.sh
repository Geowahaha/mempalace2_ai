#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/mempalace_ai}"
SERVICE_NAME="${SERVICE_NAME:-mempalace-trader}"
SYMBOL="${SYMBOL:-BTCUSD}"
ROUNDS="${ROUNDS:-3}"
WITH_BROKER_HEALTH="${WITH_BROKER_HEALTH:-1}"
INCLUDE_SELF_IMPROVEMENT="${INCLUDE_SELF_IMPROVEMENT:-1}"
TARGET_HOLD_RATE="${TARGET_HOLD_RATE:-0.55}"
TOURNAMENT_TIMEOUT_SEC="${TOURNAMENT_TIMEOUT_SEC:-300}"
PYTHON_BIN="${PYTHON_BIN:-./.venv/bin/python}"

cd "$APP_DIR"
mkdir -p data/reports data/runtime

exec 9>data/runtime/tencent_hermes_autotune.lock
if ! flock -n 9; then
  echo "[autotune] lock busy, skip this cycle"
  exit 0
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "[autotune] python not found: $PYTHON_BIN"
  exit 1
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
report_path="data/reports/tencent_model_tournament_auto_${timestamp}.json"
latest_path="data/reports/tencent_model_tournament_auto_latest.json"

cmd=(
  "$PYTHON_BIN" ops/tencent_model_tournament.py
  --symbol "$SYMBOL"
  --rounds "$ROUNDS"
  --target-hold-rate "$TARGET_HOLD_RATE"
  --timeout-sec "$TOURNAMENT_TIMEOUT_SEC"
  --apply
  --output "$report_path"
)

if [[ "$INCLUDE_SELF_IMPROVEMENT" == "1" ]]; then
  cmd+=(--include-self-improvement)
else
  cmd+=(--no-include-self-improvement)
fi

if [[ "$WITH_BROKER_HEALTH" == "1" ]]; then
  cmd+=(--with-broker-health)
else
  cmd+=(--no-with-broker-health)
fi

echo "[autotune] starting tournament symbol=$SYMBOL rounds=$ROUNDS"
"${cmd[@]}"

cp -f "$report_path" "$latest_path"
systemctl restart "$SERVICE_NAME"
echo "[autotune] applied winner and restarted $SERVICE_NAME"
