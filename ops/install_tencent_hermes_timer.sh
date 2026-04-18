#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "run as root: sudo bash ops/install_tencent_hermes_timer.sh"
  exit 1
fi

APP_DIR="${APP_DIR:-/opt/mempalace_ai}"
SERVICE_NAME="${SERVICE_NAME:-mempalace-trader}"
SYMBOL="${SYMBOL:-BTCUSD}"
ROUNDS="${ROUNDS:-3}"
WITH_BROKER_HEALTH="${WITH_BROKER_HEALTH:-1}"
INCLUDE_SELF_IMPROVEMENT="${INCLUDE_SELF_IMPROVEMENT:-1}"
TARGET_HOLD_RATE="${TARGET_HOLD_RATE:-0.55}"
TOURNAMENT_TIMEOUT_SEC="${TOURNAMENT_TIMEOUT_SEC:-300}"
PYTHON_BIN="${PYTHON_BIN:-./.venv/bin/python}"
ON_CALENDAR="${ON_CALENDAR:-*-*-* 00,06,12,18:08:00}"

cycle_script="$APP_DIR/ops/tencent_hermes_autotune_cycle.sh"
if [[ ! -f "$cycle_script" ]]; then
  echo "missing cycle script: $cycle_script"
  exit 1
fi

chmod +x "$cycle_script"

cat >/etc/systemd/system/mempalace-hermes-autotune.service <<UNIT
[Unit]
Description=Mempalace Hermes Autotune Cycle
After=network-online.target ollama.service
Wants=network-online.target

[Service]
Type=oneshot
WorkingDirectory=$APP_DIR
Environment=APP_DIR=$APP_DIR
Environment=SERVICE_NAME=$SERVICE_NAME
Environment=SYMBOL=$SYMBOL
Environment=ROUNDS=$ROUNDS
Environment=WITH_BROKER_HEALTH=$WITH_BROKER_HEALTH
Environment=INCLUDE_SELF_IMPROVEMENT=$INCLUDE_SELF_IMPROVEMENT
Environment=TARGET_HOLD_RATE=$TARGET_HOLD_RATE
Environment=TOURNAMENT_TIMEOUT_SEC=$TOURNAMENT_TIMEOUT_SEC
Environment=PYTHON_BIN=$PYTHON_BIN
ExecStart=/bin/bash $cycle_script
UNIT

cat >/etc/systemd/system/mempalace-hermes-autotune.timer <<TIMER
[Unit]
Description=Run Mempalace Hermes Autotune on schedule

[Timer]
OnCalendar=$ON_CALENDAR
Persistent=true
Unit=mempalace-hermes-autotune.service

[Install]
WantedBy=timers.target
TIMER

systemctl daemon-reload
systemctl enable --now mempalace-hermes-autotune.timer

echo "installed mempalace-hermes-autotune.timer"
systemctl list-timers --all | grep -E "mempalace-hermes-autotune|NEXT|LEFT" || true
