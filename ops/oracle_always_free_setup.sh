#!/usr/bin/env bash
set -euo pipefail

# Oracle Always Free VM bootstrap for Dexter Pro monitor mode.
# Usage:
#   sudo REPO_URL="https://github.com/<you>/<repo>.git" APP_USER="$(id -un)" bash ops/oracle_always_free_setup.sh
# Optional vars:
#   APP_USER=ubuntu|opc
#   APP_DIR=/opt/dexter_pro
#   BRANCH=main

DEFAULT_USER="${SUDO_USER:-ubuntu}"
if [[ -z "${DEFAULT_USER}" || "${DEFAULT_USER}" == "root" ]]; then
  DEFAULT_USER="ubuntu"
fi

APP_USER="${APP_USER:-${DEFAULT_USER}}"
APP_DIR="${APP_DIR:-/opt/dexter_pro}"
BRANCH="${BRANCH:-main}"
REPO_URL="${REPO_URL:-}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root (sudo)." >&2
  exit 1
fi

if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  echo "APP_USER '${APP_USER}' not found." >&2
  exit 1
fi

install_system_packages() {
  if command -v apt-get >/dev/null 2>&1; then
    echo "[1/7] Installing system packages via apt..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y git python3 python3-venv python3-pip ca-certificates build-essential
    return
  fi

  if command -v dnf >/dev/null 2>&1; then
    echo "[1/7] Installing system packages via dnf..."
    dnf -y makecache
    dnf -y install git python3 python3-pip ca-certificates gcc make
    return
  fi

  if command -v yum >/dev/null 2>&1; then
    echo "[1/7] Installing system packages via yum..."
    yum -y makecache
    yum -y install git python3 python3-pip ca-certificates gcc make
    return
  fi

  echo "Unsupported package manager (need apt-get/dnf/yum)." >&2
  exit 1
}

create_virtualenv() {
  if sudo -u "${APP_USER}" bash -lc "cd '${APP_DIR}' && python3 -m venv .venv"; then
    return
  fi

  echo "python3 -m venv failed; trying virtualenv fallback..."
  sudo -u "${APP_USER}" bash -lc "cd '${APP_DIR}' && python3 -m pip install --user virtualenv"
  sudo -u "${APP_USER}" bash -lc "cd '${APP_DIR}' && python3 -m virtualenv .venv"
}

install_system_packages

echo "[2/7] Preparing app directory: ${APP_DIR}"
mkdir -p "${APP_DIR}"
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}"

if [[ -n "${REPO_URL}" ]]; then
  echo "[3/7] Syncing repository from ${REPO_URL} (branch=${BRANCH})"
  if [[ -d "${APP_DIR}/.git" ]]; then
    sudo -u "${APP_USER}" git -C "${APP_DIR}" fetch --all --prune
    sudo -u "${APP_USER}" git -C "${APP_DIR}" checkout "${BRANCH}"
    sudo -u "${APP_USER}" git -C "${APP_DIR}" pull --ff-only
  else
    sudo -u "${APP_USER}" git clone --branch "${BRANCH}" "${REPO_URL}" "${APP_DIR}"
  fi
else
  echo "[3/7] REPO_URL not set -> expecting code already in ${APP_DIR}"
fi

if [[ ! -f "${APP_DIR}/requirements.txt" ]]; then
  echo "requirements.txt not found in ${APP_DIR}" >&2
  exit 1
fi

echo "[4/7] Creating virtualenv and installing Python deps..."
create_virtualenv
sudo -u "${APP_USER}" bash -lc "cd '${APP_DIR}' && . .venv/bin/activate && pip install --upgrade pip wheel"
sudo -u "${APP_USER}" bash -lc "cd '${APP_DIR}' && . .venv/bin/activate && pip install -r requirements.txt"

if [[ ! -f "${APP_DIR}/.env.local" ]]; then
  echo "[5/7] Creating minimal .env.local template (fill keys before starting)"
  cat > "${APP_DIR}/.env.local" <<'EOF'
# Required
ANTHROPIC_API_KEY=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
TELEGRAM_ADMIN_IDS=

# Provider routing
AI_PROVIDER=auto
AI_MODEL=claude-sonnet-4-6
GEMINI_VERTEX_MODEL=gemini-2.5-flash-lite

# Cloud-safe defaults (no local MT5 bridge on VM)
MT5_ENABLED=0
MT5_EXECUTE_XAUUSD=0
MT5_EXECUTE_CRYPTO=0
MT5_EXECUTE_FX=0
SCALPING_EXECUTE_MT5=0
EOF
  chown "${APP_USER}:${APP_USER}" "${APP_DIR}/.env.local"
fi

echo "[6/7] Installing systemd service..."
cat > /etc/systemd/system/dexter-monitor.service <<EOF
[Unit]
Description=Dexter Pro Monitor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
WorkingDirectory=${APP_DIR}
Environment=PYTHONUNBUFFERED=1
ExecStart=${APP_DIR}/.venv/bin/python main.py monitor
Restart=always
RestartSec=5
TimeoutStopSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable dexter-monitor.service

if grep -qE '^(ANTHROPIC_API_KEY|TELEGRAM_BOT_TOKEN|TELEGRAM_CHAT_ID)=$' "${APP_DIR}/.env.local"; then
  echo "[7/7] Service installed but NOT started yet (missing required keys in .env.local)."
  echo "Edit ${APP_DIR}/.env.local then run:"
  echo "  sudo systemctl start dexter-monitor"
else
  systemctl restart dexter-monitor.service
  echo "[7/7] Service started."
fi

echo
echo "Useful commands:"
echo "  sudo systemctl status dexter-monitor --no-pager"
echo "  sudo journalctl -u dexter-monitor -f"
echo "  sudo systemctl restart dexter-monitor"
echo "  sudo systemctl stop dexter-monitor"
