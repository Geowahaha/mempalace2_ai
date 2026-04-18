#!/usr/bin/env bash
set -euo pipefail

# Tencent/Ubuntu VM bootstrap for Mempalace live autotrading.
# Usage (first install):
#   sudo REPO_URL="https://github.com/<you>/mempalace2_ai.git" APP_USER="$(id -un)" APP_DIR=/opt/mempalace_ai BRANCH=main bash ops/tencent_mempalace_setup.sh
#
# Optional vars:
#   INSTALL_OLLAMA=1            # install/start Ollama + pull LOCAL_MODEL_NAME
#   LOCAL_MODEL_NAME=gemma3:1b  # model to pull when INSTALL_OLLAMA=1

DEFAULT_USER="${SUDO_USER:-ubuntu}"
if [[ -z "${DEFAULT_USER}" || "${DEFAULT_USER}" == "root" ]]; then
  DEFAULT_USER="ubuntu"
fi

APP_USER="${APP_USER:-${DEFAULT_USER}}"
APP_DIR="${APP_DIR:-/opt/mempalace_ai}"
BRANCH="${BRANCH:-main}"
REPO_URL="${REPO_URL:-}"
SERVICE_NAME="${SERVICE_NAME:-mempalace-trader}"
INSTALL_OLLAMA="${INSTALL_OLLAMA:-0}"
LOCAL_MODEL_NAME="${LOCAL_MODEL_NAME:-gemma3:1b-it-qat}"

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
    echo "[1/8] Installing system packages via apt..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y git curl python3 python3-venv python3-pip ca-certificates build-essential
    return
  fi

  if command -v dnf >/dev/null 2>&1; then
    echo "[1/8] Installing system packages via dnf..."
    dnf -y makecache
    dnf -y install git curl python3 python3-pip ca-certificates gcc make
    return
  fi

  if command -v yum >/dev/null 2>&1; then
    echo "[1/8] Installing system packages via yum..."
    yum -y makecache
    yum -y install git curl python3 python3-pip ca-certificates gcc make
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

maybe_install_ollama() {
  if [[ "${INSTALL_OLLAMA}" != "1" ]]; then
    echo "[6/8] INSTALL_OLLAMA=0 -> skip Ollama installation"
    return
  fi

  echo "[6/8] Installing/starting Ollama..."
  if ! command -v ollama >/dev/null 2>&1; then
    curl -fsSL https://ollama.com/install.sh | sh
  fi
  systemctl enable ollama >/dev/null 2>&1 || true
  systemctl restart ollama >/dev/null 2>&1 || true
  sleep 2
  if command -v ollama >/dev/null 2>&1; then
    sudo -u "${APP_USER}" bash -lc "ollama pull '${LOCAL_MODEL_NAME}'" || true
  fi
}

install_system_packages

echo "[2/8] Preparing app directory: ${APP_DIR}"
mkdir -p "${APP_DIR}"
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}"

if [[ -n "${REPO_URL}" ]]; then
  echo "[3/8] Syncing repository from ${REPO_URL} (branch=${BRANCH})"
  if [[ -d "${APP_DIR}/.git" ]]; then
    sudo -u "${APP_USER}" git -C "${APP_DIR}" remote set-url origin "${REPO_URL}"
    sudo -u "${APP_USER}" git -C "${APP_DIR}" fetch --all --prune
    sudo -u "${APP_USER}" git -C "${APP_DIR}" checkout "${BRANCH}"
    sudo -u "${APP_USER}" git -C "${APP_DIR}" pull --ff-only
  else
    sudo -u "${APP_USER}" git clone --branch "${BRANCH}" "${REPO_URL}" "${APP_DIR}"
  fi
else
  echo "[3/8] REPO_URL not set -> expecting code already in ${APP_DIR}"
fi

if [[ ! -f "${APP_DIR}/requirements.txt" ]]; then
  echo "requirements.txt not found in ${APP_DIR}" >&2
  exit 1
fi

echo "[4/8] Creating virtualenv and installing Python dependencies..."
create_virtualenv
sudo -u "${APP_USER}" bash -lc "cd '${APP_DIR}' && . .venv/bin/activate && pip install --upgrade pip wheel"
sudo -u "${APP_USER}" bash -lc "cd '${APP_DIR}' && . .venv/bin/activate && pip install -r requirements.txt"

ENV_FILE="${APP_DIR}/trading_ai/.env"
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "[5/8] Creating ${ENV_FILE} template"
  cat > "${ENV_FILE}" <<EOF
# --- runtime ---
INSTANCE_NAME=mempalace_tencent
SYMBOL=XAUUSD
DRY_RUN=false
LIVE_EXECUTION_ENABLED=true
LOOP_INTERVAL_SEC=12

# --- local llm (Ollama) ---
LLM_PROVIDER=local
LOCAL_LLM_BASE_URL=http://127.0.0.1:11434/v1
LOCAL_MODEL_NAME=${LOCAL_MODEL_NAME}
LOCAL_API_KEY=ollama
LLM_TIMEOUT_SEC=25
LLM_MAX_TOKENS=120
LLM_MAX_RETRIES=1
LLM_FALLBACK_ENABLED=true
LOCAL_FALLBACK_MODELS=qwen2.5:0.5b,gemma3:1b-it-qat

# --- execution ---
CTRADER_DEXTER_WORKER=1
CTRADER_WORKER_SCRIPT=${APP_DIR}/ops/ctrader_execute_once.py
CTRADER_WORKER_PYTHON=${APP_DIR}/.venv/bin/python
CTRADER_WORKER_TIMEOUT_SEC=120
CTRADER_QUOTE_SOURCE=auto
CTRADER_USE_DEMO=0
CTRADER_ACCOUNT_ID=

# --- cTrader OpenAPI credentials ---
OpenAPI_ClientID=
OpenAPI_Secreat=
OpenAPI_Access_token_API_key3=
OpenAPI_Refresh_token_API_key3=
EOF
  chown "${APP_USER}:${APP_USER}" "${ENV_FILE}"
fi

maybe_install_ollama

echo "[7/8] Installing systemd service: ${SERVICE_NAME}"
cat > "/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=Mempalace Trading AI Autotrader
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
WorkingDirectory=${APP_DIR}
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONIOENCODING=utf-8
ExecStart=${APP_DIR}/.venv/bin/python -m trading_ai --no-dry-run
Restart=always
RestartSec=5
TimeoutStopSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service"

MISSING=0
for key in CTRADER_ACCOUNT_ID OpenAPI_ClientID OpenAPI_Secreat OpenAPI_Access_token_API_key3 OpenAPI_Refresh_token_API_key3; do
  if grep -qE "^${key}=$" "${ENV_FILE}"; then
    echo "Missing ${key} in ${ENV_FILE}"
    MISSING=1
  fi
done

if [[ "${MISSING}" -eq 1 ]]; then
  echo "[8/8] Service installed but not started (missing required cTrader keys)."
  echo "Edit ${ENV_FILE} then run:"
  echo "  sudo systemctl restart ${SERVICE_NAME}"
else
  systemctl restart "${SERVICE_NAME}.service"
  echo "[8/8] Service started."
fi

echo
echo "Useful commands:"
echo "  sudo systemctl status ${SERVICE_NAME} --no-pager"
echo "  sudo journalctl -u ${SERVICE_NAME} -f"
echo "  sudo systemctl restart ${SERVICE_NAME}"
echo "  sudo systemctl stop ${SERVICE_NAME}"
