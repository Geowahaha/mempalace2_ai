# Oracle Always Free VM (24/7 Dexter Monitor)

This guide runs Dexter continuously on an Oracle Always Free VM.

## 1) Create VM (Oracle Cloud)

- Image: `Ubuntu 22.04` (recommended)
- Shape: `VM.Standard.E2.1.Micro` (Always Free) or Always Free Ampere shape
- Inbound rules: keep only SSH (`22`) unless you intentionally expose web endpoints

## 2) Upload code to VM

Pick one:

- Git repo (recommended): use your private/public Git URL
- SCP/rsync from local machine into `/opt/dexter_pro`

## 3) SSH and run bootstrap script

```bash
ssh -i <your_key>.pem <ubuntu_or_opc>@<your_vm_public_ip>
```

If using Git:

```bash
sudo dnf -y install git || sudo apt-get update -y && sudo apt-get install -y git
git clone <YOUR_REPO_URL> ~/dexter_repo
cd ~/dexter_repo
sudo REPO_URL="<YOUR_REPO_URL>" APP_USER="$(id -un)" APP_DIR=/opt/dexter_pro BRANCH=main bash ops/oracle_always_free_setup.sh
```

If code is already copied into `/opt/dexter_pro`, run:

```bash
cd /opt/dexter_pro
sudo APP_USER="$(id -un)" APP_DIR=/opt/dexter_pro bash ops/oracle_always_free_setup.sh
```

## 4) Fill `.env.local`

Edit:

```bash
sudo -u "$(id -un)" nano /opt/dexter_pro/.env.local
```

Minimum required:

- `ANTHROPIC_API_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `TELEGRAM_ADMIN_IDS`

Cloud-safe defaults for Oracle VM (no local MT5 terminal):

- `MT5_ENABLED=0`
- `MT5_EXECUTE_XAUUSD=0`
- `MT5_EXECUTE_CRYPTO=0`
- `MT5_EXECUTE_FX=0`
- `SCALPING_EXECUTE_MT5=0`

## 5) Start + verify

```bash
sudo systemctl start dexter-monitor
sudo systemctl status dexter-monitor --no-pager
sudo journalctl -u dexter-monitor -f
```

## 6) Operations

```bash
sudo systemctl restart dexter-monitor
sudo systemctl stop dexter-monitor
sudo systemctl enable dexter-monitor
```

## Notes

- Service auto-restarts on crash and auto-starts on reboot.
- This setup is for signal generation/Telegram alerting.
- If you need live MT5 execution, you need a Windows host/VPS running MT5 bridge and set `MT5_HOST` reachable from VM.
- The setup script auto-detects package manager (`apt`, `dnf`, or `yum`), so it works for Ubuntu and Oracle Linux.
