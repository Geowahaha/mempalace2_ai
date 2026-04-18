# Tencent VM Autotrade Setup (Mempalace)

This setup runs Mempalace as a `systemd` service, auto-start on reboot, and auto-deploy on each push to `main`.

## 1) Required GitHub Actions secrets

Set these in `mempalace2_ai` repository secrets:

- `GH_PAT` (repo access token)
- `TENCENT_VM_HOST` (public IP / host)
- `TENCENT_VM_USER` (usually `ubuntu`)
- `TENCENT_VM_SSH_KEY` (private key content)
- `INSTALL_OLLAMA` (`1` to auto-install Ollama, else `0`)

Workflow file:

- `.github/workflows/deploy.yml`

## 2) First bootstrap on VM (manual one-time)

```bash
ssh -i <your_key>.pem <user>@<host>
cd /opt || true
```

Clone and run bootstrap:

```bash
git clone https://github.com/Geowahaha/mempalace2_ai.git /opt/mempalace_ai
cd /opt/mempalace_ai
sudo REPO_URL="https://github.com/Geowahaha/mempalace2_ai.git" \
     APP_USER="$(id -un)" \
     APP_DIR=/opt/mempalace_ai \
     BRANCH=main \
     INSTALL_OLLAMA=1 \
     bash ops/tencent_mempalace_setup.sh
```

## 3) Fill runtime credentials

Edit:

```bash
nano /opt/mempalace_ai/trading_ai/.env
```

Required fields:

- `CTRADER_ACCOUNT_ID`
- `OpenAPI_ClientID`
- `OpenAPI_Secreat`
- `OpenAPI_Access_token_API_key3`
- `OpenAPI_Refresh_token_API_key3`

## 4) Start + verify

```bash
sudo systemctl restart mempalace-trader
sudo systemctl status mempalace-trader --no-pager
sudo journalctl -u mempalace-trader -f
```

## 5) Continuous deployment

After secrets are configured, every push to `main` triggers deploy and service restart automatically.
