# Tencent VM Autotrade Setup (Mempalace)

This setup runs Mempalace as a `systemd` service, auto-start on reboot, and auto-deploy on each push to `main`.

## Architecture (Tencent optimized)

The stack is split into 3 lanes so Tencent resources are used efficiently:

- Lane A (live decision): low-latency local model chain with failover and circuit breaker.
- Lane B (Hermes learning): dedicated self-improvement model for post-trade distillation.
- Lane C (execution): Dexter cTrader worker for broker health/reconcile/execute.

Key runtime knobs:

- `LLM_FAILOVER_FAILURE_THRESHOLD`
- `LLM_FAILOVER_COOLDOWN_SEC`
- `LLM_FAILOVER_RUNTIME_PATH`
- `SELF_IMPROVEMENT_MODEL_NAME`

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

## 4) Burn-in test (Tencent infra + Hermes lanes)

Run an end-to-end burn-in before enabling the next live signal:

```bash
cd /opt/mempalace_ai
. .venv/bin/activate
python ops/tencent_infra_burnin.py --rounds 6 --include-self-improvement --with-broker-health --fail-on-error
```

When XAUUSD is closed (weekend/off-hours), run the same burn-in on BTCUSD:

```bash
python ops/tencent_infra_burnin.py --symbol BTCUSD --rounds 6 --include-self-improvement --with-broker-health
```

What this validates:

- primary local LLM chain latency and failure rate
- dedicated self-improvement/Hermes model lane (if configured)
- failover circuit state (`LLM_FAILOVER_*`)
- cTrader worker `health` probe with current account credentials

## 5) Start + verify service

```bash
sudo systemctl restart mempalace-trader
sudo systemctl status mempalace-trader --no-pager
sudo journalctl -u mempalace-trader -f
```

Optional API checks:

```bash
curl -s http://127.0.0.1:8091/llm/failover | jq
curl -s http://127.0.0.1:8091/broker/health | jq
```

## 5.1) Automatic model tournament (latency + hold-rate + rejection-rate)

Run a tournament across local model profiles and auto-select winner by weighted score:

```bash
cd /opt/mempalace_ai
. .venv/bin/activate
python ops/tencent_model_tournament.py --symbol BTCUSD --rounds 4 --include-self-improvement --with-broker-health --output data/reports/tencent_model_tournament.json
```

Apply winner patch to `trading_ai/.env` automatically:

```bash
python ops/tencent_model_tournament.py --symbol BTCUSD --rounds 4 --include-self-improvement --with-broker-health --apply --output data/reports/tencent_model_tournament_apply.json
sudo systemctl restart mempalace-trader
```

## 6) Continuous deployment

After secrets are configured, every push to `main` triggers deploy and service restart automatically.
