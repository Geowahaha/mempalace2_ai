# cTrader-related environment keys (from Dexter `config.py`)

**Names only** — paste real values into Mempalac `.env` locally; do not commit secrets.

This list is derived from Dexter Pro configuration so you can copy blocks from `.env.local` knowing which symbols Mempalac also understands.

## Open API OAuth (primary)

| Variable | Notes |
|----------|--------|
| `CTRADER_OPENAPI_CLIENT_ID` | Also legacy: `OpenAPI_ClientID` in Dexter |
| `CTRADER_OPENAPI_CLIENT_SECRET` | Legacy: `OpenAPI_Secreat` / `OpenAPI_Secret` |
| `CTRADER_OPENAPI_REDIRECT_URI` | Default `http://localhost` |
| `CTRADER_OPENAPI_ACCESS_TOKEN` | Legacy: `OpenAPI_Access_token_API_key` |
| `CTRADER_OPENAPI_REFRESH_TOKEN` | Legacy: `OpenAPI_Refresh_token_API_key`; used by refresh in `ctrader_execute_once.py` |

## Account / mode flags (Dexter)

| Variable | Notes |
|----------|--------|
| `CTRADER_ENABLED` | Master enable |
| `CTRADER_USE_DEMO` | Demo vs live host (`EndPoints.PROTOBUF_*`) |
| `CTRADER_ACCOUNT_ID` | Numeric ctid account id |
| `CTRADER_ACCOUNT_LOGIN` | Login lookup via `find_ctrader_account` |
| `CTRADER_DRY_RUN` | Dexter execution dry-run |
| `CTRADER_DB_PATH` | SQLite journal path |
| `CTRADER_EXECUTOR_TIMEOUT_SEC` | Subprocess / worker timeout |
| `CTRADER_WORKER_DEBUG` | Verbose worker logging |

## Operational (subset)

Dexter defines many more gates (`CTRADER_ALLOWED_SYMBOLS`, risk USD per family, position manager, etc.). See Dexter `config.py` if you need the full surface. Mempalac intentionally keeps a smaller env set for the AI loop; extend as you integrate.
