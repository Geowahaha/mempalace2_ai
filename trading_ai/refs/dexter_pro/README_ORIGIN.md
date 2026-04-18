# Dexter Pro reference files (read-only copies)

These files were copied **verbatim** from:

`D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed`

**No files under that path were modified.** Use this folder as a local reference when wiring Mempalac’s `integrations/ctrader.py` to real OpenAPI (Twisted + `ctrader_open_api` + protobuf), OAuth refresh, and execution patterns.

## Contents

| File | Source path |
|------|-------------|
| `ctrader_openapi_business.md` | `docs/ctrader_openapi_business.md` |
| `store_ctrader_README.md` | `store/ctrader/README.md` |
| `ctrader_execute_once.py` | `ops/ctrader_execute_once.py` |
| `ctrader_stream.py` | `execution/ctrader_stream.py` |
| `ctrader_executor.py` | `execution/ctrader_executor.py` |

## Dependencies (Dexter)

The worker and stream expect the Spotware stack, for example:

- `ctrader_open_api`
- `twisted`
- `google.protobuf`

Install versions compatible with your Python environment per Spotware / Dexter docs.

## OAuth / env naming

Dexter uses `CTRADER_OPENAPI_*` (and legacy `OpenAPI_*` fallbacks in `config.py`). Mempalac’s `config.py` accepts the same names so lines can be pasted from a Dexter `.env.local` without edits. See `ENV_KEYS_FROM_DEXTER.md`.
