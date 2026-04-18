# Dexter cTrader Store Path

## Purpose

This folder is the seller/distribution path for cTrader users.

The private owner path uses:

- `execution/ctrader_executor.py`
- `ops/ctrader_execute_once.py`

The store path should use:

- `api/ctrader/feed`
- `api/ctrader/status`

## Recommended Product Shape

1. cBot client
Poll the bridge feed, filter allowed symbols, place orders with local cTrader Automate API.

2. Read-only signal dashboard plugin
Show active Dexter signals, confidence, entry, SL, TP, and health state.

3. Trial + paid editions
Trial can run delayed feed or read-only mode. Paid version can enable live execution.

## Manual Publish Notes

1. Build and package the cBot in cTrader Desktop.
2. Upload the compiled algorithm to cTrader Store.
3. Point the product to your bridge URL and token flow.

## Stable Bridge Endpoints

1. `GET /api/ctrader/feed`
2. `GET /api/ctrader/status`
3. `GET /api/ctrader/journal`
