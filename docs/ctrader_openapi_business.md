# cTrader OpenAPI Business Lanes

## Goal

Run cTrader as a second execution lane beside MT5 without sharing runtime state, then use the same signal engine for a seller path on cTrader Store.

## Revenue Lanes

1. Direct owner execution
Use Dexter signals on your own cTrader account through OpenAPI.

2. Seller/store distribution
Expose a stable bridge feed for a cBot or plugin product that subscribers can install from cTrader Store.

3. Premium bridge access
Sell private bridge/API access to approved users or prop teams without exposing core strategy code.

## Current Architecture

1. `scheduler.py`
Generates signals once, then dispatches independently to MT5 and cTrader.

2. `execution/ctrader_executor.py`
Own journal, own config gates, own worker process.

3. `ops/ctrader_execute_once.py`
One-shot OpenAPI worker. Twisted lives here only.

4. `api/bridge_server.py`
Publishes `/api/ctrader/status`, `/api/ctrader/journal`, `/api/ctrader/feed`.

## Why This Model

1. MT5 remains isolated
No shared reactor, no shared broker session, no shared execution journal.

2. cTrader seller path stays reusable
Store products can consume a stable HTTP feed without embedding all Dexter internals.

3. Monetization is flexible
You can run private execution, signal subscriptions, or Store products in parallel.

## Constraints

1. cTrader OpenAPI execution needs valid OAuth tokens.
2. cTrader Store publishing is still a manual portal workflow.
3. Store products should use cBot/plugin packaging, not the Python OpenAPI worker directly.

## Near-Term Next Steps

1. Refresh OAuth tokens and verify live account auth.
2. Add cTrader position manager for multi-TP and trailing logic.
3. Build a production cBot client for `/api/ctrader/feed`.
