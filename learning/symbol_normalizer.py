"""
learning/symbol_normalizer.py

Canonical symbol mapping utilities used by learning/training pipelines.
Keeps aliases (e.g. BTCUSD vs BTC/USDT) in one normalized key so feedback
and model training do not fragment across equivalent symbols.
"""

from __future__ import annotations

import re


_FX_PAIRS = {
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "USDCAD",
    "AUDUSD",
    "NZDUSD",
    "EURJPY",
    "EURGBP",
    "GBPJPY",
    "AUDJPY",
    "CHFJPY",
    "CADJPY",
    "NZDJPY",
    "EURCHF",
    "EURAUD",
    "EURNZD",
    "GBPAUD",
    "GBPCAD",
    "GBPNZD",
    "AUDCAD",
    "AUDNZD",
    "NZDCAD",
}

_CRYPTO_CANON = {
    "BTC": "BTC/USDT",
    "ETH": "ETH/USDT",
}


def _flat(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def canonical_symbol(symbol: str) -> str:
    """
    Convert raw broker/provider symbols to a stable canonical key.
    """
    raw = str(symbol or "").strip().upper()
    if not raw:
        return ""

    compact = raw.replace(" ", "")
    flat = _flat(compact)

    # Gold aliases
    if "GOLD" in compact or flat.startswith("XAUUSD"):
        return "XAUUSD"

    # Canonicalize BTC/ETH aliases (USD/USDT/USDC/BUSD variants).
    for base, canon in _CRYPTO_CANON.items():
        if flat.startswith(f"{base}USD") or flat.startswith(f"{base}USDT") or flat.startswith(f"{base}USDC") or flat.startswith(f"{base}BUSD"):
            return canon

    # Slash pairs.
    if "/" in compact:
        base, quote = compact.split("/", 1)
        base = _flat(base)
        quote = _flat(quote)
        if not base or not quote:
            return compact
        if base in _CRYPTO_CANON and quote in {"USD", "USDT", "USDC", "BUSD"}:
            return _CRYPTO_CANON[base]
        return f"{base}/{quote}"

    # Major FX with broker suffixes (EURUSDm -> EURUSD).
    if len(flat) >= 6 and flat[:6] in _FX_PAIRS:
        return flat[:6]

    # Common crypto compact symbols (BTCUSDT, ETHUSDT, ...)
    for base, canon in _CRYPTO_CANON.items():
        if flat.startswith(base) and flat.endswith(("USD", "USDT", "USDC", "BUSD")):
            return canon

    return compact

