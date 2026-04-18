"""
Shared entry-timing hints mined offline (see learning/mine_entry_templates.py).

Merged into signal.raw_scores so neural brains, execution journal, and downstream
models read the same template snapshot without per-model forks.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from config import config

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_cache: dict[str, Any] = {"path": "", "data": None, "mtime": 0.0}


def _default_catalog_path() -> Path:
    cfg = str(getattr(config, "ENTRY_TEMPLATE_CATALOG_PATH", "") or "").strip()
    if cfg:
        return Path(cfg)
    root = Path(__file__).resolve().parent.parent
    return root / "data" / "reports" / "entry_template_library.json"


def _session_bucket_from_signal(signal) -> str:
    raw = {}
    try:
        raw = dict(getattr(signal, "raw_scores", {}) or {})
    except Exception:
        raw = {}
    s = str(getattr(signal, "session", "") or raw.get("session") or "").strip().lower()
    if any(x in s for x in ("london", "europe", "frankfurt")):
        return "london"
    if any(x in s for x in ("ny", "new_york", "us_", "us ", "america")):
        return "us"
    if "asia" in s or "tokyo" in s or "sydney" in s:
        return "asia"
    h = datetime.now(timezone.utc).hour
    if 12 <= h < 21:
        return "us"
    if 7 <= h < 12:
        return "london"
    return "asia"


def _direction_token(signal) -> str:
    d = str(getattr(signal, "direction", "") or "").strip().lower()
    if d == "buy":
        return "long"
    if d == "sell":
        return "short"
    return d if d in {"long", "short"} else ""


def load_catalog(*, force: bool = False) -> Optional[dict]:
    path = _default_catalog_path()
    if not path.is_file():
        return None
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    with _lock:
        if not force and _cache["path"] == str(path) and _cache["mtime"] == mtime and isinstance(_cache["data"], dict):
            return dict(_cache["data"])
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("[entry_template_catalog] failed to read %s: %s", path, exc)
            return None
        if not isinstance(data, dict):
            return None
        _cache["path"] = str(path)
        _cache["mtime"] = mtime
        _cache["data"] = data
        return dict(data)


def session_bucket_for_entry_template(signal) -> str:
    """Public alias for session bucketing used by scalping_scanner M1 template bias."""
    return _session_bucket_from_signal(signal)


def pick_template_block(catalog: dict, *, symbol: str, session_bucket: str, direction: str) -> Optional[dict]:
    sym = str(symbol or "").strip().upper()
    if sym != "XAUUSD":
        return None
    by_sess = catalog.get("by_session")
    if not isinstance(by_sess, dict):
        return None
    block = by_sess.get(session_bucket) or by_sess.get("global")
    if not isinstance(block, dict):
        return None
    side = block.get(direction)
    return side if isinstance(side, dict) else None


def apply_entry_template_hints(signal) -> bool:
    """
    Attach mined template stats to raw_scores (audit + model consumption).
    Returns True if any field was written.
    """
    if not bool(getattr(config, "ENTRY_TEMPLATE_CATALOG_ENABLED", False)):
        return False
    if signal is None:
        return False
    sym = str(getattr(signal, "symbol", "") or "").strip().upper()
    if sym != "XAUUSD":
        return False
    direction = _direction_token(signal)
    if direction not in {"long", "short"}:
        return False
    catalog = load_catalog()
    if not catalog:
        return False
    try:
        raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
    except Exception:
        raw_scores = {}
    if raw_scores.get("entry_template_catalog_applied"):
        return False
    bucket = _session_bucket_from_signal(signal)
    tpl = pick_template_block(catalog, symbol=sym, session_bucket=bucket, direction=direction)
    if not tpl:
        tpl = pick_template_block(catalog, symbol=sym, session_bucket="global", direction=direction)
    if not tpl:
        return False
    ver = catalog.get("version", 0)
    raw_scores["entry_template_catalog_version"] = int(ver) if isinstance(ver, int) else ver
    raw_scores["entry_template_session_bucket"] = bucket
    raw_scores["entry_template_best_lookback_bars_1m"] = int(tpl.get("best_lookback_bars") or 0)
    raw_scores["entry_template_median_mae_pct"] = round(float(tpl.get("median_mae_pct_best") or 0.0), 5)
    raw_scores["entry_template_n_impulses"] = int(tpl.get("n_impulses") or 0)
    raw_scores["entry_template_missed_impulses"] = int(tpl.get("missed_impulses") or 0)
    raw_scores["entry_template_captured_impulses"] = int(tpl.get("captured_impulses") or 0)
    cap = tpl.get("capture_rate")
    if cap is not None:
        raw_scores["entry_template_impulse_capture_rate"] = round(float(cap), 4)
    lb = tpl.get("lookback_mae_table")
    if isinstance(lb, list):
        raw_scores["entry_template_lookback_mae_table"] = lb[:12]
    raw_scores["entry_template_catalog_applied"] = True
    try:
        signal.raw_scores = raw_scores
    except Exception:
        return False
    return True


def apply_entry_template_conf_tailwind(signal) -> None:
    """
    When catalog shows strong historical capture for this session×side, add a small
    confidence bump before neural soft-adjust (bounded; default max=0 = disabled).
    """
    max_tw = float(getattr(config, "ENTRY_TEMPLATE_CONF_TAILWIND_MAX", 0.0) or 0.0)
    if max_tw <= 0.0 or signal is None:
        return
    try:
        raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
    except Exception:
        return
    if not raw_scores.get("entry_template_catalog_applied"):
        return
    min_cap = float(getattr(config, "ENTRY_TEMPLATE_CONF_TAILWIND_MIN_CAPTURE", 0.52) or 0.52)
    min_n = int(getattr(config, "ENTRY_TEMPLATE_CONF_TAILWIND_MIN_IMPULSES", 30) or 30)
    cap_val = raw_scores.get("entry_template_impulse_capture_rate")
    n_imp = int(raw_scores.get("entry_template_n_impulses") or 0)
    if cap_val is None or n_imp < min_n:
        return
    try:
        cap = float(cap_val)
    except (TypeError, ValueError):
        return
    if cap < min_cap:
        return
    excess = max(0.0, cap - min_cap)
    delta = min(max_tw, excess * 2.5)
    if delta <= 1e-6:
        return
    try:
        base = float(getattr(signal, "confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        return
    signal.confidence = round(min(95.0, base + delta), 1)
    raw_scores["entry_template_conf_tailwind_delta"] = round(delta, 3)
    try:
        signal.raw_scores = raw_scores
    except Exception:
        return
