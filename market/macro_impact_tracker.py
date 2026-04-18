"""
market/macro_impact_tracker.py
Post-news impact tracker for macro headlines.

Tracks headline publication times and samples proxy prices around:
- T-15m (pre-move check)
- T0
- T+5m
- T+15m
- T+60m

Outputs a human-readable classification:
- impact_confirmed
- impact_developing
- no_clear_impact
- priced_in
- faded
- pending / incomplete
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf

from config import config
from market.data_fetcher import crypto_provider, xauusd_provider
from market.macro_news import MacroHeadline, macro_news

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).isoformat()


def _parse_iso(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


@dataclass
class _HeadlineRow:
    headline_id: str
    title: str
    link: str
    source: str
    published_utc: datetime
    score: int
    themes: list[str]
    impact_hint: str


class MacroImpactTracker:
    HORIZONS_MIN: tuple[int, ...] = (-15, 0, 5, 15, 60)
    ASSET_ORDER: tuple[str, ...] = ("XAUUSD", "BTCUSD", "ETHUSD", "US500")
    # Heuristic significance threshold per asset (% move from T0).
    ASSET_SIG_PCT: dict[str, float] = {
        "XAUUSD": 0.15,
        "BTCUSD": 0.60,
        "ETHUSD": 0.80,
        "US500": 0.35,
    }

    def __init__(self, db_path: Optional[str] = None):
        db_default = Path(__file__).resolve().parent.parent / "data" / "macro_impact.db"
        cfg_path = str(getattr(config, "MACRO_IMPACT_TRACKER_DB_PATH", "") or "").strip()
        self.db_path = Path(db_path or cfg_path or db_default)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()
        try:
            self.apply_theme_weights_to_macro_news()
        except Exception as e:
            logger.debug("[MacroImpact] apply stored theme weights on init failed: %s", e)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS macro_headlines (
                        headline_id TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        link TEXT,
                        source TEXT,
                        published_utc TEXT NOT NULL,
                        score INTEGER NOT NULL,
                        themes_json TEXT NOT NULL,
                        impact_hint TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS macro_price_samples (
                        headline_id TEXT NOT NULL,
                        asset TEXT NOT NULL,
                        horizon_min INTEGER NOT NULL,
                        price REAL NOT NULL,
                        target_utc TEXT NOT NULL,
                        sample_utc TEXT NOT NULL,
                        source TEXT,
                        PRIMARY KEY (headline_id, asset, horizon_min)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS macro_theme_weights (
                        theme TEXT PRIMARY KEY,
                        weight_mult REAL NOT NULL,
                        sample_count INTEGER NOT NULL,
                        confirmed_rate REAL NOT NULL,
                        priced_in_rate REAL NOT NULL,
                        no_clear_rate REAL NOT NULL,
                        faded_rate REAL NOT NULL,
                        lookback_hours INTEGER NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.commit()

    @staticmethod
    def _normalize_series(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        if df is None or getattr(df, "empty", True):
            return None
        out = df.copy()
        if "close" not in out.columns:
            cols = {str(c).lower(): c for c in out.columns}
            if "close" in cols:
                out = out.rename(columns={cols["close"]: "close"})
            else:
                return None
        try:
            idx = pd.to_datetime(out.index, utc=True)
            out.index = idx
        except Exception:
            try:
                out.index = pd.to_datetime(out.index).tz_localize("UTC")
            except Exception:
                return None
        out = out.sort_index()
        out = out[~out.index.duplicated(keep="last")]
        return out

    @staticmethod
    def _nearest_price(df: Optional[pd.DataFrame], target_dt: datetime, tolerance_min: int = 20) -> Optional[float]:
        if df is None or getattr(df, "empty", True):
            return None
        target = target_dt.astimezone(timezone.utc)
        try:
            idx = df.index
            # nearest index position
            pos = idx.get_indexer([pd.Timestamp(target)], method="nearest")[0]
            if pos < 0:
                return None
            nearest_ts = idx[pos].to_pydatetime().astimezone(timezone.utc)
            if abs((nearest_ts - target).total_seconds()) > max(60, int(tolerance_min) * 60):
                return None
            return float(df.iloc[pos]["close"])
        except Exception:
            # fallback scan
            try:
                deltas = (df.index - pd.Timestamp(target)).to_series().abs()
                if deltas.empty:
                    return None
                nearest_idx = deltas.idxmin()
                if abs((nearest_idx.to_pydatetime().astimezone(timezone.utc) - target).total_seconds()) > max(60, int(tolerance_min) * 60):
                    return None
                return float(df.loc[nearest_idx]["close"])
            except Exception:
                return None

    def _fetch_yf_series(self, ticker: str, period: str = "5d", interval: str = "5m") -> Optional[pd.DataFrame]:
        try:
            raw = yf.download(ticker, period=period, interval=interval, auto_adjust=True, progress=False, timeout=15)
            if raw is None or raw.empty:
                return None
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw = raw.rename(columns={"Close": "close", "Open": "open", "High": "high", "Low": "low", "Volume": "volume"})
            raw.columns = [str(c).lower() for c in raw.columns]
            if "close" not in raw.columns:
                return None
            return self._normalize_series(raw)
        except Exception as e:
            logger.debug("[MacroImpact] yf fetch %s failed: %s", ticker, e)
            return None

    def _asset_series_cache(self) -> dict[str, Optional[pd.DataFrame]]:
        cache: dict[str, Optional[pd.DataFrame]] = {}

        def load_xau():
            return self._normalize_series(xauusd_provider.fetch("5m", bars=1000))

        def load_crypto(sym: str):
            if crypto_provider is None:
                return None
            return self._normalize_series(crypto_provider.fetch_ohlcv(sym, timeframe="5m", bars=1000))

        cache["XAUUSD"] = load_xau()
        cache["BTCUSD"] = load_crypto("BTC/USDT")
        cache["ETHUSD"] = load_crypto("ETH/USDT")
        cache["US500"] = self._fetch_yf_series("SPY", period="5d", interval="5m")
        return cache

    def ingest_headlines(self, headlines: list[MacroHeadline]) -> int:
        now_iso = _iso(_utc_now())
        inserted = 0
        with self._lock:
            with closing(self._connect()) as conn:
                for h in headlines or []:
                    try:
                        conn.execute(
                            """
                            INSERT INTO macro_headlines (
                                headline_id, title, link, source, published_utc, score, themes_json, impact_hint, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(headline_id) DO UPDATE SET
                                title=excluded.title,
                                link=excluded.link,
                                source=excluded.source,
                                published_utc=excluded.published_utc,
                                score=excluded.score,
                                themes_json=excluded.themes_json,
                                impact_hint=excluded.impact_hint,
                                updated_at=excluded.updated_at
                            """,
                            (
                                str(h.headline_id),
                                str(h.title),
                                str(h.link),
                                str(h.source),
                                _iso(h.published_utc),
                                int(h.score),
                                json.dumps(list(h.themes or []), ensure_ascii=False),
                                str(h.impact_hint or ""),
                                now_iso,
                                now_iso,
                            ),
                        )
                        inserted += 1
                    except Exception:
                        continue
                conn.commit()
        return inserted

    def _load_recent_headlines(self, hours: int, min_score: int, limit: int) -> list[_HeadlineRow]:
        lookback_iso = _iso(_utc_now() - timedelta(hours=max(1, int(hours))))
        with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    """
                    SELECT headline_id, title, link, source, published_utc, score, themes_json, impact_hint
                    FROM macro_headlines
                    WHERE published_utc >= ? AND score >= ?
                    ORDER BY score DESC, published_utc DESC
                    LIMIT ?
                    """,
                    (lookback_iso, int(min_score), max(1, int(limit))),
                ).fetchall()
        out: list[_HeadlineRow] = []
        for r in rows:
            dt = _parse_iso(r[4])
            if dt is None:
                continue
            try:
                themes = json.loads(r[6] or "[]")
            except Exception:
                themes = []
            out.append(
                _HeadlineRow(
                    headline_id=str(r[0]),
                    title=str(r[1] or ""),
                    link=str(r[2] or ""),
                    source=str(r[3] or ""),
                    published_utc=dt,
                    score=int(r[5] or 0),
                    themes=list(themes or []),
                    impact_hint=str(r[7] or ""),
                )
            )
        return out

    def _load_samples_map(self, headline_id: str) -> dict[str, dict[int, dict]]:
        with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    """
                    SELECT asset, horizon_min, price, target_utc, sample_utc, source
                    FROM macro_price_samples
                    WHERE headline_id = ?
                    """,
                    (str(headline_id),),
                ).fetchall()
        out: dict[str, dict[int, dict]] = {}
        for r in rows:
            asset = str(r[0])
            horizon = int(r[1])
            out.setdefault(asset, {})[horizon] = {
                "price": float(r[2]),
                "target_utc": str(r[3] or ""),
                "sample_utc": str(r[4] or ""),
                "source": str(r[5] or ""),
            }
        return out

    def _upsert_sample(self, headline_id: str, asset: str, horizon_min: int, price: float, target_dt: datetime, source: str) -> bool:
        if price is None:
            return False
        try:
            val = float(price)
        except Exception:
            return False
        with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO macro_price_samples (
                        headline_id, asset, horizon_min, price, target_utc, sample_utc, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(headline_id),
                        str(asset),
                        int(horizon_min),
                        val,
                        _iso(target_dt),
                        _iso(_utc_now()),
                        str(source or ""),
                    ),
                )
                conn.commit()
        return True

    def sync(self, hours: Optional[int] = None, min_score: Optional[int] = None, limit: Optional[int] = None) -> dict:
        if not bool(getattr(config, "MACRO_IMPACT_TRACKER_ENABLED", True)):
            return {"ok": False, "status": "disabled", "ingested": 0, "sampled": 0, "headlines": 0}

        lookback_h = max(1, int(hours or getattr(config, "MACRO_IMPACT_TRACKER_LOOKBACK_HOURS", 72)))
        score_min = max(1, int(min_score or getattr(config, "MACRO_IMPACT_TRACKER_MIN_SCORE", 5)))
        max_heads = max(1, int(limit or getattr(config, "MACRO_IMPACT_TRACKER_MAX_HEADLINES_PER_SYNC", 20)))

        heads = macro_news.high_impact_headlines(hours=lookback_h, min_score=score_min, limit=max_heads)
        ingested = self.ingest_headlines(heads)
        if not heads:
            weights_status = self.refresh_adaptive_weights()
            return {
                "ok": True,
                "status": "ok",
                "ingested": ingested,
                "sampled": 0,
                "headlines": 0,
                "weights_updated": int(weights_status.get("updated", 0) or 0),
            }

        series_cache = self._asset_series_cache()
        sampled = 0
        now = _utc_now()
        db_heads = self._load_recent_headlines(hours=lookback_h, min_score=score_min, limit=max_heads)
        for h in db_heads:
            sample_map = self._load_samples_map(h.headline_id)
            for asset in self.ASSET_ORDER:
                series = series_cache.get(asset)
                if series is None:
                    continue
                source = "ccxt" if asset in {"BTCUSD", "ETHUSD"} else ("xau_provider" if asset == "XAUUSD" else "yfinance:SPY")
                for horizon in self.HORIZONS_MIN:
                    if horizon in (sample_map.get(asset) or {}):
                        continue
                    target_dt = h.published_utc + timedelta(minutes=int(horizon))
                    if target_dt > now:
                        continue  # future sample not available yet
                    price = self._nearest_price(series, target_dt, tolerance_min=20)
                    if price is None:
                        continue
                    if self._upsert_sample(h.headline_id, asset, horizon, price, target_dt, source):
                        sampled += 1
        weights_status = self.refresh_adaptive_weights()
        return {
            "ok": True,
            "status": "ok",
            "ingested": ingested,
            "sampled": sampled,
            "headlines": len(db_heads),
            "weights_updated": int(weights_status.get("updated", 0) or 0),
            "weights_applied": bool(weights_status.get("applied", False)),
        }

    @staticmethod
    def _pct_change(from_price: Optional[float], to_price: Optional[float]) -> Optional[float]:
        try:
            a = float(from_price)
            b = float(to_price)
            if a == 0:
                return None
            return ((b - a) / abs(a)) * 100.0
        except Exception:
            return None

    @classmethod
    def _classify_asset(cls, asset: str, samples: dict[int, float], age_sec: int) -> tuple[str, dict]:
        t0 = samples.get(0)
        pre15 = samples.get(-15)
        t5 = samples.get(5)
        t15 = samples.get(15)
        t60 = samples.get(60)
        out = {
            "pre15_pct": cls._pct_change(pre15, t0) if (pre15 is not None and t0 is not None) else None,
            "t5_pct": cls._pct_change(t0, t5) if (t0 is not None and t5 is not None) else None,
            "t15_pct": cls._pct_change(t0, t15) if (t0 is not None and t15 is not None) else None,
            "t60_pct": cls._pct_change(t0, t60) if (t0 is not None and t60 is not None) else None,
            "latest_horizon_min": None,
            "latest_pct": None,
        }
        for hz in (60, 15, 5):
            val = out.get(f"t{hz}_pct")
            if val is not None:
                out["latest_horizon_min"] = hz
                out["latest_pct"] = val
                break
        if t0 is None:
            return "incomplete", out
        if age_sec < 5 * 60:
            return "pending", out
        sig = float(cls.ASSET_SIG_PCT.get(asset, 0.5))
        post_vals = [v for k, v in out.items() if k in {"t5_pct", "t15_pct", "t60_pct"} and v is not None]
        post_max_abs = max([abs(v) for v in post_vals], default=0.0)
        pre_abs = abs(float(out["pre15_pct"])) if out["pre15_pct"] is not None else 0.0
        if age_sec >= 15 * 60 and pre_abs >= (sig * 0.8) and post_max_abs < (sig * 0.6):
            return "priced_in", out
        if age_sec >= 60 * 60 and post_max_abs < sig:
            return "no_clear_impact", out
        if out["t5_pct"] is not None and out["t60_pct"] is not None:
            if abs(float(out["t5_pct"])) >= sig and abs(float(out["t60_pct"])) <= abs(float(out["t5_pct"])) * 0.35:
                return "faded", out
        if post_max_abs >= sig:
            return ("impact_developing" if age_sec < 60 * 60 else "impact_confirmed"), out
        return "monitoring", out

    @classmethod
    def _classify_headline(cls, per_asset: dict[str, dict], age_sec: int) -> tuple[str, str]:
        labels = [str(v.get("asset_label") or "") for v in per_asset.values()]
        classes = [str(v.get("classification") or "") for v in per_asset.values()]
        if not per_asset:
            return "incomplete", "No asset samples available yet."
        if age_sec < 5 * 60:
            return "pending", "Headline is fresh; waiting for post-news market reaction samples."

        confirmed = sum(1 for c in classes if c in {"impact_confirmed", "impact_developing"})
        priced_in = sum(1 for c in classes if c == "priced_in")
        faded = sum(1 for c in classes if c == "faded")
        clear = sum(1 for c in classes if c == "no_clear_impact")

        if confirmed >= 2:
            label = "impact_confirmed" if age_sec >= 60 * 60 else "impact_developing"
        elif confirmed == 1:
            label = "impact_developing"
        elif priced_in >= 1 and clear >= 1:
            label = "priced_in"
        elif faded >= 1:
            label = "faded"
        elif clear >= 2 or (clear >= 1 and age_sec >= 60 * 60):
            label = "no_clear_impact"
        else:
            label = "monitoring"

        # Build compact move summary from strongest latest moves.
        asset_parts: list[str] = []
        ranked = []
        for asset, rec in per_asset.items():
            pct = rec.get("latest_pct")
            hz = rec.get("latest_horizon_min")
            if pct is None:
                continue
            ranked.append((abs(float(pct)), asset, float(pct), int(hz or 0)))
        ranked.sort(reverse=True)
        for _, asset, pct, hz in ranked[:4]:
            sign = "+" if pct >= 0 else ""
            asset_parts.append(f"{asset} {sign}{pct:.2f}%@T+{hz}m")
        summary = " | ".join(asset_parts) if asset_parts else "Insufficient post-news samples yet."
        return label, summary

    @staticmethod
    def _label_to_human(label: str) -> str:
        mapping = {
            "impact_confirmed": "Impact Confirmed",
            "impact_developing": "Impact Developing",
            "no_clear_impact": "No Clear Impact",
            "priced_in": "Likely Priced-In",
            "faded": "Impact Faded",
            "pending": "Pending Reaction",
            "monitoring": "Monitoring",
            "incomplete": "Incomplete Data",
        }
        return mapping.get(str(label or ""), str(label or "unknown"))

    @staticmethod
    def _clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, float(v)))

    def _save_theme_weights(self, rows: list[dict], lookback_hours: int) -> int:
        if not rows:
            return 0
        now_iso = _iso(_utc_now())
        updated = 0
        with self._lock:
            with closing(self._connect()) as conn:
                for r in rows:
                    try:
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO macro_theme_weights (
                                theme, weight_mult, sample_count, confirmed_rate, priced_in_rate, no_clear_rate, faded_rate, lookback_hours, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                str(r.get("theme")),
                                float(r.get("weight_mult", 1.0)),
                                int(r.get("count", 0)),
                                float(r.get("confirmed_rate", 0.0)),
                                float(r.get("priced_in_rate", 0.0)),
                                float(r.get("no_clear_rate", 0.0)),
                                float(r.get("faded_rate", 0.0)),
                                int(lookback_hours),
                                now_iso,
                            ),
                        )
                        updated += 1
                    except Exception:
                        continue
                conn.commit()
        return updated

    def load_theme_weights(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    """
                    SELECT theme, weight_mult, sample_count, confirmed_rate, priced_in_rate, no_clear_rate, faded_rate, lookback_hours, updated_at
                    FROM macro_theme_weights
                    """
                ).fetchall()
        for r in rows:
            out[str(r[0])] = {
                "weight_mult": float(r[1]),
                "sample_count": int(r[2]),
                "confirmed_rate": float(r[3]),
                "priced_in_rate": float(r[4]),
                "no_clear_rate": float(r[5]),
                "faded_rate": float(r[6]),
                "lookback_hours": int(r[7]),
                "updated_at": str(r[8] or ""),
            }
        return out

    def compute_theme_weight_adjustments(self, lookback_hours: Optional[int] = None, min_score: Optional[int] = None) -> dict:
        hours = max(24, int(lookback_hours or getattr(config, "MACRO_ADAPTIVE_WEIGHT_UPDATE_HOURS", 168)))
        score_min = max(1, int(min_score or getattr(config, "MACRO_IMPACT_TRACKER_MIN_SCORE", 5)))
        report = self.build_report(
            hours=hours,
            min_score=score_min,
            min_risk_stars=macro_news.score_to_stars(score_min),
            limit=max(50, int(getattr(config, "MACRO_IMPACT_TRACKER_MAX_HEADLINES_PER_SYNC", 20))),
        )
        min_samples = max(1, int(getattr(config, "MACRO_ADAPTIVE_WEIGHT_MIN_SAMPLES", 3)))
        lo = float(getattr(config, "MACRO_ADAPTIVE_WEIGHT_MIN_MULT", 0.80))
        hi = float(getattr(config, "MACRO_ADAPTIVE_WEIGHT_MAX_MULT", 1.25))
        rows: list[dict] = []
        for ts in list(report.get("theme_stats", []) or []):
            count = int(ts.get("count", 0) or 0)
            if count < min_samples:
                continue
            confirmed = float(ts.get("confirmed_rate", 0.0) or 0.0)
            priced_in = float(ts.get("priced_in_rate", 0.0) or 0.0)
            no_clear = float(ts.get("no_clear_rate", 0.0) or 0.0)
            faded = float(ts.get("faded_rate", 0.0) or 0.0)
            # Weighted effectiveness score; priced-in still counts partially (headline relevance exists),
            # no_clear penalizes more heavily.
            effective = confirmed + (0.5 * priced_in) + (0.2 * faded) - (0.7 * no_clear)
            # Center around 25; slope chosen to remain conservative, then bounded.
            mult = 1.0 + ((effective - 25.0) / 100.0) * 0.35
            mult = self._clamp(mult, lo, hi)
            rows.append(
                {
                    "theme": str(ts.get("theme")),
                    "count": count,
                    "confirmed_rate": round(confirmed, 1),
                    "priced_in_rate": round(priced_in, 1),
                    "no_clear_rate": round(no_clear, 1),
                    "faded_rate": round(faded, 1),
                    "effective_score": round(effective, 2),
                    "weight_mult": round(mult, 4),
                    "lookback_hours": hours,
                }
            )
        return {"ok": True, "hours": hours, "min_score": score_min, "weights": rows, "theme_stats": report.get("theme_stats", [])}

    def apply_theme_weights_to_macro_news(self) -> dict:
        stored = self.load_theme_weights()
        weight_map = {k: float(v.get("weight_mult", 1.0)) for k, v in stored.items()}
        meta = {k: dict(v) for k, v in stored.items()}
        macro_news.set_dynamic_theme_weights(weight_map, meta=meta)
        return {"ok": True, "applied": True, "count": len(weight_map)}

    def refresh_adaptive_weights(self) -> dict:
        if not bool(getattr(config, "MACRO_ADAPTIVE_WEIGHTING_ENABLED", True)):
            # Clear runtime dynamic weights when feature disabled.
            try:
                macro_news.set_dynamic_theme_weights({}, meta={})
            except Exception:
                pass
            return {"ok": True, "status": "disabled", "updated": 0, "applied": False}
        comp = self.compute_theme_weight_adjustments()
        weights = list(comp.get("weights", []) or [])
        updated = self._save_theme_weights(weights, int(comp.get("hours", 0) or 0)) if weights else 0
        apply_res = self.apply_theme_weights_to_macro_news()
        return {
            "ok": True,
            "status": "ok",
            "updated": updated,
            "applied": bool(apply_res.get("applied")),
            "count": int(apply_res.get("count", 0) or 0),
        }

    def build_weights_report(self, limit: Optional[int] = None) -> dict:
        """
        Snapshot current adaptive macro theme weights (stored + runtime-applied).
        Used by /macro_weights command and CLI diagnostics.
        """
        top_n = max(1, int(limit or getattr(config, "MACRO_WEIGHTS_DEFAULT_TOP", 8)))
        stored = self.load_theme_weights()
        runtime = macro_news.dynamic_theme_weights_snapshot()

        rows: list[dict] = []
        seen = set(runtime.keys()) | set(stored.keys())
        for theme in seen:
            s = dict(stored.get(theme, {}) or {})
            r = dict(runtime.get(theme, {}) or {})
            weight_mult = float(s.get("weight_mult", r.get("weight_mult", 1.0)) or 1.0)
            base_score = float(r.get("base_score", 0.0) or 0.0)
            effective_score = float(r.get("effective_score", base_score * weight_mult) or 0.0)
            sample_count = int(s.get("sample_count", r.get("sample_count", 0)) or 0)
            rows.append(
                {
                    "theme": str(theme),
                    "weight_mult": round(weight_mult, 4),
                    "base_score": round(base_score, 3),
                    "effective_score": round(effective_score, 3),
                    "sample_count": sample_count,
                    "confirmed_rate": float(s.get("confirmed_rate", r.get("confirmed_rate", 0.0)) or 0.0),
                    "priced_in_rate": float(s.get("priced_in_rate", r.get("priced_in_rate", 0.0)) or 0.0),
                    "no_clear_rate": float(s.get("no_clear_rate", r.get("no_clear_rate", 0.0)) or 0.0),
                    "faded_rate": float(s.get("faded_rate", r.get("faded_rate", 0.0)) or 0.0),
                    "lookback_hours": int(s.get("lookback_hours", 0) or 0),
                    "updated_at": str(s.get("updated_at", "") or ""),
                }
            )

        rows.sort(
            key=lambda x: (
                abs(float(x.get("weight_mult", 1.0)) - 1.0),
                int(x.get("sample_count", 0) or 0),
                str(x.get("theme", "")),
            ),
            reverse=True,
        )
        rows = rows[:top_n]

        return {
            "ok": True,
            "enabled": bool(getattr(config, "MACRO_ADAPTIVE_WEIGHTING_ENABLED", True)),
            "generated_at_utc": _utc_now(),
            "rows": rows,
            "top_n": top_n,
            "stored_count": len(stored),
            "runtime_count": len(runtime),
            "thresholds": {
                "min_samples": int(getattr(config, "MACRO_ADAPTIVE_WEIGHT_MIN_SAMPLES", 3)),
                "min_mult": float(getattr(config, "MACRO_ADAPTIVE_WEIGHT_MIN_MULT", 0.80)),
                "max_mult": float(getattr(config, "MACRO_ADAPTIVE_WEIGHT_MAX_MULT", 1.25)),
                "update_hours": int(getattr(config, "MACRO_ADAPTIVE_WEIGHT_UPDATE_HOURS", 168)),
            },
            "alert_adaptive": {
                "enabled": bool(getattr(config, "MACRO_ALERT_ADAPTIVE_PRIORITY_ENABLED", True)),
                "min_samples": int(getattr(config, "MACRO_ALERT_ADAPTIVE_MIN_SAMPLES", getattr(config, "MACRO_ADAPTIVE_WEIGHT_MIN_SAMPLES", 3))),
                "min_theme_mult": float(getattr(config, "MACRO_ALERT_ADAPTIVE_MIN_THEME_MULT", 0.90)),
                "skip_no_clear_rate": float(getattr(config, "MACRO_ALERT_ADAPTIVE_SKIP_NO_CLEAR_RATE", 65)),
                "ultra_score_floor": int(getattr(config, "MACRO_ALERT_ADAPTIVE_ULTRA_SCORE_FLOOR", 10)),
            },
        }

    def build_report(
        self,
        hours: int = 24,
        min_score: Optional[int] = None,
        min_risk_stars: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> dict:
        lookback_h = max(1, int(hours or getattr(config, "MACRO_REPORT_DEFAULT_HOURS", 24)))
        score_min = int(min_score or max(1, int(getattr(config, "MACRO_NEWS_MIN_SCORE", 6))))
        max_rows = max(1, int(limit or getattr(config, "MACRO_REPORT_MAX_HEADLINES", 5)))
        rows = self._load_recent_headlines(hours=lookback_h, min_score=score_min, limit=max_rows)
        entries: list[dict] = []
        now = _utc_now()

        theme_rollup: dict[str, dict] = {}
        for h in rows:
            raw_samples = self._load_samples_map(h.headline_id)
            age_sec = max(0, int((now - h.published_utc).total_seconds()))
            per_asset: dict[str, dict] = {}
            for asset in self.ASSET_ORDER:
                samples_by_h = {
                    int(hz): float((meta or {}).get("price"))
                    for hz, meta in (raw_samples.get(asset) or {}).items()
                    if (meta or {}).get("price") is not None
                }
                cls_label, metrics = self._classify_asset(asset, samples_by_h, age_sec)
                per_asset[asset] = {
                    "classification": cls_label,
                    "classification_human": self._label_to_human(cls_label),
                    "samples": samples_by_h,
                    **metrics,
                }
            headline_label, headline_summary = self._classify_headline(per_asset, age_sec)
            risk_stars = macro_news.score_to_stars(int(h.score))
            entry = {
                "headline_id": h.headline_id,
                "title": h.title,
                "link": h.link,
                "source": h.source,
                "published_utc": h.published_utc,
                "score": int(h.score),
                "risk_stars": risk_stars,
                "themes": list(h.themes or []),
                "impact_hint": h.impact_hint,
                "age_sec": age_sec,
                "classification": headline_label,
                "classification_human": self._label_to_human(headline_label),
                "reaction_summary": headline_summary,
                "assets": per_asset,
            }
            entries.append(entry)
            for t in (h.themes or []):
                rec = theme_rollup.setdefault(t, {"count": 0, "confirmed": 0, "priced_in": 0, "no_clear": 0, "faded": 0})
                rec["count"] += 1
                if headline_label in {"impact_confirmed", "impact_developing"}:
                    rec["confirmed"] += 1
                elif headline_label == "priced_in":
                    rec["priced_in"] += 1
                elif headline_label == "no_clear_impact":
                    rec["no_clear"] += 1
                elif headline_label == "faded":
                    rec["faded"] += 1

        theme_stats = []
        for theme, rec in sorted(theme_rollup.items(), key=lambda kv: kv[1]["count"], reverse=True):
            count = max(1, int(rec["count"]))
            theme_stats.append(
                {
                    "theme": theme,
                    "count": count,
                    "confirmed_rate": round((int(rec["confirmed"]) / count) * 100.0, 1),
                    "priced_in_rate": round((int(rec["priced_in"]) / count) * 100.0, 1),
                    "no_clear_rate": round((int(rec["no_clear"]) / count) * 100.0, 1),
                    "faded_rate": round((int(rec["faded"]) / count) * 100.0, 1),
                }
            )

        return {
            "ok": True,
            "hours": lookback_h,
            "min_score": score_min,
            "min_risk_stars": min_risk_stars or macro_news.score_to_stars(score_min),
            "generated_at_utc": now,
            "entries": entries,
            "theme_stats": theme_stats,
            "adaptive_weights": self.load_theme_weights(),
        }


macro_impact_tracker = MacroImpactTracker()
