"""
market/macro_news.py
Macro headline watcher with risk scoring and impact hints.
"""
from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Optional
from xml.etree import ElementTree as ET

import requests

from config import config

logger = logging.getLogger(__name__)


@dataclass
class MacroHeadline:
    headline_id: str
    title: str
    link: str
    source: str
    published_utc: datetime
    score: int
    themes: list[str]
    impact_hint: str
    source_quality: float = 0.5
    source_tier: str = "standard"
    verification: str = "unverified"
    source_key: str = ""


class MacroNewsMonitor:
    """
    Pull macro-sensitive headlines from RSS and score risk relevance.
    Designed as lightweight and provider-free (Google News RSS).
    """

    THEME_KEYWORDS: dict[str, dict] = {
        "trump_policy": {
            "keywords": ("trump", "white house", "executive order", "campaign policy"),
            "score": 4,
        },
        "tariff_trade": {
            "keywords": ("tariff", "trade war", "import duty", "export ban", "trade tension"),
            "score": 5,
        },
        "fed_policy": {
            "keywords": ("federal reserve", "fed", "fomc", "powell", "rate cut", "rate hike", "hawkish", "dovish"),
            "score": 4,
        },
        "inflation": {
            "keywords": ("inflation", "cpi", "ppi", "core pce"),
            "score": 3,
        },
        "labor_growth": {
            "keywords": ("nfp", "nonfarm payroll", "unemployment", "jobless claims", "gdp"),
            "score": 3,
        },
        "geopolitics": {
            "keywords": ("sanction", "war", "missile", "conflict", "middle east", "strait", "nato", "taiwan strait", "red sea"),
            "score": 5,
        },
        "oil_energy_shock": {
            "keywords": ("oil", "crude", "brent", "wti", "opec", "pipeline attack", "energy shock"),
            "score": 5,
        },
        "crypto_regulation": {
            "keywords": ("crypto regulation", "sec", "etf approval", "exchange ban", "stablecoin law"),
            "score": 3,
        },
    }
    PRIORITY_THEMES: set[str] = {"tariff_trade", "geopolitics", "oil_energy_shock", "trump_policy", "fed_policy"}
    STAR_SCORE_THRESHOLDS: tuple[tuple[str, int], ...] = (
        ("***", 8),
        ("**", 5),
        ("*", 1),
    )
    SOURCE_QUALITY_DEFAULTS: dict[str, float] = {
        "REUTERS": 0.96,
        "BLOOMBERG": 0.95,
        "WSJ": 0.93,
        "CNBC": 0.84,
        "FXSTREET": 0.83,
        "FOREXLIVE": 0.79,
        "INVESTINGCOM": 0.72,
        "MARKETWATCH": 0.74,
        "YAHOOFINANCE": 0.66,
    }
    RUMOR_KEYWORDS: tuple[str, ...] = (
        "rumor",
        "rumour",
        "unconfirmed",
        "according to sources",
        "sources say",
        "reportedly",
        "alleged",
        "could",
        "might",
        "may ",
        "speculation",
        "social media post",
        "unverified",
    )
    CONFIRMED_KEYWORDS: tuple[str, ...] = (
        "confirmed",
        "official statement",
        "officially",
        "announced",
        "announcement",
        "press release",
        "ministry said",
        "central bank said",
        "white house said",
        "reuters",
        "bloomberg",
    )

    def __init__(self):
        self.feed_url = str(getattr(config, "MACRO_NEWS_FEED_URL", "") or "").strip()
        self.cache_ttl_sec = max(60, int(getattr(config, "MACRO_NEWS_CACHE_TTL_SEC", 300)))
        self._cache_items: list[MacroHeadline] = []
        self._cache_ts: float = 0.0
        self._dynamic_theme_weight_mult: dict[str, float] = {}
        self._dynamic_theme_meta: dict[str, dict] = {}
        self._source_quality_overrides = self._load_source_quality_overrides()

    @staticmethod
    def _parse_float_map(raw: str) -> dict[str, float]:
        out: dict[str, float] = {}
        for chunk in str(raw or "").split(","):
            item = chunk.strip()
            if (not item) or ("=" not in item):
                continue
            left, right = item.split("=", 1)
            key = str(left or "").strip()
            if not key:
                continue
            try:
                out[key] = float(right.strip())
            except Exception:
                continue
        return out

    @classmethod
    def _normalize_source_key(cls, source: str) -> str:
        raw = "".join(ch for ch in str(source or "").upper() if ch.isalnum())
        aliases = {
            "REUTERSCOM": "REUTERS",
            "BLOOMBERGCOM": "BLOOMBERG",
            "FXSTREETCOM": "FXSTREET",
            "FOREXLIVECOM": "FOREXLIVE",
            "INVESTINGCOM": "INVESTINGCOM",
            "YAHOOFINANCE": "YAHOOFINANCE",
            "WALLSTREETJOURNAL": "WSJ",
        }
        return aliases.get(raw, raw)

    def _load_source_quality_overrides(self) -> dict[str, float]:
        raw_map: dict[str, float] = {}
        try:
            getter = getattr(config, "get_macro_news_source_quality_overrides", None)
            if callable(getter):
                raw_map = dict(getter() or {})
            else:
                raw_map = self._parse_float_map(str(getattr(config, "MACRO_NEWS_SOURCE_QUALITY_OVERRIDES", "") or ""))
        except Exception:
            raw_map = {}
        out: dict[str, float] = {}
        for k, v in raw_map.items():
            key = self._normalize_source_key(str(k or ""))
            if not key:
                continue
            try:
                out[key] = max(0.0, min(1.0, float(v)))
            except Exception:
                continue
        return out

    def _source_quality(self, source: str) -> tuple[float, str, str]:
        key = self._normalize_source_key(source)
        quality = float(self._source_quality_overrides.get(key, self.SOURCE_QUALITY_DEFAULTS.get(key, 0.55)) or 0.55)
        quality = max(0.0, min(1.0, quality))
        trusted_min = float(getattr(config, "MACRO_NEWS_TRUSTED_MIN_QUALITY", 0.80) or 0.80)
        if quality >= trusted_min:
            tier = "trusted"
        elif quality >= 0.65:
            tier = "standard"
        else:
            tier = "low"
        return quality, tier, key

    def _verification_state(self, text: str) -> str:
        body = str(text or "").lower()
        rumor_hit = any(k in body for k in self.RUMOR_KEYWORDS)
        confirmed_hit = any(k in body for k in self.CONFIRMED_KEYWORDS)
        if rumor_hit and confirmed_hit:
            return "mixed"
        if confirmed_hit:
            return "confirmed"
        if rumor_hit:
            return "rumor"
        return "unverified"

    @staticmethod
    def _safe_text(node: ET.Element, tag: str) -> str:
        return str(node.findtext(tag, default="") or "").strip()

    @staticmethod
    def _parse_pubdate(value: str) -> datetime:
        raw = str(value or "").strip()
        if not raw:
            return datetime.now(timezone.utc)
        try:
            dt = parsedate_to_datetime(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return datetime.now(timezone.utc)

    def _score_themes(self, title: str) -> tuple[int, list[str]]:
        text = str(title or "").lower()
        score = 0.0
        themes: list[str] = []
        for theme, meta in self.THEME_KEYWORDS.items():
            if any(k in text for k in meta.get("keywords", ())):
                themes.append(theme)
                base = float(meta.get("score", 0))
                mult = float(self._dynamic_theme_weight_mult.get(theme, 1.0) or 1.0)
                # Adaptive multipliers are bounded upstream; keep local safety guard.
                mult = max(0.5, min(2.0, mult))
                score += base * mult
        return int(round(score)), themes

    def set_dynamic_theme_weights(self, weights: Optional[dict[str, float]], meta: Optional[dict[str, dict]] = None) -> None:
        cleaned: dict[str, float] = {}
        for k, v in dict(weights or {}).items():
            theme = str(k or "").strip()
            if theme not in self.THEME_KEYWORDS:
                continue
            try:
                mult = float(v)
            except Exception:
                continue
            cleaned[theme] = max(0.5, min(2.0, mult))
        self._dynamic_theme_weight_mult = cleaned
        self._dynamic_theme_meta = dict(meta or {})
        # Invalidate cache so rescoring uses updated weights.
        self._cache_ts = 0.0

    def dynamic_theme_weights_snapshot(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for theme, meta in self.THEME_KEYWORDS.items():
            base = float(meta.get("score", 0))
            mult = float(self._dynamic_theme_weight_mult.get(theme, 1.0) or 1.0)
            extra = dict(self._dynamic_theme_meta.get(theme, {}) or {})
            out[theme] = {
                "base_score": base,
                "weight_mult": round(mult, 4),
                "effective_score": round(base * mult, 3),
                **extra,
            }
        return out

    @classmethod
    def score_to_stars(cls, score: int) -> str:
        try:
            val = int(score or 0)
        except Exception:
            val = 0
        for stars, min_score in cls.STAR_SCORE_THRESHOLDS:
            if val >= int(min_score):
                return stars
        return "-"

    @classmethod
    def stars_to_min_score(cls, level: str | None) -> Optional[int]:
        raw = str(level or "").strip().lower()
        if not raw:
            return None
        aliases = {
            "*": "*",
            "1": "*",
            "low": "*",
            "l": "*",
            "**": "**",
            "2": "**",
            "medium": "**",
            "med": "**",
            "m": "**",
            "***": "***",
            "3": "***",
            "high": "***",
            "h": "***",
        }
        norm = aliases.get(raw)
        if not norm:
            return None
        for stars, min_score in cls.STAR_SCORE_THRESHOLDS:
            if stars == norm:
                return int(min_score)
        return None

    @classmethod
    def normalize_star_level(cls, level: str | None) -> Optional[str]:
        min_score = cls.stars_to_min_score(level)
        if min_score is None:
            return None
        return cls.score_to_stars(min_score)

    @staticmethod
    def _impact_hint(themes: list[str], title: str) -> str:
        t = set(themes or [])
        title_l = str(title or "").lower()
        if "oil_energy_shock" in t:
            return "Oil/energy shock risk: inflation expectations can jump; Gold and energy rise, equities may pressure."
        if "tariff_trade" in t or "trump_policy" in t:
            return "Policy/trade shock risk: Gold up bias, US stocks pressure, USD mixed, crypto volatile."
        if "geopolitics" in t:
            return "Geopolitical risk-off: Gold up bias, indices can weaken, crypto volatility rises."
        if "fed_policy" in t:
            if any(k in title_l for k in ("rate hike", "hawkish", "higher for longer")):
                return "Hawkish Fed risk: USD/yields up, Gold and growth stocks may soften."
            if any(k in title_l for k in ("rate cut", "dovish", "easing")):
                return "Dovish Fed tilt: USD/yields down, Gold and risk assets may support."
            return "Fed-policy sensitivity: expect cross-asset volatility around rates narrative."
        if "inflation" in t or "labor_growth" in t:
            return "Macro data volatility: can reprice rates, impacting Gold, DXY, US equities, BTC."
        if "crypto_regulation" in t:
            return "Crypto policy sensitivity: BTC/ETH direction can move sharply with regulation headlines."
        return "Macro-sensitive headline: monitor cross-asset reaction closely."

    def _parse_item(self, node: ET.Element) -> Optional[MacroHeadline]:
        title = self._safe_text(node, "title")
        if not title:
            return None
        link = self._safe_text(node, "link")
        source = self._safe_text(node, "source") or "news"
        description = self._safe_text(node, "description")
        published = self._parse_pubdate(self._safe_text(node, "pubDate"))
        scoring_text = f"{title} {description}".strip()
        base_score, themes = self._score_themes(scoring_text)
        if base_score <= 0:
            return None
        source_quality, source_tier, source_key = self._source_quality(source)
        verification = self._verification_state(f"{source} {scoring_text}")
        score_adj = float(base_score) * (0.80 + (0.40 * float(source_quality)))
        rumor_penalty = float(getattr(config, "MACRO_NEWS_RUMOR_SCORE_PENALTY", 2.0) or 2.0)
        unverified_penalty = float(getattr(config, "MACRO_NEWS_UNVERIFIED_SCORE_PENALTY", 1.0) or 1.0)
        confirmed_bonus = float(getattr(config, "MACRO_NEWS_CONFIRMED_SCORE_BONUS", 0.8) or 0.8)
        if verification == "rumor":
            score_adj -= rumor_penalty
        elif verification == "mixed":
            score_adj -= max(0.5, rumor_penalty * 0.5)
        elif verification == "confirmed":
            score_adj += confirmed_bonus
        else:
            score_adj -= unverified_penalty
        score = max(0, int(round(score_adj)))
        if score <= 0:
            return None
        impact_hint = self._impact_hint(themes, title)
        if verification == "rumor":
            impact_hint = f"{impact_hint} Treat as rumor until confirmed by trusted source."
        elif verification == "confirmed":
            impact_hint = f"{impact_hint} Confirmed headline from a trusted/public source."
        key = f"{title}|{link}|{published.isoformat()}"
        hid = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
        return MacroHeadline(
            headline_id=hid,
            title=title,
            link=link,
            source=source,
            published_utc=published,
            score=score,
            themes=themes,
            impact_hint=impact_hint,
            source_quality=round(float(source_quality), 3),
            source_tier=source_tier,
            verification=verification,
            source_key=source_key,
        )

    def _download(self) -> str:
        if not self.feed_url:
            return ""
        resp = requests.get(
            self.feed_url,
            timeout=20,
            headers={"User-Agent": "DexterPro/1.0"},
        )
        resp.raise_for_status()
        return resp.text or ""

    def fetch_headlines(self, force: bool = False) -> list[MacroHeadline]:
        now_ts = time.time()
        if (not force) and self._cache_items and (now_ts - self._cache_ts <= self.cache_ttl_sec):
            return list(self._cache_items)
        try:
            xml_text = self._download()
            if not xml_text:
                return list(self._cache_items)
            root = ET.fromstring(xml_text)
            items: list[MacroHeadline] = []
            for node in root.findall(".//item"):
                headline = self._parse_item(node)
                if headline is not None:
                    items.append(headline)
            items.sort(key=lambda x: x.published_utc, reverse=True)
            self._cache_items = items
            self._cache_ts = now_ts
            return list(items)
        except Exception as e:
            logger.warning("[MacroNews] feed fetch failed: %s", e)
            return list(self._cache_items)

    def high_impact_headlines(self, hours: int = 24, min_score: int = 5, limit: int = 8) -> list[MacroHeadline]:
        lookback = datetime.now(timezone.utc) - timedelta(hours=max(1, int(hours)))
        out = [
            h for h in self.fetch_headlines()
            if h.published_utc >= lookback and int(h.score) >= int(min_score)
        ]
        out.sort(
            key=lambda x: (
                int(getattr(x, "score", 0) or 0),
                float(getattr(x, "source_quality", 0.5) or 0.5),
                getattr(x, "published_utc", lookback),
            ),
            reverse=True,
        )
        return out[: max(1, int(limit))]

    def is_trusted_source(self, headline: MacroHeadline, min_quality: float | None = None) -> bool:
        if headline is None:
            return False
        threshold = float(
            min_quality
            if min_quality is not None
            else getattr(config, "MACRO_NEWS_TRUSTED_MIN_QUALITY", 0.80)
        )
        q = float(getattr(headline, "source_quality", 0.0) or 0.0)
        return q >= max(0.0, min(1.0, threshold))

    @staticmethod
    def is_rumor_headline(headline: MacroHeadline) -> bool:
        state = str(getattr(headline, "verification", "") or "").strip().lower()
        return state in {"rumor", "mixed"}

    @classmethod
    def is_priority_theme(cls, headline: MacroHeadline) -> bool:
        themes = set(getattr(headline, "themes", []) or [])
        return bool(themes.intersection(cls.PRIORITY_THEMES))


macro_news = MacroNewsMonitor()
