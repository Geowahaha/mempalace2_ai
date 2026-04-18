"""
market/economic_calendar.py
Economic calendar feed client with upcoming-event filters for alerting.
"""
from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

import requests

from config import config

logger = logging.getLogger(__name__)


IMPACT_RANK = {
    "low": 1,
    "medium": 2,
    "high": 3,
}


@dataclass
class EconomicEvent:
    event_id: str
    title: str
    currency: str
    impact: str
    forecast: str
    previous: str
    actual: str
    source_url: str
    time_utc: datetime

    @property
    def minutes_to_event(self) -> int:
        delta = self.time_utc - datetime.now(timezone.utc)
        return int(delta.total_seconds() // 60)


def _impact_rank(impact: str) -> int:
    return IMPACT_RANK.get(str(impact or "").strip().lower(), 0)


class EconomicCalendar:
    """
    Pulls calendar events from FF XML feed and provides upcoming-event filters.
    FF timestamps are interpreted in America/New_York timezone.
    """

    def __init__(self):
        self.feed_url = str(getattr(config, "ECON_CALENDAR_FEED_URL", "") or "").strip()
        self.cache_ttl_sec = max(30, int(getattr(config, "ECON_CALENDAR_CACHE_TTL_SEC", 300)))
        self._cache_events: list[EconomicEvent] = []
        self._cache_ts: float = 0.0
        self._source_tz = ZoneInfo("America/New_York")

    @staticmethod
    def _node_text(node: ET.Element, tag: str) -> str:
        value = node.findtext(tag, default="") or ""
        return value.strip()

    def _parse_datetime_utc(self, date_text: str, time_text: str) -> Optional[datetime]:
        date_clean = str(date_text or "").strip()
        time_clean = str(time_text or "").strip().lower()
        if not date_clean or not time_clean:
            return None
        if any(x in time_clean for x in ("all day", "tentative", "day ", "holiday")):
            return None

        compact = time_clean.replace(" ", "")
        patterns = ("%m-%d-%Y %I:%M%p", "%m-%d-%Y %I%p")
        for pattern in patterns:
            try:
                parsed_local = datetime.strptime(f"{date_clean} {compact}", pattern)
                local_dt = parsed_local.replace(tzinfo=self._source_tz)
                return local_dt.astimezone(timezone.utc)
            except Exception:
                continue
        return None

    def _parse_event(self, node: ET.Element) -> Optional[EconomicEvent]:
        title = self._node_text(node, "title")
        currency = self._node_text(node, "country").upper()
        date_text = self._node_text(node, "date")
        time_text = self._node_text(node, "time")
        impact = self._node_text(node, "impact").lower()
        forecast = self._node_text(node, "forecast")
        previous = self._node_text(node, "previous")
        actual = self._node_text(node, "actual")
        source_url = self._node_text(node, "url")
        event_time = self._parse_datetime_utc(date_text, time_text)
        if event_time is None or not title or not currency:
            return None

        key_raw = f"{currency}|{title}|{event_time.isoformat()}|{impact}"
        event_id = hashlib.sha1(key_raw.encode("utf-8")).hexdigest()[:16]
        return EconomicEvent(
            event_id=event_id,
            title=title,
            currency=currency,
            impact=impact,
            forecast=forecast,
            previous=previous,
            actual=actual,
            source_url=source_url,
            time_utc=event_time,
        )

    def _download_feed(self) -> str:
        if not self.feed_url:
            return ""
        resp = requests.get(
            self.feed_url,
            timeout=8,
            headers={"User-Agent": "DexterPro/1.0"},
        )
        resp.raise_for_status()
        resp.encoding = resp.encoding or "windows-1252"
        return resp.text or ""

    def fetch_events(self, force: bool = False) -> list[EconomicEvent]:
        now_ts = time.time()
        if (not force) and self._cache_events and (now_ts - self._cache_ts <= self.cache_ttl_sec):
            return list(self._cache_events)
        try:
            xml_text = self._download_feed()
            if not xml_text:
                return list(self._cache_events)

            root = ET.fromstring(xml_text)
            parsed: list[EconomicEvent] = []
            for ev in root.findall(".//event"):
                item = self._parse_event(ev)
                if item is not None:
                    parsed.append(item)
            parsed.sort(key=lambda e: e.time_utc)
            self._cache_events = parsed
            self._cache_ts = now_ts
            return list(parsed)
        except Exception as e:
            logger.warning("[EcoCalendar] feed fetch failed: %s", e)
            return list(self._cache_events)

    def upcoming_events(
        self,
        within_minutes: int = 180,
        min_impact: str = "high",
        currencies: Optional[set[str]] = None,
    ) -> list[EconomicEvent]:
        within = max(1, int(within_minutes))
        threshold = _impact_rank(min_impact)
        ccy = {str(x).strip().upper() for x in (currencies or set()) if str(x).strip()}
        now = datetime.now(timezone.utc)
        end = now + timedelta(minutes=within)

        out: list[EconomicEvent] = []
        for ev in self.fetch_events():
            if ev.time_utc < now or ev.time_utc > end:
                continue
            if _impact_rank(ev.impact) < threshold:
                continue
            if ccy and ev.currency not in ccy:
                continue
            out.append(ev)
        out.sort(key=lambda e: e.time_utc)
        return out

    def next_events(
        self,
        hours: int = 24,
        limit: int = 8,
        min_impact: str = "medium",
        currencies: Optional[set[str]] = None,
    ) -> list[EconomicEvent]:
        lookahead_min = max(30, int(hours) * 60)
        items = self.upcoming_events(
            within_minutes=lookahead_min,
            min_impact=min_impact,
            currencies=currencies,
        )
        return items[: max(1, int(limit))]


economic_calendar = EconomicCalendar()

