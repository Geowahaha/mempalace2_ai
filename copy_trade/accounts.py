"""
copy_trade/accounts.py — Follower account registry.

Stores follower account configuration in a JSON file.
Each account has: broker type, credentials, risk scaling, and enable flag.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_REGISTRY = _ROOT / "data" / "copy_trade_accounts.json"


@dataclass
class FollowerAccount:
    """Single follower account configuration."""

    account_id: str
    label: str
    broker: str  # "ctrader" or "mt5"
    enabled: bool = True

    # cTrader fields
    ctrader_account_id: Optional[int] = None
    ctrader_access_token: Optional[str] = None

    # MT5 fields
    mt5_host: Optional[str] = None
    mt5_port: Optional[int] = None
    mt5_login: Optional[int] = None
    mt5_password: Optional[str] = None
    mt5_server: Optional[str] = None
    mt5_magic: Optional[int] = None

    # Risk / filtering
    risk_multiplier: float = 1.0
    max_risk_usd: float = 50.0
    allowed_symbols: list[str] = field(default_factory=list)
    blocked_symbols: list[str] = field(default_factory=list)
    allowed_sources: list[str] = field(default_factory=list)

    # Metadata
    created_at: str = ""
    last_trade_at: str = ""
    total_trades: int = 0
    paused_until: str = ""

    def is_symbol_allowed(self, symbol: str) -> bool:
        sym = symbol.strip().upper()
        if self.blocked_symbols and sym in {s.strip().upper() for s in self.blocked_symbols}:
            return False
        if self.allowed_symbols:
            return sym in {s.strip().upper() for s in self.allowed_symbols}
        return True

    def is_source_allowed(self, source: str) -> bool:
        if not self.allowed_sources:
            return True
        src = source.strip().lower()
        return src in {s.strip().lower() for s in self.allowed_sources}

    def scale_risk(self, master_risk_usd: float) -> float:
        scaled = master_risk_usd * self.risk_multiplier
        return min(scaled, self.max_risk_usd)


class AccountRegistry:
    """Thread-safe follower account store backed by JSON file."""

    def __init__(self, path: Optional[Path] = None):
        self._path = path or _DEFAULT_REGISTRY
        self._lock = threading.Lock()
        self._accounts: dict[str, FollowerAccount] = {}
        self._loaded_at: float = 0
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            self._accounts = {}
            return
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            accounts = {}
            for entry in data.get("accounts", []):
                try:
                    acc = FollowerAccount(**{
                        k: v for k, v in entry.items()
                        if k in FollowerAccount.__dataclass_fields__
                    })
                    accounts[acc.account_id] = acc
                except Exception as e:
                    logger.warning("skip bad account entry: %s", e)
            self._accounts = accounts
            self._loaded_at = time.time()
            logger.info("[CopyTrade] loaded %d follower accounts from %s", len(accounts), self._path)
        except Exception as e:
            logger.error("[CopyTrade] failed to load accounts: %s", e)
            self._accounts = {}

    def _save(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "version": 1,
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "accounts": [asdict(acc) for acc in self._accounts.values()],
            }
            tmp = self._path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            tmp.replace(self._path)
        except Exception as e:
            logger.error("[CopyTrade] failed to save accounts: %s", e)

    def reload(self) -> int:
        with self._lock:
            self._load()
            return len(self._accounts)

    def list_accounts(self, *, broker: str = "") -> list[FollowerAccount]:
        with self._lock:
            accs = list(self._accounts.values())
        if broker:
            accs = [a for a in accs if a.broker == broker]
        return accs

    def get(self, account_id: str) -> Optional[FollowerAccount]:
        with self._lock:
            return self._accounts.get(account_id)

    def add_ctrader(
        self,
        label: str,
        ctrader_account_id: int,
        *,
        access_token: str = "",
        risk_multiplier: float = 1.0,
        max_risk_usd: float = 50.0,
    ) -> FollowerAccount:
        acc = FollowerAccount(
            account_id=f"ct_{ctrader_account_id}",
            label=label,
            broker="ctrader",
            ctrader_account_id=ctrader_account_id,
            ctrader_access_token=access_token or None,
            risk_multiplier=risk_multiplier,
            max_risk_usd=max_risk_usd,
            created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        )
        with self._lock:
            self._accounts[acc.account_id] = acc
            self._save()
        logger.info("[CopyTrade] added cTrader follower: %s (%s)", label, ctrader_account_id)
        return acc

    def add_mt5(
        self,
        label: str,
        *,
        mt5_login: int,
        mt5_server: str = "",
        mt5_password: str = "",
        mt5_host: str = "",
        mt5_port: int = 0,
        mt5_magic: int = 0,
        risk_multiplier: float = 1.0,
        max_risk_usd: float = 50.0,
    ) -> FollowerAccount:
        acc = FollowerAccount(
            account_id=f"mt5_{mt5_login}",
            label=label,
            broker="mt5",
            mt5_login=mt5_login,
            mt5_server=mt5_server or None,
            mt5_password=mt5_password or None,
            mt5_host=mt5_host or None,
            mt5_port=mt5_port or None,
            mt5_magic=mt5_magic or None,
            risk_multiplier=risk_multiplier,
            max_risk_usd=max_risk_usd,
            created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        )
        with self._lock:
            self._accounts[acc.account_id] = acc
            self._save()
        logger.info("[CopyTrade] added MT5 follower: %s (login=%d)", label, mt5_login)
        return acc

    def remove(self, account_id: str) -> bool:
        with self._lock:
            if account_id in self._accounts:
                del self._accounts[account_id]
                self._save()
                return True
        return False

    def set_enabled(self, account_id: str, enabled: bool) -> bool:
        with self._lock:
            acc = self._accounts.get(account_id)
            if acc:
                acc.enabled = enabled
                self._save()
                return True
        return False

    def update_trade_stats(self, account_id: str) -> None:
        with self._lock:
            acc = self._accounts.get(account_id)
            if acc:
                acc.total_trades += 1
                acc.last_trade_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                self._save()


account_registry = AccountRegistry()
