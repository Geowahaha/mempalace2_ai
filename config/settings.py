"""
Configuration — Settings, exchange configs, risk parameters.
"""

from __future__ import annotations

import os
import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ExchangeConfig:
    """Exchange connection settings."""
    primary: str = "binance"
    sandbox: bool = True
    api_key: str = ""
    api_secret: str = ""
    testnet: bool = True
    rate_limit: int = 10  # requests per second


@dataclass
class RiskConfig:
    """Risk management parameters."""
    initial_capital: float = 10000.0
    max_position_pct: float = 5.0        # max % per position
    max_portfolio_risk_pct: float = 6.0   # max total portfolio risk
    max_correlated_positions: int = 3     # max positions in correlated assets
    min_risk_reward: float = 2.0          # minimum R:R ratio
    max_daily_loss_pct: float = 3.0       # circuit breaker
    max_drawdown_pct: float = 15.0        # max drawdown before halt
    kelly_fraction: float = 0.25          # fractional Kelly (conservative)
    atr_multiplier_sl: float = 1.5        # ATR multiplier for stop loss
    atr_multiplier_tp: float = 3.0        # ATR multiplier for TP1
    trailing_stop_atr: float = 2.0        # trailing stop in ATR units
    max_open_trades: int = 5


@dataclass
class AnalysisConfig:
    """Technical analysis parameters."""
    timeframes: List[str] = field(default_factory=lambda: ["15m", "1h", "4h", "1d"])
    primary_timeframe: str = "1h"
    ema_fast: int = 9
    ema_slow: int = 21
    ema_trend: int = 200
    rsi_period: int = 14
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    bb_period: int = 20
    bb_std: float = 2.0
    atr_period: int = 14
    volume_ma_period: int = 20
    lookback_candles: int = 200


@dataclass
class AgentConfig:
    """Agent behavior settings."""
    scan_interval_seconds: int = 60
    max_concurrent_analyses: int = 5
    signal_confidence_threshold: float = 65.0
    require_multi_timeframe: bool = True
    enable_momentum: bool = True
    enable_mean_reversion: bool = True
    enable_breakout: bool = True


@dataclass
class AppConfig:
    """Root application configuration."""
    exchanges: ExchangeConfig = field(default_factory=ExchangeConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    agents: AgentConfig = field(default_factory=AgentConfig)
    symbols: List[str] = field(default_factory=lambda: [
        "XAUUSD", "XAGUSD", "EURUSD", "GBPUSD", "USDJPY"
    ])
    log_level: str = "INFO"


def load_config(path: Optional[str] = None) -> AppConfig:
    """Load configuration from YAML file or defaults."""
    config = AppConfig()

    if path and os.path.exists(path):
        with open(path) as f:
            data = yaml.safe_load(f) or {}

        if "exchanges" in data:
            config.exchanges = ExchangeConfig(**data["exchanges"])
        if "risk" in data:
            config.risk = RiskConfig(**data["risk"])
        if "analysis" in data:
            config.analysis = AnalysisConfig(**data["analysis"])
        if "agents" in data:
            config.agents = AgentConfig(**data["agents"])
        if "symbols" in data:
            config.symbols = data["symbols"]
        if "log_level" in data:
            config.log_level = data["log_level"]

    # Override from environment
    config.exchanges.api_key = os.getenv("EXCHANGE_API_KEY", config.exchanges.api_key)
    config.exchanges.api_secret = os.getenv("EXCHANGE_API_SECRET", config.exchanges.api_secret)

    return config
