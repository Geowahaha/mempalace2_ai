"""
XAUUSD Paper Trading Simulation — Full hermes-agent loop demo.

Runs the complete pipeline with synthetic but realistic XAUUSD data:
  Boot → Scan → Analyze → Risk → Execute → Close → Memory update → Repeat

Demonstrates the self-improving loop:
  - Trades stored as patterns
  - Skills updated with win rates
  - Trajectory logging for every decision
  - Context engine compression
  - Periodic scheduler reports

Usage:
  python3 sim_trade.py                    # 5 cycles, default
  python3 sim_trade.py --cycles 20        # more cycles
  python3 sim_trade.py --log-level DEBUG  # verbose
"""

import asyncio
import json
import logging
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass

# Ensure imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd

from config.settings import AppConfig, load_config
from core.state import GlobalState, TradeSignal, ActiveTrade
from agents.coordinator import CoordinatorAgent
from agents.analyst import AnalystAgent
from agents.executor import ExecutorAgent
from agents.risk_manager import RiskManagerAgent
from enhanced.boot import EnhancedBootPipeline
from tools.base import ToolResult


# ── Synthetic XAUUSD Data Generator ─────────────────────────────────────────

class XAUUSDSimulator:
    """
    Generates realistic XAUUSD price action with embedded setups.

    Models:
      - Geometric Brownian Motion for price
      - Volatility clustering (GARCH-like)
      - Embedded setups (EMA crossovers, RSI extremes, trend alignments)
      - Realistic spread and noise
    """

    def __init__(self, start_price: float = 3245.0, seed: int = 42):
        self.price = start_price
        self.rng = np.random.RandomState(seed)
        self.tick_count = 0

    def generate_candles(self, n: int = 200, timeframe: str = "1h") -> pd.DataFrame:
        """Generate n OHLCV candles with embedded setups."""
        # Parameters for XAUUSD
        dt = 1/252/24 if timeframe == "1h" else 1/252  # hourly vs daily
        mu = 0.0001       # slight upward drift
        sigma_base = 0.008  # ~0.8% hourly vol

        prices = [self.price]
        volatility = sigma_base

        for i in range(n - 1):
            # Volatility clustering
            vol_shock = abs(self.rng.normal(0, 0.002))
            volatility = 0.9 * volatility + 0.1 * (sigma_base + vol_shock)
            volatility = np.clip(volatility, 0.003, 0.025)

            # GBM price step
            z = self.rng.normal(0, 1)
            drift = mu * dt
            diffusion = volatility * z * np.sqrt(dt)

            # Embed setups at specific points
            setup_boost = self._setup_boost(i, n)

            new_price = prices[-1] * (1 + drift + diffusion + setup_boost)
            new_price = max(new_price, 2800)  # floor
            new_price = min(new_price, 3500)  # ceiling
            prices.append(new_price)

        self.price = prices[-1]

        # Build OHLCV
        close = np.array(prices)
        noise = np.abs(self.rng.randn(n)) * close * 0.001
        high = close + noise
        low = close - noise
        open_p = np.roll(close, 1)
        open_p[0] = close[0] * (1 + self.rng.normal(0, 0.001))
        volume = self.rng.randint(5000, 50000, n).astype(float)

        df = pd.DataFrame({
            "open": open_p,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        })

        # Add technical indicators
        df = self._add_indicators(df)

        return df

    def _setup_boost(self, i: int, n: int) -> float:
        """Embed price movements that create detectable setups."""
        # Create an EMA crossover around candle 150-160
        if 148 <= i <= 155:
            return 0.002  # bullish push
        # Create oversold bounce around 100-110
        if 98 <= i <= 105:
            return -0.003  # drop then bounce
        # Create trend alignment push at end
        if i >= n - 15:
            return self.rng.choice([-0.001, 0.002])
        return 0.0

    def _add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add technical indicators to OHLCV data."""
        close = df["close"]

        # EMAs
        df["ema_fast"] = close.ewm(span=9, adjust=False).mean()
        df["ema_slow"] = close.ewm(span=21, adjust=False).mean()
        df["ema_trend"] = close.ewm(span=200, adjust=False).mean()

        # RSI
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss.replace(0, np.nan)
        df["rsi"] = 100 - (100 / (1 + rs))
        df["rsi"] = df["rsi"].fillna(50)

        # MACD
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        df["macd"] = ema12 - ema26
        df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
        df["macd_histogram"] = df["macd"] - df["macd_signal"]

        # Supertrend (simplified)
        atr = self._calc_atr(df, 14)
        df["atr"] = atr
        hl2 = (df["high"] + df["low"]) / 2
        upper = hl2 + 2 * atr
        lower = hl2 - 2 * atr
        st_dir = [1]
        for i in range(1, len(df)):
            if close.iloc[i] > upper.iloc[i - 1]:
                st_dir.append(1)
            elif close.iloc[i] < lower.iloc[i - 1]:
                st_dir.append(-1)
            else:
                st_dir.append(st_dir[-1])
        df["supertrend_direction"] = st_dir

        # Bollinger Bands
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        df["bb_upper"] = sma20 + 2 * std20
        df["bb_middle"] = sma20
        df["bb_lower"] = sma20 - 2 * std20
        df["bb_width"] = ((df["bb_upper"] - df["bb_lower"]) / df["bb_middle"] * 100).fillna(2)

        # ADX (simplified)
        df["adx"] = 20 + np.abs(np.random.randn(len(df)) * 10)

        # Volume
        df["volume_ratio"] = df["volume"] / df["volume"].rolling(20).mean()
        df["volume_ratio"] = df["volume_ratio"].fillna(1)

        # Support/Resistance (swing points)
        df["support_1"] = df["low"].rolling(20).min()
        df["support_2"] = df["low"].rolling(50).min()
        df["resistance_1"] = df["high"].rolling(20).max()
        df["resistance_2"] = df["high"].rolling(50).max()
        df["trend_score"] = 50 + np.random.randn(len(df)) * 15

        return df.dropna()

    def _calc_atr(self, df: pd.DataFrame, period: int) -> pd.Series:
        """Calculate Average True Range."""
        high = df["high"]
        low = df["low"]
        close = df["close"]
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(period).mean().fillna(df["atr"].mean() if "atr" in df.columns else 5.0)


# ── Simulation Runner ────────────────────────────────────────────────────────

async def run_simulation(cycles: int = 5, log_level: str = "INFO"):
    """Run the full trading simulation."""

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s │ %(name)-28s │ %(levelname)-5s │ %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("simulation")

    # ── 1. Boot enhanced system ──
    logger.info("=" * 70)
    logger.info("  🏛️  MEMPALACE2 AI — XAUUSD PAPER TRADING SIMULATION")
    logger.info("=" * 70)

    pipeline = EnhancedBootPipeline(
        db_path="/tmp/mempalace2_sim.db",
        trajectory_dir="/tmp/mempalace2_sim_traj/",
    )
    state = await pipeline.boot()

    # Override config for simulation
    state.config.symbols = ["XAUUSD"]

    # ── 2. Create price simulator ──
    sim = XAUUSDSimulator(start_price=3245.0, seed=42)

    # ── 3. Initialize agents ──
    analyst = AnalystAgent(state)
    executor = ExecutorAgent(state)
    risk_mgr = RiskManagerAgent(state)

    # Initialize agents in state
    await analyst.initialize()
    await executor.initialize()
    await risk_mgr.initialize()

    logger.info("")
    logger.info(f"  Starting {cycles} trading cycles on XAUUSD...")
    logger.info(f"  Initial equity: ${state.portfolio.total_equity:,.2f}")
    logger.info(f"  Skills loaded: {len(state.skills_manager._skills)}")
    logger.info("")

    # ── 4. Run simulation cycles ──
    stats = {
        "scans": 0, "setups_detected": 0, "signals_generated": 0,
        "trades_executed": 0, "trades_won": 0, "trades_lost": 0,
        "total_pnl": 0.0,
    }

    for cycle in range(1, cycles + 1):
        logger.info(f"{'─' * 70}")
        logger.info(f"  CYCLE {cycle}/{cycles}")
        logger.info(f"{'─' * 70}")

        # Generate fresh market data
        ohlcv = sim.generate_candles(200, "1h")
        current_price = float(ohlcv["close"].iloc[-1])
        prev_close = float(ohlcv["close"].iloc[-2])
        price_change = (current_price - prev_close) / prev_close * 100

        logger.info(f"  📊 XAUUSD: ${current_price:.2f} ({price_change:+.2f}%)")
        stats["scans"] += 1

        # ── Context engine tick ──
        if hasattr(state, 'context_engine'):
            state.context_engine.tick_scan()

        # ── Detect setups (scanner logic) ──
        setups = detect_setups(state, ohlcv)
        if setups:
            stats["setups_detected"] += len(setups)
            logger.info(f"  🔍 {len(setups)} setup(s) detected:")
            for s in setups:
                logger.info(f"      • {s['type']} {s['direction']} (strength={s['strength']:.0f})")

            # Take the best setup
            best = max(setups, key=lambda s: s["strength"])

            # ── Start trajectory ──
            trajectory_id = None
            if hasattr(state, 'trajectory_logger'):
                trajectory_id = state.trajectory_logger.start_trajectory(
                    session_id=state.state_store_session_id,
                    symbol="XAUUSD",
                    direction=best["direction"],
                )
                state.trajectory_logger.add_step(trajectory_id, "scan", {
                    "symbol": "XAUUSD",
                    "setup_type": best["type"],
                    "direction": best["direction"],
                    "strength": best["strength"],
                    "price": current_price,
                })

            # ── Analyst: memory + skills ──
            memory_block = ""
            if hasattr(state, 'memory'):
                memory_block = state.memory.build_context_for_analysis(
                    symbol="XAUUSD",
                    setup_type=best["type"],
                    direction=best["direction"],
                )

            skills_block = ""
            if hasattr(state, 'skills_manager'):
                skills_block = state.skills_manager.build_skills_context_block({
                    "symbol": "XAUUSD",
                    "setup_type": best["type"],
                    "direction": best["direction"],
                    "timeframe": "1h",
                })

            if memory_block:
                logger.info(f"  🧠 Memory: {len(memory_block)} chars recalled")
            if skills_block:
                logger.info(f"  🎯 Skills: matched relevant skills")

            if trajectory_id:
                state.trajectory_logger.add_step(trajectory_id, "analysis", {
                    "has_memory": bool(memory_block),
                    "has_skills": bool(skills_block),
                    "price": current_price,
                })

            # ── Build signal ──
            indicators = _get_indicators(ohlcv)
            atr = indicators["atr"]
            direction = best["direction"]

            if direction == "long":
                sl = current_price - atr * 1.5
                tp1 = current_price + atr * 3.0
                tp2 = current_price + atr * 4.5
                tp3 = current_price + atr * 6.0
            else:
                sl = current_price + atr * 1.5
                tp1 = current_price - atr * 3.0
                tp2 = current_price - atr * 4.5
                tp3 = current_price - atr * 6.0

            signal = TradeSignal(
                symbol="XAUUSD",
                timeframe="1h",
                direction=direction,
                entry_price=current_price,
                stop_loss=round(sl, 2),
                take_profit_1=round(tp1, 2),
                take_profit_2=round(tp2, 2),
                take_profit_3=round(tp3, 2),
                confidence=min(100, best["strength"]),
                risk_reward_ratio=2.0,
                strategy=best["type"],
                atr=atr,
                reasoning=best["reasoning"],
                position_size_pct=2.0,
            )
            stats["signals_generated"] += 1

            logger.info(
                f"  📈 Signal: {signal.direction} @ ${signal.entry_price:.2f} "
                f"SL=${signal.stop_loss:.2f} TP1=${signal.take_profit_1:.2f} "
                f"(R:R={signal.risk_reward_ratio:.1f})"
            )

            # ── Risk check ──
            if trajectory_id:
                state.trajectory_logger.add_step(trajectory_id, "risk_check", {
                    "position_size_pct": signal.position_size_pct,
                    "confidence": signal.confidence,
                })

            # Simple risk check (skip full risk engine for sim)
            approved = signal.risk_reward_ratio >= 1.5 and signal.confidence >= 60

            if approved:
                logger.info(f"  ✅ Risk: APPROVED (size={signal.position_size_pct:.1f}%)")

                if trajectory_id:
                    state.trajectory_logger.add_step(trajectory_id, "risk_approved", {})

                # ── Execute trade ──
                trade = ActiveTrade(
                    signal=signal,
                    entry_filled_price=current_price,
                    entry_filled_time=datetime.now(timezone.utc),
                    quantity=state.portfolio.total_equity * 0.02 / current_price,
                    trajectory_id=trajectory_id or "",
                )
                state.portfolio.active_trades.append(trade)
                state.portfolio.open_positions += 1
                state.total_trades += 1
                stats["trades_executed"] += 1

                logger.info(
                    f"  ⚡ Executed: {signal.direction} {trade.quantity:.4f} "
                    f"@ ${current_price:.2f}"
                )

                if trajectory_id:
                    state.trajectory_logger.add_step(trajectory_id, "execution", {
                        "entry_price": current_price,
                        "quantity": trade.quantity,
                    })

                # ── Simulate trade outcome (next candle) ──
                outcome = _simulate_outcome(trade, ohlcv, sim)
                exit_price = outcome["exit_price"]
                pnl = outcome["pnl"]
                pnl_pct = outcome["pnl_pct"]
                won = outcome["won"]
                close_reason = outcome["reason"]

                emoji = "✅" if won else "❌"
                logger.info(
                    f"  {emoji} Closed: {close_reason} @ ${exit_price:.2f} "
                    f"P&L=${pnl:+.2f} ({pnl_pct:+.2f}%)"
                )

                stats["total_pnl"] += pnl
                if won:
                    stats["trades_won"] += 1
                else:
                    stats["trades_lost"] += 1

                # Update portfolio
                state.portfolio.daily_pnl += pnl
                state.portfolio.total_pnl += pnl
                state.portfolio.active_trades.remove(trade)
                state.portfolio.closed_trades.append(trade)
                state.portfolio.open_positions -= 1

                # ── Store in memory ──
                if hasattr(state, 'memory'):
                    state.memory.store_trade_pattern(
                        symbol="XAUUSD",
                        setup_type=signal.strategy,
                        direction=signal.direction,
                        conditions={
                            "timeframe": "1h",
                            "entry_price": current_price,
                            "atr": atr,
                            "confidence": signal.confidence,
                            "rsi": indicators["rsi"],
                        },
                        outcome={
                            "pnl": pnl,
                            "pnl_pct": pnl_pct,
                            "risk_reward_ratio": abs(pnl) / (abs(current_price - sl) * trade.quantity) if sl != current_price else 0,
                            "close_reason": close_reason,
                        },
                    )

                    if not won:
                        state.memory.store_lesson(
                            event_type="loss",
                            trade_id=signal.id,
                            description=f"XAUUSD {signal.direction} {signal.strategy}: {close_reason}",
                            lesson=f"Lost ${abs(pnl):.2f} ({pnl_pct:+.2f}%) — review {signal.strategy} in similar conditions",
                        )

                # ── Update skill ──
                if hasattr(state, 'skills_manager'):
                    rr_achieved = abs(pnl) / (abs(current_price - sl) * trade.quantity) if sl != current_price else 0
                    state.skills_manager.update_skill_from_trade(
                        skill_name=signal.strategy.replace("_flip_bullish", "").replace("_flip_bearish", "").replace("_oversold", "").replace("_overbought", ""),
                        won=won,
                        pnl_pct=pnl_pct,
                        risk_reward=rr_achieved,
                    )

                # ── Record in state store ──
                if hasattr(state, 'state_store'):
                    state.state_store.record_trade(
                        session_id=state.state_store_session_id,
                        trade={
                            "trade_id": signal.id,
                            "symbol": "XAUUSD",
                            "direction": signal.direction,
                            "setup_type": signal.strategy,
                            "entry_price": current_price,
                            "exit_price": exit_price,
                            "quantity": trade.quantity,
                            "pnl": pnl,
                            "pnl_pct": pnl_pct,
                            "close_reason": close_reason,
                        },
                    )

                # ── Finalize trajectory ──
                if trajectory_id and hasattr(state, 'trajectory_logger'):
                    state.trajectory_logger.finalize(trajectory_id, "closed", {
                        "pnl": pnl,
                        "pnl_pct": pnl_pct,
                        "won": won,
                        "close_reason": close_reason,
                        "exit_price": exit_price,
                    })

            else:
                logger.info(f"  ❌ Risk: REJECTED")
                if trajectory_id and hasattr(state, 'trajectory_logger'):
                    state.trajectory_logger.add_step(trajectory_id, "risk_rejected", {"reason": "below threshold"})
                    state.trajectory_logger.finalize(trajectory_id, "rejected", {"reason": "risk"})
        else:
            logger.info(f"  🔍 No setups detected — waiting...")

        # Show running stats
        logger.info(
            f"  📊 Portfolio: ${state.portfolio.total_equity + state.portfolio.total_pnl:,.2f} "
            f"(P&L: ${state.portfolio.total_pnl:+.2f})"
        )

    # ── 5. Final report ──
    logger.info("")
    logger.info("=" * 70)
    logger.info("  📊 SIMULATION COMPLETE — FINAL REPORT")
    logger.info("=" * 70)
    logger.info(f"  Cycles run:          {cycles}")
    logger.info(f"  Scans:               {stats['scans']}")
    logger.info(f"  Setups detected:     {stats['setups_detected']}")
    logger.info(f"  Signals generated:   {stats['signals_generated']}")
    logger.info(f"  Trades executed:     {stats['trades_executed']}")
    logger.info(f"  Trades won:          {stats['trades_won']}")
    logger.info(f"  Trades lost:         {stats['trades_lost']}")
    if stats['trades_executed'] > 0:
        wr = stats['trades_won'] / stats['trades_executed'] * 100
        logger.info(f"  Win rate:            {wr:.1f}%")
    logger.info(f"  Total P&L:           ${stats['total_pnl']:+.2f}")
    logger.info(f"  Final equity:        ${state.portfolio.total_equity + state.portfolio.total_pnl:,.2f}")
    logger.info("")

    # Enhanced stats
    if hasattr(state, 'skills_manager'):
        skill_stats = state.skills_manager.get_stats()
        logger.info(f"  Skills: {skill_stats.get('total', 0)} loaded, "
                     f"{skill_stats.get('total_trades', 0)} trades recorded")
    if hasattr(state, 'trajectory_logger'):
        traj_stats = state.trajectory_logger.get_stats()
        logger.info(f"  Trajectories: {traj_stats['total']} total, "
                     f"{traj_stats['executed']} executed, {traj_stats['rejected']} rejected")
    if hasattr(state, 'memory'):
        mem_stats = state.memory.get_memory_stats()
        logger.info(f"  Memory: {mem_stats.get('patterns_stored', 0)} patterns, "
                     f"{mem_stats.get('lessons_stored', 0)} lessons")

    # List trajectory files
    traj_dir = Path("/tmp/mempalace2_sim_traj/")
    if traj_dir.exists():
        for f in traj_dir.glob("*.jsonl"):
            lines = sum(1 for _ in open(f))
            logger.info(f"  Trajectory file: {f.name} ({lines} entries)")

    logger.info("=" * 70)

    # Cleanup
    await state.scheduler.stop()

    return stats


# ── Helper Functions ─────────────────────────────────────────────────────────

def detect_setups(state, ohlcv: pd.DataFrame):
    """Detect trading setups from OHLCV data."""
    setups = []
    close = float(ohlcv["close"].iloc[-1])
    ema_fast = float(ohlcv["ema_fast"].iloc[-1])
    ema_slow = float(ohlcv["ema_slow"].iloc[-1])
    ema_fast_prev = float(ohlcv["ema_fast"].iloc[-2])
    ema_slow_prev = float(ohlcv["ema_slow"].iloc[-2])
    rsi = float(ohlcv["rsi"].iloc[-1])
    macd_hist = float(ohlcv["macd_histogram"].iloc[-1])
    macd_hist_prev = float(ohlcv["macd_histogram"].iloc[-2])
    st_dir = int(ohlcv["supertrend_direction"].iloc[-1])
    st_prev = int(ohlcv["supertrend_direction"].iloc[-2])
    adx = float(ohlcv["adx"].iloc[-1])
    bb_width = float(ohlcv["bb_width"].iloc[-1])

    # EMA Crossover
    if ema_fast > ema_slow and ema_fast_prev <= ema_slow_prev:
        setups.append({"type": "ema_crossover", "direction": "long",
                       "strength": 70 + min(20, adx), "reasoning": "EMA bullish crossover"})
    elif ema_fast < ema_slow and ema_fast_prev >= ema_slow_prev:
        setups.append({"type": "ema_crossover", "direction": "short",
                       "strength": 70 + min(20, adx), "reasoning": "EMA bearish crossover"})

    # RSI
    rsi_os = state.config.analysis.rsi_oversold
    rsi_ob = state.config.analysis.rsi_overbought
    if rsi < rsi_os:
        setups.append({"type": "rsi_oversold", "direction": "long",
                       "strength": 60 + (rsi_os - rsi), "reasoning": f"RSI {rsi:.1f} oversold"})
    elif rsi > rsi_ob:
        setups.append({"type": "rsi_overbought", "direction": "short",
                       "strength": 60 + (rsi - rsi_ob), "reasoning": f"RSI {rsi:.1f} overbought"})

    # MACD
    if macd_hist > 0 and macd_hist_prev <= 0:
        setups.append({"type": "macd_bullish_cross", "direction": "long",
                       "strength": 65, "reasoning": "MACD turning bullish"})
    elif macd_hist < 0 and macd_hist_prev >= 0:
        setups.append({"type": "macd_bearish_cross", "direction": "short",
                       "strength": 65, "reasoning": "MACD turning bearish"})

    # Supertrend
    if st_dir == 1 and st_prev == -1:
        setups.append({"type": "supertrend_flip_bullish", "direction": "long",
                       "strength": 75, "reasoning": "Supertrend flipped bullish"})
    elif st_dir == -1 and st_prev == 1:
        setups.append({"type": "supertrend_flip_bearish", "direction": "short",
                       "strength": 75, "reasoning": "Supertrend flipped bearish"})

    # Trend alignment
    ema_trend = float(ohlcv["ema_trend"].iloc[-1])
    if ema_fast > ema_slow > ema_trend and close > ema_trend and st_dir == 1 and macd_hist > 0:
        setups.append({"type": "trend_alignment", "direction": "long",
                       "strength": 80, "reasoning": "Full bullish alignment"})
    elif ema_fast < ema_slow < ema_trend and close < ema_trend and st_dir == -1 and macd_hist < 0:
        setups.append({"type": "trend_alignment", "direction": "short",
                       "strength": 80, "reasoning": "Full bearish alignment"})

    return setups


def _get_indicators(ohlcv: pd.DataFrame) -> dict:
    """Extract latest indicator values."""
    return {
        "atr": float(ohlcv["atr"].iloc[-1]) if "atr" in ohlcv.columns else 5.0,
        "rsi": float(ohlcv["rsi"].iloc[-1]),
        "ema_fast": float(ohlcv["ema_fast"].iloc[-1]),
        "ema_slow": float(ohlcv["ema_slow"].iloc[-1]),
    }


def _simulate_outcome(trade: ActiveTrade, ohlcv: pd.DataFrame, sim) -> dict:
    """Simulate trade outcome using next few candles."""
    entry = trade.entry_filled_price
    sl = trade.signal.stop_loss
    tp1 = trade.signal.take_profit_1
    direction = trade.signal.direction
    is_long = direction == "long"

    # Generate next price movement (simple GBM from current price)
    rng = np.random.RandomState()
    prices = [entry]
    for _ in range(15):
        z = rng.normal(0.0002, 0.006)  # slight drift + vol
        prices.append(prices[-1] * (1 + z))
    prices = np.array(prices)

    for price in prices:
        if is_long:
            if price <= sl:
                pnl_pts = sl - entry
                return {"exit_price": sl, "pnl": pnl_pts * trade.quantity,
                        "pnl_pct": pnl_pts / entry * 100, "won": False, "reason": "SL hit"}
            if price >= tp1:
                pnl_pts = tp1 - entry
                return {"exit_price": tp1, "pnl": pnl_pts * trade.quantity,
                        "pnl_pct": pnl_pts / entry * 100, "won": True, "reason": "TP1 hit"}
        else:
            if price >= sl:
                pnl_pts = entry - sl
                return {"exit_price": sl, "pnl": -pnl_pts * trade.quantity,
                        "pnl_pct": -pnl_pts / entry * 100, "won": False, "reason": "SL hit"}
            if price <= tp1:
                pnl_pts = entry - tp1
                return {"exit_price": tp1, "pnl": pnl_pts * trade.quantity,
                        "pnl_pct": pnl_pts / entry * 100, "won": True, "reason": "TP1 hit"}

    # Neither hit — close at last price
    last_price = prices[-1]
    if is_long:
        pnl_pts = last_price - entry
    else:
        pnl_pts = entry - last_price
    won = pnl_pts > 0
    return {"exit_price": last_price, "pnl": pnl_pts * trade.quantity,
            "pnl_pct": pnl_pts / entry * 100, "won": won, "reason": "Time exit"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="XAUUSD Paper Trading Simulation")
    parser.add_argument("--cycles", type=int, default=10, help="Number of trading cycles")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING"])
    args = parser.parse_args()

    asyncio.run(run_simulation(cycles=args.cycles, log_level=args.log_level))
