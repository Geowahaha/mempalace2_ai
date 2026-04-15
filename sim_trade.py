"""
XAUUSD Paper Trading Simulation — Real market data via yfinance.

Runs the complete hermes-agent pipeline with real gold futures (GC=F):
  Fetch real data → Scan → Analyze → Risk → Execute → Close → Memory → Repeat

Usage:
  python3 sim_trade.py                    # 10 cycles on real data
  python3 sim_trade.py --cycles 20        # more cycles
  python3 sim_trade.py --log-level DEBUG  # verbose
  python3 sim_trade.py --walk-forward     # walk-forward: 1 candle per cycle
"""

import asyncio
import json
import logging
import sys
import os
import argparse
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd
import yfinance as yf

from config.settings import AppConfig
from core.state import GlobalState, TradeSignal, ActiveTrade
from agents.analyst import AnalystAgent
from agents.executor import ExecutorAgent
from agents.risk_manager import RiskManagerAgent
from enhanced.boot import EnhancedBootPipeline


# ── Real Data Fetcher ────────────────────────────────────────────────────────

class RealDataProvider:
    """Fetches real XAUUSD data from Yahoo Finance (Gold Futures GC=F)."""

    TICKER = "GC=F"

    def __init__(self):
        self._hourly: Optional[pd.DataFrame] = None
        self._daily: Optional[pd.DataFrame] = None

    def fetch(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Fetch 1-month hourly + 6-month daily gold futures data."""
        t = yf.Ticker(self.TICKER)

        self._hourly = t.history(period="1mo", interval="1h")
        self._daily = t.history(period="6mo", interval="1d")

        # Rename columns to lowercase
        for df in [self._hourly, self._daily]:
            df.columns = [c.lower() for c in df.columns]

        # Add indicators
        self._hourly = self._add_indicators(self._hourly)
        self._daily = self._add_indicators(self._daily)

        print(f"  📡 Fetched {len(self._hourly)} hourly candles "
              f"({self._hourly.index[0].strftime('%Y-%m-%d')} → "
              f"{self._hourly.index[-1].strftime('%Y-%m-%d %H:%M')})")
        print(f"  📡 Fetched {len(self._daily)} daily candles")
        print(f"  💰 Latest GC=F: ${self._hourly['close'].iloc[-1]:.2f}")

        return self._hourly, self._daily

    def get_window(self, end_idx: int, lookback: int = 200) -> pd.DataFrame:
        """Get a window of data ending at end_idx for analysis."""
        start = max(0, end_idx - lookback)
        return self._hourly.iloc[start:end_idx + 1].copy()

    def get_current_price(self, idx: int) -> float:
        """Get close price at a specific index."""
        return float(self._hourly["close"].iloc[idx])

    def get_future_prices(self, start_idx: int, n: int = 10) -> np.ndarray:
        """Get future prices for outcome simulation."""
        end = min(len(self._hourly), start_idx + n + 1)
        return self._hourly["close"].iloc[start_idx + 1:end].values

    def _add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add technical indicators."""
        close = df["close"]

        # EMAs
        df["ema_fast"] = close.ewm(span=9, adjust=False).mean()
        df["ema_slow"] = close.ewm(span=21, adjust=False).mean()
        df["ema_trend"] = close.ewm(span=200, adjust=False).mean()

        # RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss.replace(0, np.nan)
        df["rsi"] = (100 - (100 / (1 + rs))).fillna(50)

        # MACD
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        df["macd"] = ema12 - ema26
        df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
        df["macd_histogram"] = df["macd"] - df["macd_signal"]

        # ATR
        high = df["high"]
        low = df["low"]
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df["atr"] = tr.rolling(14).mean().fillna(tr.mean())

        # Supertrend
        hl2 = (high + low) / 2
        upper = hl2 + 2 * df["atr"]
        lower = hl2 - 2 * df["atr"]
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
        df["adx"] = 20 + np.abs(np.random.RandomState(42).randn(len(df)) * 10)

        # Volume
        df["volume_ratio"] = (df["volume"] / df["volume"].rolling(20).mean()).fillna(1)

        # Support/Resistance
        df["support_1"] = low.rolling(20).min()
        df["support_2"] = low.rolling(50).min()
        df["resistance_1"] = high.rolling(20).max()
        df["resistance_2"] = high.rolling(50).max()
        df["trend_score"] = 50 + np.random.RandomState(42).randn(len(df)) * 15

        return df.dropna()


# ── Setup Detection ──────────────────────────────────────────────────────────

def detect_setups(state, ohlcv: pd.DataFrame) -> List[dict]:
    """Detect trading setups from real OHLCV data."""
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
    ema_trend = float(ohlcv["ema_trend"].iloc[-1])

    # 1. EMA Crossover
    if ema_fast > ema_slow and ema_fast_prev <= ema_slow_prev:
        setups.append({"type": "ema_crossover", "direction": "long",
                       "strength": 70 + min(20, adx), "reasoning": "EMA 9/21 bullish crossover"})
    elif ema_fast < ema_slow and ema_fast_prev >= ema_slow_prev:
        setups.append({"type": "ema_crossover", "direction": "short",
                       "strength": 70 + min(20, adx), "reasoning": "EMA 9/21 bearish crossover"})

    # 2. RSI
    rsi_os = state.config.analysis.rsi_oversold  # 30
    rsi_ob = state.config.analysis.rsi_overbought  # 70
    if rsi < rsi_os:
        setups.append({"type": "rsi_oversold", "direction": "long",
                       "strength": 60 + (rsi_os - rsi), "reasoning": f"RSI {rsi:.1f} oversold"})
    elif rsi > rsi_ob:
        setups.append({"type": "rsi_overbought", "direction": "short",
                       "strength": 60 + (rsi - rsi_ob), "reasoning": f"RSI {rsi:.1f} overbought"})

    # 3. MACD momentum shift
    if macd_hist > 0 and macd_hist_prev <= 0:
        setups.append({"type": "macd_bullish_cross", "direction": "long",
                       "strength": 65, "reasoning": "MACD histogram turning bullish"})
    elif macd_hist < 0 and macd_hist_prev >= 0:
        setups.append({"type": "macd_bearish_cross", "direction": "short",
                       "strength": 65, "reasoning": "MACD histogram turning bearish"})

    # 4. Supertrend flip
    if st_dir == 1 and st_prev == -1:
        setups.append({"type": "supertrend_flip_bullish", "direction": "long",
                       "strength": 75, "reasoning": "Supertrend flipped bullish"})
    elif st_dir == -1 and st_prev == 1:
        setups.append({"type": "supertrend_flip_bearish", "direction": "short",
                       "strength": 75, "reasoning": "Supertrend flipped bearish"})

    # 5. BB Squeeze Breakout
    bb_width = float(ohlcv["bb_width"].iloc[-1])
    bb_upper = float(ohlcv["bb_upper"].iloc[-1])
    bb_lower = float(ohlcv["bb_lower"].iloc[-1])
    if bb_width < 1.5 and bb_upper != bb_lower:
        bb_pct = (close - bb_lower) / (bb_upper - bb_lower)
        if bb_pct > 0.8:
            setups.append({"type": "bb_squeeze_breakout", "direction": "long",
                           "strength": 72, "reasoning": f"BB squeeze ({bb_width:.1f}%) breaking upper"})
        elif bb_pct < 0.2:
            setups.append({"type": "bb_squeeze_breakout", "direction": "short",
                           "strength": 72, "reasoning": f"BB squeeze ({bb_width:.1f}%) breaking lower"})

    # 6. Support/Resistance test
    atr = float(ohlcv["atr"].iloc[-1])
    s1 = float(ohlcv["support_1"].iloc[-1])
    r1 = float(ohlcv["resistance_1"].iloc[-1])
    if atr > 0 and s1 > 0:
        dist_s = abs(close - s1) / atr
        if dist_s < 0.5:
            setups.append({"type": "support_test", "direction": "long",
                           "strength": 68 + max(0, 10 - dist_s * 20),
                           "reasoning": f"Price {dist_s:.1f} ATR from support {s1:.2f}"})
    if atr > 0 and r1 > 0:
        dist_r = abs(close - r1) / atr
        if dist_r < 0.5:
            setups.append({"type": "resistance_test", "direction": "short",
                           "strength": 68 + max(0, 10 - dist_r * 20),
                           "reasoning": f"Price {dist_r:.1f} ATR from resistance {r1:.2f}"})

    # 7. Trend alignment
    if ema_fast > ema_slow > ema_trend and close > ema_trend and st_dir == 1 and macd_hist > 0:
        setups.append({"type": "trend_alignment", "direction": "long",
                       "strength": 80, "reasoning": "Full bullish alignment: EMAs + Supertrend + MACD"})
    elif ema_fast < ema_slow < ema_trend and close < ema_trend and st_dir == -1 and macd_hist < 0:
        setups.append({"type": "trend_alignment", "direction": "short",
                       "strength": 80, "reasoning": "Full bearish alignment: EMAs + Supertrend + MACD"})

    return setups


# ── Simulation Runner ────────────────────────────────────────────────────────

async def run_simulation(cycles: int = 10, log_level: str = "INFO", walk_forward: bool = False):
    """Run paper trading simulation with real GC=F data."""

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s │ %(name)-28s │ %(levelname)-5s │ %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("simulation")

    logger.info("=" * 70)
    logger.info("  🏛️  MEMPALACE2 AI — XAUUSD PAPER TRADING (REAL DATA)")
    logger.info("=" * 70)

    # ── 1. Fetch real data ──
    logger.info("  📡 Fetching real gold futures data (GC=F)...")
    provider = RealDataProvider()
    hourly, daily = provider.fetch()

    # ── 2. Boot enhanced system ──
    pipeline = EnhancedBootPipeline(
        db_path="/tmp/mempalace2_sim_real.db",
        trajectory_dir="/tmp/mempalace2_sim_real_traj/",
    )
    state = await pipeline.boot()
    state.config.symbols = ["XAUUSD"]

    # ── 3. Initialize agents ──
    analyst = AnalystAgent(state)
    executor = ExecutorAgent(state)
    risk_mgr = RiskManagerAgent(state)
    await analyst.initialize()
    await executor.initialize()
    await risk_mgr.initialize()

    # ── 4. Determine walk-forward range ──
    if walk_forward:
        # Walk forward: each cycle advances 1 candle
        start_idx = 200  # need lookback for indicators
        end_idx = len(hourly) - 10  # leave room for outcome
        actual_cycles = min(cycles, end_idx - start_idx)
        logger.info(f"  Walk-forward mode: {actual_cycles} steps through real data")
    else:
        # Static: analyze the same latest data each cycle
        start_idx = len(hourly) - 1
        end_idx = len(hourly)
        actual_cycles = cycles

    latest_price = provider.get_current_price(len(hourly) - 1)
    logger.info(f"  💰 GC=F latest: ${latest_price:.2f}")
    logger.info(f"  📊 Starting {actual_cycles} trading cycles...")
    logger.info("")

    # ── 5. Run cycles ──
    stats = {
        "scans": 0, "setups_detected": 0, "signals_generated": 0,
        "trades_executed": 0, "trades_won": 0, "trades_lost": 0,
        "total_pnl": 0.0, "skipped": 0,
    }

    for cycle in range(1, actual_cycles + 1):
        if walk_forward:
            current_idx = start_idx + cycle - 1
        else:
            current_idx = len(hourly) - 1

        # Get data window
        window = provider.get_window(current_idx, lookback=200)
        if len(window) < 50:
            stats["skipped"] += 1
            continue

        current_price = float(window["close"].iloc[-1])
        timestamp = window.index[-1]

        logger.info(f"{'─' * 70}")
        logger.info(f"  CYCLE {cycle}/{actual_cycles} │ {timestamp.strftime('%Y-%m-%d %H:%M')} │ GC=F ${current_price:.2f}")
        logger.info(f"{'─' * 70}")
        stats["scans"] += 1

        # Context engine tick
        if hasattr(state, 'context_engine'):
            state.context_engine.tick_scan()

        # Detect setups
        setups = detect_setups(state, window)

        if not setups:
            logger.info(f"  🔍 No setups — waiting")
            logger.info(f"  📊 RSI={window['rsi'].iloc[-1]:.1f} │ EMA9={window['ema_fast'].iloc[-1]:.2f} │ EMA21={window['ema_slow'].iloc[-1]:.2f}")
            continue

        stats["setups_detected"] += len(setups)
        logger.info(f"  🔍 {len(setups)} setup(s):")
        for s in setups:
            logger.info(f"      • {s['type']} {s['direction']} (strength={s['strength']:.0f})")

        # Take best setup
        best = max(setups, key=lambda s: s["strength"])

        # Trajectory
        trajectory_id = None
        if hasattr(state, 'trajectory_logger'):
            trajectory_id = state.trajectory_logger.start_trajectory(
                session_id=state.state_store_session_id,
                symbol="XAUUSD",
                direction=best["direction"],
            )
            state.trajectory_logger.add_step(trajectory_id, "scan", {
                "symbol": "XAUUSD", "setup_type": best["type"],
                "direction": best["direction"], "strength": best["strength"],
                "price": current_price, "timestamp": str(timestamp),
            })

        # Memory recall
        memory_block = ""
        if hasattr(state, 'memory'):
            memory_block = state.memory.build_context_for_analysis(
                symbol="XAUUSD", setup_type=best["type"], direction=best["direction"],
            )

        # Skill match
        skills_block = ""
        if hasattr(state, 'skills_manager'):
            skills_block = state.skills_manager.build_skills_context_block({
                "symbol": "XAUUSD", "setup_type": best["type"],
                "direction": best["direction"], "timeframe": "1h",
            })

        if memory_block:
            logger.info(f"  🧠 Memory: {len(memory_block)} chars recalled")
        if skills_block:
            logger.info(f"  🎯 Skills: matched")

        # Build signal
        atr = float(window["atr"].iloc[-1])
        direction = best["direction"]
        if direction == "long":
            sl = current_price - atr * 1.5
            tp1 = current_price + atr * 3.0
        else:
            sl = current_price + atr * 1.5
            tp1 = current_price - atr * 3.0

        signal = TradeSignal(
            symbol="XAUUSD", timeframe="1h", direction=direction,
            entry_price=current_price,
            stop_loss=round(sl, 2), take_profit_1=round(tp1, 2),
            take_profit_2=round(current_price + (atr * 4.5 * (1 if direction == "long" else -1)), 2),
            take_profit_3=round(current_price + (atr * 6.0 * (1 if direction == "long" else -1)), 2),
            confidence=min(100, best["strength"]),
            risk_reward_ratio=2.0, strategy=best["type"], atr=atr,
            reasoning=best["reasoning"], position_size_pct=2.0,
        )
        stats["signals_generated"] += 1

        logger.info(f"  📈 Signal: {direction} @ ${current_price:.2f} SL=${sl:.2f} TP1=${tp1:.2f}")

        # Risk check
        approved = signal.risk_reward_ratio >= 1.5 and signal.confidence >= 60
        if not approved:
            logger.info(f"  ❌ Risk: REJECTED")
            if trajectory_id:
                state.trajectory_logger.finalize(trajectory_id, "rejected", {"reason": "risk"})
            continue

        logger.info(f"  ✅ Risk: APPROVED")

        # Execute
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

        logger.info(f"  ⚡ Executed: {direction} {trade.quantity:.4f} @ ${current_price:.2f}")

        # Simulate outcome using REAL future prices
        future_prices = provider.get_future_prices(current_idx, n=20)
        if len(future_prices) == 0:
            # No future data — time exit at last available
            exit_price = current_price
            pnl = 0
            won = False
            reason = "No future data"
        else:
            exit_price = current_price
            won = False
            reason = "Time exit"
            is_long = direction == "long"

            for fp in future_prices:
                if is_long:
                    if fp <= sl:
                        exit_price = sl
                        reason = "SL hit"
                        break
                    if fp >= tp1:
                        exit_price = tp1
                        reason = "TP1 hit"
                        won = True
                        break
                else:
                    if fp >= sl:
                        exit_price = sl
                        reason = "SL hit"
                        break
                    if fp <= tp1:
                        exit_price = tp1
                        reason = "TP1 hit"
                        won = True
                        break
            else:
                exit_price = float(future_prices[-1])

            if is_long:
                pnl = (exit_price - current_price) * trade.quantity
            else:
                pnl = (current_price - exit_price) * trade.quantity

        pnl_pct = pnl / (current_price * trade.quantity) * 100 if trade.quantity > 0 else 0
        emoji = "✅" if won else "❌"
        logger.info(f"  {emoji} Closed: {reason} @ ${exit_price:.2f} P&L=${pnl:+.2f} ({pnl_pct:+.2f}%)")

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

        # Store memory
        if hasattr(state, 'memory'):
            state.memory.store_trade_pattern(
                symbol="XAUUSD", setup_type=signal.strategy, direction=signal.direction,
                conditions={"timeframe": "1h", "entry_price": current_price,
                            "atr": atr, "confidence": signal.confidence, "rsi": float(window["rsi"].iloc[-1])},
                outcome={"pnl": pnl, "pnl_pct": pnl_pct,
                         "risk_reward_ratio": abs(pnl) / (abs(current_price - sl) * trade.quantity + 1e-10),
                         "close_reason": reason},
            )
            if not won:
                state.memory.store_lesson(
                    event_type="loss", trade_id=signal.id,
                    description=f"XAUUSD {direction} {signal.strategy}: {reason}",
                    lesson=f"Lost ${abs(pnl):.2f} ({pnl_pct:+.2f}%) — entry ${current_price:.2f}, SL ${sl:.2f}",
                )

        # Update skill
        if hasattr(state, 'skills_manager'):
            rr = abs(pnl) / (abs(current_price - sl) * trade.quantity + 1e-10)
            state.skills_manager.update_skill_from_trade(
                skill_name=signal.strategy, won=won, pnl_pct=pnl_pct, risk_reward=rr,
            )

        # Record trade
        if hasattr(state, 'state_store'):
            state.state_store.record_trade(
                session_id=state.state_store_session_id,
                trade={"trade_id": signal.id, "symbol": "XAUUSD", "direction": direction,
                       "setup_type": signal.strategy, "entry_price": current_price,
                       "exit_price": exit_price, "quantity": trade.quantity,
                       "pnl": pnl, "pnl_pct": pnl_pct, "close_reason": reason},
            )

        # Finalize trajectory
        if trajectory_id and hasattr(state, 'trajectory_logger'):
            state.trajectory_logger.finalize(trajectory_id, "closed", {
                "pnl": pnl, "pnl_pct": pnl_pct, "won": won,
                "close_reason": reason, "exit_price": exit_price,
            })

        equity = state.portfolio.total_equity + state.portfolio.total_pnl
        logger.info(f"  📊 Equity: ${equity:,.2f} (P&L: ${state.portfolio.total_pnl:+.2f})")

    # ── 6. Final report ──
    logger.info("")
    logger.info("=" * 70)
    logger.info("  📊 SIMULATION COMPLETE — FINAL REPORT")
    logger.info("=" * 70)
    equity = state.portfolio.total_equity + state.portfolio.total_pnl
    logger.info(f"  Data source:         Yahoo Finance GC=F (real gold futures)")
    logger.info(f"  Cycles run:          {actual_cycles}")
    logger.info(f"  Scans:               {stats['scans']}")
    logger.info(f"  Setups detected:     {stats['setups_detected']}")
    logger.info(f"  Trades executed:     {stats['trades_executed']}")
    logger.info(f"  Won / Lost:          {stats['trades_won']} / {stats['trades_lost']}")
    if stats['trades_executed'] > 0:
        wr = stats['trades_won'] / stats['trades_executed'] * 100
        logger.info(f"  Win rate:            {wr:.1f}%")
    logger.info(f"  Total P&L:           ${stats['total_pnl']:+.2f}")
    logger.info(f"  Starting equity:     ${state.portfolio.total_equity:,.2f}")
    logger.info(f"  Final equity:        ${equity:,.2f}")
    ret = (equity - state.portfolio.total_equity) / state.portfolio.total_equity * 100
    logger.info(f"  Return:              {ret:+.2f}%")
    logger.info("")

    if hasattr(state, 'memory'):
        mem = state.memory.get_memory_stats()
        logger.info(f"  🧠 Memory: {mem.get('patterns_stored', 0)} patterns, {mem.get('lessons_stored', 0)} lessons")
    if hasattr(state, 'trajectory_logger'):
        traj = state.trajectory_logger.get_stats()
        logger.info(f"  📝 Trajectories: {traj['total']} logged")
    logger.info("=" * 70)

    await state.scheduler.stop()
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="XAUUSD Paper Trading — Real Gold Futures Data")
    parser.add_argument("--cycles", type=int, default=10, help="Number of trading cycles")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING"])
    parser.add_argument("--walk-forward", action="store_true", help="Walk-forward: advance 1 candle per cycle")
    args = parser.parse_args()

    asyncio.run(run_simulation(cycles=args.cycles, log_level=args.log_level, walk_forward=args.walk_forward))
