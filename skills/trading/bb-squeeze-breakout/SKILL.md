---
name: bb-squeeze-breakout
description: Trade Bollinger Band squeeze breakouts with volume confirmation for momentum entries on XAUUSD
version: 1.0.0
category: trading
tags: [bollinger-bands, squeeze, breakout, volume, xauusd]
conditions:
  symbols: [XAUUSD]
  timeframes: [15m, 1h, 4h]
  setup_types: [bb_squeeze, breakout]
  market_regimes: [ranging, low_volatility]
win_rate: 0.0
sample_count: 0
avg_pnl_pct: 0
avg_risk_reward: 0
---

# Bollinger Band Squeeze Breakout

## Setup Description
Trade breakouts from Bollinger Band squeeze periods. When BB width contracts
below its 20-period percentile, volatility is compressed and a breakout is
imminent. Volume spike confirms the breakout direction.

## Entry Conditions
1. **BB Squeeze**: BB width at 10-period low (or below 20th percentile of last 50 periods)
2. **Breakout candle**: Close above upper BB (long) or below lower BB (short)
3. **Volume spike**: Current volume > 1.5× the 20-period average
4. **Direction**: Enter in the breakout direction

## Entry Rules
- **Long**: Squeeze + close above upper BB + volume spike
- **Short**: Squeeze + close below lower BB + volume spike
- Enter on close of the breakout candle (don't anticipate)

## Stop Loss
- Long: Middle of the BB (20 SMA) at entry
- Short: Middle of the BB (20 SMA) at entry
- Max stop: 1.5 × ATR(14) from entry

## Take Profit
- TP1: 2R — close 50%
- TP2: 3R — close 30%
- TP3: Trail using BB midline — close 20%
- Exit if BB expands and then contracts again (squeeze re-forms)

## Risk Management
- Max position: 1.5% of equity (breakouts can fake out)
- Maximum 2 concurrent squeeze trades
- If breakout fails (price re-enters BB within 3 candles), exit at 1R loss

## Notes
- Best on 1h timeframe for XAUUSD
- The longer the squeeze, the more powerful the breakout
- Fake breakouts are common; volume confirmation is critical
- Works in ranging markets; less reliable in strong trends
