---
name: supertrend-reversal
description: Catch supertrend direction flips with RSI confirmation for high-probability reversal entries on XAUUSD
version: 1.0.0
category: trading
tags: [supertrend, rsi, reversal, xauusd]
conditions:
  symbols: [XAUUSD]
  timeframes: [1h, 4h, 1d]
  setup_types: [supertrend_flip, reversal]
  market_regimes: [trending, transitioning]
win_rate: 0.0
sample_count: 0
avg_pnl_pct: 0
avg_risk_reward: 0
---

# Supertrend Reversal

## Setup Description
Catch supertrend direction flips (bullish/bearish) with RSI confirmation.
The supertrend indicator flipping direction signals a potential trend change;
RSI confirmation filters false flips.

## Entry Conditions
1. **Supertrend flip**: Indicator changes from bullish→bearish or bearish→bullish
2. **RSI confirmation**: RSI(14) crosses above 30 (long) or below 70 (short)
3. **Price action**: Candle closes above/below supertrend line
4. **Volume**: Volume above 20-period average (preferred, not required)

## Entry Rules
- **Long**: Supertrend flips bullish + RSI crosses above 30 + close > supertrend
- **Short**: Supertrend flips bearish + RSI crosses below 70 + close < supertrend

## Stop Loss
- Long: Below the supertrend flip candle low
- Short: Above the supertrend flip candle high
- Override: If ATR-based stop is tighter, use ATR(14) × 1.5

## Take Profit
- TP1: 1.5R (risk:reward) — close 50%
- TP2: 2.5R — close 30%
- TP3: 3.5R or next major S/R level — close 20%

## Risk Management
- Max position: 2% of equity
- Only one supertrend reversal trade at a time per symbol
- Skip if ATR(14) < 5 (low volatility chop)

## Notes
- Best performance in transitioning markets (range → trend)
- Avoid during major news events (FOMC, NFP)
- Works better on 1h+ timeframes; 15m has more noise
