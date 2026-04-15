---
name: ema-crossover-mtf
description: EMA crossover with multi-timeframe alignment for high-confidence trend-following entries on XAUUSD
version: 1.0.0
category: trading
tags: [ema, crossover, multi-timeframe, trend-following, xauusd]
conditions:
  symbols: [XAUUSD]
  timeframes: [1h, 4h]
  setup_types: [ema_crossover, trend]
  market_regimes: [trending]
win_rate: 0.0
sample_count: 0
avg_pnl_pct: 0
avg_risk_reward: 0
---

# EMA Crossover with Multi-Timeframe Alignment

## Setup Description
Trade EMA crossovers (EMA 9/21) when confirmed by higher timeframe trend
alignment. The fast EMA crossing the slow EMA signals momentum shift;
multi-timeframe confirmation filters counter-trend crossovers.

## Entry Conditions
1. **EMA Cross**: EMA(9) crosses above/below EMA(21) on entry timeframe
2. **HTF Alignment**: Higher timeframe EMA(9) is on same side of EMA(21)
   - 1h entry → check 4h alignment
   - 4h entry → check 1d alignment
3. **Price position**: Price above both EMAs (long) or below both (short)
4. **ADX**: ADX(14) > 20 (confirms trend strength)

## Entry Rules
- **Long**: EMA(9) crosses above EMA(21) + HTF bullish + ADX > 20
- **Short**: EMA(9) crosses below EMA(21) + HTF bearish + ADX > 20
- Enter on the candle after the crossover candle

## Stop Loss
- Long: Below EMA(21) or below recent swing low (whichever is lower)
- Short: Above EMA(21) or above recent swing high (whichever is higher)
- Max stop: 2 × ATR(14) from entry

## Take Profit
- TP1: 1.5R — close 40%
- TP2: 2.5R — close 35%
- TP3: Trail using EMA(21) as dynamic support/resistance — close 25%
- Exit if EMA(9) crosses back below EMA(21) (trend reversal)

## Risk Management
- Max position: 2% of equity
- Skip if ADX < 15 (no clear trend)
- Skip if price is far from EMA(21) (> 2×ATR)
- One active crossover trade per symbol

## Notes
- Strongest signals when crossover aligns with support/resistance level
- Works best in trending markets; filter with ADX
- Higher timeframes (4h, 1d) produce fewer but more reliable signals
- Avoid trading crossovers that happen during consolidation/low volume
