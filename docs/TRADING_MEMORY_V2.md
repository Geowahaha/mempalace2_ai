# Trading Memory V2

## Goal

Use MemPalace as a trading intelligence system, not a passive trade log.

The memory layer must answer five operator questions:

1. Which rooms keep winning and are safe to repeat?
2. Which rooms keep losing and should be blocked?
3. Where is the model overconfident?
4. Where is the model underconfident and leaving money on the table?
5. Which candidate lanes deserve promotion from research to shadow to live?

## Palace mapping for trading

### Wings

- `wing:symbol:<symbol>`
  Primary market memory per instrument.
- `wing:execution`
  Broker, routing, slippage, partial-close, and quote-path anomalies.
- `wing:risk`
  Drawdown, kill-switch triggers, correlation, and portfolio constraints.
- `wing:research`
  Backtests, MiMo analyst notes, postmortems, and candidate strategy ideas.

### Hall types

- `hall_facts`
  Locked decisions and rules. Example: max loss blocks, allowed session windows.
- `hall_events`
  Raw trade journals and session events. This is the current live default.
- `hall_discoveries`
  Newly found edges, recurring traps, and structural insights.
- `hall_advice`
  Analyst output from MiMo or human review.
- `hall_preferences`
  Broker quirks, execution preferences, and operational settings.

### Rooms

Rooms should represent repeatable market structures, not generic notes.

Examples:

- `room:ny-breakout-high-vol`
- `room:us-open-trend-follow`
- `room:asia-range-fade`
- `room:spread-anomaly`
- `room:high-confidence-false-positive`

## Live usage

### L0

Identity and runtime guards.

### L1

Wake-up memory for the current symbol and session:

- top recent winners
- top recent losers
- repeatable winner rooms
- danger rooms

### L2

Room-scoped recall for the current candidate setup.

### L3

Cross-wing deep search for research, execution anomalies, and prior analyst notes.

## Intelligence outputs

The engine should always be able to compute:

- `winner_rooms`
  Rooms with repeated wins, acceptable confidence, and positive pnl.
- `danger_rooms`
  Rooms with repeated losses, especially overconfident losses.
- `opportunity_rooms`
  Rooms with good win rate but low model confidence.
- `confidence_calibration`
  Buckets showing whether confidence maps to actual outcomes.
- `lane_scoreboard`
  Strategy or setup lanes ranked by outcome quality.
- `tunnels`
  Same room recurring across multiple wings, halls, or symbols.

## Analyst loop

MiMo Pro should run asynchronously, not inside the live execution loop.

Recommended cadence:

- once per day
- after the main session closes
- against closed trades only

MiMo should receive:

- daily analyst brief
- winner rooms
- danger rooms
- lane scoreboard
- calibration report

MiMo should output:

- lanes to promote to shadow
- lanes to demote from live
- risk rules to tighten
- recurring execution issues
- candidate hypotheses for research

MiMo must not modify live execution policy directly.
Promotion flow stays:

1. `lab`
2. `shadow`
3. `live`

## Current implementation status

Implemented now:

- raw trade journal storage
- `wing / hall / room` metadata
- operator note memories across `hall_facts`, `hall_discoveries`, `hall_advice`, `hall_preferences`
- wake-up context
- taxonomy view
- winner, danger, opportunity, anti-pattern, calibration, lane, promotion, and tunnel analytics
- room guardrail feedback into the trading loop
- daily analyst brief endpoint
- analyst packet endpoint
- MiMo daily analyst worker that stores advice back into `hall_advice`

Still to add:

- first-class research memories
- first-class execution memories
- shadow-lane promotion workflow
- MiMo daily analyst job
- tunnel-aware recall between symbols and research wings
