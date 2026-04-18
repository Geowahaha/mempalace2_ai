# Hermes-Inspired Self Improvement

## Goal

Borrow the parts of Hermes that matter for Mempalac:

1. procedural memory, not just raw logs
2. post-task self-critique
3. automatic skill document creation
4. reuse of accumulated skills on the next similar task
5. team-style prompting with separate strategist / memory / risk / evolution voices

This implementation stays inside `D:\Mempalac_AI` and does not modify Dexter.

## Added Components

- `trading_ai/core/skillbook.py`
  Stores reusable procedural skills as:
  - markdown documents in `data/skills/`
  - indexed JSON in `data/skillbook_index.json`
- `trading_ai/core/self_improvement.py`
  Runs the observe -> critique -> distill -> reuse loop after each closed trade.

## Runtime Flow

### Before a decision

1. Build current market features.
2. Predict the most likely setup / strategy key for the current lane.
3. Recall matching skill documents from the skillbook.
4. Build a team brief:
   - strategist
   - memory_librarian
   - risk_guardian
   - evolution_coach
5. Inject both the skill context and the team brief into the LLM decision prompt.

### After a closed trade

1. Store the normal trade memory in Chroma.
2. Update strategy registry stats.
3. Run the self-improvement review.
4. Update or create a skill document for that lane.
5. Mirror the distilled lesson back into MemPalace notes.
6. Use that new skill next time similar context appears.

## Why This Is Safer Than Blind Auto-Optimization

- The system does not promise guaranteed profit.
- It optimizes reusable, risk-aware lessons, not just aggression.
- Overconfident losses create negative skill pressure and guardrails.
- Underconfident wins become opportunity notes, not automatic live promotion.
- Existing room guards, risk gates, and lane stages still remain in force.

## API Visibility

New inspection endpoints:

- `GET /skills`
- `GET /skills/context`

`GET /memory/analyst-packet` now also includes the current skillbook snapshot.
