# Repository operating contract

This repository is a research and paper-trading laboratory. It is not a live
trading system.

## Non-negotiable safety rules

- `ExecutionMode` may only be `BACKTEST` or `PAPER`.
- Do not add exchange authentication, account endpoints, API keys, secrets,
  order submission, order cancellation, leverage, borrowing, or shorting.
- Network code may use `GET` requests only, and only against the documented
  Binance market-data-only host.
- A signal derived from bar `t` may execute no earlier than the open of bar
  `t+1`.
- Reject incomplete, duplicate, unordered, non-hour-aligned, or unexplained-gap
  data. An exchange-source outage may be admitted only when every missing hour
  is recorded in the immutable anomaly ledger and the frozen
  `CARRY_FORWARD_NO_FILL` policy is used: synthetic hours carry the last close,
  freeze every strategy indicator/regime/pending-intent state, and can never
  execute a fill. Official zero-volume or zero-trade bars have the same frozen
  state and no-fill semantics. Only an official, non-synthetic bar with both
  positive volume and positive trade count is strategy-state/fill eligible.
- Backtests must declare fees and slippage and retain the data/config/code
  fingerprint used to produce every result.
- Acceptance thresholds are fixed before the locked holdout is opened. A failed
  threshold must not be weakened after seeing the holdout.
- Never describe a backtest candidate as profitable, proven, safe, or suitable
  for live funds. Use the maturity labels in the research contract.

## Code review rules

- Treat same-bar fills, shifted rolling windows, mutable holdouts, hidden cost
  defaults, and non-deterministic output as P0/P1 defects.
- Treat any live-order or secret-bearing capability as a P0 defect.
- Require tests for cash/position conservation, long-or-cash limits,
  deterministic fingerprints, boundary-aware first-day metrics, authenticated
  one-shot state receipts, and backtest/paper parity.
