# Research contract v1.1.0

## Question

Can one simple, pre-declared BTC/USDT spot trend family produce a stable,
positive out-of-sample risk-adjusted result after conservative trading costs,
without leverage or shorting?

The valid answer may be **no**. The purpose of this repository is to make that
answer difficult to manipulate.

## Frozen scope

- Binance BTCUSDT spot, completed 1-hour UTC bars.
- Long or cash only; gross exposure is within `[0, 1]`.
- Signals use information available by the close of bar `t`; fills occur at the
  open of bar `t+1` with adverse slippage and explicit fees.
- One strategy family: Donchian entry/exit, slow trend filter, and entry-time
  volatility targeting. Exactly the 16 canonical combinations in the TOML
  contract are eligible; there are no spare trials.
- Initial capital is a bookkeeping unit, not a claim about deployable capacity.

## Frozen chronology

- Indicator initialization: first available bar through 2020-01-31 UTC; this
  segment produces no scored return and no candidate ranking.
- Pre-OOS validation: nine fixed, non-overlapping six-month folds from
  2020-02-01 through 2024-07-31 UTC. Each candidate runs one continuous state
  machine: cash reset only at Fold 1, then positions continue across folds.
- Retrospective locked OOS: 2024-08-01 through 2026-07-31 UTC. Because this
  study began after that interval ended, it is explicitly evidence level
  `RETROSPECTIVE_LOCKED_OOS`, never prospective proof.

The holdout is opened only after code tests, the candidate rule, the metric
implementation, and cost assumptions are frozen and fingerprinted. After the
holdout is opened, a failed gate is not repaired by changing parameters,
thresholds, dates, or costs. A new hypothesis requires a new contract version
and a new future holdout.

## Costs and execution

- Base cost: 10 basis points fee plus 5 basis points adverse slippage per side.
- Stress test: all per-side costs are multiplied by 2 and 3.
- Fractional units are allowed in the simulator; cash and position may never be
  negative.
- No same-bar signal fills, intrabar stop assumptions, limit-order rebates, or
  unmodelled maker fills.
- A fill requires an official bar with positive source volume. A pending regime
  change retains its original signal timestamp through synthetic or zero-volume
  bars, executes at the first eligible bar, or is cancelled if a newer signal
  returns to the current portfolio regime. Positive target changes never add to
  or rebalance an existing long.
- Any terminal open position is valued as an adverse sale at the final close,
  including exit slippage and fee. This liquidation affects performance but is
  not counted as a completed strategy round trip.
- Official source gaps are never silently treated as observed trading. Each
  missing UTC hour is ledgered; the deterministic `CARRY_FORWARD_NO_FILL`
  sensitivity policy carries the last close, freezes strategy state, and
  prohibits fills on synthetic hours. Results must disclose the event and
  missing-hour counts. Any unledgered, duplicate, unordered, or misaligned gap
  fails closed.

## Acceptance gates

All gates are conjunctive. A candidate is `BACKTEST_CANDIDATE` only when:

1. Locked-holdout annualized Sharpe is at least 0.80.
2. Locked-holdout Calmar is at least 0.75 and maximum drawdown is no worse than
   25%.
3. There are at least 30 completed holdout round trips.
4. Terminal return remains positive at 2x base costs.
5. At least 75% of the nine pre-OOS folds are positive, which means at least
   seven folds because zero return is not positive.
6. At least 70% of the selected candidate's predeclared adjacent parameter
   neighbours are positive in the same one-shot retrospective OOS run. The
   primary is excluded from the denominator, neighbor results are diagnostic,
   and no neighbor may replace a failed primary.
7. No single calendar quarter contributes more than 50% of positive holdout
   daily mark-to-market equity gains. This is a concentration diagnostic, not
   realized-trade accounting.
8. Every data, accounting, look-ahead, determinism, and zero-live-authority
   invariant passes. Paper replay parity is a later `PAPER_READY` gate.

Failure yields `NOT_PROVEN`, not a revised test.

## Maturity labels

- `RESEARCH_NOT_YET_VALIDATED`: implementation or evaluation incomplete.
- `NOT_PROVEN`: one or more pre-registered gates failed.
- `BACKTEST_CANDIDATE`: retrospective historical gates passed; still not
  forward evidence.
- `PAPER_READY`: engineering and replay parity passed.
- `PAPER_VALIDATED_PENDING_HUMAN_REVIEW`: at least 180 calendar days and 30
  completed round trips of timestamped forward paper operation passed.
- `LIVE_DISABLED`: permanent capability label for this repository.

No maturity label is a promise of future profit or authorization to use money.

## Independent review

Before the retrospective OOS is opened, an independent ChatGPT Pro review is asked
to attack leakage, accounting, cost assumptions, metric correctness, and
threshold integrity. Its sanitized prompt, visible mode labels, findings, and
the disposition of every finding are recorded under `docs/reviews/` without
account identifiers or private data.

## Holdout mechanism

The full source snapshot is deterministically partitioned into a pre-OOS
manifest and a separately committed retrospective-OOS manifest. Candidate
selection must reject any manifest containing an OOS bar. Finalization requires exact
config, source, code, data, passing-test, and Pro-review receipts. An atomic
experiment receipt transitions `FROZEN -> HOLDOUT_OPENED -> FINALIZED`; once
`HOLDOUT_OPENED` exists, a failed or interrupted run cannot inspect that OOS
segment again.

The nine pre-OOS folds share one continuous strategy/account run. The
retrospective OOS separately resets both strategy and portfolio to cash while
retaining at most 720 pre-boundary hours only for indicator warmup. A fresh OOS
signal is required; no pre-OOS position is synthesized.

## Exact signal semantics

- Entry channel: maximum official or policy-derived high of the previous `N`
  bars, excluding bar `t`; entry requires strict `close[t] > channel`.
- Exit channel: minimum low of the previous `N` bars, excluding `t`; exit uses
  strict `close[t] < channel`.
- Trend filter: simple mean of the most recent `N` closes including `t`; entry
  requires strict `close[t] > mean`, while an existing long exits on strict
  `close[t] < mean`.
- Volatility: sample standard deviation of hourly log returns through `t`, over
  the frozen lookback, annualized by `sqrt(365.25 * 24)`. A 720-return window
  requires 721 closes; no artificial zero return is inserted, and volatility
  is unavailable one bar earlier.
- Exposure is `min(1, target_vol / max(realized_vol, volatility_floor))`, fixed
  when the entry signal occurs and unchanged until exit.
- When long, exit logic has priority and the same close cannot exit and re-enter.
- Folds and OOS score UTC daily end-equity returns with zero cash risk-free
  rate, hourly mark-to-market drawdown, exact elapsed-time CAGR, and terminal
  liquidation value.

## Registered source anomalies

Source bytes are never repaired. The exact 14 close-time variance rows and 28
gap events are pinned by `configs/data_exceptions_v1.json`, including full-row
hashes and a full-source snapshot hash. Any changed row, additional event,
missing registered event, or changed count is `SOURCE_DRIFT` or
`UNREGISTERED_DATA_EXCEPTION` and stops the study before performance code runs.
