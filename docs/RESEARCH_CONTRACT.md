# Research contract v1.2.0

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
- A strategy-state/fill-eligible bar must be official, non-synthetic, and have
  both positive source volume and positive source trade count. Every other bar
  advances UTC time only: it cannot age or update Donchian, trend, return, or
  volatility buffers; generate, resize, or cancel an intent; change the
  long/cash regime; or execute a fill. A pending regime change from an earlier
  eligible signal retains its original signal timestamp until the first later
  eligible bar executes it or a later eligible signal cancels it. Positive
  target changes never add to or rebalance an existing long.
- Any terminal open position is valued as an adverse sale at the final close,
  including exit slippage and fee, only when the final bar is itself
  strategy-state/fill eligible. If terminal liquidation is required but the
  final bar is ineligible, evaluation fails closed as
  `TERMINAL_LIQUIDATION_NOT_EXECUTABLE`; it may not extend the window or assume
  a synthetic sale. A valid liquidation affects performance but is not counted
  as a completed strategy round trip.
- Official source gaps are never silently treated as observed trading. Each
  missing UTC hour is ledgered; the deterministic `CARRY_FORWARD_NO_FILL`
  sensitivity policy carries the last close, freezes every indicator, regime,
  and pending-intent state, and prohibits fills on synthetic hours. Results
  must disclose the event and missing-hour counts. Any unledgered, duplicate,
  unordered, or misaligned gap fails closed.
- The buy-and-hold diagnostic follows the same causal engine and costs: its
  100% long target is first signalled at the close of the first eligible OOS
  bar, can fill no earlier than the open of the next eligible OOS bar, and is
  valued by the same costed final-eligible-close rule. It is a benchmark only
  and cannot replace the selected primary candidate.

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
selection performs a metadata-only PRE/config gate before any price I/O and
must reject FULL or LOCKED inputs before their price files are read. The PRE
manifest binds the exact LOCKED manifest path, file hash, paired descriptor,
data commitment, and lockbox ID without loading or scoring LOCKED prices.
Finalization requires
exact config, source, code, data, passing-test, and Pro-review receipts. A
canonical, crash-durable, self-hashed experiment receipt chain transitions
`FROZEN -> external HOLDOUT_OPENED anchor -> local HOLDOUT_OPENED -> FINALIZED`.
The external store is an absolute, explicitly supplied directory outside the
repository and is bound by both the canonical 64-hex store ID and the unique
canonical descriptor SHA-256 frozen in the v1.2 configuration and source before
candidate selection. The store is provisioned once; ordinary CLI operation can
verify it but cannot initialize, replace, reset, repair, or delete it. Deployment
must copy the complete store, including every record, while no absolute machine
path is stored in research artifacts.
Every state binds the previous state hash and the exact
selection/config/source/data/test/review/report
commitments appropriate to that transition. `FROZEN`, manifest metadata, and
both audit receipts are fully validated before the opening event. The external
record is then exclusively created and durably flushed before the linked local
state, and both are re-read before any locked price byte is loaded. Any existing,
partial, corrupt, missing, or inconsistent external/local opening record fails
closed. Once the external record exists, the experiment is permanently
consumed even if the local experiment directory is deleted or the process
crashes. Neither a retry, repair, force, reset, nor prune operation exists.

This create-only store protects against ordinary repository rollback and
accidental local-state deletion. Its pinned descriptor rejects a newly created
empty store that merely reuses the public store ID. Without a signing key,
operating-system WORM, or an independent remote witness, it does not claim to
defeat an administrator who duplicates the descriptor before use, selectively
deletes records, or deletes or rewrites every copy of both the repository and
anchor store.

Before opening the holdout, a separately gated data-only provenance replay may
parse the full source solely to verify raw bytes, canonical normalization,
registered anomalies, partition boundaries, and commitments. It must not
return price rows to the researcher/model, call strategy or metric code, or
emit any locked-period performance. The ordinary test suite leaves this replay
disabled; explicit isolated mode may expose only hashes, counts, and PASS/FAIL.
The strategy/evaluation path cannot load the locked manifest until the
authenticated `HOLDOUT_OPENED` state has been durably committed.

The nine pre-OOS folds share one continuous strategy/account run. The
retrospective OOS separately resets both strategy and portfolio to cash while
retaining at most 720 pre-boundary hours only for indicator warmup. A fresh OOS
signal is required; no pre-OOS position is synthesized.

## Exact signal semantics

- Indicator buffers contain strategy-state-eligible observations only.
  Synthetic, zero-volume, and zero-trade bars advance UTC elapsed time but do
  not enter or age any rolling buffer. The existing `_hours` parameter names
  retain their frozen numeric values and denote counts of eligible hourly-source
  observations under this policy.
- Entry channel: maximum eligible high of the previous `N` eligible
  observations, excluding eligible bar `t`; entry requires strict
  `close[t] > channel`.
- Exit channel: minimum eligible low of the previous `N` eligible observations,
  excluding `t`; exit uses strict `close[t] < channel`.
- Trend filter: simple mean of the most recent `N` eligible closes including
  `t`; entry requires strict `close[t] > mean`, while an existing long exits on
  strict `close[t] < mean`.
- Volatility: sample standard deviation of log returns between consecutive
  eligible closes through `t`, over the frozen lookback, annualized by
  `sqrt(365.25 * 24)`. A 720-return window requires 721 eligible closes; no
  artificial zero return is inserted, and volatility is unavailable one
  eligible observation earlier.
- Exposure is `min(1, target_vol / max(realized_vol, volatility_floor))`, fixed
  when the entry signal occurs and unchanged until exit.
- When long, exit logic has priority and the same close cannot exit and re-enter.
- Folds and OOS score UTC daily end-equity returns with zero cash risk-free
  rate, hourly mark-to-market drawdown, exact elapsed-time CAGR, and valid
  terminal liquidation value. The first daily return is always the evaluation
  boundary equity to the first complete UTC day end; that same boundary-aware
  daily series is used for fold, OOS, cost stress, latency stress, benchmark,
  and calendar-quarter concentration metrics.

## Registered source anomalies

Source bytes are never repaired. The exact 14 close-time variance rows and 28
gap events are pinned by `configs/data_exceptions_v1.json`, including full-row
hashes and a full-source snapshot hash. Any changed row, additional event,
missing registered event, or changed count is `SOURCE_DRIFT` or
`UNREGISTERED_DATA_EXCEPTION` and stops the study before performance code runs.
