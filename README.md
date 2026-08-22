# Ben AutoTrade Lab

An evidence-first, open-source laboratory for researching one deliberately
narrow hypothesis: a BTC/USDT spot, 1-hour, long-or-cash trend strategy can
retain a reproducible out-of-sample edge after explicit costs.

The project starts with zero live-trading authority. It can download public
market data, verify and fingerprint it, run deterministic backtests, perform
walk-forward and cost-stress validation, and maintain a local paper ledger. It
cannot authenticate to an exchange or place a real order.

## Status

`RESEARCH_NOT_YET_VALIDATED`

No result is called effective until it passes the pre-registered gates in
[`docs/RESEARCH_CONTRACT.md`](docs/RESEARCH_CONTRACT.md). Backtest performance
does not guarantee future returns.

## Safe default scope

- Market: BTC/USDT spot
- Bars: 1 hour, UTC, completed bars only
- Position: long or cash; no leverage, shorting, or borrowing
- Signal/fill rule: close of bar `t` -> earliest fill at open of `t+1`
- State/fill eligibility: official, non-synthetic bars with positive volume and
  positive trade count; all other hours advance UTC time but freeze strategy
  and pending-intent state
- Data host: Binance's unauthenticated market-data-only endpoint
- Modes: `BACKTEST`, `PAPER`; live execution is structurally unavailable

## Boundary

This software is for research and simulated trading only. It is not investment
advice. The repository contains no broker SDK, secret fields, authenticated
endpoint, or live order path.

## Why this is not a Freqtrade fork

Mature systems such as Freqtrade, NautilusTrader, and LEAN deliberately include
account, order, and live-execution surfaces. Removing those surfaces and then
proving their absence would be harder than auditing this small standard-library
core. Their event-model and testing ideas are useful references, but no live
adapter is imported here. A vectorized library may later be used in an isolated
research accelerator; its output can never finalize a candidate without replay
through this event engine.

## Reproducible workflow

Python 3.12 is required. The runtime has no third-party dependency.

```powershell
python -m venv .venv
$env:PYTHONPATH = "$PWD\src"
& .\.venv\Scripts\python.exe -m ben_trade_lab doctor
& .\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The ordinary suite deliberately skips the local full-snapshot provenance
replay. It is safe for routine development and must not be treated as authority
to open the holdout. The source-bound `audit tests` command below launches an
isolated, credential-free replay of the exact PRE/LOCKED manifest pair bound by
the frozen selection. It emits only hashes, counters, and test status. Its
receipt authorizes review/finalization only when
`full_provenance_replay.status` is exactly `PASS`; `SKIPPED`, `ABSENT`,
`AMBIGUOUS`, or `FAIL` remains blocked.

Linux or WSL uses the same dependency-free core:

```bash
python3 -m venv .venv-linux
PYTHONPATH="$PWD/src" .venv-linux/bin/python -m ben_trade_lab doctor
PYTHONPATH="$PWD/src" .venv-linux/bin/python -m unittest discover -s tests -v
```

Fetch and verify one immutable public-data snapshot:

```powershell
& .\.venv\Scripts\python.exe -m ben_trade_lab data fetch
& .\.venv\Scripts\python.exe -m ben_trade_lab data verify --manifest <full-manifest>
& .\.venv\Scripts\python.exe -m ben_trade_lab data partition-lockbox --manifest <full-manifest>
```

The partition command emits two different content-addressed manifests. Only the
`PREHOLDOUT` manifest is accepted by candidate selection. Before any price read,
selection performs a metadata-only PRE/config gate; a supplied FULL or LOCKED
manifest is rejected. PRE binds the separate `LOCKED_HOLDOUT` manifest by exact
path, file SHA-256, paired descriptor, data commitment, and lockbox ID, but does
not read or score its price bytes.

```powershell
& .\.venv\Scripts\python.exe -m ben_trade_lab research select --manifest <preholdout-manifest>
& .\.venv\Scripts\python.exe -m ben_trade_lab audit tests --selection <selection>
& .\.venv\Scripts\python.exe -m ben_trade_lab audit record-pro-review `
  --selection <selection> --review <sanitized-review.md> --verdict PROCEED `
  --model-visible "<visible label>" --reasoning-visible "<visible label>"
```

The moving-block bootstrap stored with the selection is narrowly a family-wise
max-Sharpe null diagnostic. It is non-gating and is not a complete correction
for selecting by median walk-forward Calmar; it cannot rescue a failed gate or
upgrade the evidence label.

The canonical create-only anchor store was provisioned once outside the
repository before candidate selection. v1.2 freezes both its 64-hex `store_id`
and unique descriptor hash in the configuration. Deployment must copy the
complete store—including all records—rather than initialize an empty
replacement; there is intentionally no `anchor init`, reset, repair, or delete
command. Its machine-specific absolute path is not embedded in research
artifacts.

```powershell
& .\.venv\Scripts\python.exe -m ben_trade_lab anchor verify `
  --anchor-root '<absolute-external-anchor-root>' `
  --anchor-store-id <64-hex-store-id>
```

Opening and finalization also require the exact pre-provisioned append-only
witness inode on WSL/ext4. The frozen configuration pins its store ID, canonical
header hash, filesystem device, inode, and policy; its machine-specific absolute
path is supplied only at runtime. Verification is read-only:

```bash
PYTHONPATH="$PWD/src" .venv-linux/bin/python -m ben_trade_lab witness verify \
  --witness-ledger '<absolute-linux-ext4-witness-ledger>' \
  --witness-store-id <64-hex-store-id>
```

There is intentionally no witness initialize, reset, repair, truncate, delete,
or inode-flag command. A witness on Windows, a Drives-backed `/mnt/c` or
`/mnt/d` file, a replaced inode, or a Linux file without confirmed
`FS_APPEND_FL` fails closed.

Finalization is deliberately one shot. It requires the exact source-bound test
and independent-review receipts. The already-ended 2024-08 through 2026-07
segment is reported as `RETROSPECTIVE_LOCKED_OOS`, not prospective proof.

```bash
PYTHONPATH="$PWD/src" .venv-linux/bin/python -m ben_trade_lab research finalize \
  --manifest <locked-holdout-manifest> --selection <selection> \
  --test-receipt <test-receipt> --review-receipt <review-receipt> \
  --anchor-root '<absolute-external-anchor-root>' \
  --anchor-store-id <64-hex-store-id> \
  --witness-ledger '<absolute-linux-ext4-witness-ledger>' \
  --witness-store-id <64-hex-store-id>
```

Finalization must run under Linux/WSL. After all price-free preflight checks, it
allocates the locked OOS globally by experiment ID, lockbox ID, and holdout data
commitment. Before any locked price byte is loaded, it durably appends the
witness opening burn, creates the external `HOLDOUT_OPENED` anchor, and creates
the linked local `HOLDOUT_OPENED` state, in that order. A surviving witness burn
prevents the same experiment, lockbox, or holdout commitment from being opened
again even if the repository and primary anchor are both restored to pre-open
bytes.

After evaluation, the content-addressed report is written first. Its hash,
status, kind, opened-state hash, and external-anchor hash are then appended as a
witness finalization record before local `FINALIZED` is created. Interruption
after either append is consumed and cannot be repaired or retried by this
runtime. A failed gate is `NOT_PROVEN`; it is not permission to weaken dates,
costs, parameters, or gates.

Paper initialization is also gated. It requires a self-hashed
`BACKTEST_CANDIDATE` report plus the exact selection, passing test receipt,
`PROCEED` review receipt, one-shot experiment-state chain, frozen source tree,
configuration, lockbox, and data commitments bound by that report:

```bash
PYTHONPATH="$PWD/src" .venv-linux/bin/python -m ben_trade_lab paper init \
  --report <holdout-report> \
  --anchor-root '<absolute-external-anchor-root>' \
  --anchor-store-id <64-hex-store-id> \
  --witness-ledger '<absolute-linux-ext4-witness-ledger>' \
  --witness-store-id <64-hex-store-id>
```

Before creating the PAPER journal, initialization re-verifies the frozen
selection, receipts, LOCKED manifest metadata, witness opening and finalization
records, external anchor, and local state chain. It then deterministically
replays the primary, benchmark, cost-stress, latency-stress, and every frozen
neighbor OOS scenario and requires exact agreement with the report, including
recomputed aggregates and gates.

`paper run-once` remains fail-closed until the causal forward runner and replay
parity suite are complete. There is no command, configuration value, dependency,
or network adapter for live trading.

## Evidence layout

- `configs/`: frozen study and exact source-exception contracts
- `data/`: ignored raw/normalized bytes and generated manifests; one price-free,
  content-addressed provenance-root manifest is tracked so a fresh clone can
  reject upstream source drift before accepting downloaded bytes
- `artifacts/`: ignored selections, test receipts, reviews, and OOS reports
- `state/experiments/`: ignored one-shot state-transition receipts
- external anchor store: portable, create-only per-experiment opening records;
  it must remain outside the repository and is never committed
- external WSL/ext4 witness ledger: globally sequenced opening and finalization
  commitments on the exact frozen append-only inode; it is never committed and
  its absolute path is never embedded in research artifacts
- `docs/reviews/`: sanitized review summaries only

Start with [the research contract](docs/RESEARCH_CONTRACT.md) and
[data provenance](docs/DATA_PROVENANCE.md). The downloaded market data and all
private state remain outside Git.
