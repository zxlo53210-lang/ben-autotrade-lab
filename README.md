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
`PREHOLDOUT` manifest is accepted by candidate selection. It commits to, but
does not score, the separate `LOCKED_HOLDOUT` bytes.

```powershell
& .\.venv\Scripts\python.exe -m ben_trade_lab research select --manifest <preholdout-manifest>
& .\.venv\Scripts\python.exe -m ben_trade_lab audit tests --selection <selection>
& .\.venv\Scripts\python.exe -m ben_trade_lab audit record-pro-review `
  --selection <selection> --review <sanitized-review.md> --verdict PROCEED `
  --model-visible "<visible label>" --reasoning-visible "<visible label>"
```

Finalization is deliberately one shot. It requires the exact source-bound test
and independent-review receipts. The already-ended 2024-08 through 2026-07
segment is reported as `RETROSPECTIVE_LOCKED_OOS`, not prospective proof.

```powershell
& .\.venv\Scripts\python.exe -m ben_trade_lab research finalize `
  --manifest <locked-holdout-manifest> --selection <selection> `
  --test-receipt <test-receipt> --review-receipt <review-receipt>
```

Once an experiment records `HOLDOUT_OPENED`, interruption or failure cannot be
retried against the same study generation. A failure is `NOT_PROVEN`; it is not
permission to weaken dates, costs, parameters, or gates.

Paper initialization is also gated. It requires a self-hashed
`BACKTEST_CANDIDATE` report plus the exact selection, passing test receipt,
`PROCEED` review receipt, one-shot experiment-state chain, frozen source tree,
configuration, lockbox, and data commitments bound by that report:

```powershell
& .\.venv\Scripts\python.exe -m ben_trade_lab paper init --report <holdout-report>
```

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
- `docs/reviews/`: sanitized review summaries only

Start with [the research contract](docs/RESEARCH_CONTRACT.md) and
[data provenance](docs/DATA_PROVENANCE.md). The downloaded market data and all
private state remain outside Git.
