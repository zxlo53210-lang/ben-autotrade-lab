# Security policy

Only offline backtests and unauthenticated public-data paper simulations are in
scope. Do not submit exchange credentials, wallet data, account exports, or
personal financial information in an issue or pull request.

Live execution is unavailable by design. A proposal that adds authentication,
private account data, order placement, order cancellation, leverage, shorting,
or secret storage violates this project's safety contract and must not be
merged.

The only permitted network surface is unauthenticated `GET` access implemented
in `data.py` against the exact Binance market-data-only origin. Paper mode is a
local simulation and has no broker, wallet, exchange-account, or order API.

Test subprocesses receive an allowlisted infrastructure environment rather than
the caller's environment, so credentials and tokens are not inherited. A test
receipt binds the source tree both before and after the run and binds the
content-addressed normalized test log. The ordinary suite skips the full local
snapshot replay; only an isolated audit receipt whose
`full_provenance_replay.status` is exactly `PASS` can authorize holdout review
or finalization. That replay may emit hashes, counters, and PASS/FAIL only; it
must never emit locked price rows or call strategy/performance code. A
Pro-review receipt similarly binds a sanitized UTF-8 review under
`docs/reviews`.

Paper initialization requires the complete, mutually consistent provenance
chain produced by holdout finalization: frozen selection, passing test receipt,
`PROCEED` review receipt, holdout-opened state, finalized state, and an exact set
of passing pre-registered gates. It also revalidates the external create-only
opening anchor bound by the local state. Journal events have a hash chain, a
current head commitment, an immutable per-event commitment, and a single-writer
lock.

The external anchor prevents an ordinary repository rollback or deletion from
silently reopening the same experiment. Configuration pins both the store ID
and its unique canonical descriptor hash, so a newly initialized empty store
with the same public ID is rejected. These unsigned hashes still do not
authenticate against an administrator who copied the descriptor before use,
selectively deletes records, or can delete or rewrite every copy of both the
repository and the entire anchor store. This project intentionally stores no
signing secret. Preserve the repository, artifacts, descriptor, and complete
anchor store on trusted storage; use ACLs, an offline read-only copy, or WORM
storage if that stronger threat model matters.

Please report vulnerabilities without including secrets or private datasets.
For public demonstrations, use only the synthetic test fixture.
