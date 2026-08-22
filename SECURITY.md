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
`PROCEED` review receipt, append-only witness opening burn, external opening
anchor, holdout-opened state, immutable report, append-only witness finalization,
finalized state, and an exact set of passing pre-registered gates. It validates
every nested performance object, re-derives selection aggregates, recomputes
the gates from the frozen metrics and thresholds, and rejects changed cost,
latency, neighbor, window, or initial-cash semantics. It also deterministically
replays the primary, benchmark, cost-stress, latency-stress, and frozen-neighbor
LOCKED scenarios and requires exact report agreement before creating the PAPER
journal. Journal events have a hash chain, a current head commitment, an
immutable per-event commitment, and a single-writer lock.

The external Linux witness is a pre-provisioned regular-file inode on a
filesystem that enforces `FS_APPEND_FL`, normally WSL/ext4. Configuration pins
its store ID, header SHA-256, filesystem device, inode, and policy; the runtime
path is supplied explicitly and is not stored in research artifacts. Canonical
JSONL records are globally sequenced, self-hashed, and hash-chained. An opening
burn is unique across experiment ID, lockbox ID, and holdout data commitment.
The finalization record binds the burn, local opened-state hash, external anchor,
report hash, status, and kind. The runtime exposes verification and append-only
commit operations but no initializer, reset, repair, truncate, delete, or
inode-flag operation.

Within its stated threat model, a surviving witness burn prevents an
unprivileged laboratory process from silently re-arming a consumed allocation
by deleting or rolling back the repository and primary anchor. This is not an
administrator-proof or hardware-WORM claim. Linux root or a process with
`CAP_LINUX_IMMUTABLE` can clear the append-only flag, and rollback of the whole
WSL/ext4 volume can restore the inode identity and ledger bytes together. A
genuinely retained WORM device, TPM-backed monotonic service, or independent
remote transparency witness is required for those stronger threats. This
project intentionally stores no signing secret.

Please report vulnerabilities without including secrets or private datasets.
For public demonstrations, use only the synthetic test fixture.
