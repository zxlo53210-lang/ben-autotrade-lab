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
content-addressed normalized test log. A Pro-review receipt similarly binds a
sanitized UTF-8 review under `docs/reviews`.

Paper initialization requires the complete, mutually consistent provenance
chain produced by holdout finalization: frozen selection, passing test receipt,
`PROCEED` review receipt, holdout-opened state, finalized state, and an exact set
of passing pre-registered gates. Journal events have a hash chain, a current
head commitment, an immutable per-event commitment, and a single-writer lock.

These hashes provide local tamper evidence, not identity authentication against
an administrator who can rewrite or delete the entire repository. This project
intentionally stores no signing secret. Preserve the repository and its
artifacts on trusted storage if that stronger threat model matters.

Please report vulnerabilities without including secrets or private datasets.
For public demonstrations, use only the synthetic test fixture.
