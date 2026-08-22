# Data provenance

The downloader uses only Binance's market-data-only REST host and the public
`GET /api/v3/klines` route. This route requires no API key. Each response batch
is saved locally, and the normalized dataset is accepted only after schema,
ordering, uniqueness, hourly alignment, declared-gap, OHLC, and completed-bar
checks pass.

The authoritative time axis is the documented kline `open time`, which must be
strictly increasing and hour-aligned except for exactly registered exchange
gaps. Some early Binance rows report a source
`close time` inside the hour instead of the nominal final millisecond. Such a
row is accepted only when the close time remains inside its own interval; every
variance is preserved verbatim and enumerated in the content-addressed
manifest. A close
time outside its interval remains a hard failure. Strategy code never uses
`close time` as a feature.

Exchange outages also produce missing hourly rows. The raw and normalized
datasets remain untouched and every gap is enumerated in the manifest. For the
simulator only, the pre-registered `CARRY_FORWARD_NO_FILL` policy inserts an
in-memory synthetic flat bar at the last official close, marks it synthetic,
forbids fills, signal changes, pending-intent changes, and rolling-indicator
aging on it, and resumes strategy-state updates only on an official bar with
positive volume and positive trade count. UTC timestamps still advance for
elapsed-time accounting, but strategy lookbacks contain eligible observations
only; an outage price is never treated as tradable evidence. Official
zero-volume or zero-trade bars use the same frozen-state/no-fill rule while
remaining identified as official source rows.

The full-source manifest records the request boundary, first and last open time,
row count, raw batch hashes, normalized CSV SHA-256, and retrieval time. It is
not a digital signature. Full verification replays each raw batch, checks its
metadata, reconstructs normalized bytes, validates manifest fields, and rejects
paths outside the repository data directories. Market data is mutable upstream,
so byte hashes are part of the experiment identity.

The deterministic partition step writes exact-schema v1.2 PRE and LOCKED
manifests. Their descriptors bind the common parent/config/registry/lockbox/data
metadata in both directions, while PRE additionally binds the exact LOCKED
manifest path and file SHA-256. Ordinary candidate selection first accepts only
PRE metadata, then reads only PRE prices. FULL and LOCKED inputs fail before any
price-bearing normalized or raw file is opened.

Full-source replay is an explicitly gated, data-only operation. It reloads the
FULL source and proves that the exact selection-bound PRE and LOCKED bytes are
canonical parent slices. It cannot call strategy/metric code or return price
rows to the researcher/model. Its allowed external evidence is limited to
hashes, counts, and PASS/FAIL. The default test suite does not enable this
replay. Ordinary LOCKED access remains blocked until the exact WSL/ext4
append-only witness has durably allocated the experiment ID, lockbox ID, and
holdout data commitment, after which the external anchor and local one-shot
`HOLDOUT_OPENED` state must also be durably committed and cross-validated. The
price-bearing load is re-bound to the complete pre-open manifest object before
any metric is calculated.

After evaluation, the content-addressed report is written before a second
append-only witness record binds its hash, status, kind, opened-state hash, and
external anchor. Local `FINALIZED` is written only after that witness
finalization is durable. PAPER initialization revalidates this full chain and
then independently reloads the exact LOCKED manifest and deterministically
replays every reported primary, benchmark, cost-stress, latency-stress, and
frozen-neighbor metric scenario before granting PAPER authority.

Downloaded market data is intentionally excluded from Git. The public
repository tracks one price-free provenance-root manifest containing request
boundaries, raw-batch paths and hashes, aggregate counts, and declared anomaly
metadata. It contains no OHLCV price rows. That immutable root lets a fresh
clone authenticate newly downloaded raw and normalized bytes against the
frozen study identity; all price-bearing files and generated partition
manifests remain local. Public tests otherwise use deterministic synthetic
fixtures.
