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
forbids fills and signal changes on it, and resumes trading only on an official
bar. This preserves wall-clock windows without pretending an outage price was
tradable.

Every manifest records the request boundary, first and last open time, row
count, raw batch hashes, normalized CSV SHA-256, and retrieval time. It is not a
digital signature. Verification replays each raw batch, checks its metadata,
reconstructs normalized bytes, validates manifest fields, and rejects paths
outside the repository data directories. Market data is mutable upstream, so
byte hashes are part of the experiment identity.

Downloaded market data is intentionally excluded from Git. The public
repository tracks one price-free provenance-root manifest containing request
boundaries, raw-batch paths and hashes, aggregate counts, and declared anomaly
metadata. It contains no OHLCV price rows. That immutable root lets a fresh
clone authenticate newly downloaded raw and normalized bytes against the
frozen study identity; all price-bearing files and generated partition
manifests remain local. Public tests otherwise use deterministic synthetic
fixtures.
