# Committed data anomalies

`configs/data_exceptions_v1.json` is the exact exception registry for the
verified full BTCUSDT 1-hour dataset. It is bound to both the manifest bytes
and normalized CSV bytes by SHA-256. The registry does not modify, repair, or
replace any source row (`source_row_modified` is `false`).

The registry admits exactly two observed source conditions:

- 14 `CLOSE_TIME_SOURCE_VARIANCE` rows. Each entry records its open time,
  observed close time, nominal interval end, trade count, zero-volume flag,
  and a SHA-256 over the complete canonical normalized row.
- 28 `MISSING_HOURLY_BARS` events containing 128 absent hours in total. Each
  entry records the adjacent official open times and missing-bar count. The
  largest event contains 33 absent hours.

For a full-row hash, serialize the 12 fields in the registry's `column_order`
exactly as they appear in the normalized CSV, separated by one ASCII comma,
with no quoting and no line terminator. Hash those UTF-8 bytes with SHA-256.

The only admitted simulation treatment is `CARRY_FORWARD_NO_FILL` policy
version `1.0.0`: missing hours may be represented in memory by a flat synthetic
bar at the preceding official close; every indicator, regime, and
pending-intent state is frozen, and no fill may occur on it. Official
zero-volume or zero-trade bars have the same state/fill ineligibility without
being relabelled synthetic. Raw and normalized source data remain unchanged.
Any additional event, changed row hash, changed source hash, or changed count
requires a new versioned registry and a new experiment identity.

The registry file itself is pinned by SHA-256 in both the frozen market config
and the data verifier. The original full-source manifest predates the registry
field, so it is accepted only as a narrowly grandfathered provenance root when
both its canonical relative path and complete file SHA-256 exactly match the
registry. Newly generated full-source manifests commit the registry SHA-256
directly. Partition manifests are never grandfathered: each must commit the
registry, its exact full-source parent manifest and normalized dataset, both
partition hashes, and the deterministically recomputed lockbox identifier.

Verification of the full source replays every content-addressed raw response
batch and requires the resulting canonical CSV bytes—not merely parsed
values—to equal the normalized dataset. Raw batch paths, hashes, boundaries,
and row counts must also exactly match the registered source manifest, so a
semantically equivalent but byte-different replacement is still source drift.
The explicitly isolated provenance replay independently requires each
partition's canonical CSV bytes to equal its exact timestamp slice of the
verified full-source parent. Ordinary PRE verification is metadata-only with
respect to the FULL parent and LOCKED prices. An unregistered event, changed
row, noncanonical CSV representation, missing commitment, or mismatched
partition is rejected as source drift.
