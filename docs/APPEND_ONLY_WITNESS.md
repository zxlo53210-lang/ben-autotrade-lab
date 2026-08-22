# Non-rollback holdout-opening witness

The primary external anchor is create-only through the application, but an
ordinary filesystem snapshot is not intrinsically append-only. Deleting its
record, experiment directory, shard, `records` tree, or complete store and then
restoring pre-opening bytes can otherwise make a used experiment look unused.
A self-hash or Merkle root stored only in the repository or primary anchor is
also erased by a repository-plus-anchor rollback.

`ben_trade_lab.witness` adds an independently provisioned WSL/ext4 append-only
ledger to the implemented opening and finalization protocol. The witness is a
global one-shot allocator, not a second research result store.

## Ledger contract

The frozen configuration pins the witness `store_id`, canonical header
SHA-256, filesystem device, filesystem inode, and policy. The runtime opens that
exact regular-file inode and rejects a missing or replaced inode, a hardlink,
symlink/reparse indirection, absent `FS_APPEND_FL`, noncanonical or truncated
JSONL, a broken sequence or self-hash chain, and any conflicting allocation.

Every opening burn binds at least:

- the experiment ID and lockbox ID;
- the locked-manifest SHA-256 and holdout commitment;
- the opening commitment and opening authority;
- the primary external anchor `store_id` and canonical store-descriptor
  SHA-256.

The opening burn precedes creation of the external opening record, so it cannot
bind that record's SHA-256. The external-anchor record hash is created at the
anchor step and is subsequently bound by local `HOLDOUT_OPENED` and the witness
finalization record.

An opening is globally one-shot by each of the experiment ID, lockbox ID, and
holdout commitment. Reusing any one of them for another opening is rejected;
equality is checked across the witness, primary anchor, and local state.

The locked-opening order is fail closed:

1. Verify the frozen witness identity and prove that all three allocation keys
   are unused.
2. Append, lock, and `fsync` the witness opening burn.
3. Create and verify the external `HOLDOUT_OPENED` anchor.
4. Commit local `HOLDOUT_OPENED` state.
5. Re-read and cross-check the witness, anchor, and local state before any
   locked price-bearing bytes are loaded.

The burn is deliberately first. If a later step fails, the allocation remains
consumed; the runtime does not erase, repair, or retry it. A partial or invalid
append fails verification and is not silently repaired.

After locked evaluation, the finalizer writes and verifies the immutable final
report, appends a witness finalization bound to the opening-burn hash, opened
state hash, external-anchor hash, report hash, report kind, and report status,
and only then commits local `FINALIZED` state. PAPER initialization requires
that complete chain. It re-verifies the witness and both local/external records,
then reloads the exact locked inputs and deterministically replays the selected
strategy, costs, latency scenarios, neighbor variants, benchmark, gap counters,
and report metrics before it can create a paper journal.

## Read-only verification and CLI bindings

Use the WSL/Linux CLI against the exact preprovisioned ledger:

```bash
python -m ben_trade_lab.cli witness verify \
  --witness-ledger /var/lib/ben-trade-lab/witness/holdout-witness.jsonl \
  --witness-store-id <frozen-store-id>
```

`research finalize` and `paper init` likewise require
`--witness-ledger` and `--witness-store-id`. The ordinary CLI exposes only the
read-only witness verification command; it has no initializer, reset, repair,
truncate, delete, migration, or inode-flag command. Provisioning is a separate,
audited administrator operation. Windows/NTFS paths, `/mnt/c`, `/mnt/d`, drvfs,
and Linux files without a confirmed append-only inode flag fail closed.

## Threat boundary

The control prevents an unprivileged laboratory runtime from removing a
surviving burn while rolling back the repository and/or primary anchor. It is
not protection against Linux root or an actor holding
`CAP_LINUX_IMMUTABLE`, either of which can clear the append-only flag. It also
cannot detect rollback of the whole WSL/ext4 volume containing the witness,
because that can restore both inode identity and earlier bytes.

Stronger attackers require a genuinely retained WORM device, TPM-backed
monotonic service, or independent append-only remote transparency log. Local
hashes do not prove an event after every copy of that event has been rolled
back. The witness grants no LIVE, account, credential, order, market-semantic,
or profitability authority.
