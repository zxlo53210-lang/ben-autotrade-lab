"""Linux append-only witness for irreversible holdout-opening allocation.

The ledger inode must be provisioned outside this runtime, on a filesystem that
implements ``FS_APPEND_FL``.  Its device, inode, store ID, and canonical header
hash are pinned before use.  Runtime code can verify and append one burn record;
it exposes no initialize, reset, repair, truncate, or delete operation.

This protects against rollback by the unprivileged laboratory process.  Linux
root or a process with ``CAP_LINUX_IMMUTABLE`` can clear the append-only flag,
and rollback of the entire filesystem/WSL volume can restore the inode and its
contents together.  Those stronger threats require genuine WORM storage or an
independent remote transparency witness and cannot be solved by local hashes.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import canonical_json
from .integrity import fsync_directory, is_link_or_reparse, is_sha256

WITNESS_SCHEMA_VERSION = "1.0.0"
WITNESS_LEDGER_TYPE = "BEN_AUTOTRADE_NONROLLBACK_WITNESS_LEDGER"
WITNESS_BURN_TYPE = "BEN_AUTOTRADE_HOLDOUT_OPENING_BURN"
WITNESS_BURN_EVENT = "HOLDOUT_OPENING_BURNED"
WITNESS_FINALIZATION_TYPE = "BEN_AUTOTRADE_HOLDOUT_FINALIZATION_COMMITMENT"
WITNESS_FINALIZATION_EVENT = "HOLDOUT_REPORT_FINALIZED"
WITNESS_POLICY = "LINUX_FS_APPEND_FL_ONE_SHOT_BURN_LEDGER_V1"
GENESIS_RECORD_SHA256 = "0" * 64

# linux/fs.h.  A filesystem that does not implement this ioctl/flag is not an
# acceptable witness medium and is rejected rather than silently downgraded.
FS_IOC_GETFLAGS = 0x80086601
FS_APPEND_FL = 0x00000020

HEADER_FIELDS = frozenset(
    {
        "schema_version",
        "type",
        "store_id",
        "created_at_utc",
        "policy",
        "filesystem_device",
        "filesystem_inode",
        "sequence",
        "previous_record_sha256",
        "authority",
        "capability",
        "record_sha256",
    }
)
BURN_FIELDS = frozenset(
    {
        "schema_version",
        "type",
        "event",
        "store_id",
        "sequence",
        "previous_record_sha256",
        "experiment_id",
        "lockbox_id",
        "locked_holdout_manifest_sha256",
        "holdout_commitment_sha256",
        "burned_at_utc",
        "anchor_store_id",
        "anchor_store_sha256",
        "opening_commitment_sha256",
        "authority",
        "capability",
        "record_sha256",
    }
)
FINALIZATION_FIELDS = frozenset(
    {
        "schema_version",
        "type",
        "event",
        "store_id",
        "sequence",
        "previous_record_sha256",
        "experiment_id",
        "finalized_at_utc",
        "opening_burn_record_sha256",
        "opened_state_sha256",
        "external_anchor_sha256",
        "report_sha256",
        "report_status",
        "report_kind",
        "authority",
        "capability",
        "record_sha256",
    }
)


class WitnessError(RuntimeError):
    """Base error for the non-rollback opening witness."""


class WitnessPlatformError(WitnessError):
    """The runtime or backing filesystem cannot enforce the witness contract."""


class WitnessPathError(WitnessError):
    """The witness path is missing, linked, or overlaps the repository."""


class WitnessCorruption(WitnessError):
    """The append-only ledger is malformed, truncated, replaced, or ambiguous."""


class WitnessAlreadyBurned(WitnessError):
    """The experiment has already consumed its only holdout opening allocation."""


class WitnessAlreadyFinalized(WitnessError):
    """The experiment already has an immutable final-report commitment."""


@dataclass(frozen=True, slots=True)
class OpeningBurn:
    experiment_id: str
    sequence: int
    lockbox_id: str
    locked_holdout_manifest_sha256: str
    holdout_commitment_sha256: str
    anchor_store_id: str
    anchor_store_sha256: str
    opening_commitment_sha256: str
    burned_at_utc: str
    record_sha256: str


@dataclass(frozen=True, slots=True)
class FinalizationCommitment:
    experiment_id: str
    sequence: int
    opening_burn_record_sha256: str
    opened_state_sha256: str
    external_anchor_sha256: str
    report_sha256: str
    report_status: str
    report_kind: str
    finalized_at_utc: str
    record_sha256: str


@dataclass(frozen=True, slots=True)
class AppendOnlyWitnessLedger:
    path: Path
    repository_root: Path
    store_id: str
    header_sha256: str
    filesystem_device: int
    filesystem_inode: int
    head_sha256: str
    burns: tuple[OpeningBurn, ...]
    finalizations: tuple[FinalizationCommitment, ...]

    def burn_for(self, experiment_id: str) -> OpeningBurn | None:
        for burn in self.burns:
            if burn.experiment_id == experiment_id:
                return burn
        return None

    def finalization_for(self, experiment_id: str) -> FinalizationCommitment | None:
        for finalization in self.finalizations:
            if finalization.experiment_id == experiment_id:
                return finalization
        return None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _is_utc_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not value.endswith("Z") or value != value.strip():
        return False
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        return False
    return parsed.utcoffset() == UTC.utcoffset(parsed)


def _require_supported_platform() -> None:
    if (
        os.name != "posix"
        or not sys.platform.startswith("linux")
        or not hasattr(os, "O_NOFOLLOW")
        or not hasattr(os, "O_APPEND")
    ):
        raise WitnessPlatformError("WITNESS_REQUIRES_LINUX_FS_APPEND_FL")


def _reject_link_components(path: Path) -> None:
    if not path.is_absolute():
        raise WitnessPathError("WITNESS_PATH_MUST_BE_ABSOLUTE")
    if ".." in path.parts:
        raise WitnessPathError("WITNESS_PATH_PARENT_TRAVERSAL_REJECTED")
    components = path.parts
    current = Path(components[0])
    if is_link_or_reparse(current):
        raise WitnessPathError("WITNESS_PATH_LINK_OR_REPARSE_REJECTED")
    for component in components[1:]:
        current /= component
        if (current.exists() or current.is_symlink()) and is_link_or_reparse(current):
            raise WitnessPathError("WITNESS_PATH_LINK_OR_REPARSE_REJECTED")


def _resolve_witness_path(
    path: str | Path,
    *,
    repository_root: str | Path,
) -> tuple[Path, Path]:
    _require_supported_platform()
    supplied_path = Path(path)
    supplied_repository = Path(repository_root)
    _reject_link_components(supplied_path)
    _reject_link_components(supplied_repository)
    try:
        repository = supplied_repository.resolve(strict=True)
        ledger = supplied_path.resolve(strict=True)
    except FileNotFoundError as exc:
        raise WitnessPathError("WITNESS_OR_REPOSITORY_MISSING") from exc
    try:
        repository_metadata = repository.lstat()
    except FileNotFoundError as exc:
        raise WitnessPathError("WITNESS_REPOSITORY_MISSING") from exc
    if is_link_or_reparse(repository) or not stat.S_ISDIR(repository_metadata.st_mode):
        raise WitnessPathError("WITNESS_REPOSITORY_NOT_PLAIN_DIRECTORY")
    if ledger == repository or ledger.is_relative_to(repository):
        raise WitnessPathError("WITNESS_MUST_BE_OUTSIDE_REPOSITORY")
    return ledger, repository


def _require_expected_identity(expected_device: object, expected_inode: object) -> None:
    if (
        not isinstance(expected_device, int)
        or isinstance(expected_device, bool)
        or expected_device < 0
    ):
        raise ValueError("EXPECTED_WITNESS_DEVICE_INVALID")
    if (
        not isinstance(expected_inode, int)
        or isinstance(expected_inode, bool)
        or expected_inode <= 0
    ):
        raise ValueError("EXPECTED_WITNESS_INODE_INVALID")


def _require_plain_identity(
    metadata: os.stat_result,
    *,
    expected_device: int,
    expected_inode: int,
) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise WitnessCorruption("WITNESS_NOT_REGULAR_FILE")
    if int(metadata.st_nlink) != 1:
        raise WitnessCorruption("WITNESS_HARDLINK_REJECTED")
    if int(metadata.st_dev) != expected_device:
        raise WitnessCorruption("WITNESS_FILESYSTEM_DEVICE_MISMATCH")
    if int(metadata.st_ino) != expected_inode:
        raise WitnessCorruption("WITNESS_FILESYSTEM_INODE_MISMATCH")


def _require_path_identity(
    path: Path,
    *,
    expected_device: int,
    expected_inode: int,
) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise WitnessCorruption("WITNESS_LEDGER_MISSING") from exc
    if is_link_or_reparse(path):
        raise WitnessCorruption("WITNESS_LINK_OR_REPARSE_REJECTED")
    _require_plain_identity(
        metadata,
        expected_device=expected_device,
        expected_inode=expected_inode,
    )
    return metadata


def _read_inode_flags(descriptor: int) -> int:
    try:
        import array
        import fcntl

        flags = array.array("I", [0])
        fcntl.ioctl(descriptor, FS_IOC_GETFLAGS, flags, True)
    except (ImportError, OSError) as exc:
        raise WitnessPlatformError("WITNESS_FS_IOC_GETFLAGS_UNAVAILABLE") from exc
    return int(flags[0])


def _require_append_only(descriptor: int) -> None:
    if _read_inode_flags(descriptor) & FS_APPEND_FL == 0:
        raise WitnessPlatformError("WITNESS_FS_APPEND_FL_REQUIRED")


@contextmanager
def _shared_lock(descriptor: int) -> Iterator[None]:
    try:
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_SH)
    except (ImportError, OSError) as exc:
        raise WitnessPlatformError("WITNESS_SHARED_LOCK_UNAVAILABLE") from exc
    try:
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)


@contextmanager
def _exclusive_lock(descriptor: int) -> Iterator[None]:
    try:
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_EX)
    except (ImportError, OSError) as exc:
        raise WitnessPlatformError("WITNESS_EXCLUSIVE_LOCK_UNAVAILABLE") from exc
    try:
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)


def _read_all(descriptor: int) -> bytes:
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    while True:
        chunk = os.read(descriptor, 64 * 1024)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


def _validate_self_hash(value: Mapping[str, Any], *, label: str) -> None:
    supplied = value.get("record_sha256")
    if not is_sha256(supplied):
        raise WitnessCorruption(f"{label}_RECORD_SHA256_MALFORMED")
    unsigned = {key: item for key, item in value.items() if key != "record_sha256"}
    expected = hashlib.sha256(canonical_json(unsigned)).hexdigest()
    if supplied != expected:
        raise WitnessCorruption(f"{label}_RECORD_SHA256_MISMATCH")


def _validate_common_authority(value: Mapping[str, Any], *, label: str) -> None:
    if value.get("authority") != "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY":
        raise WitnessCorruption(f"{label}_AUTHORITY_MISMATCH")
    if value.get("capability") != "LIVE_DISABLED":
        raise WitnessCorruption(f"{label}_CAPABILITY_MISMATCH")


def _parse_ledger(
    payload: bytes,
    *,
    expected_store_id: str,
    expected_header_sha256: str,
    expected_device: int,
    expected_inode: int,
) -> tuple[
    str,
    tuple[OpeningBurn, ...],
    tuple[FinalizationCommitment, ...],
]:
    if not payload or not payload.endswith(b"\n"):
        raise WitnessCorruption("WITNESS_LEDGER_TRUNCATED")
    encoded_lines = payload[:-1].split(b"\n")
    if not encoded_lines or any(not line for line in encoded_lines):
        raise WitnessCorruption("WITNESS_LEDGER_EMPTY_OR_AMBIGUOUS_LINE")

    values: list[dict[str, Any]] = []
    for encoded in encoded_lines:
        try:
            value = json.loads(encoded.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WitnessCorruption("WITNESS_LEDGER_INVALID_JSON") from exc
        if not isinstance(value, dict):
            raise WitnessCorruption("WITNESS_LEDGER_RECORD_NOT_OBJECT")
        if encoded != canonical_json(value):
            raise WitnessCorruption("WITNESS_LEDGER_RECORD_NONCANONICAL")
        values.append(value)

    header = values[0]
    if set(header) != HEADER_FIELDS:
        raise WitnessCorruption("WITNESS_HEADER_SCHEMA_MISMATCH")
    if header.get("schema_version") != WITNESS_SCHEMA_VERSION:
        raise WitnessCorruption("WITNESS_HEADER_SCHEMA_VERSION_MISMATCH")
    if header.get("type") != WITNESS_LEDGER_TYPE:
        raise WitnessCorruption("WITNESS_HEADER_TYPE_MISMATCH")
    if header.get("policy") != WITNESS_POLICY:
        raise WitnessCorruption("WITNESS_HEADER_POLICY_MISMATCH")
    if header.get("store_id") != expected_store_id:
        raise WitnessCorruption("WITNESS_STORE_ID_MISMATCH")
    if not is_sha256(header.get("store_id")):
        raise WitnessCorruption("WITNESS_STORE_ID_MALFORMED")
    if not _is_utc_timestamp(header.get("created_at_utc")):
        raise WitnessCorruption("WITNESS_HEADER_CREATED_AT_INVALID")
    if header.get("sequence") != 0 or isinstance(header.get("sequence"), bool):
        raise WitnessCorruption("WITNESS_HEADER_SEQUENCE_MISMATCH")
    if header.get("previous_record_sha256") != GENESIS_RECORD_SHA256:
        raise WitnessCorruption("WITNESS_HEADER_PREVIOUS_HASH_MISMATCH")
    if header.get("filesystem_device") != expected_device:
        raise WitnessCorruption("WITNESS_HEADER_DEVICE_MISMATCH")
    if header.get("filesystem_inode") != expected_inode:
        raise WitnessCorruption("WITNESS_HEADER_INODE_MISMATCH")
    _validate_common_authority(header, label="WITNESS_HEADER")
    _validate_self_hash(header, label="WITNESS_HEADER")
    if header["record_sha256"] != expected_header_sha256:
        raise WitnessCorruption("WITNESS_HEADER_SHA256_MISMATCH")

    previous = str(header["record_sha256"])
    seen_experiments: set[str] = set()
    seen_lockboxes: set[str] = set()
    seen_holdout_commitments: set[str] = set()
    finalized_experiments: set[str] = set()
    burns: list[OpeningBurn] = []
    finalizations: list[FinalizationCommitment] = []
    for expected_sequence, value in enumerate(values[1:], start=1):
        if value.get("sequence") != expected_sequence or isinstance(
            value.get("sequence"), bool
        ):
            raise WitnessCorruption("WITNESS_RECORD_SEQUENCE_MISMATCH")
        if value.get("previous_record_sha256") != previous:
            raise WitnessCorruption("WITNESS_RECORD_PREVIOUS_HASH_MISMATCH")
        if value.get("store_id") != expected_store_id:
            raise WitnessCorruption("WITNESS_RECORD_STORE_ID_MISMATCH")

        record_type = value.get("type")
        if record_type == WITNESS_BURN_TYPE:
            if set(value) != BURN_FIELDS:
                raise WitnessCorruption("WITNESS_BURN_SCHEMA_MISMATCH")
            if value.get("schema_version") != WITNESS_SCHEMA_VERSION:
                raise WitnessCorruption("WITNESS_BURN_SCHEMA_VERSION_MISMATCH")
            if value.get("event") != WITNESS_BURN_EVENT:
                raise WitnessCorruption("WITNESS_BURN_EVENT_MISMATCH")
            if not _is_utc_timestamp(value.get("burned_at_utc")):
                raise WitnessCorruption("WITNESS_BURN_TIMESTAMP_INVALID")
            for field in (
                "experiment_id",
                "lockbox_id",
                "locked_holdout_manifest_sha256",
                "holdout_commitment_sha256",
                "anchor_store_id",
                "anchor_store_sha256",
                "opening_commitment_sha256",
            ):
                if not is_sha256(value.get(field)):
                    raise WitnessCorruption(f"WITNESS_BURN_{field.upper()}_MALFORMED")
            experiment_id = str(value["experiment_id"])
            lockbox_id = str(value["lockbox_id"])
            holdout_commitment = str(value["holdout_commitment_sha256"])
            if experiment_id in seen_experiments:
                raise WitnessCorruption("WITNESS_DUPLICATE_EXPERIMENT_BURN")
            if lockbox_id in seen_lockboxes:
                raise WitnessCorruption("WITNESS_DUPLICATE_LOCKBOX_BURN")
            if holdout_commitment in seen_holdout_commitments:
                raise WitnessCorruption("WITNESS_DUPLICATE_HOLDOUT_COMMITMENT_BURN")
            _validate_common_authority(value, label="WITNESS_BURN")
            _validate_self_hash(value, label="WITNESS_BURN")
            previous = str(value["record_sha256"])
            seen_experiments.add(experiment_id)
            seen_lockboxes.add(lockbox_id)
            seen_holdout_commitments.add(holdout_commitment)
            burns.append(
                OpeningBurn(
                    experiment_id=experiment_id,
                    sequence=expected_sequence,
                    lockbox_id=lockbox_id,
                    locked_holdout_manifest_sha256=str(
                        value["locked_holdout_manifest_sha256"]
                    ),
                    holdout_commitment_sha256=holdout_commitment,
                    anchor_store_id=str(value["anchor_store_id"]),
                    anchor_store_sha256=str(value["anchor_store_sha256"]),
                    opening_commitment_sha256=str(
                        value["opening_commitment_sha256"]
                    ),
                    burned_at_utc=str(value["burned_at_utc"]),
                    record_sha256=previous,
                )
            )
            continue

        if record_type == WITNESS_FINALIZATION_TYPE:
            if set(value) != FINALIZATION_FIELDS:
                raise WitnessCorruption("WITNESS_FINALIZATION_SCHEMA_MISMATCH")
            if value.get("schema_version") != WITNESS_SCHEMA_VERSION:
                raise WitnessCorruption("WITNESS_FINALIZATION_SCHEMA_VERSION_MISMATCH")
            if value.get("event") != WITNESS_FINALIZATION_EVENT:
                raise WitnessCorruption("WITNESS_FINALIZATION_EVENT_MISMATCH")
            if not _is_utc_timestamp(value.get("finalized_at_utc")):
                raise WitnessCorruption("WITNESS_FINALIZATION_TIMESTAMP_INVALID")
            for field in (
                "experiment_id",
                "opening_burn_record_sha256",
                "opened_state_sha256",
                "external_anchor_sha256",
                "report_sha256",
            ):
                if not is_sha256(value.get(field)):
                    raise WitnessCorruption(
                        f"WITNESS_FINALIZATION_{field.upper()}_MALFORMED"
                    )
            experiment_id = str(value["experiment_id"])
            opening = next(
                (item for item in burns if item.experiment_id == experiment_id),
                None,
            )
            if opening is None:
                raise WitnessCorruption("WITNESS_FINALIZATION_WITHOUT_OPENING_BURN")
            if value.get("opening_burn_record_sha256") != opening.record_sha256:
                raise WitnessCorruption("WITNESS_FINALIZATION_OPENING_BURN_MISMATCH")
            if experiment_id in finalized_experiments:
                raise WitnessCorruption("WITNESS_DUPLICATE_EXPERIMENT_FINALIZATION")
            if value.get("report_status") not in {"BACKTEST_CANDIDATE", "NOT_PROVEN"}:
                raise WitnessCorruption("WITNESS_FINALIZATION_REPORT_STATUS_INVALID")
            if value.get("report_kind") not in {
                "LOCKED_OOS_EVALUATION",
                "TERMINAL_LIQUIDATION_FAILURE",
            }:
                raise WitnessCorruption("WITNESS_FINALIZATION_REPORT_KIND_INVALID")
            if (
                value.get("report_kind") == "TERMINAL_LIQUIDATION_FAILURE"
                and value.get("report_status") != "NOT_PROVEN"
            ):
                raise WitnessCorruption(
                    "WITNESS_FINALIZATION_REPORT_KIND_STATUS_MISMATCH"
                )
            _validate_common_authority(value, label="WITNESS_FINALIZATION")
            _validate_self_hash(value, label="WITNESS_FINALIZATION")
            previous = str(value["record_sha256"])
            finalized_experiments.add(experiment_id)
            finalizations.append(
                FinalizationCommitment(
                    experiment_id=experiment_id,
                    sequence=expected_sequence,
                    opening_burn_record_sha256=str(
                        value["opening_burn_record_sha256"]
                    ),
                    opened_state_sha256=str(value["opened_state_sha256"]),
                    external_anchor_sha256=str(value["external_anchor_sha256"]),
                    report_sha256=str(value["report_sha256"]),
                    report_status=str(value["report_status"]),
                    report_kind=str(value["report_kind"]),
                    finalized_at_utc=str(value["finalized_at_utc"]),
                    record_sha256=previous,
                )
            )
            continue

        raise WitnessCorruption("WITNESS_RECORD_TYPE_INVALID")
    return previous, tuple(burns), tuple(finalizations)


def _verify_locked_descriptor(
    descriptor: int,
    *,
    path: Path,
    repository_root: Path,
    expected_store_id: str,
    expected_header_sha256: str,
    expected_device: int,
    expected_inode: int,
) -> AppendOnlyWitnessLedger:
    before = os.fstat(descriptor)
    _require_plain_identity(
        before,
        expected_device=expected_device,
        expected_inode=expected_inode,
    )
    _require_append_only(descriptor)
    payload = _read_all(descriptor)
    head, burns, finalizations = _parse_ledger(
        payload,
        expected_store_id=expected_store_id,
        expected_header_sha256=expected_header_sha256,
        expected_device=expected_device,
        expected_inode=expected_inode,
    )
    after = os.fstat(descriptor)
    _require_plain_identity(
        after,
        expected_device=expected_device,
        expected_inode=expected_inode,
    )
    if before.st_size != after.st_size:
        raise WitnessCorruption("WITNESS_LEDGER_CHANGED_DURING_READ")
    _require_append_only(descriptor)
    _require_path_identity(
        path,
        expected_device=expected_device,
        expected_inode=expected_inode,
    )
    return AppendOnlyWitnessLedger(
        path=path,
        repository_root=repository_root,
        store_id=expected_store_id,
        header_sha256=expected_header_sha256,
        filesystem_device=expected_device,
        filesystem_inode=expected_inode,
        head_sha256=head,
        burns=burns,
        finalizations=finalizations,
    )


def _open_flags(*, writable: bool) -> int:
    access = os.O_RDWR if writable else os.O_RDONLY
    flags = access | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_BINARY", 0)
    # ``_require_supported_platform`` guarantees a non-zero O_NOFOLLOW in
    # production.  The fallback keeps the pure parsing tests portable.
    flags |= getattr(os, "O_NOFOLLOW", 0)
    if writable:
        flags |= os.O_APPEND
    return flags


def verify_witness_ledger(
    path: str | Path,
    *,
    repository_root: str | Path,
    expected_store_id: str,
    expected_header_sha256: str,
    expected_device: int,
    expected_inode: int,
) -> AppendOnlyWitnessLedger:
    """Verify the exact pre-provisioned Linux append-only witness inode.

    This function never provisions, repairs, truncates, or replaces the ledger.
    The header hash plus ``st_dev``/``st_ino`` must be pinned outside runtime
    state (normally the frozen configuration) before any opening is burned.
    """

    if not is_sha256(expected_store_id):
        raise ValueError("EXPECTED_WITNESS_STORE_ID_MALFORMED")
    if not is_sha256(expected_header_sha256):
        raise ValueError("EXPECTED_WITNESS_HEADER_SHA256_MALFORMED")
    _require_expected_identity(expected_device, expected_inode)
    ledger_path, repository = _resolve_witness_path(
        path,
        repository_root=repository_root,
    )
    _require_path_identity(
        ledger_path,
        expected_device=expected_device,
        expected_inode=expected_inode,
    )
    try:
        descriptor = os.open(ledger_path, _open_flags(writable=False))
    except OSError as exc:
        raise WitnessPathError("WITNESS_LEDGER_OPEN_FAILED") from exc
    try:
        with _shared_lock(descriptor):
            return _verify_locked_descriptor(
                descriptor,
                path=ledger_path,
                repository_root=repository,
                expected_store_id=expected_store_id,
                expected_header_sha256=expected_header_sha256,
                expected_device=expected_device,
                expected_inode=expected_inode,
            )
    finally:
        os.close(descriptor)


def _require_no_opening_allocation(
    ledger: AppendOnlyWitnessLedger,
    *,
    experiment_id: str,
    lockbox_id: str | None,
    holdout_commitment_sha256: str | None,
) -> None:
    for burn in ledger.burns:
        if (
            burn.experiment_id == experiment_id
            or (lockbox_id is not None and burn.lockbox_id == lockbox_id)
            or (
                holdout_commitment_sha256 is not None
                and burn.holdout_commitment_sha256 == holdout_commitment_sha256
            )
        ):
            raise WitnessAlreadyBurned("WITNESS_HOLDOUT_ALREADY_OPENED_NOT_RETRYABLE")


def assert_unburned(
    ledger: AppendOnlyWitnessLedger,
    experiment_id: str,
    *,
    lockbox_id: str | None = None,
    holdout_commitment_sha256: str | None = None,
) -> None:
    """Fail if this experiment or locked OOS allocation was ever consumed."""

    for field, value in (
        ("EXPERIMENT_ID", experiment_id),
        ("LOCKBOX_ID", lockbox_id),
        ("HOLDOUT_COMMITMENT_SHA256", holdout_commitment_sha256),
    ):
        if value is not None and not is_sha256(value):
            raise ValueError(f"{field}_MALFORMED")
    current = verify_witness_ledger(
        ledger.path,
        repository_root=ledger.repository_root,
        expected_store_id=ledger.store_id,
        expected_header_sha256=ledger.header_sha256,
        expected_device=ledger.filesystem_device,
        expected_inode=ledger.filesystem_inode,
    )
    _require_no_opening_allocation(
        current,
        experiment_id=experiment_id,
        lockbox_id=lockbox_id,
        holdout_commitment_sha256=holdout_commitment_sha256,
    )


def _self_hashed(unsigned: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(unsigned)
    value["record_sha256"] = hashlib.sha256(canonical_json(value)).hexdigest()
    return value


def burn_opening(
    ledger: AppendOnlyWitnessLedger,
    *,
    experiment_id: str,
    lockbox_id: str,
    locked_holdout_manifest_sha256: str,
    holdout_commitment_sha256: str,
    anchor_store_id: str,
    anchor_store_sha256: str,
    opening_commitment_sha256: str,
    burned_at_utc: str | None = None,
) -> OpeningBurn:
    """Irreversibly allocate one experiment before the primary anchor write.

    The caller must treat any exception after this function starts as consumed.
    A complete canonical record is issued with one locked ``O_APPEND`` write and
    fsynced.  A short/partial write intentionally corrupts (burns) the ledger;
    it is never repaired or retried.
    """

    for field, value in (
        ("EXPERIMENT_ID", experiment_id),
        ("LOCKBOX_ID", lockbox_id),
        ("LOCKED_HOLDOUT_MANIFEST_SHA256", locked_holdout_manifest_sha256),
        ("HOLDOUT_COMMITMENT_SHA256", holdout_commitment_sha256),
        ("ANCHOR_STORE_ID", anchor_store_id),
        ("ANCHOR_STORE_SHA256", anchor_store_sha256),
        ("OPENING_COMMITMENT_SHA256", opening_commitment_sha256),
    ):
        if not is_sha256(value):
            raise ValueError(f"{field}_MALFORMED")
    burned_at = _utc_now() if burned_at_utc is None else burned_at_utc
    if not _is_utc_timestamp(burned_at):
        raise ValueError("WITNESS_BURN_TIMESTAMP_INVALID")

    # This preliminary read catches obvious corruption early.  The entire
    # decision is repeated while holding the exclusive ledger lock below.
    current = verify_witness_ledger(
        ledger.path,
        repository_root=ledger.repository_root,
        expected_store_id=ledger.store_id,
        expected_header_sha256=ledger.header_sha256,
        expected_device=ledger.filesystem_device,
        expected_inode=ledger.filesystem_inode,
    )
    _require_no_opening_allocation(
        current,
        experiment_id=experiment_id,
        lockbox_id=lockbox_id,
        holdout_commitment_sha256=holdout_commitment_sha256,
    )

    try:
        descriptor = os.open(ledger.path, _open_flags(writable=True))
    except OSError as exc:
        raise WitnessPathError("WITNESS_LEDGER_APPEND_OPEN_FAILED") from exc
    try:
        with _exclusive_lock(descriptor):
            locked = _verify_locked_descriptor(
                descriptor,
                path=ledger.path,
                repository_root=ledger.repository_root,
                expected_store_id=ledger.store_id,
                expected_header_sha256=ledger.header_sha256,
                expected_device=ledger.filesystem_device,
                expected_inode=ledger.filesystem_inode,
            )
            _require_no_opening_allocation(
                locked,
                experiment_id=experiment_id,
                lockbox_id=lockbox_id,
                holdout_commitment_sha256=holdout_commitment_sha256,
            )
            unsigned = {
                "schema_version": WITNESS_SCHEMA_VERSION,
                "type": WITNESS_BURN_TYPE,
                "event": WITNESS_BURN_EVENT,
                "store_id": locked.store_id,
                "sequence": len(locked.burns) + len(locked.finalizations) + 1,
                "previous_record_sha256": locked.head_sha256,
                "experiment_id": experiment_id,
                "lockbox_id": lockbox_id,
                "locked_holdout_manifest_sha256": locked_holdout_manifest_sha256,
                "holdout_commitment_sha256": holdout_commitment_sha256,
                "burned_at_utc": burned_at,
                "anchor_store_id": anchor_store_id,
                "anchor_store_sha256": anchor_store_sha256,
                "opening_commitment_sha256": opening_commitment_sha256,
                "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
                "capability": "LIVE_DISABLED",
            }
            value = _self_hashed(unsigned)
            encoded = canonical_json(value) + b"\n"
            os.lseek(descriptor, 0, os.SEEK_END)
            written = os.write(descriptor, encoded)
            if written != len(encoded):
                os.fsync(descriptor)
                raise WitnessCorruption("WITNESS_APPEND_PARTIAL_WRITE_BURNED")
            os.fsync(descriptor)
            updated = _verify_locked_descriptor(
                descriptor,
                path=ledger.path,
                repository_root=ledger.repository_root,
                expected_store_id=ledger.store_id,
                expected_header_sha256=ledger.header_sha256,
                expected_device=ledger.filesystem_device,
                expected_inode=ledger.filesystem_inode,
            )
            committed = updated.burn_for(experiment_id)
            if (
                committed is None
                or committed.record_sha256 != value["record_sha256"]
                or len(updated.burns) != len(locked.burns) + 1
            ):
                raise WitnessCorruption("WITNESS_POST_APPEND_MISMATCH")
            fsync_directory(ledger.path.parent)
            return committed
    finally:
        os.close(descriptor)


def assert_unfinalized(
    ledger: AppendOnlyWitnessLedger,
    experiment_id: str,
) -> None:
    """Fail if the durable witness already commits a final report."""

    if not is_sha256(experiment_id):
        raise ValueError("EXPERIMENT_ID_MALFORMED")
    current = verify_witness_ledger(
        ledger.path,
        repository_root=ledger.repository_root,
        expected_store_id=ledger.store_id,
        expected_header_sha256=ledger.header_sha256,
        expected_device=ledger.filesystem_device,
        expected_inode=ledger.filesystem_inode,
    )
    if current.finalization_for(experiment_id) is not None:
        raise WitnessAlreadyFinalized("WITNESS_REPORT_ALREADY_FINALIZED_NOT_RETRYABLE")


def commit_finalization(
    ledger: AppendOnlyWitnessLedger,
    *,
    experiment_id: str,
    opening_burn_record_sha256: str,
    opened_state_sha256: str,
    external_anchor_sha256: str,
    report_sha256: str,
    report_status: str,
    report_kind: str,
    finalized_at_utc: str | None = None,
) -> FinalizationCommitment:
    """Append the immutable report hash before local FINALIZED state is written."""

    for field, value in (
        ("EXPERIMENT_ID", experiment_id),
        ("OPENING_BURN_RECORD_SHA256", opening_burn_record_sha256),
        ("OPENED_STATE_SHA256", opened_state_sha256),
        ("EXTERNAL_ANCHOR_SHA256", external_anchor_sha256),
        ("REPORT_SHA256", report_sha256),
    ):
        if not is_sha256(value):
            raise ValueError(f"{field}_MALFORMED")
    if report_status not in {"BACKTEST_CANDIDATE", "NOT_PROVEN"}:
        raise ValueError("WITNESS_FINALIZATION_REPORT_STATUS_INVALID")
    if report_kind not in {
        "LOCKED_OOS_EVALUATION",
        "TERMINAL_LIQUIDATION_FAILURE",
    }:
        raise ValueError("WITNESS_FINALIZATION_REPORT_KIND_INVALID")
    if report_kind == "TERMINAL_LIQUIDATION_FAILURE" and report_status != "NOT_PROVEN":
        raise ValueError("WITNESS_FINALIZATION_REPORT_KIND_STATUS_MISMATCH")
    finalized_at = _utc_now() if finalized_at_utc is None else finalized_at_utc
    if not _is_utc_timestamp(finalized_at):
        raise ValueError("WITNESS_FINALIZATION_TIMESTAMP_INVALID")

    current = verify_witness_ledger(
        ledger.path,
        repository_root=ledger.repository_root,
        expected_store_id=ledger.store_id,
        expected_header_sha256=ledger.header_sha256,
        expected_device=ledger.filesystem_device,
        expected_inode=ledger.filesystem_inode,
    )
    opening = current.burn_for(experiment_id)
    if opening is None:
        raise WitnessCorruption("WITNESS_FINALIZATION_WITHOUT_OPENING_BURN")
    if opening.record_sha256 != opening_burn_record_sha256:
        raise WitnessCorruption("WITNESS_FINALIZATION_OPENING_BURN_MISMATCH")
    if current.finalization_for(experiment_id) is not None:
        raise WitnessAlreadyFinalized("WITNESS_REPORT_ALREADY_FINALIZED_NOT_RETRYABLE")

    try:
        descriptor = os.open(ledger.path, _open_flags(writable=True))
    except OSError as exc:
        raise WitnessPathError("WITNESS_LEDGER_APPEND_OPEN_FAILED") from exc
    try:
        with _exclusive_lock(descriptor):
            locked = _verify_locked_descriptor(
                descriptor,
                path=ledger.path,
                repository_root=ledger.repository_root,
                expected_store_id=ledger.store_id,
                expected_header_sha256=ledger.header_sha256,
                expected_device=ledger.filesystem_device,
                expected_inode=ledger.filesystem_inode,
            )
            opening = locked.burn_for(experiment_id)
            if opening is None:
                raise WitnessCorruption("WITNESS_FINALIZATION_WITHOUT_OPENING_BURN")
            if opening.record_sha256 != opening_burn_record_sha256:
                raise WitnessCorruption("WITNESS_FINALIZATION_OPENING_BURN_MISMATCH")
            if locked.finalization_for(experiment_id) is not None:
                raise WitnessAlreadyFinalized(
                    "WITNESS_REPORT_ALREADY_FINALIZED_NOT_RETRYABLE"
                )
            unsigned = {
                "schema_version": WITNESS_SCHEMA_VERSION,
                "type": WITNESS_FINALIZATION_TYPE,
                "event": WITNESS_FINALIZATION_EVENT,
                "store_id": locked.store_id,
                "sequence": len(locked.burns) + len(locked.finalizations) + 1,
                "previous_record_sha256": locked.head_sha256,
                "experiment_id": experiment_id,
                "finalized_at_utc": finalized_at,
                "opening_burn_record_sha256": opening_burn_record_sha256,
                "opened_state_sha256": opened_state_sha256,
                "external_anchor_sha256": external_anchor_sha256,
                "report_sha256": report_sha256,
                "report_status": report_status,
                "report_kind": report_kind,
                "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
                "capability": "LIVE_DISABLED",
            }
            value = _self_hashed(unsigned)
            encoded = canonical_json(value) + b"\n"
            os.lseek(descriptor, 0, os.SEEK_END)
            written = os.write(descriptor, encoded)
            if written != len(encoded):
                os.fsync(descriptor)
                raise WitnessCorruption("WITNESS_APPEND_PARTIAL_WRITE_BURNED")
            os.fsync(descriptor)
            updated = _verify_locked_descriptor(
                descriptor,
                path=ledger.path,
                repository_root=ledger.repository_root,
                expected_store_id=ledger.store_id,
                expected_header_sha256=ledger.header_sha256,
                expected_device=ledger.filesystem_device,
                expected_inode=ledger.filesystem_inode,
            )
            committed = updated.finalization_for(experiment_id)
            if (
                committed is None
                or committed.record_sha256 != value["record_sha256"]
                or len(updated.finalizations) != len(locked.finalizations) + 1
                or len(updated.burns) != len(locked.burns)
            ):
                raise WitnessCorruption("WITNESS_FINALIZATION_POST_APPEND_MISMATCH")
            fsync_directory(ledger.path.parent)
            return committed
    finally:
        os.close(descriptor)
