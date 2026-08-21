from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import canonical_json
from .integrity import fsync_directory, is_sha256

STORE_SCHEMA_VERSION = "1.0.0"
STORE_TYPE = "BEN_AUTOTRADE_EXTERNAL_ANCHOR_STORE"
STORE_POLICY = "CREATE_ONLY_PER_EXPERIMENT_HASH_CHAIN_V1"
ANCHOR_TYPE = "BEN_AUTOTRADE_EXTERNAL_ANCHOR"
ANCHOR_EVENT = "HOLDOUT_OPENED"
ANCHOR_RECORD_NAME = "000000-HOLDOUT_OPENED.json"
GENESIS_ANCHOR_SHA256 = "0" * 64

STORE_FIELDS = frozenset(
    {
        "schema_version",
        "type",
        "store_id",
        "created_at_utc",
        "policy",
        "store_sha256",
    }
)
OPENED_STATE_BASE_FIELDS = frozenset(
    {
        "state",
        "experiment_id",
        "previous_state_sha256",
        "selection_sha256",
        "holdout_manifest_sha256",
        "test_receipt_sha256",
        "review_receipt_sha256",
        "opened_at_utc",
    }
)
ANCHOR_FIELDS = frozenset(
    {
        "schema_version",
        "type",
        "event",
        "sequence",
        "previous_anchor_sha256",
        "anchor_store_id",
        "experiment_id",
        "opened_at_utc",
        "opened_state_base_sha256",
        "previous_state_sha256",
        "selection_sha256",
        "config_sha256",
        "source_tree_sha256",
        "preholdout_data_sha256",
        "holdout_commitment_sha256",
        "holdout_manifest_sha256",
        "test_receipt_sha256",
        "review_receipt_sha256",
        "authority",
        "capability",
        "anchor_sha256",
    }
)

_SHARD_PATTERN = re.compile(r"[0-9a-f]{2}")


class ExternalAnchorError(RuntimeError):
    """Base error for the external create-only anchor store."""


class ExternalAnchorPathError(ExternalAnchorError):
    """The anchor path is unsafe, non-portable, or overlaps the repository."""


class ExternalAnchorCorruption(ExternalAnchorError):
    """The anchor store is malformed, ambiguous, or has been modified."""


class ExternalAnchorAlreadyInitialized(ExternalAnchorError):
    """Initialization was attempted against an existing anchor store."""


class ExternalAnchorAlreadyOpened(ExternalAnchorError):
    """The experiment already has an external HOLDOUT_OPENED commitment."""


class ExternalAnchorNotFound(ExternalAnchorError):
    """The requested experiment has no external HOLDOUT_OPENED commitment."""


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


def _self_hashed(unsigned: Mapping[str, Any], field: str) -> dict[str, Any]:
    if field in unsigned:
        raise ValueError(f"{field} input must be unsigned")
    value = dict(unsigned)
    value[field] = hashlib.sha256(canonical_json(dict(unsigned))).hexdigest()
    return value


def _is_link_or_reparse(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(metadata.st_mode) or path.is_symlink():
        return True
    if hasattr(path, "is_junction") and path.is_junction():
        return True
    if os.name == "nt":
        attributes = int(getattr(metadata, "st_file_attributes", 0))
        reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
        return bool(attributes & reparse_flag)
    return False


def _reject_link_components(path: Path) -> None:
    if not path.is_absolute():
        raise ExternalAnchorPathError("ANCHOR_ROOT_MUST_BE_ABSOLUTE")
    if ".." in path.parts:
        raise ExternalAnchorPathError("ANCHOR_ROOT_PARENT_TRAVERSAL_REJECTED")
    parts = path.parts
    current = Path(parts[0])
    if _is_link_or_reparse(current):
        raise ExternalAnchorPathError("ANCHOR_PATH_LINK_OR_REPARSE_REJECTED")
    for part in parts[1:]:
        current /= part
        if (current.exists() or current.is_symlink()) and _is_link_or_reparse(current):
            raise ExternalAnchorPathError("ANCHOR_PATH_LINK_OR_REPARSE_REJECTED")


def _require_directory(path: Path, label: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise ExternalAnchorPathError(f"{label}_MISSING") from exc
    if _is_link_or_reparse(path) or not stat.S_ISDIR(metadata.st_mode):
        raise ExternalAnchorPathError(f"{label}_NOT_PLAIN_DIRECTORY")


def _require_regular_single_link(path: Path, label: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise ExternalAnchorCorruption(f"{label}_MISSING") from exc
    if _is_link_or_reparse(path) or not stat.S_ISREG(metadata.st_mode):
        raise ExternalAnchorCorruption(f"{label}_NOT_REGULAR_FILE")
    if int(metadata.st_nlink) != 1:
        raise ExternalAnchorCorruption(f"{label}_HARDLINK_REJECTED")


def _resolved_outside_repository(anchor_root: Path, repository_root: Path) -> tuple[Path, Path]:
    _reject_link_components(repository_root)
    _reject_link_components(anchor_root)
    try:
        repository = repository_root.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ExternalAnchorPathError("REPOSITORY_ROOT_MISSING") from exc
    _require_directory(repository, "REPOSITORY_ROOT")
    if anchor_root.exists():
        anchor = anchor_root.resolve(strict=True)
    else:
        try:
            parent = anchor_root.parent.resolve(strict=True)
        except FileNotFoundError as exc:
            raise ExternalAnchorPathError("ANCHOR_ROOT_PARENT_MISSING") from exc
        _require_directory(parent, "ANCHOR_ROOT_PARENT")
        anchor = parent / anchor_root.name
    if (
        anchor == repository
        or anchor.is_relative_to(repository)
        or repository.is_relative_to(anchor)
    ):
        raise ExternalAnchorPathError("ANCHOR_ROOT_MUST_BE_DISJOINT_FROM_REPOSITORY")
    return anchor, repository


def _read_canonical_hashed_object(
    path: Path,
    *,
    exact_fields: frozenset[str],
    hash_field: str,
    label: str,
) -> dict[str, Any]:
    _require_regular_single_link(path, label)
    try:
        payload = path.read_bytes()
        value = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalAnchorCorruption(f"{label}_INVALID_JSON") from exc
    if not isinstance(value, dict):
        raise ExternalAnchorCorruption(f"{label}_NOT_OBJECT")
    if set(value) != exact_fields:
        raise ExternalAnchorCorruption(f"{label}_SCHEMA_MISMATCH")
    if payload != canonical_json(value) + b"\n":
        raise ExternalAnchorCorruption(f"{label}_NONCANONICAL")
    supplied = value.get(hash_field)
    if not is_sha256(supplied):
        raise ExternalAnchorCorruption(f"{label}_{hash_field.upper()}_MALFORMED")
    unsigned = {key: item for key, item in value.items() if key != hash_field}
    if hashlib.sha256(canonical_json(unsigned)).hexdigest() != supplied:
        raise ExternalAnchorCorruption(f"{label}_{hash_field.upper()}_MISMATCH")
    _require_regular_single_link(path, label)
    return value


def _write_canonical_exclusive(path: Path, value: Mapping[str, Any]) -> None:
    payload = canonical_json(dict(value)) + b"\n"
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    fsync_directory(path.parent)


def _validate_store_descriptor(
    value: Mapping[str, Any],
    expected_store_id: str,
    expected_store_sha256: str,
) -> None:
    if value.get("schema_version") != STORE_SCHEMA_VERSION:
        raise ExternalAnchorCorruption("ANCHOR_STORE_SCHEMA_VERSION_MISMATCH")
    if value.get("type") != STORE_TYPE:
        raise ExternalAnchorCorruption("ANCHOR_STORE_TYPE_MISMATCH")
    if value.get("policy") != STORE_POLICY:
        raise ExternalAnchorCorruption("ANCHOR_STORE_POLICY_MISMATCH")
    if value.get("store_id") != expected_store_id or not is_sha256(value.get("store_id")):
        raise ExternalAnchorCorruption("ANCHOR_STORE_ID_MISMATCH")
    if (
        value.get("store_sha256") != expected_store_sha256
        or not is_sha256(value.get("store_sha256"))
    ):
        raise ExternalAnchorCorruption("ANCHOR_STORE_DESCRIPTOR_MISMATCH")
    if not _is_utc_timestamp(value.get("created_at_utc")):
        raise ExternalAnchorCorruption("ANCHOR_STORE_CREATED_AT_INVALID")


def _validate_opened_base(value: Mapping[str, Any]) -> None:
    if set(value) != OPENED_STATE_BASE_FIELDS:
        raise ValueError("HOLDOUT_OPENED_BASE_SCHEMA_MISMATCH")
    if value.get("state") != "HOLDOUT_OPENED":
        raise ValueError("HOLDOUT_OPENED_BASE_STATE_MISMATCH")
    for field in OPENED_STATE_BASE_FIELDS:
        if field.endswith("_sha256") and not is_sha256(value.get(field)):
            raise ValueError(f"HOLDOUT_OPENED_BASE_{field.upper()}_MALFORMED")
    if not is_sha256(value.get("experiment_id")):
        raise ValueError("HOLDOUT_OPENED_BASE_EXPERIMENT_ID_MALFORMED")
    if not _is_utc_timestamp(value.get("opened_at_utc")):
        raise ValueError("HOLDOUT_OPENED_BASE_OPENED_AT_INVALID")


def _validate_anchor_record(
    value: Mapping[str, Any],
    *,
    expected_store_id: str,
    expected_experiment_id: str,
) -> None:
    if value.get("schema_version") != STORE_SCHEMA_VERSION:
        raise ExternalAnchorCorruption("EXTERNAL_ANCHOR_SCHEMA_VERSION_MISMATCH")
    if value.get("type") != ANCHOR_TYPE or value.get("event") != ANCHOR_EVENT:
        raise ExternalAnchorCorruption("EXTERNAL_ANCHOR_TYPE_OR_EVENT_MISMATCH")
    if value.get("sequence") != 0 or isinstance(value.get("sequence"), bool):
        raise ExternalAnchorCorruption("EXTERNAL_ANCHOR_SEQUENCE_MISMATCH")
    if value.get("previous_anchor_sha256") != GENESIS_ANCHOR_SHA256:
        raise ExternalAnchorCorruption("EXTERNAL_ANCHOR_PREVIOUS_HASH_MISMATCH")
    if value.get("anchor_store_id") != expected_store_id:
        raise ExternalAnchorCorruption("EXTERNAL_ANCHOR_STORE_ID_MISMATCH")
    if value.get("experiment_id") != expected_experiment_id:
        raise ExternalAnchorCorruption("EXTERNAL_ANCHOR_EXPERIMENT_ID_MISMATCH")
    for field in ANCHOR_FIELDS:
        if field.endswith("_sha256") and not is_sha256(value.get(field)):
            raise ExternalAnchorCorruption(f"EXTERNAL_ANCHOR_{field.upper()}_MALFORMED")
    if not is_sha256(value.get("anchor_store_id")):
        raise ExternalAnchorCorruption("EXTERNAL_ANCHOR_STORE_ID_MALFORMED")
    if not _is_utc_timestamp(value.get("opened_at_utc")):
        raise ExternalAnchorCorruption("EXTERNAL_ANCHOR_OPENED_AT_INVALID")
    if value.get("authority") != "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY":
        raise ExternalAnchorCorruption("EXTERNAL_ANCHOR_AUTHORITY_MISMATCH")
    if value.get("capability") != "LIVE_DISABLED":
        raise ExternalAnchorCorruption("EXTERNAL_ANCHOR_CAPABILITY_MISMATCH")


def _record_path(root: Path, experiment_id: str) -> Path:
    if not is_sha256(experiment_id):
        raise ValueError("EXPERIMENT_ID_MALFORMED")
    return root / "records" / experiment_id[:2] / experiment_id / ANCHOR_RECORD_NAME


def _verify_record_tree(root: Path, store_id: str) -> None:
    records = root / "records"
    _require_directory(records, "ANCHOR_RECORDS_DIRECTORY")
    for shard in records.iterdir():
        if _is_link_or_reparse(shard):
            raise ExternalAnchorCorruption("ANCHOR_RECORD_TREE_LINK_OR_REPARSE_REJECTED")
        if _SHARD_PATTERN.fullmatch(shard.name) is None:
            raise ExternalAnchorCorruption("ANCHOR_RECORD_SHARD_NAME_INVALID")
        try:
            _require_directory(shard, "ANCHOR_RECORD_SHARD")
        except ExternalAnchorPathError as exc:
            raise ExternalAnchorCorruption("ANCHOR_RECORD_SHARD_INVALID") from exc
        for experiment in shard.iterdir():
            if _is_link_or_reparse(experiment):
                raise ExternalAnchorCorruption("ANCHOR_EXPERIMENT_LINK_OR_REPARSE_REJECTED")
            if not is_sha256(experiment.name) or not experiment.name.startswith(shard.name):
                raise ExternalAnchorCorruption("ANCHOR_EXPERIMENT_DIRECTORY_NAME_INVALID")
            try:
                _require_directory(experiment, "ANCHOR_EXPERIMENT_DIRECTORY")
            except ExternalAnchorPathError as exc:
                raise ExternalAnchorCorruption("ANCHOR_EXPERIMENT_DIRECTORY_INVALID") from exc
            entries = list(experiment.iterdir())
            if not entries:
                continue
            if {entry.name for entry in entries} != {ANCHOR_RECORD_NAME} or len(entries) != 1:
                raise ExternalAnchorCorruption("ANCHOR_EXPERIMENT_ENTRIES_AMBIGUOUS")
            record = _read_canonical_hashed_object(
                entries[0],
                exact_fields=ANCHOR_FIELDS,
                hash_field="anchor_sha256",
                label="EXTERNAL_ANCHOR",
            )
            _validate_anchor_record(
                record,
                expected_store_id=store_id,
                expected_experiment_id=experiment.name,
            )


@dataclass(frozen=True, slots=True)
class ExternalAnchorStore:
    root: Path
    repository_root: Path
    store_id: str
    store_sha256: str

    def record_path(self, experiment_id: str) -> Path:
        return _record_path(self.root, experiment_id)


def initialize_anchor_store(
    anchor_root: str | Path,
    *,
    repository_root: str | Path,
    store_id: str | None = None,
    created_at_utc: str | None = None,
) -> ExternalAnchorStore:
    supplied_root = Path(anchor_root)
    supplied_repository = Path(repository_root)
    root, repository = _resolved_outside_repository(supplied_root, supplied_repository)
    chosen_store_id = secrets.token_hex(32) if store_id is None else store_id
    if not is_sha256(chosen_store_id):
        raise ValueError("ANCHOR_STORE_ID_MALFORMED")
    created_at = _utc_now() if created_at_utc is None else created_at_utc
    if not _is_utc_timestamp(created_at):
        raise ValueError("ANCHOR_STORE_CREATED_AT_INVALID")

    if root.exists():
        _require_directory(root, "ANCHOR_ROOT")
        entries = list(root.iterdir())
        if (root / "ANCHOR_STORE.json").exists():
            raise ExternalAnchorAlreadyInitialized("ANCHOR_STORE_ALREADY_INITIALIZED")
        if entries:
            raise ExternalAnchorCorruption("ANCHOR_ROOT_NOT_EMPTY")
    else:
        root.mkdir()
        _require_directory(root, "ANCHOR_ROOT")
        fsync_directory(root.parent)

    records = root / "records"
    records.mkdir()
    _require_directory(records, "ANCHOR_RECORDS_DIRECTORY")
    fsync_directory(root)
    descriptor = _self_hashed(
        {
            "schema_version": STORE_SCHEMA_VERSION,
            "type": STORE_TYPE,
            "store_id": chosen_store_id,
            "created_at_utc": created_at,
            "policy": STORE_POLICY,
        },
        "store_sha256",
    )
    _write_canonical_exclusive(root / "ANCHOR_STORE.json", descriptor)
    fsync_directory(records)
    fsync_directory(root)
    return verify_anchor_store(
        root,
        repository_root=repository,
        expected_store_id=chosen_store_id,
        expected_store_sha256=str(descriptor["store_sha256"]),
    )


def verify_anchor_store(
    anchor_root: str | Path,
    *,
    repository_root: str | Path,
    expected_store_id: str,
    expected_store_sha256: str,
) -> ExternalAnchorStore:
    if not is_sha256(expected_store_id):
        raise ValueError("EXPECTED_ANCHOR_STORE_ID_MALFORMED")
    if not is_sha256(expected_store_sha256):
        raise ValueError("EXPECTED_ANCHOR_STORE_SHA256_MALFORMED")
    supplied_root = Path(anchor_root)
    supplied_repository = Path(repository_root)
    root, repository = _resolved_outside_repository(supplied_root, supplied_repository)
    _require_directory(root, "ANCHOR_ROOT")
    entries = {entry.name for entry in root.iterdir()}
    if entries != {"ANCHOR_STORE.json", "records"}:
        raise ExternalAnchorCorruption("ANCHOR_ROOT_SCHEMA_MISMATCH")
    descriptor = _read_canonical_hashed_object(
        root / "ANCHOR_STORE.json",
        exact_fields=STORE_FIELDS,
        hash_field="store_sha256",
        label="ANCHOR_STORE",
    )
    _validate_store_descriptor(descriptor, expected_store_id, expected_store_sha256)
    _verify_record_tree(root, expected_store_id)
    return ExternalAnchorStore(
        root=root,
        repository_root=repository,
        store_id=expected_store_id,
        store_sha256=expected_store_sha256,
    )


def read_holdout_opened_anchor(
    store: ExternalAnchorStore, experiment_id: str
) -> dict[str, Any]:
    current = verify_anchor_store(
        store.root,
        repository_root=store.repository_root,
        expected_store_id=store.store_id,
        expected_store_sha256=store.store_sha256,
    )
    path = current.record_path(experiment_id)
    if not path.exists() and not path.is_symlink():
        raise ExternalAnchorNotFound("EXTERNAL_HOLDOUT_OPENED_ANCHOR_NOT_FOUND")
    value = _read_canonical_hashed_object(
        path,
        exact_fields=ANCHOR_FIELDS,
        hash_field="anchor_sha256",
        label="EXTERNAL_ANCHOR",
    )
    _validate_anchor_record(
        value,
        expected_store_id=current.store_id,
        expected_experiment_id=experiment_id,
    )
    return value


def assert_holdout_unopened(store: ExternalAnchorStore, experiment_id: str) -> None:
    try:
        read_holdout_opened_anchor(store, experiment_id)
    except ExternalAnchorNotFound:
        return
    raise ExternalAnchorAlreadyOpened("EXTERNAL_HOLDOUT_ALREADY_OPENED_NOT_RETRYABLE")


def _ensure_create_only_directory(path: Path) -> None:
    created = False
    try:
        path.mkdir()
        created = True
    except FileExistsError:
        pass
    try:
        _require_directory(path, "ANCHOR_RECORD_PATH")
    except ExternalAnchorPathError as exc:
        raise ExternalAnchorCorruption("ANCHOR_RECORD_PATH_INVALID") from exc
    if created:
        fsync_directory(path)
        fsync_directory(path.parent)


def commit_holdout_opened_anchor(
    store: ExternalAnchorStore,
    *,
    opened_state_base: Mapping[str, Any],
    config_sha256: str,
    source_tree_sha256: str,
    preholdout_data_sha256: str,
    holdout_commitment_sha256: str,
) -> dict[str, Any]:
    current = verify_anchor_store(
        store.root,
        repository_root=store.repository_root,
        expected_store_id=store.store_id,
        expected_store_sha256=store.store_sha256,
    )
    opened = dict(opened_state_base)
    _validate_opened_base(opened)
    experiment_id = str(opened["experiment_id"])
    for field, value in (
        ("config_sha256", config_sha256),
        ("source_tree_sha256", source_tree_sha256),
        ("preholdout_data_sha256", preholdout_data_sha256),
        ("holdout_commitment_sha256", holdout_commitment_sha256),
    ):
        if not is_sha256(value):
            raise ValueError(f"EXTERNAL_ANCHOR_{field.upper()}_MALFORMED")
    assert_holdout_unopened(current, experiment_id)

    unsigned = {
        "schema_version": STORE_SCHEMA_VERSION,
        "type": ANCHOR_TYPE,
        "event": ANCHOR_EVENT,
        "sequence": 0,
        "previous_anchor_sha256": GENESIS_ANCHOR_SHA256,
        "anchor_store_id": current.store_id,
        "experiment_id": experiment_id,
        "opened_at_utc": opened["opened_at_utc"],
        "opened_state_base_sha256": hashlib.sha256(canonical_json(opened)).hexdigest(),
        "previous_state_sha256": opened["previous_state_sha256"],
        "selection_sha256": opened["selection_sha256"],
        "config_sha256": config_sha256,
        "source_tree_sha256": source_tree_sha256,
        "preholdout_data_sha256": preholdout_data_sha256,
        "holdout_commitment_sha256": holdout_commitment_sha256,
        "holdout_manifest_sha256": opened["holdout_manifest_sha256"],
        "test_receipt_sha256": opened["test_receipt_sha256"],
        "review_receipt_sha256": opened["review_receipt_sha256"],
        "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
        "capability": "LIVE_DISABLED",
    }
    anchor = _self_hashed(unsigned, "anchor_sha256")
    path = current.record_path(experiment_id)
    shard = path.parent.parent
    experiment = path.parent
    _ensure_create_only_directory(shard)
    _ensure_create_only_directory(experiment)
    entries = list(experiment.iterdir())
    if entries:
        if len(entries) == 1 and entries[0].name == ANCHOR_RECORD_NAME:
            read_holdout_opened_anchor(current, experiment_id)
            raise ExternalAnchorAlreadyOpened(
                "EXTERNAL_HOLDOUT_ALREADY_OPENED_NOT_RETRYABLE"
            )
        raise ExternalAnchorCorruption("ANCHOR_EXPERIMENT_ENTRIES_AMBIGUOUS")
    try:
        _write_canonical_exclusive(path, anchor)
    except FileExistsError:
        read_holdout_opened_anchor(current, experiment_id)
        raise ExternalAnchorAlreadyOpened(
            "EXTERNAL_HOLDOUT_ALREADY_OPENED_NOT_RETRYABLE"
        ) from None

    for directory in (experiment, shard, current.root / "records", current.root):
        fsync_directory(directory)
    committed = read_holdout_opened_anchor(current, experiment_id)
    if committed != anchor:
        raise ExternalAnchorCorruption("EXTERNAL_ANCHOR_POST_WRITE_MISMATCH")
    return committed
