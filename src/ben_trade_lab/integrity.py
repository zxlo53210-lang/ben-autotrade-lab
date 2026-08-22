from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any

from .config import canonical_json

HASHED_ROOT_FILES = (
    ".gitattributes",
    ".gitignore",
    "AGENTS.md",
    "LICENSE",
    "README.md",
    "SECURITY.md",
    "pyproject.toml",
)
HASHED_DIRECTORIES = ("src", "tests", "configs", ".github/workflows")
HASHED_DOCS = (
    "docs/RESEARCH_CONTRACT.md",
    "docs/DATA_PROVENANCE.md",
    "docs/DATA_ANOMALIES.md",
    "docs/APPEND_ONLY_WITNESS.md",
)
HASHED_DATA_FILES = (
    "data/manifests/BTCUSDT-1h-1502942400000-1785542400000-afaef07eb47c4613-cc3f4f474f84a96f.json",
)
IGNORED_PARTS = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"}
IGNORED_SUFFIXES = {".pyc", ".pyo"}
SHA256_HEX_LENGTH = 64
MAX_SANITIZED_REVIEW_BYTES = 1_000_000
FULL_PROVENANCE_REPLAY_ENVIRONMENT = "BEN_ISOLATED_PROVENANCE_REPLAY=1"
FULL_PROVENANCE_REPLAY_TEST_ID = (
    "test_data_registry.LocalSnapshotIntegrationTests."
    "test_full_raw_rebuild_emits_only_hashes_and_counters"
)
RECEIPT_BINDING_FIELDS = frozenset(
    {
        "experiment_id",
        "selection_sha256",
        "config_sha256",
        "source_tree_sha256",
        "preholdout_data_sha256",
        "holdout_commitment_sha256",
    }
)
TEST_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "type",
        "status",
        *RECEIPT_BINDING_FIELDS,
        "runner",
        "command",
        "return_code",
        "test_count",
        "environment_policy",
        "full_provenance_replay",
        "source_tree_sha256_before",
        "source_tree_sha256_after",
        "normalized_output_sha256",
        "normalized_output_path",
        "receipt_sha256",
    }
)
PRO_REVIEW_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "type",
        "verdict",
        *RECEIPT_BINDING_FIELDS,
        "reviewer",
        "visible_model_label",
        "visible_reasoning_label",
        "sanitized_review_path",
        "sanitized_review_sha256",
        "receipt_sha256",
    }
)


def full_provenance_replay_evidence(output: str) -> dict[str, str]:
    """Derive the isolated full-source replay outcome from unittest evidence.

    The frozen verbose runner emits one result line for the source-bound test.
    Anything other than one unambiguous result is deliberately non-PASS so a
    missing, renamed, duplicated, failed, or skipped integration test cannot
    authorize opening the holdout.
    """

    method_name = FULL_PROVENANCE_REPLAY_TEST_ID.rsplit(".", 1)[-1]
    prefix = f"{method_name} ({FULL_PROVENANCE_REPLAY_TEST_ID}) ... "
    outcomes = [line[len(prefix) :] for line in output.splitlines() if line.startswith(prefix)]
    if not outcomes:
        status = "ABSENT"
    elif len(outcomes) != 1:
        status = "AMBIGUOUS"
    elif outcomes[0] == "ok":
        status = "PASS"
    elif outcomes[0].startswith("skipped "):
        status = "SKIPPED"
    elif outcomes[0] in {"FAIL", "ERROR"}:
        status = "FAIL"
    else:
        status = "AMBIGUOUS"
    return {
        "requested_mode": FULL_PROVENANCE_REPLAY_ENVIRONMENT,
        "test_id": FULL_PROVENANCE_REPLAY_TEST_ID,
        "status": status,
    }


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: str | Path) -> str:
    return sha256_bytes(Path(path).read_bytes())


def is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == SHA256_HEX_LENGTH
        and all(character in "0123456789abcdef" for character in value)
    )


def is_link_or_reparse(path: str | Path) -> bool:
    """Return whether *path* is a symlink, junction, or Windows reparse point."""

    candidate = Path(path)
    try:
        metadata = candidate.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(metadata.st_mode) or candidate.is_symlink():
        return True
    if hasattr(candidate, "is_junction") and candidate.is_junction():
        return True
    attributes = int(getattr(metadata, "st_file_attributes", 0))
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & reparse_flag)


def require_plain_regular_single_link(path: str | Path, label: str) -> Path:
    """Require one plain regular file inode with no link/reparse indirection."""

    candidate = Path(path)
    absolute = candidate.absolute()
    components = absolute.parts
    current = Path(components[0])
    if is_link_or_reparse(current):
        raise ValueError(f"{label} path must not contain links or reparse points")
    for component in components[1:]:
        current /= component
        if is_link_or_reparse(current):
            raise ValueError(f"{label} path must not contain links or reparse points")
    try:
        metadata = candidate.lstat()
    except FileNotFoundError as exc:
        raise ValueError(f"{label} is missing") from exc
    if is_link_or_reparse(candidate) or not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{label} must be a plain regular file")
    if int(metadata.st_nlink) != 1:
        raise ValueError(f"{label} must have exactly one hard link")
    return candidate


def require_plain_parent_chain_for_create(path: str | Path, label: str) -> Path:
    """Reject indirection or non-directories before creating *path*.

    Missing descendants are allowed so callers can create them, but every
    already-existing component through the nearest existing parent must be a
    plain directory.  The walk deliberately uses ``absolute()`` plus ``lstat``
    rather than ``resolve()`` so a dangling symlink or Windows reparse point is
    observed instead of followed.
    """

    candidate = Path(path)
    parent = candidate.absolute().parent
    components = parent.parts
    current = Path(components[0])
    for index, component in enumerate(components):
        if index:
            current /= component
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            continue
        if is_link_or_reparse(current):
            raise ValueError(f"{label} parent path must not contain links or reparse points")
        if not stat.S_ISDIR(metadata.st_mode):
            raise ValueError(f"{label} nearest existing parent must be a plain directory")
    return candidate


def fsync_directory(directory: str | Path) -> None:
    """Durably flush directory metadata, or fail closed when unsupported."""

    directory_path = Path(directory).resolve()
    if not directory_path.is_dir():
        raise RuntimeError("DIRECTORY_FSYNC_TARGET_INVALID")
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        generic_write = 0x40000000
        file_share_all = 0x00000001 | 0x00000002 | 0x00000004
        open_existing = 3
        file_flag_write_through = 0x80000000
        file_flag_backup_semantics = 0x02000000
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateFileW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        kernel32.CreateFileW.restype = wintypes.HANDLE
        kernel32.FlushFileBuffers.argtypes = [wintypes.HANDLE]
        kernel32.FlushFileBuffers.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.CreateFileW(
            str(directory_path),
            generic_write,
            file_share_all,
            None,
            open_existing,
            file_flag_write_through | file_flag_backup_semantics,
            None,
        )
        if handle == wintypes.HANDLE(-1).value:
            error = ctypes.get_last_error()
            raise RuntimeError(f"DIRECTORY_OPEN_FAILED:{error}")
        flush_error: int | None = None
        try:
            if not kernel32.FlushFileBuffers(handle):
                flush_error = ctypes.get_last_error()
        finally:
            closed = bool(kernel32.CloseHandle(handle))
            close_error = ctypes.get_last_error() if not closed else None
        if flush_error is not None:
            raise RuntimeError(f"DIRECTORY_FSYNC_FAILED:{flush_error}")
        if close_error is not None:
            raise RuntimeError(f"DIRECTORY_CLOSE_FAILED:{close_error}")
        return

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(directory_path, flags)
    except OSError as exc:
        raise RuntimeError("DIRECTORY_FSYNC_UNSUPPORTED") from exc
    try:
        os.fsync(descriptor)
    except OSError as exc:
        raise RuntimeError("DIRECTORY_FSYNC_FAILED") from exc
    finally:
        os.close(descriptor)


def resolve_regular_file_inside(root: str | Path, relative: object, allowed_directory: str) -> Path:
    """Resolve a receipt-bound path without allowing escape or special files."""

    if not isinstance(relative, str) or not relative or "\\" in relative:
        raise ValueError("bound path must be a non-empty POSIX relative path")
    candidate_relative = Path(relative)
    if candidate_relative.is_absolute() or ".." in candidate_relative.parts:
        raise ValueError("bound path must not be absolute or contain parent traversal")
    root_path = Path(root).resolve()
    allowed_unresolved = root_path / allowed_directory
    component = root_path
    for part in Path(allowed_directory).parts:
        component /= part
        if is_link_or_reparse(component):
            raise ValueError(f"{allowed_directory} must not contain links or junctions")
    allowed = allowed_unresolved.resolve()
    try:
        allowed.relative_to(root_path)
    except ValueError as exc:
        raise ValueError(f"{allowed_directory} escapes the repository") from exc
    unresolved = root_path / candidate_relative
    component = root_path
    for part in candidate_relative.parts:
        component /= part
        if is_link_or_reparse(component):
            raise ValueError("bound path must not contain links or junctions")
    candidate = unresolved.resolve()
    try:
        candidate.relative_to(allowed)
    except ValueError as exc:
        raise ValueError(f"bound path must stay inside {allowed_directory}") from exc
    require_plain_regular_single_link(candidate, "bound path")
    return candidate


def source_files(root: str | Path) -> tuple[Path, ...]:
    root_path = Path(root).resolve()
    files: list[Path] = []
    for relative in (*HASHED_ROOT_FILES, *HASHED_DOCS, *HASHED_DATA_FILES):
        candidate = root_path / relative
        if candidate.is_file():
            if candidate.is_symlink() or (
                hasattr(candidate, "is_junction") and candidate.is_junction()
            ):
                raise RuntimeError(f"source fingerprint refuses linked file: {relative}")
            try:
                candidate.resolve().relative_to(root_path)
            except ValueError as exc:
                raise RuntimeError(f"source file escapes repository: {relative}") from exc
            files.append(candidate)
    for relative in HASHED_DIRECTORIES:
        candidate = root_path / relative
        if not candidate.is_dir():
            continue
        if candidate.is_symlink() or (
            hasattr(candidate, "is_junction") and candidate.is_junction()
        ):
            raise RuntimeError(f"source fingerprint refuses linked directory: {relative}")
        try:
            candidate.resolve().relative_to(root_path)
        except ValueError as exc:
            raise RuntimeError(f"source directory escapes repository: {relative}") from exc
        for path in candidate.rglob("*"):
            if not path.is_file():
                continue
            if any(part in IGNORED_PARTS for part in path.parts):
                continue
            if path.suffix.lower() in IGNORED_SUFFIXES:
                continue
            if path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction()):
                raise RuntimeError(
                    f"source fingerprint refuses linked file: {path.relative_to(root_path)}"
                )
            try:
                path.resolve().relative_to(root_path)
            except ValueError as exc:
                raise RuntimeError(
                    f"source file escapes repository: {path.relative_to(root_path)}"
                ) from exc
            files.append(path)
    return tuple(sorted(set(files), key=lambda item: item.relative_to(root_path).as_posix()))


def source_tree_sha256(root: str | Path) -> str:
    root_path = Path(root).resolve()
    digest = hashlib.sha256()
    for path in source_files(root_path):
        relative = path.relative_to(root_path).as_posix().encode("utf-8")
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def verified_hashed_object(
    path: str | Path,
    hash_field: str,
    *,
    root: str | Path | None = None,
) -> dict[str, Any]:
    unresolved = Path(path)
    require_plain_regular_single_link(unresolved, "hashed object")
    object_path = unresolved.resolve()
    payload = object_path.read_bytes()
    require_plain_regular_single_link(object_path, "hashed object")
    value = json.loads(payload.decode("utf-8"))
    if not isinstance(value, dict):
        raise TypeError("hashed object must be a JSON object")
    supplied = value.get(hash_field)
    if not is_sha256(supplied):
        raise ValueError(f"missing or malformed {hash_field}")
    unsigned = {key: item for key, item in value.items() if key != hash_field}
    if sha256_bytes(canonical_json(unsigned)) != supplied:
        raise ValueError(f"{hash_field} mismatch")
    if value.get("type") in {"TEST_RECEIPT", "PRO_REVIEW_RECEIPT"}:
        if payload != canonical_json(value) + b"\n":
            raise ValueError("receipt is not canonical JSON")
        _verify_receipt_evidence(object_path, value, expected_root=root)
    return value


def _verify_receipt_evidence(
    path: Path,
    receipt: dict[str, Any],
    *,
    expected_root: str | Path | None,
) -> None:
    """Verify receipt placement plus every file the receipt claims to bind.

    A bare self-hash detects accidental edits to the JSON but not replacement of
    both the receipt and its evidence.  Requiring content-addressed placement
    and re-hashing the evidence closes that substitution gap inside the local
    artifact chain (without introducing a signing secret).
    """

    supplied = receipt["receipt_sha256"]
    receipt_type = receipt["type"]
    root = path.parent.parent if expected_root is None else Path(expected_root).resolve()
    if expected_root is not None:
        try:
            receipt_relative = path.relative_to(root).as_posix()
        except ValueError as exc:
            raise ValueError("receipt must stay inside the current repository") from exc
        if resolve_regular_file_inside(root, receipt_relative, "artifacts") != path:
            raise ValueError("receipt path is not canonical for the current repository")
    if path.parent != (root / "artifacts").resolve():
        raise ValueError("receipt must be stored directly inside artifacts")
    prefix = "test-receipt" if receipt_type == "TEST_RECEIPT" else "pro-review-receipt"
    if path.name != f"{prefix}-{supplied[:16]}.json":
        raise ValueError("receipt filename is not content-addressed")

    expected_fields = (
        TEST_RECEIPT_FIELDS if receipt_type == "TEST_RECEIPT" else PRO_REVIEW_RECEIPT_FIELDS
    )
    if set(receipt) != expected_fields:
        raise ValueError(f"{receipt_type} receipt schema mismatch")

    common_hashes = (
        "selection_sha256",
        "config_sha256",
        "source_tree_sha256",
        "preholdout_data_sha256",
        "holdout_commitment_sha256",
    )
    if not is_sha256(receipt.get("experiment_id")):
        raise ValueError("receipt experiment_id is malformed")
    for field in common_hashes:
        if not is_sha256(receipt.get(field)):
            raise ValueError(f"receipt {field} is malformed")

    if receipt_type == "TEST_RECEIPT":
        if receipt.get("schema_version") != "1.1.0":
            raise ValueError("test receipt schema version does not include provenance replay")
        runner = receipt.get("runner")
        if not isinstance(runner, str) or re.fullmatch(r"CPython [0-9]+\.[0-9]+", runner) is None:
            raise ValueError("test receipt runner is invalid")
        log_sha = receipt.get("normalized_output_sha256")
        if not is_sha256(log_sha):
            raise ValueError("test receipt output hash is malformed")
        log = resolve_regular_file_inside(root, receipt.get("normalized_output_path"), "artifacts")
        if log.name != f"test-log-{log_sha[:16]}.txt":
            raise ValueError("test log filename is not content-addressed")
        payload = log.read_bytes()
        require_plain_regular_single_link(log, "test receipt output evidence")
        if sha256_bytes(payload) != log_sha:
            raise ValueError("test receipt output evidence hash mismatch")
        output = payload.decode("utf-8")
        count = receipt.get("test_count")
        return_code = receipt.get("return_code")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError("test receipt count is invalid")
        if not isinstance(return_code, int) or isinstance(return_code, bool):
            raise ValueError("test receipt return code is invalid")
        output_reports_count = f"Ran {count} tests" in output
        output_reports_ok = bool(output.rstrip().endswith("OK") or "\nOK (" in output)
        expected_status = (
            "PASS"
            if return_code == 0 and count > 0 and output_reports_count and output_reports_ok
            else "FAIL"
        )
        if receipt.get("status") != expected_status:
            raise ValueError("test receipt status does not match bound evidence")
        if receipt.get("environment_policy") != "ALLOWLIST_NO_CREDENTIAL_ENV":
            raise ValueError("test receipt did not use the credential-free environment policy")
        if receipt.get("source_tree_sha256_before") != receipt["source_tree_sha256"]:
            raise ValueError("test receipt pre-run source binding is inconsistent")
        if receipt.get("source_tree_sha256_after") != receipt["source_tree_sha256"]:
            raise ValueError("test receipt post-run source binding is inconsistent")
        if receipt.get("command") != [
            "python",
            "-P",
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests",
            "-v",
        ]:
            raise ValueError("test receipt command is not the frozen isolated runner")
        expected_replay = full_provenance_replay_evidence(output)
        if receipt.get("full_provenance_replay") != expected_replay:
            raise ValueError("test receipt full provenance replay evidence mismatch")
        return

    if receipt.get("schema_version") != "1.0.0":
        raise ValueError("Pro review receipt schema version is invalid")
    review_sha = receipt.get("sanitized_review_sha256")
    if not is_sha256(review_sha):
        raise ValueError("review receipt evidence hash is malformed")
    review = resolve_regular_file_inside(root, receipt.get("sanitized_review_path"), "docs/reviews")
    review_payload = review.read_bytes()
    require_plain_regular_single_link(review, "review receipt evidence")
    if not review_payload or len(review_payload) > MAX_SANITIZED_REVIEW_BYTES:
        raise ValueError("review receipt evidence must be non-empty and no larger than 1 MB")
    try:
        review_text = review_payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("review receipt evidence must be UTF-8") from exc
    if sha256_bytes(review_payload) != review_sha:
        raise ValueError("review receipt evidence hash mismatch")
    if receipt.get("reviewer") != "ChatGPT Pro":
        raise ValueError("review receipt reviewer is invalid")
    if receipt.get("verdict") not in {"PROCEED", "BLOCKED"}:
        raise ValueError("review receipt verdict is invalid")
    nonblank_lines = [line for line in review_text.splitlines() if line.strip()]
    if not nonblank_lines or nonblank_lines[-1] != receipt["verdict"]:
        raise ValueError("review receipt verdict does not match the review final sentinel")
    for field in ("visible_model_label", "visible_reasoning_label"):
        label = receipt.get(field)
        if (
            not isinstance(label, str)
            or not label
            or label != label.strip()
            or len(label) > 128
            or any(character.isspace() and character not in {" "} for character in label)
            or any(ord(character) < 32 for character in label)
        ):
            raise ValueError(f"review receipt {field} is invalid")


def write_immutable(path: str | Path, payload: bytes) -> None:
    destination = Path(path)
    require_plain_parent_chain_for_create(destination, "immutable artifact")
    destination.parent.mkdir(parents=True, exist_ok=True)
    require_plain_parent_chain_for_create(destination, "immutable artifact")
    try:
        with destination.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        require_plain_regular_single_link(destination, "immutable artifact")
        if destination.read_bytes() != payload:
            raise RuntimeError(f"immutable artifact collision: {destination}")
    require_plain_regular_single_link(destination, "immutable artifact")
    fsync_directory(destination.parent)


def write_exclusive(path: str | Path, value: dict[str, Any]) -> None:
    destination = Path(path)
    require_plain_parent_chain_for_create(destination, "exclusive artifact")
    destination.parent.mkdir(parents=True, exist_ok=True)
    require_plain_parent_chain_for_create(destination, "exclusive artifact")
    payload = canonical_json(value) + b"\n"
    with destination.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    require_plain_regular_single_link(destination, "exclusive artifact")
    fsync_directory(destination.parent)
