from __future__ import annotations

import hashlib
import json
import os
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
HASHED_DOCS = ("docs/RESEARCH_CONTRACT.md", "docs/DATA_PROVENANCE.md")
HASHED_DATA_FILES = (
    "data/manifests/BTCUSDT-1h-1502942400000-1785542400000-afaef07eb47c4613-cc3f4f474f84a96f.json",
)
IGNORED_PARTS = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"}
IGNORED_SUFFIXES = {".pyc", ".pyo"}
SHA256_HEX_LENGTH = 64


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


def resolve_regular_file_inside(root: str | Path, relative: object, allowed_directory: str) -> Path:
    """Resolve a receipt-bound path without allowing escape or special files."""

    if not isinstance(relative, str) or not relative or "\\" in relative:
        raise ValueError("bound path must be a non-empty POSIX relative path")
    candidate_relative = Path(relative)
    if candidate_relative.is_absolute() or ".." in candidate_relative.parts:
        raise ValueError("bound path must not be absolute or contain parent traversal")
    root_path = Path(root).resolve()
    allowed_unresolved = root_path / allowed_directory
    if allowed_unresolved.is_symlink() or (
        hasattr(allowed_unresolved, "is_junction") and allowed_unresolved.is_junction()
    ):
        raise ValueError(f"{allowed_directory} must not be a link or junction")
    allowed = allowed_unresolved.resolve()
    try:
        allowed.relative_to(root_path)
    except ValueError as exc:
        raise ValueError(f"{allowed_directory} escapes the repository") from exc
    unresolved = root_path / candidate_relative
    if unresolved.is_symlink():
        raise ValueError("bound path must not be a symbolic link")
    candidate = unresolved.resolve()
    try:
        candidate.relative_to(allowed)
    except ValueError as exc:
        raise ValueError(f"bound path must stay inside {allowed_directory}") from exc
    if not candidate.is_file():
        raise ValueError("bound path must identify an existing regular file")
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
) -> dict[str, Any]:
    object_path = Path(path).resolve()
    value = json.loads(object_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError("hashed object must be a JSON object")
    supplied = value.get(hash_field)
    if not is_sha256(supplied):
        raise ValueError(f"missing or malformed {hash_field}")
    unsigned = {key: item for key, item in value.items() if key != hash_field}
    if sha256_bytes(canonical_json(unsigned)) != supplied:
        raise ValueError(f"{hash_field} mismatch")
    if value.get("type") in {"TEST_RECEIPT", "PRO_REVIEW_RECEIPT"}:
        _verify_receipt_evidence(object_path, value)
    return value


def _verify_receipt_evidence(path: Path, receipt: dict[str, Any]) -> None:
    """Verify receipt placement plus every file the receipt claims to bind.

    A bare self-hash detects accidental edits to the JSON but not replacement of
    both the receipt and its evidence.  Requiring content-addressed placement
    and re-hashing the evidence closes that substitution gap inside the local
    artifact chain (without introducing a signing secret).
    """

    supplied = receipt["receipt_sha256"]
    receipt_type = receipt["type"]
    root = path.parent.parent
    if path.parent != (root / "artifacts").resolve():
        raise ValueError("receipt must be stored directly inside artifacts")
    prefix = "test-receipt" if receipt_type == "TEST_RECEIPT" else "pro-review-receipt"
    if path.name != f"{prefix}-{supplied[:16]}.json":
        raise ValueError("receipt filename is not content-addressed")

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
        log_sha = receipt.get("normalized_output_sha256")
        if not is_sha256(log_sha):
            raise ValueError("test receipt output hash is malformed")
        log = resolve_regular_file_inside(root, receipt.get("normalized_output_path"), "artifacts")
        if log.name != f"test-log-{log_sha[:16]}.txt":
            raise ValueError("test log filename is not content-addressed")
        payload = log.read_bytes()
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
        return

    review_sha = receipt.get("sanitized_review_sha256")
    if not is_sha256(review_sha):
        raise ValueError("review receipt evidence hash is malformed")
    review = resolve_regular_file_inside(root, receipt.get("sanitized_review_path"), "docs/reviews")
    if sha256_file(review) != review_sha:
        raise ValueError("review receipt evidence hash mismatch")
    if receipt.get("reviewer") != "ChatGPT Pro":
        raise ValueError("review receipt reviewer is invalid")
    if receipt.get("verdict") not in {"PROCEED", "BLOCKED"}:
        raise ValueError("review receipt verdict is invalid")
    for field in ("visible_model_label", "visible_reasoning_label"):
        label = receipt.get(field)
        if (
            not isinstance(label, str)
            or not label
            or len(label) > 128
            or any(character.isspace() and character not in {" "} for character in label)
            or any(ord(character) < 32 for character in label)
        ):
            raise ValueError(f"review receipt {field} is invalid")


def write_immutable(path: str | Path, payload: bytes) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        if destination.read_bytes() != payload:
            raise RuntimeError(f"immutable artifact collision: {destination}")


def write_exclusive(path: str | Path, value: dict[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json(value) + b"\n"
    with destination.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
