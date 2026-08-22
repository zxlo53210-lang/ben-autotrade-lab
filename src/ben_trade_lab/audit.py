from __future__ import annotations

import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from .config import LabConfig, canonical_json
from .integrity import (
    MAX_SANITIZED_REVIEW_BYTES,
    fsync_directory,
    full_provenance_replay_evidence,
    require_plain_parent_chain_for_create,
    resolve_regular_file_inside,
    source_tree_sha256,
    write_immutable,
)
from .validation import _load_selection_artifact

SAFE_TEST_ENVIRONMENT_KEYS = (
    "COMSPEC",
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "TMPDIR",
    "WINDIR",
)

AUDIT_TEMP_DIRECTORY = "artifacts/.audit-tmp"


def _prepare_audit_temp_root(root_path: Path) -> Path:
    """Create one repository-local temp root without following indirection."""

    temp_root = root_path / AUDIT_TEMP_DIRECTORY
    probe = temp_root / ".plain-directory-probe"
    require_plain_parent_chain_for_create(probe, "audit temp root")
    temp_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    require_plain_parent_chain_for_create(probe, "audit temp root")
    if not temp_root.is_dir():
        raise ValueError("audit temp root must be a plain directory")
    resolved = temp_root.resolve()
    try:
        resolved.relative_to(root_path)
    except ValueError as exc:
        raise ValueError("audit temp root must stay inside the repository") from exc
    return resolved


def _redact_local_paths(
    output: str, root_path: Path, environment: dict[str, str]
) -> str:
    """Remove host-specific repository and temporary-directory prefixes."""

    path_values = {str(root_path)}
    path_values.update(
        value
        for key in ("TEMP", "TMP", "TMPDIR")
        if (value := environment.get(key))
    )
    variants: set[str] = set()
    for value in path_values:
        normalized = os.path.normpath(value)
        variants.update({value, normalized, normalized.replace("\\", "/")})
        escaped = normalized
        for _ in range(3):
            escaped = escaped.replace("\\", "\\\\")
            variants.add(escaped)
    redacted = output
    for variant in sorted(variants, key=len, reverse=True):
        if variant:
            redacted = redacted.replace(variant, "<REDACTED_LOCAL_PATH>")
    return redacted


def _normalize_test_output(
    output: str, root_path: Path, environment: dict[str, str]
) -> str:
    without_timing = re.sub(
        r"Ran (\d+) tests? in [0-9.]+s", r"Ran \1 tests", output
    )
    return _redact_local_paths(without_timing, root_path, environment)


def _bound_fields(selection: dict[str, Any]) -> dict[str, Any]:
    return {
        "experiment_id": selection["experiment_id"],
        "selection_sha256": selection["selection_sha256"],
        "config_sha256": selection["config_sha256"],
        "source_tree_sha256": selection["source_tree_sha256"],
        "preholdout_data_sha256": selection["preholdout_data_sha256"],
        "holdout_commitment_sha256": selection["holdout_commitment_sha256"],
    }


def _load_current_selection(
    selection_path: str | Path, config: LabConfig, root: Path
) -> tuple[dict[str, Any], str]:
    selection, _, selection_relative = _load_selection_artifact(selection_path, root)
    if selection.get("status") != "FROZEN_CANDIDATE":
        raise ValueError("audit receipts require a frozen eligible candidate")
    if selection.get("config_sha256") != config.config_sha256:
        raise ValueError("selection config hash mismatch")
    if selection.get("source_tree_sha256") != source_tree_sha256(root):
        raise ValueError("selection source tree is no longer current")
    return selection, selection_relative


def create_test_receipt(root: str | Path, selection_path: str | Path, config: LabConfig) -> Path:
    root_path = Path(root).resolve()
    selection, selection_relative = _load_current_selection(
        selection_path, config, root_path
    )
    source_before = source_tree_sha256(root_path)
    audit_temp_root = str(_prepare_audit_temp_root(root_path))
    environment_candidates = {
        "COMSPEC": os.environ.get("COMSPEC"),
        "PATH": os.environ.get("PATH"),
        "PATHEXT": os.environ.get("PATHEXT"),
        "SYSTEMROOT": os.environ.get("SYSTEMROOT"),
        "TEMP": audit_temp_root,
        "TMP": audit_temp_root,
        "TMPDIR": audit_temp_root,
        "WINDIR": os.environ.get("WINDIR"),
    }
    if set(environment_candidates) != set(SAFE_TEST_ENVIRONMENT_KEYS):
        raise AssertionError("test environment allowlist is internally inconsistent")
    environment = {
        key: value for key, value in environment_candidates.items() if value is not None
    }
    source_path = str(root_path / "src")
    environment.update(
        {
            "PYTHONHASHSEED": "0",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONNOUSERSITE": "1",
            "PYTHONPATH": source_path,
            "PYTHONSAFEPATH": "1",
            "PYTHONUTF8": "1",
            "BEN_ISOLATED_PROVENANCE_REPLAY": "1",
            "BEN_AUDIT_SELECTION_PATH": selection_relative,
            "BEN_AUDIT_SELECTION_SHA256": selection["selection_sha256"],
        }
    )
    completed = subprocess.run(
        [sys.executable, "-P", "-m", "unittest", "discover", "-s", "tests", "-v"],
        cwd=root_path,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    combined = completed.stdout + completed.stderr
    normalized = _normalize_test_output(combined, root_path, environment)
    source_after = source_tree_sha256(root_path)
    source_unchanged = source_after == source_before == selection["source_tree_sha256"]
    if not source_unchanged:
        normalized += "\nSOURCE_TREE_CHANGED_DURING_TEST_RUN\n"
    match = re.search(r"Ran (\d+) tests", normalized)
    count = int(match.group(1)) if match else 0
    output_reports_ok = normalized.rstrip().endswith("OK") or "\nOK (" in normalized
    status = (
        "PASS"
        if completed.returncode == 0 and count > 0 and output_reports_ok and source_unchanged
        else "FAIL"
    )
    log_payload = normalized.encode("utf-8")
    log_sha = hashlib.sha256(log_payload).hexdigest()
    log_path = root_path / "artifacts" / f"test-log-{log_sha[:16]}.txt"
    write_immutable(log_path, log_payload)
    fsync_directory(root_path)
    receipt = {
        "schema_version": "1.1.0",
        "type": "TEST_RECEIPT",
        "status": status,
        **_bound_fields(selection),
        "runner": f"CPython {sys.version_info.major}.{sys.version_info.minor}",
        "command": ["python", "-P", "-m", "unittest", "discover", "-s", "tests", "-v"],
        "return_code": completed.returncode,
        "test_count": count,
        "environment_policy": "ALLOWLIST_NO_CREDENTIAL_ENV",
        "full_provenance_replay": full_provenance_replay_evidence(normalized),
        "source_tree_sha256_before": source_before,
        "source_tree_sha256_after": source_after,
        "normalized_output_sha256": log_sha,
        "normalized_output_path": log_path.relative_to(root_path).as_posix(),
    }
    receipt["receipt_sha256"] = hashlib.sha256(canonical_json(receipt)).hexdigest()
    path = root_path / "artifacts" / f"test-receipt-{receipt['receipt_sha256'][:16]}.json"
    write_immutable(path, canonical_json(receipt) + b"\n")
    fsync_directory(root_path)
    if status != "PASS":
        raise RuntimeError(
            "test suite failed; "
            f"return_code={completed.returncode}, test_count={count}, "
            f"output_ok={output_reports_ok}, source_unchanged={source_unchanged}; "
            f"immutable evidence: {path}"
        )
    return path


def _visible_label(value: str, field: str) -> str:
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > 128
        or any(character.isspace() and character != " " for character in normalized)
        or any(ord(character) < 32 for character in normalized)
    ):
        raise ValueError(f"{field} must be a short single-line visible label")
    return normalized


def _require_review_terminal_verdict(payload: bytes, verdict: str) -> None:
    """Bind the declared verdict to the review's final non-blank line."""

    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("sanitized review must be UTF-8") from exc
    nonblank_lines = [line for line in text.splitlines() if line.strip()]
    if not nonblank_lines or nonblank_lines[-1] != verdict:
        raise ValueError(
            "sanitized review final non-blank line must exactly equal the declared verdict"
        )


def create_pro_review_receipt(
    root: str | Path,
    selection_path: str | Path,
    config: LabConfig,
    review_path: str | Path,
    verdict: str,
    model_visible: str,
    reasoning_visible: str,
) -> Path:
    root_path = Path(root).resolve()
    selection, _ = _load_current_selection(selection_path, config, root_path)
    if verdict not in {"PROCEED", "BLOCKED"}:
        raise ValueError("review verdict must be PROCEED or BLOCKED")
    review_input = Path(review_path)
    if review_input.is_absolute():
        try:
            review_relative = review_input.resolve().relative_to(root_path).as_posix()
        except ValueError as exc:
            raise ValueError("sanitized review must be inside the repository") from exc
    else:
        review_relative = review_input.as_posix()
    review = resolve_regular_file_inside(root_path, review_relative, "docs/reviews")
    try:
        review.relative_to((root_path / "docs" / "reviews").resolve())
    except ValueError as exc:
        raise ValueError("sanitized review must be inside docs/reviews") from exc
    payload = review.read_bytes()
    if not payload or len(payload) > MAX_SANITIZED_REVIEW_BYTES:
        raise ValueError("sanitized review must be non-empty and no larger than 1 MB")
    _require_review_terminal_verdict(payload, verdict)
    review_sha = hashlib.sha256(payload).hexdigest()
    receipt = {
        "schema_version": "1.0.0",
        "type": "PRO_REVIEW_RECEIPT",
        "verdict": verdict,
        **_bound_fields(selection),
        "reviewer": "ChatGPT Pro",
        "visible_model_label": _visible_label(model_visible, "model-visible"),
        "visible_reasoning_label": _visible_label(reasoning_visible, "reasoning-visible"),
        "sanitized_review_path": review.relative_to(root_path).as_posix(),
        "sanitized_review_sha256": review_sha,
    }
    receipt["receipt_sha256"] = hashlib.sha256(canonical_json(receipt)).hexdigest()
    path = root_path / "artifacts" / f"pro-review-receipt-{receipt['receipt_sha256'][:16]}.json"
    write_immutable(path, canonical_json(receipt) + b"\n")
    fsync_directory(root_path)
    return path
