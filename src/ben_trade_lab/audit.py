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
    resolve_regular_file_inside,
    source_tree_sha256,
    verified_hashed_object,
    write_immutable,
)

SAFE_TEST_ENVIRONMENT_KEYS = (
    "COMSPEC",
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "WINDIR",
)
MAX_SANITIZED_REVIEW_BYTES = 1_000_000


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
) -> dict[str, Any]:
    selection = verified_hashed_object(selection_path, "selection_sha256")
    if selection.get("status") != "FROZEN_CANDIDATE":
        raise ValueError("audit receipts require a frozen eligible candidate")
    if selection.get("config_sha256") != config.config_sha256:
        raise ValueError("selection config hash mismatch")
    if selection.get("source_tree_sha256") != source_tree_sha256(root):
        raise ValueError("selection source tree is no longer current")
    return selection


def create_test_receipt(root: str | Path, selection_path: str | Path, config: LabConfig) -> Path:
    root_path = Path(root).resolve()
    selection = _load_current_selection(selection_path, config, root_path)
    source_before = source_tree_sha256(root_path)
    environment_candidates = {
        "COMSPEC": os.environ.get("COMSPEC"),
        "PATH": os.environ.get("PATH"),
        "PATHEXT": os.environ.get("PATHEXT"),
        "SYSTEMROOT": os.environ.get("SYSTEMROOT"),
        "TEMP": os.environ.get("TEMP"),
        "TMP": os.environ.get("TMP"),
        "WINDIR": os.environ.get("WINDIR"),
    }
    if set(environment_candidates) != set(SAFE_TEST_ENVIRONMENT_KEYS):
        raise AssertionError("test environment allowlist is internally inconsistent")
    environment = {key: value for key, value in environment_candidates.items() if value is not None}
    source_path = str(root_path / "src")
    environment.update(
        {
            "PYTHONHASHSEED": "0",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONNOUSERSITE": "1",
            "PYTHONPATH": source_path,
            "PYTHONSAFEPATH": "1",
            "PYTHONUTF8": "1",
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
    normalized = re.sub(r"Ran (\d+) tests? in [0-9.]+s", r"Ran \1 tests", combined)
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
    receipt = {
        "schema_version": "1.0.0",
        "type": "TEST_RECEIPT",
        "status": status,
        **_bound_fields(selection),
        "runner": f"CPython {sys.version_info.major}.{sys.version_info.minor}",
        "command": ["python", "-P", "-m", "unittest", "discover", "-s", "tests", "-v"],
        "return_code": completed.returncode,
        "test_count": count,
        "environment_policy": "ALLOWLIST_NO_CREDENTIAL_ENV",
        "source_tree_sha256_before": source_before,
        "source_tree_sha256_after": source_after,
        "normalized_output_sha256": log_sha,
        "normalized_output_path": log_path.relative_to(root_path).as_posix(),
    }
    receipt["receipt_sha256"] = hashlib.sha256(canonical_json(receipt)).hexdigest()
    path = root_path / "artifacts" / f"test-receipt-{receipt['receipt_sha256'][:16]}.json"
    write_immutable(path, canonical_json(receipt) + b"\n")
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
    selection = _load_current_selection(selection_path, config, root_path)
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
    payload.decode("utf-8")
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
    return path
