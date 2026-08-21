from __future__ import annotations

import hashlib
import json
import math
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import LabConfig, canonical_json
from .integrity import (
    is_sha256,
    resolve_regular_file_inside,
    source_tree_sha256,
    verified_hashed_object,
    write_immutable,
)

GENESIS_HASH = "0" * 64
REQUIRED_HOLDOUT_GATES = {
    "holdout_sharpe",
    "holdout_calmar",
    "holdout_drawdown",
    "completed_round_trips",
    "required_cost_stress_return_positive",
    "walk_forward_positive_fraction",
    "holdout_parameter_neighbors_positive",
    "mark_to_market_profit_concentration",
    "source_bound_tests",
    "independent_pro_review",
}


def _event_hash(event_without_hash: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(event_without_hash)).hexdigest()


def _paper_journal(root: Path) -> Path:
    state_root = root / "state"
    paper_directory = state_root / "paper"
    for directory in (state_root, paper_directory):
        if directory.exists() and (
            directory.is_symlink()
            or (hasattr(directory, "is_junction") and directory.is_junction())
        ):
            raise RuntimeError("paper state directories must not be links or junctions")
        try:
            directory.resolve().relative_to(root)
        except ValueError as exc:
            raise RuntimeError("paper state path escapes the repository") from exc
    journal = paper_directory / "journal.jsonl"
    if journal.is_symlink():
        raise RuntimeError("paper journal must not be a symbolic link")
    return journal


@contextmanager
def _single_writer(journal: Path) -> Iterator[None]:
    journal.parent.mkdir(parents=True, exist_ok=True)
    lock = journal.with_suffix(journal.suffix + ".lock")
    acquired = False
    try:
        with lock.open("x", encoding="utf-8") as handle:
            handle.write(str(os.getpid()))
            handle.flush()
            os.fsync(handle.fileno())
        acquired = True
        yield
    finally:
        if acquired and lock.exists():
            lock.unlink()


def _head_path(journal: Path) -> Path:
    return journal.with_suffix(journal.suffix + ".head.json")


def _commit_directory(journal: Path) -> Path:
    return journal.with_suffix(journal.suffix + ".commits")


def _commit_path(journal: Path, sequence: int, event_hash: str) -> Path:
    return _commit_directory(journal) / f"{sequence:020d}-{event_hash}.json"


def _append_event(
    journal: Path, payload: dict[str, Any], *, require_empty: bool = False
) -> dict[str, Any]:
    with _single_writer(journal):
        events = verify_journal(journal)
        if require_empty and events:
            raise RuntimeError("paper journal already exists")
        if events and events[-1]["payload"].get("type") == "PAPER_STOPPED":
            raise RuntimeError("paper journal is already stopped")
        event = {
            "sequence": len(events),
            "previous_hash": events[-1]["event_hash"] if events else GENESIS_HASH,
            "timestamp_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "payload": payload,
        }
        event["event_hash"] = _event_hash(event)
        encoded = canonical_json(event) + b"\n"
        with journal.open("ab") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        commitment = {
            "sequence": event["sequence"],
            "event_hash": event["event_hash"],
            "previous_hash": event["previous_hash"],
        }
        write_immutable(
            _commit_path(journal, event["sequence"], event["event_hash"]),
            canonical_json(commitment) + b"\n",
        )
        head = (
            canonical_json({"event_count": len(events) + 1, "event_hash": event["event_hash"]})
            + b"\n"
        )
        head_path = _head_path(journal)
        temporary = head_path.with_name(f".{head_path.name}.{os.getpid()}.tmp")
        temporary.write_bytes(head)
        temporary.replace(head_path)
        return event


def verify_journal(path: str | Path) -> list[dict[str, Any]]:
    journal = Path(path)
    if not journal.exists():
        head_exists = _head_path(journal).exists()
        commits = _commit_directory(journal)
        commits_exist = commits.exists() and any(commits.iterdir())
        if head_exists or commits_exist:
            raise RuntimeError("paper journal is missing but commitments remain")
        return []
    events: list[dict[str, Any]] = []
    previous = GENESIS_HASH
    for expected_sequence, line in enumerate(journal.read_bytes().splitlines()):
        event = json.loads(line)
        if not isinstance(event, dict):
            # This is persisted-journal corruption, not a caller type error.
            raise RuntimeError("paper journal event is not an object")  # noqa: TRY004
        supplied_hash = event.pop("event_hash", None)
        if not is_sha256(supplied_hash) or not isinstance(event.get("payload"), dict):
            raise RuntimeError("paper journal event shape is invalid")
        if event.get("sequence") != expected_sequence or event.get("previous_hash") != previous:
            raise RuntimeError("paper journal sequence or hash chain is invalid")
        calculated = _event_hash(event)
        if calculated != supplied_hash:
            raise RuntimeError("paper journal event hash mismatch")
        event["event_hash"] = supplied_hash
        events.append(event)
        previous = supplied_hash

    commit_directory = _commit_directory(journal)
    commitments = []
    if commit_directory.exists():
        if commit_directory.is_symlink() or not commit_directory.is_dir():
            raise RuntimeError("paper journal commitment store is invalid")
        commitments = sorted(commit_directory.iterdir(), key=lambda item: item.name)
        if any(path.is_symlink() or not path.is_file() for path in commitments):
            raise RuntimeError("paper journal commitment store contains an invalid entry")
    if len(commitments) != len(events):
        raise RuntimeError("paper journal immutable commitment count mismatch")
    for event, commitment_path in zip(events, commitments, strict=True):
        expected_name = f"{event['sequence']:020d}-{event['event_hash']}.json"
        if commitment_path.name != expected_name:
            raise RuntimeError("paper journal immutable commitment name mismatch")
        expected_commitment = {
            "sequence": event["sequence"],
            "event_hash": event["event_hash"],
            "previous_hash": event["previous_hash"],
        }
        if commitment_path.read_bytes() != canonical_json(expected_commitment) + b"\n":
            raise RuntimeError("paper journal immutable commitment mismatch")

    head_path = _head_path(journal)
    if events and not head_path.exists():
        raise RuntimeError("paper journal head commitment is missing")
    if head_path.exists():
        if head_path.is_symlink() or not head_path.is_file():
            raise RuntimeError("paper journal head commitment is invalid")
        head_payload = head_path.read_bytes()
        head = json.loads(head_payload)
        if not isinstance(head, dict) or head_payload != canonical_json(head) + b"\n":
            raise RuntimeError("paper journal head commitment is not canonical")
        if head.get("event_count") != len(events) or head.get("event_hash") != previous:
            raise RuntimeError("paper journal was truncated or its head is inconsistent")
    return events


def _canonical_state(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"required provenance state is missing: {path.name}")
    payload = path.read_bytes()
    value = json.loads(payload)
    if not isinstance(value, dict) or payload != canonical_json(value) + b"\n":
        raise ValueError(f"provenance state is not canonical: {path.name}")
    return value


def _artifact(root: Path, prefix: str, digest: object) -> Path:
    if not is_sha256(digest):
        raise ValueError(f"{prefix} digest is malformed")
    relative = f"artifacts/{prefix}-{str(digest)[:16]}.json"
    return resolve_regular_file_inside(root, relative, "artifacts")


def _require_fields(value: dict[str, Any], expected: dict[str, Any], label: str) -> None:
    for field, wanted in expected.items():
        if value.get(field) != wanted:
            raise ValueError(f"{label} {field} mismatch")


def _verified_finalized_report(
    root: Path, report_path: str | Path, config: LabConfig
) -> dict[str, Any]:
    supplied_path = Path(report_path)
    if supplied_path.is_absolute():
        try:
            relative = supplied_path.resolve().relative_to(root).as_posix()
        except ValueError as exc:
            raise ValueError("paper report must be inside the repository") from exc
    else:
        relative = supplied_path.as_posix()
    report_file = resolve_regular_file_inside(root, relative, "artifacts")
    report = verified_hashed_object(report_file, "report_sha256")
    if report_file.name != f"holdout-{report['report_sha256'][:16]}.json":
        raise ValueError("paper report filename is not content-addressed")

    _require_fields(
        report,
        {
            "schema_version": "1.1.0",
            "status": "BACKTEST_CANDIDATE",
            "capability": "LIVE_DISABLED",
            "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
            "profit_claim": "NONE",
            "evaluation_label": "RETROSPECTIVE_LOCKED_OOS",
            "evidence_level": "RETROSPECTIVE_LOCKED_OOS",
            "config_sha256": config.config_sha256,
            "source_tree_sha256": source_tree_sha256(root),
        },
        "paper report",
    )
    gates = report.get("gates")
    if (
        not isinstance(gates, dict)
        or set(gates) != REQUIRED_HOLDOUT_GATES
        or any(value is not True for value in gates.values())
    ):
        raise ValueError("paper report does not contain the exact passing holdout gates")

    selection_file = _artifact(root, "selection", report.get("selection_sha256"))
    selection = verified_hashed_object(selection_file, "selection_sha256")
    _require_fields(
        selection,
        {
            "status": "FROZEN_CANDIDATE",
            "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
            "experiment_id": report.get("experiment_id"),
            "selection_sha256": report.get("selection_sha256"),
            "config_sha256": report.get("config_sha256"),
            "source_tree_sha256": report.get("source_tree_sha256"),
            "holdout_commitment_sha256": report.get("holdout_data_sha256"),
            "selected_params": report.get("selected_params"),
        },
        "paper selection",
    )

    test_file = _artifact(root, "test-receipt", report.get("test_receipt_sha256"))
    review_file = _artifact(root, "pro-review-receipt", report.get("review_receipt_sha256"))
    test_receipt = verified_hashed_object(test_file, "receipt_sha256")
    review_receipt = verified_hashed_object(review_file, "receipt_sha256")
    receipt_binding = {
        "experiment_id": selection.get("experiment_id"),
        "selection_sha256": selection.get("selection_sha256"),
        "config_sha256": selection.get("config_sha256"),
        "source_tree_sha256": selection.get("source_tree_sha256"),
        "preholdout_data_sha256": selection.get("preholdout_data_sha256"),
        "holdout_commitment_sha256": selection.get("holdout_commitment_sha256"),
    }
    _require_fields(
        test_receipt,
        {"type": "TEST_RECEIPT", "status": "PASS", **receipt_binding},
        "test receipt",
    )
    _require_fields(
        review_receipt,
        {"type": "PRO_REVIEW_RECEIPT", "verdict": "PROCEED", **receipt_binding},
        "Pro review receipt",
    )

    experiment_id = report.get("experiment_id")
    if not is_sha256(experiment_id):
        raise ValueError("paper report experiment_id is malformed")
    experiments = root / "state" / "experiments"
    if experiments.is_symlink() or (
        hasattr(experiments, "is_junction") and experiments.is_junction()
    ):
        raise ValueError("paper experiment store must not be a link or junction")
    try:
        experiments.resolve().relative_to(root)
    except ValueError as exc:
        raise ValueError("paper experiment store escapes the repository") from exc
    experiment = experiments / str(experiment_id)
    if experiment.is_symlink() or experiment.resolve().parent != experiments.resolve():
        raise ValueError("paper experiment state path is invalid")
    frozen = _canonical_state(experiment / "FROZEN.json")
    opened = _canonical_state(experiment / "HOLDOUT_OPENED.json")
    finalized = _canonical_state(experiment / "FINALIZED.json")
    _require_fields(
        frozen,
        {
            "state": "FROZEN",
            "experiment_id": experiment_id,
            "selection_path": selection_file.relative_to(root).as_posix(),
            "selection_sha256": selection["selection_sha256"],
            "config_sha256": selection["config_sha256"],
            "source_tree_sha256": selection["source_tree_sha256"],
            "preholdout_data_sha256": selection["preholdout_data_sha256"],
            "holdout_commitment_sha256": selection["holdout_commitment_sha256"],
        },
        "FROZEN state",
    )
    _require_fields(
        opened,
        {
            "state": "HOLDOUT_OPENED",
            "experiment_id": experiment_id,
            "selection_sha256": selection["selection_sha256"],
            "holdout_manifest_sha256": report.get("holdout_manifest_sha256"),
            "test_receipt_sha256": test_receipt["receipt_sha256"],
            "review_receipt_sha256": review_receipt["receipt_sha256"],
        },
        "HOLDOUT_OPENED state",
    )
    _require_fields(
        finalized,
        {
            "state": "FINALIZED",
            "experiment_id": experiment_id,
            "report_path": report_file.relative_to(root).as_posix(),
            "report_sha256": report["report_sha256"],
            "status": "BACKTEST_CANDIDATE",
        },
        "FINALIZED state",
    )
    return report


def initialize_paper(
    root: str | Path,
    report_path: str | Path,
    config: LabConfig,
    capital: float,
) -> dict[str, Any]:
    if not math.isfinite(capital) or capital <= 0:
        raise ValueError("paper capital must be finite and positive")
    root_path = Path(root).resolve()
    journal = _paper_journal(root_path)
    report = _verified_finalized_report(root_path, report_path, config)
    event = _append_event(
        journal,
        {
            "type": "PAPER_INITIALIZED",
            "mode": "PAPER",
            "currency": "SIMULATED_USDT",
            "capital": capital,
            "report_sha256": report["report_sha256"],
            "experiment_id": report["experiment_id"],
            "selection_sha256": report["selection_sha256"],
            "config_sha256": config.config_sha256,
            "test_receipt_sha256": report["test_receipt_sha256"],
            "review_receipt_sha256": report["review_receipt_sha256"],
            "live_execution": "UNAVAILABLE",
        },
        require_empty=True,
    )
    return {"status": "PAPER_INITIALIZED", "event_hash": event["event_hash"]}


def stop_paper(root: str | Path) -> dict[str, Any]:
    root_path = Path(root).resolve()
    journal = _paper_journal(root_path)
    if not journal.exists():
        raise RuntimeError("paper journal is not initialized")
    events = verify_journal(journal)
    if events[-1]["payload"].get("type") != "PAPER_STOPPED":
        _append_event(journal, {"type": "PAPER_STOPPED"})
    return paper_status(root_path)


def paper_status(root: str | Path) -> dict[str, Any]:
    root_path = Path(root).resolve()
    journal = _paper_journal(root_path)
    events = verify_journal(journal)
    return {
        "initialized": bool(events),
        "stopped": bool(events) and events[-1]["payload"].get("type") == "PAPER_STOPPED",
        "event_count": len(events),
        "last_event_hash": events[-1]["event_hash"] if events else None,
        "live_execution": "UNAVAILABLE",
    }
