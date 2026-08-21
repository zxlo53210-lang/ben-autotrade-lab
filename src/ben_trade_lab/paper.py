from __future__ import annotations

import hashlib
import json
import math
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .anchor import read_holdout_opened_anchor, verify_anchor_store
from .config import LabConfig, canonical_json
from .integrity import (
    fsync_directory,
    is_link_or_reparse,
    is_sha256,
    require_plain_regular_single_link,
    resolve_regular_file_inside,
    source_tree_sha256,
    verified_hashed_object,
    write_immutable,
)
from .validation import (
    FINALIZED_STATE_FIELDS,
    FROZEN_STATE_FIELDS,
    OPENED_STATE_FIELDS,
    _read_canonical_state,
    _require_anchor_matches_opened,
    _require_config_anchor_store_id,
    _verified_holdout_report_artifact,
    _verified_selection_artifact,
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


def _path_present(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _require_paper_file(path: Path, label: str) -> None:
    try:
        require_plain_regular_single_link(path, label)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc


def _require_paper_directory(path: Path, label: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise RuntimeError(f"{label} is missing") from exc
    if is_link_or_reparse(path) or not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeError(f"{label} must be a plain directory")


def _paper_journal(root: Path) -> Path:
    state_root = root / "state"
    paper_directory = state_root / "paper"
    for directory in (state_root, paper_directory):
        if is_link_or_reparse(directory):
            raise RuntimeError("paper state directories must not be links or junctions")
        try:
            directory.resolve().relative_to(root)
        except ValueError as exc:
            raise RuntimeError("paper state path escapes the repository") from exc
    journal = paper_directory / "journal.jsonl"
    if is_link_or_reparse(journal):
        raise RuntimeError("paper journal must not be a link or reparse point")
    return journal


def _flush_paper_directory_chain(journal: Path) -> None:
    directories = (journal.parent,)
    if journal.parent.name == "paper" and journal.parent.parent.name == "state":
        directories = (
            journal.parent,
            journal.parent.parent,
            journal.parent.parent.parent,
        )
    for directory in directories:
        fsync_directory(directory)


@contextmanager
def _single_writer(journal: Path) -> Iterator[None]:
    journal.parent.mkdir(parents=True, exist_ok=True)
    _flush_paper_directory_chain(journal)
    lock = journal.with_suffix(journal.suffix + ".lock")
    acquired = False
    try:
        with lock.open("x", encoding="utf-8") as handle:
            handle.write(str(os.getpid()))
            handle.flush()
            os.fsync(handle.fileno())
        _require_paper_file(lock, "paper writer lock")
        acquired = True
        fsync_directory(journal.parent)
        yield
    finally:
        if acquired and lock.exists():
            lock.unlink()
            fsync_directory(journal.parent)


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
        _require_paper_file(journal, "paper journal")
        fsync_directory(journal.parent)
        commitment = {
            "sequence": event["sequence"],
            "event_hash": event["event_hash"],
            "previous_hash": event["previous_hash"],
        }
        commit_directory = _commit_directory(journal)
        if _path_present(commit_directory):
            _require_paper_directory(commit_directory, "paper journal commitment store")
        commit_path = _commit_path(journal, event["sequence"], event["event_hash"])
        write_immutable(
            commit_path,
            canonical_json(commitment) + b"\n",
        )
        _require_paper_directory(commit_directory, "paper journal commitment store")
        _require_paper_file(commit_path, "paper journal commitment")
        fsync_directory(journal.parent)
        head = (
            canonical_json({"event_count": len(events) + 1, "event_hash": event["event_hash"]})
            + b"\n"
        )
        head_path = _head_path(journal)
        temporary = head_path.with_name(f".{head_path.name}.{os.getpid()}.tmp")
        with temporary.open("xb") as handle:
            handle.write(head)
            handle.flush()
            os.fsync(handle.fileno())
        _require_paper_file(temporary, "paper journal temporary head")
        temporary.replace(head_path)
        _require_paper_file(head_path, "paper journal head")
        fsync_directory(journal.parent)
        return event


def verify_journal(path: str | Path) -> list[dict[str, Any]]:
    journal = Path(path)
    if is_link_or_reparse(journal):
        raise RuntimeError("paper journal must not be a link or reparse point")
    if not journal.exists():
        head_path = _head_path(journal)
        if is_link_or_reparse(head_path):
            raise RuntimeError("paper journal head commitment is invalid")
        head_exists = head_path.exists()
        commits = _commit_directory(journal)
        if _path_present(commits):
            _require_paper_directory(commits, "paper journal commitment store")
        commits_exist = commits.exists() and any(commits.iterdir())
        if head_exists or commits_exist:
            raise RuntimeError("paper journal is missing but commitments remain")
        return []
    _require_paper_file(journal, "paper journal")
    journal_payload = journal.read_bytes()
    _require_paper_file(journal, "paper journal")
    if not journal_payload or not journal_payload.endswith(b"\n"):
        raise RuntimeError("paper journal is not canonical JSONL")
    lines = journal_payload[:-1].split(b"\n")
    if any(not line for line in lines):
        raise RuntimeError("paper journal contains an empty event")
    events: list[dict[str, Any]] = []
    previous = GENESIS_HASH
    expected_event_fields = {"sequence", "previous_hash", "timestamp_utc", "payload", "event_hash"}
    for expected_sequence, line in enumerate(lines):
        try:
            event = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("paper journal event is not valid UTF-8 JSON") from exc
        if not isinstance(event, dict):
            # This is persisted-journal corruption, not a caller type error.
            raise RuntimeError("paper journal event is not an object")  # noqa: TRY004
        if set(event) != expected_event_fields or line != canonical_json(event):
            raise RuntimeError("paper journal event schema or encoding is not canonical")
        supplied_hash = event.get("event_hash")
        if not is_sha256(supplied_hash) or not isinstance(event.get("payload"), dict):
            raise RuntimeError("paper journal event shape is invalid")
        sequence = event.get("sequence")
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence != expected_sequence
            or event.get("previous_hash") != previous
        ):
            raise RuntimeError("paper journal sequence or hash chain is invalid")
        timestamp = event.get("timestamp_utc")
        if not isinstance(timestamp, str) or not timestamp.endswith("Z"):
            raise RuntimeError("paper journal timestamp is invalid")
        try:
            parsed_timestamp = datetime.fromisoformat(timestamp.removesuffix("Z") + "+00:00")
        except ValueError as exc:
            raise RuntimeError("paper journal timestamp is invalid") from exc
        if parsed_timestamp.utcoffset() != UTC.utcoffset(parsed_timestamp):
            raise RuntimeError("paper journal timestamp is not UTC")
        unsigned = {key: value for key, value in event.items() if key != "event_hash"}
        calculated = _event_hash(unsigned)
        if calculated != supplied_hash:
            raise RuntimeError("paper journal event hash mismatch")
        events.append(event)
        previous = supplied_hash

    commit_directory = _commit_directory(journal)
    commitments = []
    if _path_present(commit_directory):
        _require_paper_directory(commit_directory, "paper journal commitment store")
        commitments = sorted(commit_directory.iterdir(), key=lambda item: item.name)
        for commitment_path in commitments:
            _require_paper_file(commitment_path, "paper journal commitment")
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
        commitment_payload = commitment_path.read_bytes()
        _require_paper_file(commitment_path, "paper journal commitment")
        if commitment_payload != canonical_json(expected_commitment) + b"\n":
            raise RuntimeError("paper journal immutable commitment mismatch")

    head_path = _head_path(journal)
    if events and not head_path.exists():
        raise RuntimeError("paper journal head commitment is missing")
    if is_link_or_reparse(head_path):
        raise RuntimeError("paper journal head commitment is invalid")
    if head_path.exists():
        _require_paper_file(head_path, "paper journal head commitment")
        head_payload = head_path.read_bytes()
        _require_paper_file(head_path, "paper journal head commitment")
        head = json.loads(head_payload)
        if (
            not isinstance(head, dict)
            or set(head) != {"event_count", "event_hash"}
            or head_payload != canonical_json(head) + b"\n"
        ):
            raise RuntimeError("paper journal head commitment is not canonical")
        head_count = head.get("event_count")
        if (
            not isinstance(head_count, int)
            or isinstance(head_count, bool)
            or not is_sha256(head.get("event_hash"))
        ):
            raise RuntimeError("paper journal head commitment shape is invalid")
        if head.get("event_count") != len(events) or head.get("event_hash") != previous:
            raise RuntimeError("paper journal was truncated or its head is inconsistent")
    return events


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
    root: Path,
    report_path: str | Path,
    config: LabConfig,
    *,
    anchor_root: str | Path,
    anchor_store_id: str,
) -> dict[str, Any]:
    _require_config_anchor_store_id(config, anchor_store_id)
    supplied_path = Path(report_path)
    if supplied_path.is_absolute():
        try:
            relative = supplied_path.resolve().relative_to(root).as_posix()
        except ValueError as exc:
            raise ValueError("paper report must be inside the repository") from exc
    else:
        relative = supplied_path.as_posix()
    report_file = resolve_regular_file_inside(root, relative, "artifacts")
    report = _verified_holdout_report_artifact(report_file)
    if report_file.name != f"holdout-{report['report_sha256'][:16]}.json":
        raise ValueError("paper report filename is not content-addressed")

    _require_fields(
        report,
        {
            "schema_version": "1.2.0",
            "status": "BACKTEST_CANDIDATE",
            "capability": "LIVE_DISABLED",
            "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
            "profit_claim": "NONE",
            "evaluation_label": "RETROSPECTIVE_LOCKED_OOS",
            "evidence_level": "RETROSPECTIVE_LOCKED_OOS",
            "report_kind": "LOCKED_OOS_EVALUATION",
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
    selection = _verified_selection_artifact(selection_file)
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
    test_receipt = verified_hashed_object(test_file, "receipt_sha256", root=root)
    review_receipt = verified_hashed_object(review_file, "receipt_sha256", root=root)
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
    replay = test_receipt.get("full_provenance_replay")
    if not isinstance(replay, dict) or replay.get("status") != "PASS":
        raise ValueError("test receipt full provenance replay is not PASS")
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
    if (
        experiment.is_symlink()
        or (hasattr(experiment, "is_junction") and experiment.is_junction())
        or experiment.resolve().parent != experiments.resolve()
    ):
        raise ValueError("paper experiment state path is invalid")
    frozen = _read_canonical_state(
        experiment / "FROZEN.json",
        expected_state="FROZEN",
        exact_fields=FROZEN_STATE_FIELDS,
    )
    opened = _read_canonical_state(
        experiment / "HOLDOUT_OPENED.json",
        expected_state="HOLDOUT_OPENED",
        exact_fields=OPENED_STATE_FIELDS,
    )
    finalized = _read_canonical_state(
        experiment / "FINALIZED.json",
        expected_state="FINALIZED",
        exact_fields=FINALIZED_STATE_FIELDS,
    )
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
            "previous_state_sha256": frozen["state_sha256"],
            "selection_sha256": selection["selection_sha256"],
            "holdout_manifest_sha256": report.get("holdout_manifest_sha256"),
            "test_receipt_sha256": test_receipt["receipt_sha256"],
            "review_receipt_sha256": review_receipt["receipt_sha256"],
        },
        "HOLDOUT_OPENED state",
    )
    anchor_store = verify_anchor_store(
        anchor_root,
        repository_root=root,
        expected_store_id=anchor_store_id,
        expected_store_sha256=str(config.raw["anchor"]["store_sha256"]),
    )
    external_anchor = read_holdout_opened_anchor(anchor_store, str(experiment_id))
    _require_anchor_matches_opened(
        opened,
        external_anchor,
        anchor_store_id=anchor_store.store_id,
        config_sha256=config.config_sha256,
        source_tree_sha256_value=selection["source_tree_sha256"],
        preholdout_data_sha256=selection["preholdout_data_sha256"],
        holdout_commitment_sha256=selection["holdout_commitment_sha256"],
    )
    _require_fields(
        finalized,
        {
            "state": "FINALIZED",
            "experiment_id": experiment_id,
            "previous_state_sha256": opened["state_sha256"],
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
    *,
    anchor_root: str | Path,
    anchor_store_id: str,
) -> dict[str, Any]:
    if not math.isfinite(capital) or capital <= 0:
        raise ValueError("paper capital must be finite and positive")
    _require_config_anchor_store_id(config, anchor_store_id)
    root_path = Path(root).resolve()
    journal = _paper_journal(root_path)
    report = _verified_finalized_report(
        root_path,
        report_path,
        config,
        anchor_root=anchor_root,
        anchor_store_id=anchor_store_id,
    )
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
