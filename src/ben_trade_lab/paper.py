from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import statistics
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .anchor import read_holdout_opened_anchor, verify_anchor_store
from .config import LabConfig, canonical_json, parse_utc_ms
from .data import (
    HOUR_MS,
    bind_manifest_to_config,
    load_bars_from_manifest,
    read_manifest_metadata,
)
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
from .metrics import buy_and_hold_metrics
from .validation import (
    BENCHMARK_PROTOCOL,
    FINALIZED_STATE_FIELDS,
    FROZEN_STATE_FIELDS,
    HOLDOUT_GATE_FIELDS,
    HOLDOUT_REPORT_SCHEMA_VERSION,
    MAX_HOLDOUT_CONTEXT_HOURS,
    OPENED_STATE_FIELDS,
    _apply_exact_window_duration,
    _assumptions,
    _build_opened_state_base,
    _deserialize_bar,
    _is_adjacent,
    _opened_state_base,
    _opening_commitment_sha256,
    _params,
    _preholdout_candidate_eligible,
    _read_canonical_state,
    _require_anchor_matches_opened,
    _require_config_anchor_store_id,
    _require_witness_burn_matches_opened,
    _select_primary_candidate,
    _validate_performance_metrics,
    _verified_holdout_report_artifact,
    _verified_selection_artifact,
    _verify_configured_witness,
    _window_result,
)

GENESIS_HASH = "0" * 64
REQUIRED_HOLDOUT_GATES = HOLDOUT_GATE_FIELDS


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


def _finite_report_number(value: object, label: str) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{label} must be finite")
    return float(value)


def _numbers_equal(left: object, right: object) -> bool:
    try:
        left_value = _finite_report_number(left, "left comparison value")
        right_value = _finite_report_number(right, "right comparison value")
    except ValueError:
        return False
    return math.isclose(left_value, right_value, rel_tol=1e-12, abs_tol=1e-12)


def _require_metric_window(
    metrics: dict[str, Any],
    *,
    label: str,
    start_ms: int,
    end_ms_exclusive: int,
    initial_cash: float,
) -> None:
    expected_points = (end_ms_exclusive - start_ms) // HOUR_MS
    exact = {
        "start_open_time_ms": start_ms,
        "end_open_time_ms": end_ms_exclusive - HOUR_MS,
        "equity_points": expected_points,
    }
    _require_fields(metrics, exact, label)
    if not _numbers_equal(metrics.get("elapsed_hours"), float(expected_points)):
        raise ValueError(f"{label} elapsed_hours mismatch")
    if not _numbers_equal(metrics.get("initial_cash"), initial_cash):
        raise ValueError(f"{label} initial_cash mismatch")


def _require_frozen_report_semantics(
    report: dict[str, Any], selection: dict[str, Any], config: LabConfig
) -> None:
    """Recompute every PAPER-authorizing gate from frozen report inputs."""

    selected_params = report["selected_params"]
    grid = config.strategy_grid()
    if sum(params.as_dict() == selected_params for params in grid) != 1:
        raise ValueError("paper report selected params are outside the frozen grid")
    expected_neighbors = [
        params.as_dict()
        for params in grid
        if params.as_dict() != selected_params
        and _is_adjacent(selected_params, params.as_dict(), grid)
    ]
    frozen_neighbors = selection.get("selected_parameter_neighbors")
    if frozen_neighbors != expected_neighbors:
        raise ValueError("paper selection frozen neighbor set mismatch")
    frozen_neighbor_count = selection.get("preholdout_neighbor_count")
    if (
        not isinstance(frozen_neighbor_count, int)
        or isinstance(frozen_neighbor_count, bool)
        or frozen_neighbor_count != len(expected_neighbors)
    ):
        raise ValueError("paper selection frozen neighbor count mismatch")

    report_neighbors = report["holdout_parameter_neighbors"]
    if [item["params"] for item in report_neighbors] != expected_neighbors:
        raise ValueError("paper report holdout neighbor set mismatch")
    protocol = report["holdout_parameter_neighbor_protocol"]
    if protocol != {
        "primary_excluded": True,
        "replacement_allowed": False,
        "exact_frozen_neighbor_count": len(expected_neighbors),
    }:
        raise ValueError("paper report holdout neighbor protocol mismatch")
    neighbor_fraction = (
        sum(float(item["metrics"]["total_return"]) > 0.0 for item in report_neighbors)
        / len(report_neighbors)
        if report_neighbors
        else 0.0
    )
    if not _numbers_equal(
        report.get("holdout_parameter_neighbor_positive_fraction"), neighbor_fraction
    ):
        raise ValueError("paper report holdout neighbor fraction mismatch")
    preholdout_neighbor_fraction = _finite_report_number(
        selection.get("preholdout_neighbor_positive_fraction"),
        "paper selection preholdout neighbor fraction",
    )
    if not 0.0 <= preholdout_neighbor_fraction <= 1.0 or not _numbers_equal(
        report.get("preholdout_parameter_neighbor_fraction"),
        preholdout_neighbor_fraction,
    ):
        raise ValueError("paper report preholdout neighbor fraction mismatch")

    holdout_start = parse_utc_ms(config.splits["validation_end_utc_exclusive"])
    holdout_end = parse_utc_ms(config.splits["locked_holdout_end_utc_exclusive"])
    indicator_context = max(
        int(selected_params[field])
        for field in (
            "entry_lookback",
            "exit_lookback",
            "trend_lookback",
            "volatility_lookback",
        )
    )
    if report["holdout"] != {
        "start_ms": holdout_start,
        "end_ms_exclusive": holdout_end,
        "account_state": "RESET_TO_INITIAL_CASH",
        "strategy_state": "RESET_FIRST_SIGNAL_FROM_HOLDOUT",
        "indicator_context_hours": indicator_context,
    }:
        raise ValueError("paper report holdout protocol mismatch")
    if report.get("benchmark_protocol") != BENCHMARK_PROTOCOL:
        raise ValueError("paper report benchmark protocol mismatch")

    initial_cash = float(config.execution["initial_cash"])
    metric_sets: list[tuple[str, dict[str, Any]]] = [
        ("paper report metrics", report["metrics"]),
        ("paper report benchmark metrics", report["benchmark_buy_and_hold"]),
        ("paper report latency metrics", report["latency_stress"]["metrics"]),
    ]
    metric_sets.extend(
        (f"paper report cost stress {key}", metrics)
        for key, metrics in report["cost_stress"].items()
    )
    metric_sets.extend(
        (f"paper report neighbor {index} metrics", item["metrics"])
        for index, item in enumerate(report_neighbors)
    )
    for label, metrics in metric_sets:
        _require_metric_window(
            metrics,
            label=label,
            start_ms=holdout_start,
            end_ms_exclusive=holdout_end,
            initial_cash=initial_cash,
        )

    required_multiplier = float(
        config.acceptance["minimum_positive_cost_stress_multiplier"]
    )
    required_cost_key = f"{required_multiplier:g}x"
    expected_cost_keys = {required_cost_key, "3x"}
    if set(report["cost_stress"]) != expected_cost_keys:
        raise ValueError("paper report cost stress multipliers mismatch")
    latency_delay = int(config.raw["diagnostics"]["latency_stress_delay_bars"])
    if report["latency_stress"]["signal_delay_bars"] != latency_delay:
        raise ValueError("paper report latency stress delay mismatch")

    candidates = selection.get("candidates")
    if not isinstance(candidates, list):
        raise TypeError("paper selection candidates are malformed")
    selected_candidates = [
        item
        for item in candidates
        if isinstance(item, dict) and item.get("params") == selected_params
    ]
    if len(selected_candidates) != 1:
        raise ValueError("paper selection must contain the primary exactly once")
    positive_fold_fraction = _finite_report_number(
        selected_candidates[0].get("positive_fold_fraction"),
        "paper selection positive fold fraction",
    )
    if not 0.0 <= positive_fold_fraction <= 1.0:
        raise ValueError("paper selection positive fold fraction is outside [0, 1]")

    base = report["metrics"]
    base_calmar = base.get("calmar")
    calmar_gate = (
        isinstance(base_calmar, (int, float))
        and not isinstance(base_calmar, bool)
        and math.isfinite(float(base_calmar))
        and float(base_calmar) >= float(config.acceptance["minimum_holdout_calmar"])
    )
    recomputed_gates = {
        "holdout_sharpe": float(base["annualized_sharpe_daily"])
        >= float(config.acceptance["minimum_holdout_sharpe"]),
        "holdout_calmar": calmar_gate,
        "holdout_drawdown": float(base["maximum_drawdown"])
        >= -float(config.acceptance["maximum_holdout_drawdown"]),
        "completed_round_trips": int(base["completed_round_trips"])
        >= int(config.acceptance["minimum_completed_round_trips"]),
        "required_cost_stress_return_positive": float(
            report["cost_stress"][required_cost_key]["total_return"]
        )
        > 0.0,
        "walk_forward_positive_fraction": positive_fold_fraction
        >= float(config.raw["selection"]["minimum_positive_fold_fraction"]),
        "holdout_parameter_neighbors_positive": neighbor_fraction
        >= float(config.acceptance["minimum_positive_parameter_neighbors_fraction"]),
        "mark_to_market_profit_concentration": float(
            base["maximum_positive_quarter_mark_to_market_profit_concentration"]
        )
        <= float(config.acceptance["maximum_single_quarter_profit_concentration"]),
        "source_bound_tests": True,
        "independent_pro_review": True,
    }
    if report.get("gates") != recomputed_gates:
        raise ValueError("paper report gates do not match frozen metrics and thresholds")
    if not all(recomputed_gates.values()):
        raise ValueError("paper report does not independently pass every frozen gate")


def _require_recomputed_locked_report(
    root: Path,
    report: dict[str, Any],
    selection: dict[str, Any],
    config: LabConfig,
    locked_metadata: dict[str, Any],
) -> None:
    """Re-run every reported OOS scenario before granting PAPER authority."""

    holdout_bars, holdout_manifest = load_bars_from_manifest(
        selection["locked_holdout_manifest_path"],
        root=root,
        expected_kind="LOCKED_HOLDOUT",
        allow_locked_data=True,
    )
    bind_manifest_to_config(holdout_manifest, config, "LOCKED_HOLDOUT")
    if holdout_manifest != locked_metadata:
        raise ValueError("paper locked manifest changed after metadata verification")
    context = [_deserialize_bar(value) for value in selection["warmup_context"]]
    if not context or len(context) > MAX_HOLDOUT_CONTEXT_HOURS:
        raise ValueError("paper frozen warmup context is invalid")
    selected = _params(selection["selected_params"])
    holdout_start = parse_utc_ms(config.splits["validation_end_utc_exclusive"])
    holdout_end = parse_utc_ms(config.splits["locked_holdout_end_utc_exclusive"])
    bars = context + holdout_bars
    base = _window_result(
        bars,
        selected,
        _assumptions(config),
        holdout_start,
        holdout_end,
    )
    required_multiplier = float(
        config.acceptance["minimum_positive_cost_stress_multiplier"]
    )
    expected_cost_stress = {
        f"{required_multiplier:g}x": _window_result(
            bars,
            selected,
            _assumptions(config, required_multiplier),
            holdout_start,
            holdout_end,
        ),
        "3x": _window_result(
            bars,
            selected,
            _assumptions(config, 3.0),
            holdout_start,
            holdout_end,
        ),
    }
    latency_delay = int(config.raw["diagnostics"]["latency_stress_delay_bars"])
    expected_latency = {
        "signal_delay_bars": latency_delay,
        "metrics": _window_result(
            bars,
            selected,
            _assumptions(config, signal_delay_bars=latency_delay),
            holdout_start,
            holdout_end,
        ),
    }
    expected_neighbors = [
        {
            "params": value,
            "metrics": _window_result(
                bars,
                _params(value),
                _assumptions(config),
                holdout_start,
                holdout_end,
            ),
        }
        for value in selection["selected_parameter_neighbors"]
    ]
    expected_neighbor_fraction = (
        sum(item["metrics"]["total_return"] > 0 for item in expected_neighbors)
        / len(expected_neighbors)
        if expected_neighbors
        else 0.0
    )
    expected_benchmark = _apply_exact_window_duration(
        buy_and_hold_metrics(holdout_bars, _assumptions(config)),
        holdout_start,
        holdout_end,
    )
    gap_events = [
        item
        for item in holdout_manifest.get("declared_source_anomalies", [])
        if item.get("type") == "MISSING_HOURLY_BARS"
    ]
    expected = {
        "metrics": base,
        "cost_stress": expected_cost_stress,
        "latency_stress": expected_latency,
        "holdout_parameter_neighbors": expected_neighbors,
        "holdout_parameter_neighbor_positive_fraction": expected_neighbor_fraction,
        "benchmark_buy_and_hold": expected_benchmark,
        "holdout_gap_events": len(gap_events),
        "holdout_missing_hours": sum(
            int(item["missing_bar_count"]) for item in gap_events
        ),
    }
    for field, expected_value in expected.items():
        if report.get(field) != expected_value:
            raise ValueError(f"paper report {field} differs from deterministic replay")


def _require_recomputed_selection_aggregates(
    selection: dict[str, Any],
    config: LabConfig,
) -> None:
    candidates = selection.get("candidates")
    if not isinstance(candidates, list):
        raise TypeError("paper selection candidates are malformed")
    grid = config.strategy_grid()
    expected_params = [item.as_dict() for item in grid]
    if (
        selection.get("trial_count") != len(candidates)
        or len(candidates) != int(config.raw["selection"]["maximum_trials"])
    ):
        raise ValueError("paper selection trial count mismatch")
    candidate_fields = {
        "params",
        "eligible",
        "folds",
        "median_calmar",
        "median_sharpe",
        "median_total_return",
        "positive_fold_fraction",
        "continuous_daily_returns",
    }
    minimum_trips = int(
        config.raw["selection"]["minimum_fold_completed_round_trips"]
    )
    minimum_exposure = float(
        config.raw["selection"]["minimum_fold_exposure_fraction"]
    )
    maximum_exposure = float(
        config.raw["selection"]["maximum_fold_exposure_fraction"]
    )
    minimum_positive = float(
        config.raw["selection"]["minimum_positive_fold_fraction"]
    )
    for index, candidate in enumerate(candidates):
        if not isinstance(candidate, dict) or set(candidate) != candidate_fields:
            raise ValueError("paper selection candidate schema mismatch")
        if candidate["params"] != expected_params[index]:
            raise ValueError("paper selection candidate grid/order mismatch")
        folds = candidate.get("folds")
        if not isinstance(folds, list) or len(folds) != int(
            config.raw["selection"]["expected_fold_count"]
        ):
            raise ValueError("paper selection fold count mismatch")
        for fold_number, fold in enumerate(folds, start=1):
            if not isinstance(fold, dict) or set(fold) != {
                "fold_number",
                "start_ms",
                "end_ms_exclusive",
                "account_boundary",
                "metrics",
            }:
                raise ValueError("paper selection fold schema mismatch")
            if fold.get("fold_number") != fold_number:
                raise ValueError("paper selection fold numbering mismatch")
            _validate_performance_metrics(
                fold.get("metrics"),
                f"paper selection candidate {index} fold {fold_number}",
            )
        returns = [float(item["metrics"]["total_return"]) for item in folds]
        sharpes = [float(item["metrics"]["annualized_sharpe_daily"]) for item in folds]
        calmars = [item["metrics"]["calmar"] for item in folds]
        positive_fraction = sum(value > 0.0 for value in returns) / len(returns)
        eligible = _preholdout_candidate_eligible(
            folds,
            minimum_trips=minimum_trips,
            minimum_exposure=minimum_exposure,
            maximum_exposure=maximum_exposure,
            minimum_positive_fraction=minimum_positive,
        )
        expected_scalars = {
            "eligible": eligible,
            "median_calmar": statistics.median(calmars) if eligible else None,
            "median_sharpe": statistics.median(sharpes),
            "median_total_return": statistics.median(returns),
            "positive_fold_fraction": positive_fraction,
        }
        for field, expected in expected_scalars.items():
            if candidate.get(field) != expected:
                raise ValueError(f"paper selection candidate {field} mismatch")
    if selection.get("fold_count") != int(config.raw["selection"]["expected_fold_count"]):
        raise ValueError("paper selection declared fold count mismatch")
    selected = _select_primary_candidate(candidates)
    if selected is None or selected["params"] != selection.get("selected_params"):
        raise ValueError("paper selection primary candidate mismatch")
    expected_neighbors = [
        item
        for item in candidates
        if _is_adjacent(selection["selected_params"], item["params"], grid)
    ]
    if selection.get("selected_parameter_neighbors") != [
        item["params"] for item in expected_neighbors
    ]:
        raise ValueError("paper selection neighbor set mismatch")
    expected_neighbor_fraction = (
        sum(item["eligible"] and item["median_total_return"] > 0 for item in expected_neighbors)
        / len(expected_neighbors)
        if expected_neighbors
        else 0.0
    )
    if not math.isclose(
        float(selection.get("preholdout_neighbor_positive_fraction", -1.0)),
        expected_neighbor_fraction,
        rel_tol=1e-12,
        abs_tol=1e-12,
    ):
        raise ValueError("paper selection neighbor fraction mismatch")


def _verified_finalized_report(
    root: Path,
    report_path: str | Path,
    config: LabConfig,
    *,
    anchor_root: str | Path,
    anchor_store_id: str,
    witness_ledger: str | Path,
    witness_store_id: str,
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
            "schema_version": HOLDOUT_REPORT_SCHEMA_VERSION,
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
            "locked_holdout_manifest_sha256": report.get("holdout_manifest_sha256"),
            "holdout_commitment_sha256": report.get("holdout_data_sha256"),
            "selected_params": report.get("selected_params"),
        },
        "paper selection",
    )
    _require_recomputed_selection_aggregates(selection, config)
    locked_metadata = read_manifest_metadata(
        selection["locked_holdout_manifest_path"],
        root=root,
    )
    bind_manifest_to_config(locked_metadata, config, "LOCKED_HOLDOUT")
    locked_bindings = {
        "manifest_path": selection["locked_holdout_manifest_path"],
        "manifest_file_sha256": selection["locked_holdout_manifest_sha256"],
        "partition_descriptor_sha256": selection[
            "locked_partition_descriptor_sha256"
        ],
        "paired_partition_kind": "PREHOLDOUT",
        "paired_partition_descriptor_sha256": selection[
            "preholdout_partition_descriptor_sha256"
        ],
        "lockbox_id": selection["lockbox_id"],
        "normalized_sha256": selection["holdout_commitment_sha256"],
        "preholdout_sha256": selection["preholdout_data_sha256"],
        "parent_manifest_sha256": selection["parent_manifest_sha256"],
    }
    for field, expected in locked_bindings.items():
        if locked_metadata.get(field) != expected:
            raise ValueError(f"paper locked holdout manifest {field} mismatch")

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
    _require_frozen_report_semantics(report, selection, config)

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
    witness = _verify_configured_witness(
        witness_ledger,
        witness_store_id=witness_store_id,
        config=config,
        root=root,
    )
    opening_burn = witness.burn_for(str(experiment_id))
    if opening_burn is None:
        raise ValueError("paper authorization has no append-only opening burn")
    opening_commitment = _opening_commitment_sha256(
        selection=selection,
        frozen=frozen,
        metadata=locked_metadata,
        test_receipt=test_receipt,
        review_receipt=review_receipt,
        config=config,
        anchor_store_id=anchor_store.store_id,
        anchor_store_sha256=anchor_store.store_sha256,
        witness=witness,
        opened_at_utc=opened["opened_at_utc"],
    )
    expected_opened_base = _build_opened_state_base(
        selection=selection,
        frozen=frozen,
        metadata=locked_metadata,
        test_receipt=test_receipt,
        review_receipt=review_receipt,
        config=config,
        anchor_store_id=anchor_store.store_id,
        anchor_store_sha256=anchor_store.store_sha256,
        witness=witness,
        burn=opening_burn,
        opening_commitment_sha256=opening_commitment,
    )
    if _opened_state_base(opened) != expected_opened_base:
        raise ValueError("paper HOLDOUT_OPENED base does not match the frozen opening plan")
    _require_witness_burn_matches_opened(witness, opening_burn, opened)
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
    report_provenance = {
        "holdout_opened_state_sha256": opened["state_sha256"],
        "opening_commitment_sha256": opened["opening_commitment_sha256"],
        "external_anchor_store_id": anchor_store.store_id,
        "external_anchor_store_sha256": anchor_store.store_sha256,
        "external_anchor_sha256": external_anchor["anchor_sha256"],
        "witness_store_id": witness.store_id,
        "witness_header_sha256": witness.header_sha256,
        "witness_filesystem_device": witness.filesystem_device,
        "witness_filesystem_inode": witness.filesystem_inode,
        "witness_burn_sequence": opening_burn.sequence,
        "witness_burn_sha256": opening_burn.record_sha256,
    }
    _require_fields(report, report_provenance, "paper report opening provenance")
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
    finalization = witness.finalization_for(str(experiment_id))
    if finalization is None:
        raise ValueError("paper authorization has no append-only final report commitment")
    finalization_expected = {
        "experiment_id": str(experiment_id),
        "opening_burn_record_sha256": opening_burn.record_sha256,
        "opened_state_sha256": opened["state_sha256"],
        "external_anchor_sha256": external_anchor["anchor_sha256"],
        "report_sha256": report["report_sha256"],
        "report_status": report["status"],
        "report_kind": report["report_kind"],
        "finalized_at_utc": finalized["finalized_at_utc"],
        "sequence": finalized["witness_finalization_sequence"],
        "record_sha256": finalized["witness_finalization_sha256"],
    }
    for field, expected in finalization_expected.items():
        if getattr(finalization, field) != expected:
            raise ValueError(f"paper witness finalization {field} mismatch")
    _require_recomputed_locked_report(
        root,
        report,
        selection,
        config,
        locked_metadata,
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
    witness_ledger: str | Path,
    witness_store_id: str,
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
        witness_ledger=witness_ledger,
        witness_store_id=witness_store_id,
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
            "holdout_opened_state_sha256": report["holdout_opened_state_sha256"],
            "opening_commitment_sha256": report["opening_commitment_sha256"],
            "external_anchor_sha256": report["external_anchor_sha256"],
            "witness_burn_sha256": report["witness_burn_sha256"],
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
