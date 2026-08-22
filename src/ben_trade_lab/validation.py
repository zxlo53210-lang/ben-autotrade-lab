from __future__ import annotations

import hashlib
import itertools
import json
import math
import statistics
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .anchor import (
    OPENED_STATE_BASE_FIELDS,
    ExternalAnchorNotFound,
    assert_holdout_unopened,
    commit_holdout_opened_anchor,
    read_holdout_opened_anchor,
    verify_anchor_store,
)
from .config import LabConfig, canonical_json, parse_utc_ms
from .data import HOUR_MS, bind_manifest_to_config, load_bars_from_manifest, read_manifest_metadata
from .diagnostics import moving_block_max_sharpe
from .engine import ExecutionAssumptions, run_backtest
from .integrity import (
    fsync_directory,
    is_link_or_reparse,
    require_plain_regular_single_link,
    source_tree_sha256,
    verified_hashed_object,
    write_exclusive,
    write_immutable,
)
from .metrics import (
    TerminalLiquidationNotExecutable,
    boundary_aware_utc_daily_metrics,
    buy_and_hold_metrics,
    calculate_metrics,
)
from .models import BacktestResult, Bar, StrategyParams
from .strategy import build_targets
from .witness import (
    WITNESS_POLICY,
    AppendOnlyWitnessLedger,
    OpeningBurn,
    assert_unburned,
    burn_opening,
    commit_finalization,
    verify_witness_ledger,
)

PREHOLDOUT_FOLD_COUNT = 9
MAX_HOLDOUT_CONTEXT_HOURS = 720
SELECTION_SCHEMA_VERSION = "1.2.0"
HOLDOUT_REPORT_SCHEMA_VERSION = "1.3.0"
OPENING_COMMITMENT_SCHEMA_VERSION = "1.0.0"
VALIDATION_METHOD = "CONTINUOUS_NINE_FOLD_VALIDATION_V2"
HOLDOUT_EVALUATION_LABEL = "RETROSPECTIVE_LOCKED_OOS"
REPORT_KIND_LOCKED_OOS_EVALUATION = "LOCKED_OOS_EVALUATION"
REPORT_KIND_TERMINAL_LIQUIDATION_FAILURE = "TERMINAL_LIQUIDATION_FAILURE"
BENCHMARK_PROTOCOL = (
    "FIRST_ELIGIBLE_OOS_CLOSE_SIGNAL_NEXT_ELIGIBLE_OOS_OPEN_FILL_"
    "BASE_COSTS_FINAL_ELIGIBLE_CLOSE_LIQUIDATION_V1"
)

SELECTION_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "authority",
        "method",
        "experiment_id",
        "preholdout_manifest_path",
        "preholdout_manifest_sha256",
        "preholdout_partition_descriptor_sha256",
        "locked_holdout_manifest_path",
        "locked_holdout_manifest_sha256",
        "locked_partition_descriptor_sha256",
        "preholdout_data_sha256",
        "parent_manifest_sha256",
        "holdout_commitment_sha256",
        "lockbox_id",
        "config_sha256",
        "source_tree_sha256",
        "trial_count",
        "fold_count",
        "fold_protocol",
        "selected_params",
        "selection_objective",
        "selection_bias_diagnostic",
        "parameter_adjacency_edges",
        "selected_parameter_neighbors",
        "preholdout_neighbor_count",
        "preholdout_neighbor_positive_fraction",
        "warmup_context_hours",
        "warmup_context",
        "candidates",
        "selection_sha256",
    }
)
HOLDOUT_REPORT_COMMON_FIELDS = frozenset(
    {
        "schema_version",
        "report_kind",
        "status",
        "capability",
        "authority",
        "profit_claim",
        "evaluation_label",
        "evidence_level",
        "experiment_id",
        "selection_sha256",
        "holdout_manifest_sha256",
        "holdout_data_sha256",
        "config_sha256",
        "source_tree_sha256",
        "test_receipt_sha256",
        "review_receipt_sha256",
        "holdout_opened_state_sha256",
        "opening_commitment_sha256",
        "external_anchor_store_id",
        "external_anchor_store_sha256",
        "external_anchor_sha256",
        "witness_store_id",
        "witness_header_sha256",
        "witness_filesystem_device",
        "witness_filesystem_inode",
        "witness_burn_sequence",
        "witness_burn_sha256",
        "selected_params",
        "gates",
        "report_sha256",
    }
)
HOLDOUT_SUCCESS_REPORT_FIELDS = HOLDOUT_REPORT_COMMON_FIELDS | frozenset(
    {
        "holdout",
        "metrics",
        "benchmark_protocol",
        "benchmark_buy_and_hold",
        "cost_stress",
        "latency_stress",
        "preholdout_parameter_neighbor_fraction",
        "holdout_parameter_neighbors",
        "holdout_parameter_neighbor_protocol",
        "holdout_parameter_neighbor_positive_fraction",
        "holdout_gap_events",
        "holdout_missing_hours",
    }
)
HOLDOUT_FAILURE_REPORT_FIELDS = HOLDOUT_REPORT_COMMON_FIELDS | frozenset(
    {"failure_reason", "metrics"}
)
HOLDOUT_GATE_FIELDS = frozenset(
    {
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
)
STRATEGY_PARAMETER_FIELDS = frozenset(
    {
        "entry_lookback",
        "exit_lookback",
        "trend_lookback",
        "volatility_lookback",
        "target_annualized_volatility",
        "volatility_floor",
    }
)
PERFORMANCE_METRIC_FIELDS = frozenset(
    {
        "initial_cash",
        "terminal_equity",
        "mark_to_market_terminal_equity",
        "mark_to_market_total_return",
        "terminal_liquidation_applied",
        "terminal_liquidation_executable",
        "terminal_state_eligible",
        "terminal_liquidation_value",
        "terminal_liquidation_cost",
        "terminal_liquidation_slippage_cost",
        "terminal_liquidation_fee",
        "terminal_liquidation_reference_price",
        "terminal_liquidation_execution_price",
        "terminal_open_quantity",
        "total_return",
        "cagr",
        "annualized_sharpe_daily",
        "annualized_sortino_daily",
        "maximum_drawdown",
        "calmar",
        "completed_round_trips",
        "fill_count",
        "exposure_fraction",
        "total_fees",
        "performance_total_fees_including_terminal_liquidation",
        "maximum_positive_quarter_mark_to_market_profit_concentration",
        "start_open_time_ms",
        "end_open_time_ms",
        "equity_points",
        "elapsed_hours",
    }
)
HOLDOUT_WINDOW_FIELDS = frozenset(
    {
        "start_ms",
        "end_ms_exclusive",
        "account_state",
        "strategy_state",
        "indicator_context_hours",
    }
)
LATENCY_STRESS_FIELDS = frozenset({"signal_delay_bars", "metrics"})
PARAMETER_NEIGHBOR_FIELDS = frozenset({"params", "metrics"})
PARAMETER_NEIGHBOR_PROTOCOL_FIELDS = frozenset(
    {"primary_excluded", "replacement_allowed", "exact_frozen_neighbor_count"}
)


def _report_fields_for_kind(value: dict[str, Any]) -> frozenset[str]:
    """Return the one exact schema permitted for a report kind/status pair."""

    report_kind = value.get("report_kind")
    status = value.get("status")
    if report_kind == REPORT_KIND_LOCKED_OOS_EVALUATION:
        if status not in {"BACKTEST_CANDIDATE", "NOT_PROVEN"}:
            raise ValueError("locked OOS evaluation report status is invalid")
        return HOLDOUT_SUCCESS_REPORT_FIELDS
    if report_kind == REPORT_KIND_TERMINAL_LIQUIDATION_FAILURE:
        if status != "NOT_PROVEN":
            raise ValueError("terminal-liquidation failure report must be NOT_PROVEN")
        return HOLDOUT_FAILURE_REPORT_FIELDS
    raise ValueError("holdout report kind is invalid")


def _is_strict_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _numbers_close(left: object, right: object) -> bool:
    return _is_finite_number(left) and _is_finite_number(right) and math.isclose(
        float(left), float(right), rel_tol=1e-12, abs_tol=1e-12
    )


def _require_exact_object(value: object, fields: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError(f"{label} schema mismatch")
    return value


def _validate_strategy_parameters(value: object, label: str) -> dict[str, Any]:
    params = _require_exact_object(value, STRATEGY_PARAMETER_FIELDS, label)
    for field in (
        "entry_lookback",
        "exit_lookback",
        "trend_lookback",
        "volatility_lookback",
    ):
        if not _is_strict_int(params[field]) or params[field] <= 0:
            raise ValueError(f"{label} {field} must be a positive integer")
    for field in ("target_annualized_volatility", "volatility_floor"):
        if not _is_finite_number(params[field]) or float(params[field]) <= 0:
            raise ValueError(f"{label} {field} must be finite and positive")
    return params


def _validate_performance_metrics(value: object, label: str) -> dict[str, Any]:
    metrics = _require_exact_object(value, PERFORMANCE_METRIC_FIELDS, label)
    boolean_fields = (
        "terminal_liquidation_applied",
        "terminal_liquidation_executable",
        "terminal_state_eligible",
    )
    integer_fields = (
        "completed_round_trips",
        "fill_count",
        "start_open_time_ms",
        "end_open_time_ms",
        "equity_points",
    )
    optional_numeric_fields = {"calmar"}
    for field in boolean_fields:
        if not isinstance(metrics[field], bool):
            raise TypeError(f"{label} {field} must be boolean")
    for field in integer_fields:
        if not _is_strict_int(metrics[field]):
            raise ValueError(f"{label} {field} must be an integer")
    for field in PERFORMANCE_METRIC_FIELDS - set(boolean_fields) - set(integer_fields):
        if field in optional_numeric_fields and metrics[field] is None:
            continue
        if not _is_finite_number(metrics[field]):
            raise ValueError(f"{label} {field} must be finite")

    if float(metrics["initial_cash"]) <= 0:
        raise ValueError(f"{label} initial_cash must be positive")
    for field in (
        "terminal_equity",
        "mark_to_market_terminal_equity",
        "terminal_liquidation_value",
        "terminal_liquidation_cost",
        "terminal_liquidation_slippage_cost",
        "terminal_liquidation_fee",
        "terminal_open_quantity",
        "total_fees",
        "performance_total_fees_including_terminal_liquidation",
    ):
        if float(metrics[field]) < 0:
            raise ValueError(f"{label} {field} must be non-negative")
    for field in (
        "terminal_liquidation_reference_price",
        "terminal_liquidation_execution_price",
        "elapsed_hours",
    ):
        if float(metrics[field]) <= 0:
            raise ValueError(f"{label} {field} must be positive")
    for field in ("completed_round_trips", "fill_count"):
        if int(metrics[field]) < 0:
            raise ValueError(f"{label} {field} must be non-negative")
    if int(metrics["equity_points"]) <= 0:
        raise ValueError(f"{label} equity_points must be positive")
    if int(metrics["start_open_time_ms"]) < 0 or int(metrics["end_open_time_ms"]) < int(
        metrics["start_open_time_ms"]
    ):
        raise ValueError(f"{label} time range is invalid")
    if not -1.0 <= float(metrics["maximum_drawdown"]) <= 0.0:
        raise ValueError(f"{label} maximum_drawdown is outside [-1, 0]")
    for field in (
        "exposure_fraction",
        "maximum_positive_quarter_mark_to_market_profit_concentration",
    ):
        if not 0.0 <= float(metrics[field]) <= 1.0:
            raise ValueError(f"{label} {field} is outside [0, 1]")
    if metrics["terminal_liquidation_executable"] is not True:
        raise ValueError(f"{label} terminal liquidation is not executable")
    if bool(metrics["terminal_liquidation_applied"]) != (
        float(metrics["terminal_open_quantity"]) > 0.0
    ):
        raise ValueError(f"{label} terminal liquidation flag is inconsistent")
    if not _numbers_close(metrics["terminal_equity"], metrics["terminal_liquidation_value"]):
        raise ValueError(f"{label} terminal equity/value mismatch")
    initial_cash = float(metrics["initial_cash"])
    expected_total_return = float(metrics["terminal_equity"]) / initial_cash - 1.0
    expected_mark_to_market_return = (
        float(metrics["mark_to_market_terminal_equity"]) / initial_cash - 1.0
    )
    if not _numbers_close(metrics["total_return"], expected_total_return):
        raise ValueError(f"{label} total_return is inconsistent")
    if not _numbers_close(
        metrics["mark_to_market_total_return"], expected_mark_to_market_return
    ):
        raise ValueError(f"{label} mark_to_market_total_return is inconsistent")
    if float(metrics["terminal_liquidation_execution_price"]) > float(
        metrics["terminal_liquidation_reference_price"]
    ):
        raise ValueError(f"{label} terminal execution price exceeds its reference")
    if float(metrics["performance_total_fees_including_terminal_liquidation"]) < float(
        metrics["total_fees"]
    ):
        raise ValueError(f"{label} terminal-inclusive fees are inconsistent")
    if bool(metrics["terminal_liquidation_applied"]) and not bool(
        metrics["terminal_state_eligible"]
    ):
        raise ValueError(f"{label} terminal liquidation used an ineligible state")
    expected_liquidation_cost = float(metrics["mark_to_market_terminal_equity"]) - float(
        metrics["terminal_liquidation_value"]
    )
    if not _numbers_close(metrics["terminal_liquidation_cost"], expected_liquidation_cost):
        raise ValueError(f"{label} terminal liquidation cost is inconsistent")
    if not _numbers_close(
        metrics["terminal_liquidation_cost"],
        float(metrics["terminal_liquidation_slippage_cost"])
        + float(metrics["terminal_liquidation_fee"]),
    ):
        raise ValueError(f"{label} liquidation cost components are inconsistent")
    if not _numbers_close(
        metrics["performance_total_fees_including_terminal_liquidation"],
        float(metrics["total_fees"]) + float(metrics["terminal_liquidation_fee"]),
    ):
        raise ValueError(f"{label} terminal-inclusive fees do not reconcile")
    expected_slippage_cost = float(metrics["terminal_open_quantity"]) * (
        float(metrics["terminal_liquidation_reference_price"])
        - float(metrics["terminal_liquidation_execution_price"])
    )
    if not _numbers_close(
        metrics["terminal_liquidation_slippage_cost"], expected_slippage_cost
    ):
        raise ValueError(f"{label} terminal slippage cost is inconsistent")
    elapsed_years = max(
        float(metrics["elapsed_hours"]) / (365.25 * 24.0),
        1.0 / 365.25,
    )
    expected_cagr = (
        float(metrics["terminal_equity"]) / float(metrics["initial_cash"])
    ) ** (1.0 / elapsed_years) - 1.0
    if not _numbers_close(metrics["cagr"], expected_cagr):
        raise ValueError(f"{label} cagr is inconsistent")
    drawdown = float(metrics["maximum_drawdown"])
    if drawdown < 0.0:
        expected_calmar = expected_cagr / abs(drawdown)
        if not _numbers_close(metrics["calmar"], expected_calmar):
            raise ValueError(f"{label} calmar is inconsistent")
    elif metrics["calmar"] is not None:
        raise ValueError(f"{label} calmar must be null without drawdown")
    return metrics


def _validate_holdout_report_structure(value: dict[str, Any]) -> None:
    fixed = {
        "capability": "LIVE_DISABLED",
        "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
        "profit_claim": "NONE",
        "evaluation_label": HOLDOUT_EVALUATION_LABEL,
        "evidence_level": HOLDOUT_EVALUATION_LABEL,
    }
    for field, expected in fixed.items():
        if value.get(field) != expected:
            raise ValueError(f"holdout report {field} mismatch")
    for field in ("witness_filesystem_device", "witness_burn_sequence"):
        item = value.get(field)
        if not _is_strict_int(item) or int(item) < 0:
            raise ValueError(f"holdout report {field} is invalid")
    inode = value.get("witness_filesystem_inode")
    if not _is_strict_int(inode) or int(inode) <= 0:
        raise ValueError("holdout report witness_filesystem_inode is invalid")
    if int(value["witness_burn_sequence"]) <= 0:
        raise ValueError("holdout report witness_burn_sequence is invalid")
    _validate_strategy_parameters(value.get("selected_params"), "holdout selected_params")

    if value.get("report_kind") == REPORT_KIND_TERMINAL_LIQUIDATION_FAILURE:
        if value.get("failure_reason") != TerminalLiquidationNotExecutable.code:
            raise ValueError("terminal-liquidation failure reason mismatch")
        if value.get("metrics") is not None:
            raise ValueError("terminal-liquidation failure metrics must be null")
        if value.get("gates") != {"terminal_liquidation_executable": False}:
            raise ValueError("terminal-liquidation failure gates mismatch")
        return

    holdout = _require_exact_object(
        value.get("holdout"), HOLDOUT_WINDOW_FIELDS, "holdout window"
    )
    for field in ("start_ms", "end_ms_exclusive", "indicator_context_hours"):
        if not _is_strict_int(holdout[field]):
            raise ValueError(f"holdout window {field} must be an integer")
    if holdout["start_ms"] < 0 or holdout["end_ms_exclusive"] <= holdout["start_ms"]:
        raise ValueError("holdout window range is invalid")
    if holdout["indicator_context_hours"] <= 0:
        raise ValueError("holdout indicator context must be positive")
    if holdout["account_state"] != "RESET_TO_INITIAL_CASH":
        raise ValueError("holdout account state mismatch")
    if holdout["strategy_state"] != "RESET_FIRST_SIGNAL_FROM_HOLDOUT":
        raise ValueError("holdout strategy state mismatch")

    _validate_performance_metrics(value.get("metrics"), "holdout metrics")
    if value.get("benchmark_protocol") != BENCHMARK_PROTOCOL:
        raise ValueError("holdout benchmark protocol mismatch")
    _validate_performance_metrics(
        value.get("benchmark_buy_and_hold"), "holdout benchmark metrics"
    )

    cost_stress = value.get("cost_stress")
    if not isinstance(cost_stress, dict) or not cost_stress:
        raise ValueError("holdout cost stress schema mismatch")
    for multiplier, metrics in cost_stress.items():
        if not isinstance(multiplier, str) or not multiplier.endswith("x"):
            raise ValueError("holdout cost stress multiplier is malformed")
        try:
            parsed_multiplier = float(multiplier[:-1])
        except ValueError as exc:
            raise ValueError("holdout cost stress multiplier is malformed") from exc
        if not math.isfinite(parsed_multiplier) or parsed_multiplier <= 0:
            raise ValueError("holdout cost stress multiplier is malformed")
        _validate_performance_metrics(metrics, f"holdout cost stress {multiplier}")

    latency = _require_exact_object(
        value.get("latency_stress"), LATENCY_STRESS_FIELDS, "holdout latency stress"
    )
    if not _is_strict_int(latency["signal_delay_bars"]) or latency["signal_delay_bars"] < 1:
        raise ValueError("holdout latency stress delay must be a positive integer")
    _validate_performance_metrics(latency["metrics"], "holdout latency stress metrics")

    preholdout_fraction = value.get("preholdout_parameter_neighbor_fraction")
    holdout_fraction = value.get("holdout_parameter_neighbor_positive_fraction")
    for label, fraction in (
        ("preholdout neighbor fraction", preholdout_fraction),
        ("holdout neighbor fraction", holdout_fraction),
    ):
        if not _is_finite_number(fraction) or not 0.0 <= float(fraction) <= 1.0:
            raise ValueError(f"{label} is outside [0, 1]")

    neighbors = value.get("holdout_parameter_neighbors")
    if not isinstance(neighbors, list):
        raise TypeError("holdout parameter neighbors must be an array")
    neighbor_params: list[dict[str, Any]] = []
    for index, item in enumerate(neighbors):
        neighbor = _require_exact_object(
            item, PARAMETER_NEIGHBOR_FIELDS, f"holdout parameter neighbor {index}"
        )
        neighbor_params.append(
            _validate_strategy_parameters(
                neighbor["params"], f"holdout parameter neighbor {index} params"
            )
        )
        _validate_performance_metrics(
            neighbor["metrics"], f"holdout parameter neighbor {index} metrics"
        )
    if len({canonical_json(item) for item in neighbor_params}) != len(neighbor_params):
        raise ValueError("holdout parameter neighbors contain duplicates")
    selected_params = value["selected_params"]
    if any(item == selected_params for item in neighbor_params):
        raise ValueError("holdout parameter neighbors include the primary")

    protocol = _require_exact_object(
        value.get("holdout_parameter_neighbor_protocol"),
        PARAMETER_NEIGHBOR_PROTOCOL_FIELDS,
        "holdout parameter neighbor protocol",
    )
    if protocol["primary_excluded"] is not True or protocol["replacement_allowed"] is not False:
        raise ValueError("holdout parameter neighbor protocol authority mismatch")
    if (
        not _is_strict_int(protocol["exact_frozen_neighbor_count"])
        or protocol["exact_frozen_neighbor_count"] != len(neighbors)
    ):
        raise ValueError("holdout parameter neighbor count mismatch")
    recomputed_fraction = (
        sum(float(item["metrics"]["total_return"]) > 0.0 for item in neighbors)
        / len(neighbors)
        if neighbors
        else 0.0
    )
    if not _numbers_close(holdout_fraction, recomputed_fraction):
        raise ValueError("holdout parameter neighbor fraction mismatch")

    for field in ("holdout_gap_events", "holdout_missing_hours"):
        if not _is_strict_int(value.get(field)) or int(value[field]) < 0:
            raise ValueError(f"holdout report {field} must be a non-negative integer")
    gates = value.get("gates")
    if (
        not isinstance(gates, dict)
        or set(gates) != HOLDOUT_GATE_FIELDS
        or any(not isinstance(item, bool) for item in gates.values())
    ):
        raise ValueError("holdout report gate schema mismatch")
    if value.get("status") == "BACKTEST_CANDIDATE" and not all(gates.values()):
        raise ValueError("BACKTEST_CANDIDATE report contains a failed gate")
    if value.get("status") == "NOT_PROVEN" and all(gates.values()):
        raise ValueError("NOT_PROVEN report contains only passing gates")

FROZEN_STATE_FIELDS = frozenset(
    {
        "state",
        "experiment_id",
        "selection_path",
        "selection_sha256",
        "config_sha256",
        "source_tree_sha256",
        "preholdout_data_sha256",
        "holdout_commitment_sha256",
        "state_sha256",
    }
)
OPENED_STATE_FIELDS = OPENED_STATE_BASE_FIELDS | frozenset(
    {"external_anchor_sha256", "state_sha256"}
)
FINALIZED_STATE_FIELDS = frozenset(
    {
        "state",
        "experiment_id",
        "previous_state_sha256",
        "report_path",
        "report_sha256",
        "status",
        "finalized_at_utc",
        "witness_finalization_sequence",
        "witness_finalization_sha256",
        "state_sha256",
    }
)


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _experiment_id(
    *,
    method_version: str,
    config_sha256: str,
    source_tree_sha256_value: str,
    lockbox_id: str,
    preholdout_data_sha256: str,
    holdout_commitment_sha256: str,
    selected_params: dict[str, Any] | None,
) -> str:
    identity = {
        "method_version": method_version,
        "config_sha256": config_sha256,
        "source_tree_sha256": source_tree_sha256_value,
        "lockbox_id": lockbox_id,
        "preholdout_data_sha256": preholdout_data_sha256,
        "holdout_commitment_sha256": holdout_commitment_sha256,
        "selected_params": selected_params,
    }
    return hashlib.sha256(canonical_json(identity)).hexdigest()


def _require_selection_experiment_identity(selection: dict[str, Any]) -> None:
    expected = _experiment_id(
        method_version=str(selection.get("method")),
        config_sha256=str(selection.get("config_sha256")),
        source_tree_sha256_value=str(selection.get("source_tree_sha256")),
        lockbox_id=str(selection.get("lockbox_id")),
        preholdout_data_sha256=str(selection.get("preholdout_data_sha256")),
        holdout_commitment_sha256=str(selection.get("holdout_commitment_sha256")),
        selected_params=selection.get("selected_params"),
    )
    if selection.get("experiment_id") != expected:
        raise ValueError("selection experiment identity mismatch")


def _with_state_sha256(unsigned: dict[str, Any]) -> dict[str, Any]:
    if "state_sha256" in unsigned:
        raise ValueError("state hash input must be unsigned")
    value = dict(unsigned)
    value["state_sha256"] = hashlib.sha256(canonical_json(unsigned)).hexdigest()
    return value


def _is_link_or_junction(path: Path) -> bool:
    return is_link_or_reparse(path)


def _path_entry_exists(path: Path) -> bool:
    """Return true for every directory entry, including dangling reparses."""

    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _reject_state_path_links(path: Path) -> None:
    for candidate in (path, path.parent, path.parent.parent, path.parent.parent.parent):
        if _is_link_or_junction(candidate):
            raise ValueError("state path must not contain links or junctions")


def _read_canonical_state(
    path: Path,
    *,
    expected_state: str,
    exact_fields: frozenset[str],
) -> dict[str, Any]:
    _reject_state_path_links(path)
    try:
        require_plain_regular_single_link(path, f"{expected_state} state")
    except ValueError as exc:
        if path.exists() or is_link_or_reparse(path):
            raise
        raise RuntimeError(f"experiment has no {expected_state} receipt") from exc
    payload = path.read_bytes()
    require_plain_regular_single_link(path, f"{expected_state} state")
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{expected_state} state is not canonical JSON") from exc
    if not isinstance(value, dict):
        raise TypeError(f"{expected_state} state must be a JSON object")
    if payload != canonical_json(value) + b"\n":
        raise ValueError(f"{expected_state} state is not canonical JSON")
    if set(value) != exact_fields:
        raise ValueError(f"{expected_state} state schema mismatch")
    if value.get("state") != expected_state:
        raise ValueError(f"{expected_state} state discriminator mismatch")
    if not _is_sha256(value.get("experiment_id")):
        raise ValueError(f"{expected_state} experiment_id is malformed")
    for field in exact_fields:
        if field.endswith("_sha256") and not _is_sha256(value.get(field)):
            raise ValueError(f"{expected_state} {field} is malformed")
    for field in ("selection_path", "report_path"):
        if field not in exact_fields:
            continue
        bound_path = value.get(field)
        if (
            not isinstance(bound_path, str)
            or not bound_path
            or "\\" in bound_path
            or Path(bound_path).is_absolute()
            or ".." in Path(bound_path).parts
        ):
            raise ValueError(f"{expected_state} {field} is malformed")
    if expected_state == "HOLDOUT_OPENED":
        if not _is_sha256(value.get("external_anchor_store_id")):
            raise ValueError("HOLDOUT_OPENED external_anchor_store_id is malformed")
        for field in ("witness_filesystem_device", "witness_burn_sequence"):
            item = value.get(field)
            if not _is_strict_int(item) or int(item) < 0:
                raise ValueError(f"HOLDOUT_OPENED {field} is malformed")
        inode = value.get("witness_filesystem_inode")
        if not _is_strict_int(inode) or int(inode) <= 0:
            raise ValueError("HOLDOUT_OPENED witness_filesystem_inode is malformed")
        if int(value["witness_burn_sequence"]) <= 0:
            raise ValueError("HOLDOUT_OPENED witness_burn_sequence is malformed")
        if value.get("witness_policy") != WITNESS_POLICY:
            raise ValueError("HOLDOUT_OPENED witness_policy mismatch")
        opened_at = value.get("opened_at_utc")
        if not isinstance(opened_at, str) or not opened_at.endswith("Z"):
            raise ValueError("HOLDOUT_OPENED opened_at_utc is malformed")
        try:
            parsed_opened_at = datetime.fromisoformat(opened_at.removesuffix("Z") + "+00:00")
        except ValueError as exc:
            raise ValueError("HOLDOUT_OPENED opened_at_utc is malformed") from exc
        if parsed_opened_at.utcoffset() != UTC.utcoffset(parsed_opened_at):
            raise ValueError("HOLDOUT_OPENED opened_at_utc is not UTC")
        if value.get("witness_burned_at_utc") != opened_at:
            raise ValueError("HOLDOUT_OPENED witness timestamp mismatch")
    if expected_state == "FINALIZED":
        sequence = value.get("witness_finalization_sequence")
        if not _is_strict_int(sequence) or int(sequence) <= 0:
            raise ValueError("FINALIZED witness_finalization_sequence is malformed")
        finalized_at = value.get("finalized_at_utc")
        if not isinstance(finalized_at, str) or not finalized_at.endswith("Z"):
            raise ValueError("FINALIZED finalized_at_utc is malformed")
        try:
            parsed_finalized_at = datetime.fromisoformat(
                finalized_at.removesuffix("Z") + "+00:00"
            )
        except ValueError as exc:
            raise ValueError("FINALIZED finalized_at_utc is malformed") from exc
        if parsed_finalized_at.utcoffset() != UTC.utcoffset(parsed_finalized_at):
            raise ValueError("FINALIZED finalized_at_utc is not UTC")
    supplied = value.get("state_sha256")
    if not _is_sha256(supplied):
        raise ValueError(f"{expected_state} state_sha256 is malformed")
    unsigned = {key: item for key, item in value.items() if key != "state_sha256"}
    if hashlib.sha256(canonical_json(unsigned)).hexdigest() != supplied:
        raise ValueError(f"{expected_state} state_sha256 mismatch")
    return value


def _fsync_parent_directory(directory: Path) -> None:
    """Compatibility wrapper retained for targeted failure-injection tests."""

    fsync_directory(directory)


def _write_state_exclusive(path: Path, unsigned: dict[str, Any]) -> dict[str, Any]:
    _reject_state_path_links(path)
    value = _with_state_sha256(unsigned)
    write_exclusive(path, value)
    _reject_state_path_links(path)
    for directory in (
        path.parent,
        path.parent.parent,
        path.parent.parent.parent,
        path.parent.parent.parent.parent,
    ):
        _fsync_parent_directory(directory)
    return value


def _root_relative_file(path: str | Path, root: Path, label: str) -> tuple[Path, str]:
    unresolved = Path(path)
    if not unresolved.is_absolute():
        unresolved = root / unresolved
    require_plain_regular_single_link(unresolved, label)
    resolved = unresolved.resolve()
    try:
        relative = resolved.relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError(f"{label} must stay inside the repository") from exc
    candidate = root
    for part in Path(relative).parts:
        candidate /= part
        if _is_link_or_junction(candidate):
            raise ValueError(f"{label} must not contain links or junctions")
    require_plain_regular_single_link(resolved, label)
    return resolved, relative


def _require_exact_state_binding(
    actual: dict[str, Any], expected: dict[str, Any], label: str
) -> None:
    if set(actual) != set(expected):
        raise ValueError(f"{label} state schema mismatch")
    for field, expected_value in expected.items():
        if actual.get(field) != expected_value:
            raise ValueError(f"{label} state {field} mismatch")


def _verified_exact_hashed_artifact(
    path: str | Path,
    hash_field: str,
    *,
    exact_fields: frozenset[str],
    schema_version: str,
    label: str,
    root: str | Path | None = None,
) -> dict[str, Any]:
    artifact_path = Path(path).resolve()
    value = verified_hashed_object(artifact_path, hash_field, root=root)
    if artifact_path.read_bytes() != canonical_json(value) + b"\n":
        raise ValueError(f"{label} is not canonical JSON")
    if set(value) != exact_fields:
        raise ValueError(f"{label} schema mismatch")
    if value.get("schema_version") != schema_version:
        raise ValueError(f"{label} schema version mismatch")
    for field in exact_fields:
        if field.endswith("_sha256") and not _is_sha256(value.get(field)):
            raise ValueError(f"{label} {field} is malformed")
    return value


def _verified_selection_artifact(
    path: str | Path,
    *,
    root: str | Path | None = None,
) -> dict[str, Any]:
    selection = _verified_exact_hashed_artifact(
        path,
        "selection_sha256",
        exact_fields=SELECTION_FIELDS,
        schema_version=SELECTION_SCHEMA_VERSION,
        label="selection",
        root=root,
    )
    if selection.get("method") != VALIDATION_METHOD:
        raise ValueError("selection validation method mismatch")
    for field in ("preholdout_manifest_path", "locked_holdout_manifest_path"):
        value = selection.get(field)
        if (
            not isinstance(value, str)
            or not value.startswith("data/manifests/")
            or not value.endswith(".json")
            or "\\" in value
            or Path(value).is_absolute()
            or any(part in {"", ".", ".."} for part in Path(value).parts)
            or Path(value).as_posix() != value
        ):
            raise ValueError(f"selection {field} is malformed")
    if (
        selection["preholdout_manifest_path"]
        == selection["locked_holdout_manifest_path"]
    ):
        raise ValueError("selection partition manifest paths must be distinct")
    if (
        selection["preholdout_partition_descriptor_sha256"]
        == selection["locked_partition_descriptor_sha256"]
    ):
        raise ValueError("selection partition descriptors must be distinct")
    _require_selection_experiment_identity(selection)
    return selection


def _load_selection_artifact(
    path: str | Path,
    root: str | Path,
) -> tuple[dict[str, Any], Path, str]:
    root_path = Path(root).resolve()
    selection_file, selection_relative = _root_relative_file(
        path, root_path, "selection path"
    )
    selection = _verified_selection_artifact(selection_file)
    if selection_file.parent != (root_path / "artifacts").resolve():
        raise ValueError("selection must be stored directly inside artifacts")
    if selection_file.name != f"selection-{selection['selection_sha256'][:16]}.json":
        raise ValueError("selection filename is not content-addressed")
    return selection, selection_file, selection_relative


def _verified_holdout_report_artifact(
    path: str | Path,
    *,
    root: str | Path | None = None,
) -> dict[str, Any]:
    artifact_path = Path(path).resolve()
    value = verified_hashed_object(artifact_path, "report_sha256", root=root)
    exact_fields = _report_fields_for_kind(value)
    if artifact_path.read_bytes() != canonical_json(value) + b"\n":
        raise ValueError("holdout report is not canonical JSON")
    if set(value) != exact_fields:
        raise ValueError("holdout report schema mismatch")
    if value.get("schema_version") != HOLDOUT_REPORT_SCHEMA_VERSION:
        raise ValueError("holdout report schema version mismatch")
    for field in exact_fields:
        if field.endswith("_sha256") and not _is_sha256(value.get(field)):
            raise ValueError(f"holdout report {field} is malformed")
    _validate_holdout_report_structure(value)
    return value


def _require_post_open_holdout_manifest_match(
    preopen_metadata: dict[str, Any],
    postopen_manifest: dict[str, Any],
    selection: dict[str, Any],
) -> None:
    """Bind the price-bearing load to the exact metadata approved before open.

    The external/local opening receipts are committed before locked bytes may be
    loaded.  A path replacement between that metadata preflight and the
    price-bearing load must therefore consume the one-shot opening but can
    never proceed to metrics or a report.
    """

    if postopen_manifest != preopen_metadata:
        raise ValueError("post-open holdout manifest differs from pre-open metadata")
    expected = {
        "kind": "LOCKED_HOLDOUT",
        "manifest_path": selection["locked_holdout_manifest_path"],
        "manifest_file_sha256": selection["locked_holdout_manifest_sha256"],
        "config_sha256": selection["config_sha256"],
        "partition_descriptor_sha256": selection["locked_partition_descriptor_sha256"],
        "paired_partition_kind": "PREHOLDOUT",
        "paired_partition_descriptor_sha256": selection[
            "preholdout_partition_descriptor_sha256"
        ],
        "lockbox_id": selection["lockbox_id"],
        "normalized_sha256": selection["holdout_commitment_sha256"],
        "holdout_commitment_sha256": selection["holdout_commitment_sha256"],
        "preholdout_sha256": selection["preholdout_data_sha256"],
        "parent_manifest_sha256": selection["parent_manifest_sha256"],
    }
    for field, expected_value in expected.items():
        if postopen_manifest.get(field) != expected_value:
            raise ValueError(f"post-open holdout manifest {field} mismatch")


def _opened_state_base(opened: dict[str, Any]) -> dict[str, Any]:
    return {field: opened[field] for field in sorted(OPENED_STATE_BASE_FIELDS)}


def _require_anchor_matches_opened(
    opened: dict[str, Any],
    anchor: dict[str, Any],
    *,
    anchor_store_id: str,
    config_sha256: str,
    source_tree_sha256_value: str,
    preholdout_data_sha256: str,
    holdout_commitment_sha256: str,
) -> None:
    opened_base = _opened_state_base(opened)
    expected = {
        "anchor_store_id": anchor_store_id,
        "anchor_store_sha256": opened["external_anchor_store_sha256"],
        "experiment_id": opened["experiment_id"],
        "opened_at_utc": opened["opened_at_utc"],
        "opened_state_base_sha256": hashlib.sha256(canonical_json(opened_base)).hexdigest(),
        "previous_state_sha256": opened["previous_state_sha256"],
        "selection_sha256": opened["selection_sha256"],
        "config_sha256": config_sha256,
        "source_tree_sha256": source_tree_sha256_value,
        "preholdout_data_sha256": preholdout_data_sha256,
        "holdout_commitment_sha256": holdout_commitment_sha256,
        "holdout_manifest_sha256": opened["holdout_manifest_sha256"],
        "test_receipt_sha256": opened["test_receipt_sha256"],
        "review_receipt_sha256": opened["review_receipt_sha256"],
        "opening_commitment_sha256": opened["opening_commitment_sha256"],
        "witness_policy": opened["witness_policy"],
        "witness_store_id": opened["witness_store_id"],
        "witness_header_sha256": opened["witness_header_sha256"],
        "witness_filesystem_device": opened["witness_filesystem_device"],
        "witness_filesystem_inode": opened["witness_filesystem_inode"],
        "witness_burn_sequence": opened["witness_burn_sequence"],
        "witness_burn_sha256": opened["witness_burn_sha256"],
        "witness_burned_at_utc": opened["witness_burned_at_utc"],
        "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
        "capability": "LIVE_DISABLED",
    }
    if opened.get("external_anchor_store_id") != anchor_store_id:
        raise ValueError("HOLDOUT_OPENED external_anchor_store_id mismatch")
    local_expected = {
        "config_sha256": config_sha256,
        "source_tree_sha256": source_tree_sha256_value,
        "preholdout_data_sha256": preholdout_data_sha256,
        "holdout_commitment_sha256": holdout_commitment_sha256,
    }
    for field, expected_value in local_expected.items():
        if opened.get(field) != expected_value:
            raise ValueError(f"HOLDOUT_OPENED {field} mismatch")
    if opened.get("external_anchor_sha256") != anchor.get("anchor_sha256"):
        raise ValueError("HOLDOUT_OPENED external_anchor_sha256 mismatch")
    for field, expected_value in expected.items():
        if anchor.get(field) != expected_value:
            raise ValueError(f"external HOLDOUT_OPENED anchor {field} mismatch")


def _require_config_anchor_store_id(config: LabConfig, expected_store_id: str) -> None:
    configured = config.raw.get("anchor")
    if not isinstance(configured, dict) or configured.get("store_id") != expected_store_id:
        raise ValueError("external anchor store id does not match the frozen config")


def _require_config_witness_store_id(config: LabConfig, expected_store_id: str) -> None:
    configured = config.raw.get("witness")
    if not isinstance(configured, dict) or configured.get("store_id") != expected_store_id:
        raise ValueError("append-only witness store id does not match the frozen config")


def _verify_configured_witness(
    witness_ledger: str | Path,
    *,
    witness_store_id: str,
    config: LabConfig,
    root: Path,
) -> AppendOnlyWitnessLedger:
    _require_config_witness_store_id(config, witness_store_id)
    witness = config.witness
    if set(witness) != {
        "store_id",
        "header_sha256",
        "filesystem_device",
        "filesystem_inode",
        "policy",
    }:
        raise ValueError("append-only witness config schema mismatch")
    if witness.get("policy") != WITNESS_POLICY:
        raise ValueError("append-only witness policy mismatch")
    return verify_witness_ledger(
        witness_ledger,
        repository_root=root,
        expected_store_id=witness_store_id,
        expected_header_sha256=str(witness["header_sha256"]),
        expected_device=int(witness["filesystem_device"]),
        expected_inode=int(witness["filesystem_inode"]),
    )


def _opening_commitment(
    *,
    selection: dict[str, Any],
    frozen: dict[str, Any],
    metadata: dict[str, Any],
    test_receipt: dict[str, Any],
    review_receipt: dict[str, Any],
    config: LabConfig,
    anchor_store_id: str,
    anchor_store_sha256: str,
    witness: AppendOnlyWitnessLedger,
    opened_at_utc: str,
) -> dict[str, Any]:
    """Return the exact acyclic plan burned before any locked price load."""

    return {
        "schema_version": OPENING_COMMITMENT_SCHEMA_VERSION,
        "type": "BEN_AUTOTRADE_HOLDOUT_OPENING_PLAN",
        "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
        "capability": "LIVE_DISABLED",
        "validation_method": selection["method"],
        "experiment_id": selection["experiment_id"],
        "previous_state_sha256": frozen["state_sha256"],
        "selection_sha256": selection["selection_sha256"],
        "config_sha256": selection["config_sha256"],
        "source_tree_sha256": selection["source_tree_sha256"],
        "preholdout_manifest_path": selection["preholdout_manifest_path"],
        "preholdout_manifest_sha256": selection["preholdout_manifest_sha256"],
        "preholdout_partition_descriptor_sha256": selection[
            "preholdout_partition_descriptor_sha256"
        ],
        "locked_holdout_manifest_path": selection["locked_holdout_manifest_path"],
        "locked_holdout_manifest_sha256": metadata["manifest_file_sha256"],
        "locked_partition_descriptor_sha256": metadata[
            "partition_descriptor_sha256"
        ],
        "parent_manifest_sha256": selection["parent_manifest_sha256"],
        "lockbox_id": selection["lockbox_id"],
        "preholdout_data_sha256": selection["preholdout_data_sha256"],
        "holdout_commitment_sha256": selection["holdout_commitment_sha256"],
        "test_receipt_sha256": test_receipt["receipt_sha256"],
        "review_receipt_sha256": review_receipt["receipt_sha256"],
        "opened_at_utc": opened_at_utc,
        "anchor_policy": str(config.anchor["policy"]),
        "anchor_store_id": anchor_store_id,
        "anchor_store_sha256": anchor_store_sha256,
        "witness_policy": str(config.witness["policy"]),
        "witness_store_id": witness.store_id,
        "witness_header_sha256": witness.header_sha256,
        "witness_filesystem_device": witness.filesystem_device,
        "witness_filesystem_inode": witness.filesystem_inode,
    }


def _opening_commitment_sha256(**kwargs: Any) -> str:
    return hashlib.sha256(canonical_json(_opening_commitment(**kwargs))).hexdigest()


def _require_witness_burn_matches_opened(
    witness: AppendOnlyWitnessLedger,
    burn: OpeningBurn,
    opened: dict[str, Any],
) -> None:
    expected = {
        "experiment_id": opened["experiment_id"],
        "lockbox_id": opened["lockbox_id"],
        "locked_holdout_manifest_sha256": opened["holdout_manifest_sha256"],
        "holdout_commitment_sha256": opened["holdout_commitment_sha256"],
        "anchor_store_id": opened["external_anchor_store_id"],
        "anchor_store_sha256": opened["external_anchor_store_sha256"],
        "opening_commitment_sha256": opened["opening_commitment_sha256"],
        "burned_at_utc": opened["witness_burned_at_utc"],
        "sequence": opened["witness_burn_sequence"],
        "record_sha256": opened["witness_burn_sha256"],
    }
    for field, expected_value in expected.items():
        if getattr(burn, field) != expected_value:
            raise ValueError(f"append-only witness burn {field} mismatch")
    identity = {
        "witness_store_id": witness.store_id,
        "witness_header_sha256": witness.header_sha256,
        "witness_filesystem_device": witness.filesystem_device,
        "witness_filesystem_inode": witness.filesystem_inode,
    }
    for field, expected_value in identity.items():
        if opened.get(field) != expected_value:
            raise ValueError(f"HOLDOUT_OPENED {field} mismatch")
    if opened.get("witness_policy") != WITNESS_POLICY:
        raise ValueError("HOLDOUT_OPENED witness_policy mismatch")


def _build_opened_state_base(
    *,
    selection: dict[str, Any],
    frozen: dict[str, Any],
    metadata: dict[str, Any],
    test_receipt: dict[str, Any],
    review_receipt: dict[str, Any],
    config: LabConfig,
    anchor_store_id: str,
    anchor_store_sha256: str,
    witness: AppendOnlyWitnessLedger,
    burn: OpeningBurn,
    opening_commitment_sha256: str,
) -> dict[str, Any]:
    return {
        "state": "HOLDOUT_OPENED",
        "experiment_id": selection["experiment_id"],
        "previous_state_sha256": frozen["state_sha256"],
        "selection_sha256": selection["selection_sha256"],
        "config_sha256": selection["config_sha256"],
        "source_tree_sha256": selection["source_tree_sha256"],
        "preholdout_manifest_sha256": selection["preholdout_manifest_sha256"],
        "preholdout_partition_descriptor_sha256": selection[
            "preholdout_partition_descriptor_sha256"
        ],
        "locked_partition_descriptor_sha256": metadata[
            "partition_descriptor_sha256"
        ],
        "parent_manifest_sha256": selection["parent_manifest_sha256"],
        "lockbox_id": selection["lockbox_id"],
        "preholdout_data_sha256": selection["preholdout_data_sha256"],
        "holdout_commitment_sha256": selection["holdout_commitment_sha256"],
        "holdout_manifest_sha256": metadata["manifest_file_sha256"],
        "test_receipt_sha256": test_receipt["receipt_sha256"],
        "review_receipt_sha256": review_receipt["receipt_sha256"],
        "opened_at_utc": burn.burned_at_utc,
        "opening_commitment_sha256": opening_commitment_sha256,
        "external_anchor_store_id": anchor_store_id,
        "external_anchor_store_sha256": anchor_store_sha256,
        "witness_policy": str(config.witness["policy"]),
        "witness_store_id": witness.store_id,
        "witness_header_sha256": witness.header_sha256,
        "witness_filesystem_device": witness.filesystem_device,
        "witness_filesystem_inode": witness.filesystem_inode,
        "witness_burn_sequence": burn.sequence,
        "witness_burn_sha256": burn.record_sha256,
        "witness_burned_at_utc": burn.burned_at_utc,
    }


def _source_tree_sha256(root: Path) -> str:
    """Compatibility alias for audit callers."""

    return source_tree_sha256(root)


def _assumptions(
    config: LabConfig,
    multiplier: float = 1.0,
    *,
    signal_delay_bars: int | None = None,
) -> ExecutionAssumptions:
    execution = config.execution
    return ExecutionAssumptions(
        initial_cash=float(execution["initial_cash"]),
        fee_bps_per_side=float(execution["fee_bps_per_side"]) * multiplier,
        slippage_bps_per_side=float(execution["slippage_bps_per_side"]) * multiplier,
        maximum_gross_exposure=float(execution["maximum_gross_exposure"]),
        signal_delay_bars=(
            int(execution["signal_fill_delay_bars"])
            if signal_delay_bars is None
            else signal_delay_bars
        ),
    )


def _month_number(timestamp_ms: int) -> int:
    moment = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    return moment.year * 12 + moment.month - 1


def _month_start_ms(month_number: int) -> int:
    year, month_zero = divmod(month_number, 12)
    return int(datetime(year, month_zero + 1, 1, tzinfo=UTC).timestamp() * 1000)


def _folds(start_ms: int, end_ms_exclusive: int, fold_months: int) -> list[tuple[int, int]]:
    if fold_months < 1:
        raise ValueError("fold_months must be positive")
    start_month = _month_number(start_ms)
    end_month = _month_number(end_ms_exclusive)
    if _month_start_ms(start_month) != start_ms:
        raise ValueError("scoring start must be the first instant of a UTC month")
    if _month_start_ms(end_month) != end_ms_exclusive:
        raise ValueError("scoring end must be the first instant of a UTC month")
    month_span = end_month - start_month
    if month_span != PREHOLDOUT_FOLD_COUNT * fold_months:
        raise ValueError("pre-holdout scoring window must contain exactly nine complete folds")
    folds = [
        (
            _month_start_ms(start_month + index * fold_months),
            _month_start_ms(start_month + (index + 1) * fold_months),
        )
        for index in range(PREHOLDOUT_FOLD_COUNT)
    ]
    if folds[-1][1] != end_ms_exclusive:
        raise ValueError("pre-holdout folds do not terminate at the holdout boundary")
    return folds


def _exact_window_indices(
    bars: Sequence[Bar], start_ms: int, end_ms_exclusive: int
) -> tuple[int, int]:
    if not bars or start_ms >= end_ms_exclusive:
        raise ValueError("invalid or empty evaluation window")
    first = bars[0].open_time_ms
    if (start_ms - first) % HOUR_MS != 0 or (end_ms_exclusive - first) % HOUR_MS != 0:
        raise ValueError("evaluation boundaries are not aligned to the bar sequence")
    start_index = (start_ms - first) // HOUR_MS
    end_index = (end_ms_exclusive - first) // HOUR_MS
    if start_index < 0 or end_index > len(bars):
        raise ValueError("evaluation window is not fully covered by bars")
    if bars[start_index].open_time_ms != start_ms:
        raise ValueError("evaluation start bar is missing")
    if bars[end_index - 1].open_time_ms != end_ms_exclusive - HOUR_MS:
        raise ValueError("evaluation end bar is missing")
    return start_index, end_index


def _strategy_warmup_hours(params: StrategyParams) -> int:
    return max(
        params.entry_lookback,
        params.exit_lookback,
        params.trend_lookback,
        params.volatility_lookback,
    )


def _apply_exact_window_duration(
    metrics: dict[str, Any], start_ms: int, end_ms_exclusive: int
) -> dict[str, Any]:
    """Recompute elapsed-time metrics over the full half-open bar window.

    A window containing ``N`` hourly bars spans ``N`` hours. Using only the
    difference between its first and last open timestamps would silently drop
    the final bar's hour and slightly overstate annualized performance.
    """

    elapsed_hours = (end_ms_exclusive - start_ms) / HOUR_MS
    if elapsed_hours <= 0:
        raise ValueError("exact metric window must have positive duration")
    initial = float(metrics["initial_cash"])
    terminal = float(metrics["terminal_equity"])
    if initial <= 0 or terminal < 0:
        raise ValueError("exact metric window has invalid terminal wealth")
    elapsed_years = max(elapsed_hours / (365.25 * 24.0), 1.0 / 365.25)
    cagr = (terminal / initial) ** (1.0 / elapsed_years) - 1.0
    drawdown = float(metrics["maximum_drawdown"])
    adjusted = dict(metrics)
    adjusted["elapsed_hours"] = elapsed_hours
    adjusted["cagr"] = cagr
    adjusted["calmar"] = cagr / abs(drawdown) if drawdown < 0 else None
    return adjusted


def _window_result(
    bars: Sequence[Bar],
    params: StrategyParams,
    assumptions: ExecutionAssumptions,
    start_ms: int,
    end_ms_exclusive: int,
) -> dict[str, Any]:
    start_index, end_index = _exact_window_indices(bars, start_ms, end_ms_exclusive)
    warmup = _strategy_warmup_hours(params)
    if warmup > MAX_HOLDOUT_CONTEXT_HOURS:
        raise ValueError("indicator warmup exceeds the frozen 720-hour context cap")
    history_start = start_index - warmup
    if history_start < 0:
        raise ValueError("evaluation window lacks the frozen indicator warmup")
    history = bars[history_start:end_index]
    local_start = start_index - history_start
    targets = build_targets(history, params, evaluation_start_index=local_start)
    evaluated_bars = history[local_start:]
    evaluated_targets = targets[local_start:]
    if len(evaluated_bars) != (end_ms_exclusive - start_ms) // HOUR_MS:
        raise ValueError("evaluation window length mismatch")
    metrics = calculate_metrics(run_backtest(evaluated_bars, evaluated_targets, assumptions))
    return _apply_exact_window_duration(metrics, start_ms, end_ms_exclusive)


def _fold_metrics_from_continuous(
    result: BacktestResult,
    start_index: int,
    end_index: int,
    start_ms: int,
    end_ms_exclusive: int,
    boundary_equity: float,
) -> dict[str, Any]:
    """Score one fold without resetting the continuous backtest state.

    ``boundary_equity`` is the actual prior-bar marked equity (or the Fold 1
    initialization cash). Fills and exposure are restricted to this fold, but
    the account and strategy that produced them remain continuous.
    """

    points = result.equity[start_index:end_index]
    if not points or boundary_equity <= 0:
        raise ValueError("continuous fold has no equity points or invalid boundary")
    if points[0].open_time_ms != start_ms:
        raise ValueError("continuous fold start was truncated incorrectly")
    if points[-1].open_time_ms != end_ms_exclusive - HOUR_MS:
        raise ValueError("continuous fold end was truncated incorrectly")
    fills = [fill for fill in result.fills if start_index <= fill.fill_index < end_index]

    fold_result = BacktestResult(
        equity=tuple(points),
        fills=tuple(fills),
        initial_cash=boundary_equity,
        fee_bps_per_side=result.fee_bps_per_side,
        slippage_bps_per_side=result.slippage_bps_per_side,
    )
    metrics = calculate_metrics(fold_result)
    return _apply_exact_window_duration(metrics, start_ms, end_ms_exclusive)


def _continuous_validation_result(
    bars: Sequence[Bar],
    params: StrategyParams,
    assumptions: ExecutionAssumptions,
    folds: Sequence[tuple[int, int]],
) -> tuple[BacktestResult, list[dict[str, Any]]]:
    if not folds:
        raise ValueError("continuous validation requires folds")
    for index, (start_ms, end_ms) in enumerate(folds):
        if start_ms >= end_ms:
            raise ValueError("validation fold is empty")
        if index and folds[index - 1][1] != start_ms:
            raise ValueError("validation folds must be contiguous and non-overlapping")

    scoring_start = folds[0][0]
    scoring_end = folds[-1][1]
    start_index, end_index = _exact_window_indices(bars, scoring_start, scoring_end)
    warmup = _strategy_warmup_hours(params)
    history_start = start_index - warmup
    if history_start < 0:
        raise ValueError("pre-holdout scoring lacks indicator initialization history")
    history = bars[history_start:end_index]
    local_start = start_index - history_start
    targets = build_targets(history, params, evaluation_start_index=local_start)
    scoring_bars = history[local_start:]
    scoring_targets = targets[local_start:]
    expected_points = (scoring_end - scoring_start) // HOUR_MS
    if len(scoring_bars) != expected_points:
        raise ValueError("continuous scoring window length mismatch")

    continuous = run_backtest(scoring_bars, scoring_targets, assumptions)
    if len(continuous.equity) != expected_points:
        raise ValueError("continuous backtest returned a truncated equity curve")

    fold_results: list[dict[str, Any]] = []
    for fold_number, (start_ms, end_ms) in enumerate(folds, start=1):
        fold_start_index = (start_ms - scoring_start) // HOUR_MS
        fold_end_index = (end_ms - scoring_start) // HOUR_MS
        if fold_start_index == 0:
            boundary = {
                "source": "FOLD1_INITIALIZATION",
                "cash": assumptions.initial_cash,
                "quantity": 0.0,
                "equity": assumptions.initial_cash,
            }
        else:
            previous = continuous.equity[fold_start_index - 1]
            boundary = {
                "source": "CONTINUOUS_PREVIOUS_BAR_CLOSE",
                "cash": previous.cash,
                "quantity": previous.quantity,
                "equity": previous.equity,
                "open_time_ms": previous.open_time_ms,
            }
        metrics = _fold_metrics_from_continuous(
            continuous,
            fold_start_index,
            fold_end_index,
            start_ms,
            end_ms,
            float(boundary["equity"]),
        )
        fold_results.append(
            {
                "fold_number": fold_number,
                "start_ms": start_ms,
                "end_ms_exclusive": end_ms,
                "account_boundary": boundary,
                "metrics": metrics,
            }
        )
    return continuous, fold_results


def _continuous_fold_results(
    bars: Sequence[Bar],
    params: StrategyParams,
    assumptions: ExecutionAssumptions,
    folds: Sequence[tuple[int, int]],
) -> list[dict[str, Any]]:
    """Compatibility wrapper for callers that only need fold-level scores."""

    return _continuous_validation_result(bars, params, assumptions, folds)[1]


def _continuous_daily_returns(result: BacktestResult) -> list[dict[str, Any]]:
    """Return aligned UTC close-to-close returns for selection-bias diagnostics."""

    metrics = calculate_metrics(result)
    daily = boundary_aware_utc_daily_metrics(
        result,
        boundary_equity=result.initial_cash,
        terminal_value=float(metrics["terminal_equity"]),
    )
    return [{"date_utc": point["date_utc"], "return": point["return"]} for point in daily["path"]]


def _select_primary_candidate(
    candidates: Sequence[dict[str, Any]],
) -> dict[str, Any] | None:
    eligible = [item for item in candidates if item["eligible"]]
    if not eligible:
        return None
    return min(
        eligible,
        key=lambda item: (
            -float(item["median_calmar"]),
            -float(item["positive_fold_fraction"]),
            -float(item["median_sharpe"]),
            canonical_json(item["params"]),
        ),
    )


def _params(value: dict[str, Any]) -> StrategyParams:
    return StrategyParams(
        entry_lookback=int(value["entry_lookback"]),
        exit_lookback=int(value["exit_lookback"]),
        trend_lookback=int(value["trend_lookback"]),
        volatility_lookback=int(value["volatility_lookback"]),
        target_annualized_volatility=float(value["target_annualized_volatility"]),
        volatility_floor=float(value["volatility_floor"]),
    )


def _serialize_bar(bar: Bar) -> dict[str, Any]:
    return {
        "open_time_ms": bar.open_time_ms,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "close_time_ms": bar.close_time_ms,
        "synthetic": bar.synthetic,
        "trade_count": bar.trade_count,
    }


def _deserialize_bar(value: dict[str, Any]) -> Bar:
    return Bar(
        open_time_ms=int(value["open_time_ms"]),
        open=float(value["open"]),
        high=float(value["high"]),
        low=float(value["low"]),
        close=float(value["close"]),
        volume=float(value["volume"]),
        close_time_ms=int(value["close_time_ms"]),
        synthetic=bool(value["synthetic"]),
        trade_count=int(value["trade_count"]),
    )


def _is_adjacent(
    left: dict[str, Any], right: dict[str, Any], grid: Sequence[StrategyParams]
) -> bool:
    fields = ("entry_lookback", "exit_lookback", "trend_lookback")
    differing = [field for field in fields if left[field] != right[field]]
    if len(differing) != 1:
        return False
    field = differing[0]
    values = sorted({params.as_dict()[field] for params in grid})
    return abs(values.index(left[field]) - values.index(right[field])) == 1


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def _preholdout_candidate_eligible(
    fold_results: Sequence[dict[str, Any]],
    *,
    minimum_trips: int,
    minimum_exposure: float,
    maximum_exposure: float,
    minimum_positive_fraction: float,
) -> bool:
    if len(fold_results) != PREHOLDOUT_FOLD_COUNT:
        raise ValueError("candidate eligibility requires exactly nine folds")
    returns = [float(item["metrics"]["total_return"]) for item in fold_results]
    positive_fraction = sum(value > 0 for value in returns) / len(returns)
    return positive_fraction >= minimum_positive_fraction and all(
        _finite(item["metrics"]["calmar"])
        and item["metrics"]["completed_round_trips"] >= minimum_trips
        and minimum_exposure <= item["metrics"]["exposure_fraction"] <= maximum_exposure
        for item in fold_results
    )


def select_candidate(
    bars: Sequence[Bar],
    manifest: dict[str, Any],
    config: LabConfig,
    root: str | Path = ".",
) -> Path:
    root_path = Path(root).resolve()
    bind_manifest_to_config(manifest, config, "PREHOLDOUT")
    if manifest.get("paired_partition_kind") != "LOCKED_HOLDOUT":
        raise ValueError("pre-holdout manifest paired kind mismatch")
    partition_bindings = {
        "preholdout_manifest_path": manifest.get("manifest_path"),
        "preholdout_manifest_sha256": manifest.get("manifest_file_sha256"),
        "preholdout_partition_descriptor_sha256": manifest.get(
            "partition_descriptor_sha256"
        ),
        "locked_holdout_manifest_path": manifest.get("locked_holdout_manifest_path"),
        "locked_holdout_manifest_sha256": manifest.get(
            "locked_holdout_manifest_sha256"
        ),
        "locked_partition_descriptor_sha256": manifest.get(
            "paired_partition_descriptor_sha256"
        ),
    }
    for field, value in partition_bindings.items():
        if field.endswith("_sha256") and not _is_sha256(value):
            raise ValueError(f"pre-holdout manifest {field} is malformed")
    for field in ("preholdout_manifest_path", "locked_holdout_manifest_path"):
        value = partition_bindings[field]
        if not isinstance(value, str) or not value:
            raise ValueError(f"pre-holdout manifest {field} is malformed")
    if (
        partition_bindings["preholdout_partition_descriptor_sha256"]
        == partition_bindings["locked_partition_descriptor_sha256"]
    ):
        raise ValueError("pre-holdout manifest partition descriptors must be distinct")
    holdout_start = parse_utc_ms(config.splits["validation_end_utc_exclusive"])
    if bars[0].open_time_ms != int(manifest["requested_start_ms"]):
        raise ValueError("pre-holdout first bar does not match manifest")
    if bars[-1].open_time_ms != holdout_start - HOUR_MS:
        raise ValueError("selection input is not an exact pre-holdout dataset")
    fold_months = int(config.raw["selection"]["minimum_fold_months"])
    expected_fold_count = int(config.raw["selection"]["expected_fold_count"])
    if expected_fold_count != PREHOLDOUT_FOLD_COUNT:
        raise ValueError("v1 requires exactly nine frozen pre-holdout folds")
    scoring_start = parse_utc_ms(config.raw["selection"]["scoring_start_utc"])
    folds = _folds(scoring_start, holdout_start, fold_months)
    if len(folds) != expected_fold_count:
        raise ValueError("pre-holdout fold count does not match the frozen protocol")
    assumptions = _assumptions(config)
    minimum_trips = int(config.raw["selection"]["minimum_fold_completed_round_trips"])
    minimum_exposure = float(config.raw["selection"]["minimum_fold_exposure_fraction"])
    maximum_exposure = float(config.raw["selection"]["maximum_fold_exposure_fraction"])
    minimum_positive_fraction = float(config.raw["selection"]["minimum_positive_fold_fraction"])
    grid = config.strategy_grid()
    candidates: list[dict[str, Any]] = []
    for params in grid:
        continuous, fold_results = _continuous_validation_result(bars, params, assumptions, folds)
        calmars = [item["metrics"]["calmar"] for item in fold_results]
        sharpes = [item["metrics"]["annualized_sharpe_daily"] for item in fold_results]
        returns = [item["metrics"]["total_return"] for item in fold_results]
        positive_fold_fraction = sum(value > 0 for value in returns) / len(returns)
        eligible = _preholdout_candidate_eligible(
            fold_results,
            minimum_trips=minimum_trips,
            minimum_exposure=minimum_exposure,
            maximum_exposure=maximum_exposure,
            minimum_positive_fraction=minimum_positive_fraction,
        )
        candidates.append(
            {
                "params": params.as_dict(),
                "eligible": eligible,
                "folds": fold_results,
                "median_calmar": statistics.median(calmars) if eligible else None,
                "median_sharpe": statistics.median(sharpes),
                "median_total_return": statistics.median(returns),
                "positive_fold_fraction": positive_fold_fraction,
                "continuous_daily_returns": _continuous_daily_returns(continuous),
            }
        )
    selected_item = _select_primary_candidate(candidates)
    selected_params = selected_item["params"] if selected_item else None
    neighbors = (
        [item for item in candidates if _is_adjacent(selected_params, item["params"], grid)]
        if selected_params is not None
        else []
    )
    adjacency_edges = [
        {"left": left.as_dict(), "right": right.as_dict()}
        for left_index, left in enumerate(grid)
        for right in grid[left_index + 1 :]
        if _is_adjacent(left.as_dict(), right.as_dict(), grid)
    ]
    neighbor_positive_fraction = (
        sum(item["eligible"] and item["median_total_return"] > 0 for item in neighbors)
        / len(neighbors)
        if neighbors
        else 0.0
    )
    max_warmup = max(_strategy_warmup_hours(params) for params in grid)
    if max_warmup > MAX_HOLDOUT_CONTEXT_HOURS:
        raise ValueError("strategy grid exceeds the frozen 720-hour holdout context cap")
    context = list(bars[-max_warmup:])
    if len(context) != max_warmup or context[-1].open_time_ms != holdout_start - HOUR_MS:
        raise ValueError("pre-holdout indicator context is incomplete")
    source_sha = source_tree_sha256(root_path)
    experiment_id = _experiment_id(
        method_version=VALIDATION_METHOD,
        config_sha256=config.config_sha256,
        source_tree_sha256_value=source_sha,
        lockbox_id=manifest["lockbox_id"],
        preholdout_data_sha256=manifest["normalized_sha256"],
        holdout_commitment_sha256=manifest["holdout_commitment_sha256"],
        selected_params=selected_params,
    )
    selection_bias_diagnostic: dict[str, Any] | None = None
    if selected_item is not None:
        diagnostics = config.raw["diagnostics"]
        if diagnostics["seed_policy"] != "SHA256_EXPERIMENT_ID":
            raise ValueError("selection diagnostic seed policy is not frozen")
        date_indexes = [
            [point["date_utc"] for point in item["continuous_daily_returns"]] for item in candidates
        ]
        if any(index != date_indexes[0] for index in date_indexes[1:]):
            raise ValueError("candidate daily returns do not share one UTC index")
        selected_index = next(
            index for index, item in enumerate(candidates) if item is selected_item
        )
        selection_bias_diagnostic = moving_block_max_sharpe(
            [
                [float(point["return"]) for point in item["continuous_daily_returns"]]
                for item in candidates
            ],
            selected_index=selected_index,
            resamples=int(diagnostics["moving_block_bootstrap_resamples"]),
            block_length_days=int(diagnostics["moving_block_length_days"]),
            seed_hex=experiment_id,
        )
    selection = {
        "schema_version": SELECTION_SCHEMA_VERSION,
        "status": "FROZEN_CANDIDATE" if selected_item else "NO_ELIGIBLE_CANDIDATE",
        "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
        "method": VALIDATION_METHOD,
        "experiment_id": experiment_id,
        **partition_bindings,
        "preholdout_data_sha256": manifest["normalized_sha256"],
        "parent_manifest_sha256": manifest["parent_manifest_sha256"],
        "holdout_commitment_sha256": manifest["holdout_commitment_sha256"],
        "lockbox_id": manifest["lockbox_id"],
        "config_sha256": config.config_sha256,
        "source_tree_sha256": source_sha,
        "trial_count": len(candidates),
        "fold_count": len(folds),
        "fold_protocol": {
            "scoring_start_ms": scoring_start,
            "scoring_end_ms_exclusive": holdout_start,
            "fold_months": fold_months,
            "strategy_state": "CONTINUOUS_ACROSS_ALL_FOLDS",
            "account_state": "CONTINUOUS_CASH_RESET_AT_FOLD1_ONLY",
            "indicator_initialization": "PRE_FOLD1_HISTORY_ONLY",
            "terminal_valuation": (
                "COSTED_HYPOTHETICAL_LIQUIDATION_PER_FOLD_METRIC_WITHOUT_ACCOUNT_RESET"
            ),
        },
        "selected_params": selected_params,
        "selection_objective": config.raw["selection"]["objective"],
        "selection_bias_diagnostic": selection_bias_diagnostic,
        "parameter_adjacency_edges": adjacency_edges,
        "selected_parameter_neighbors": [item["params"] for item in neighbors],
        "preholdout_neighbor_count": len(neighbors),
        "preholdout_neighbor_positive_fraction": neighbor_positive_fraction,
        "warmup_context_hours": len(context),
        "warmup_context": [_serialize_bar(bar) for bar in context],
        "candidates": candidates,
    }
    selection["selection_sha256"] = hashlib.sha256(canonical_json(selection)).hexdigest()
    path = root_path / "artifacts" / f"selection-{selection['selection_sha256'][:16]}.json"
    write_immutable(path, canonical_json(selection) + b"\n")
    _fsync_parent_directory(path.parent)
    _fsync_parent_directory(root_path)
    frozen = {
        "state": "FROZEN",
        "experiment_id": experiment_id,
        "selection_path": path.relative_to(root_path).as_posix(),
        "selection_sha256": selection["selection_sha256"],
        "config_sha256": config.config_sha256,
        "source_tree_sha256": source_sha,
        "preholdout_data_sha256": manifest["normalized_sha256"],
        "holdout_commitment_sha256": manifest["holdout_commitment_sha256"],
    }
    _write_state_exclusive(
        root_path / "state" / "experiments" / experiment_id / "FROZEN.json",
        frozen,
    )
    return path


def _require_receipt(
    path: str | Path,
    selection: dict[str, Any],
    expected_type: str,
    root: Path,
) -> dict[str, Any]:
    receipt_file, _ = _root_relative_file(path, root, f"{expected_type} receipt path")
    receipt = verified_hashed_object(receipt_file, "receipt_sha256", root=root)
    exact = {
        "type": expected_type,
        "experiment_id": selection["experiment_id"],
        "selection_sha256": selection["selection_sha256"],
        "config_sha256": selection["config_sha256"],
        "source_tree_sha256": selection["source_tree_sha256"],
        "preholdout_data_sha256": selection["preholdout_data_sha256"],
        "holdout_commitment_sha256": selection["holdout_commitment_sha256"],
    }
    for field, expected in exact.items():
        if receipt.get(field) != expected:
            raise ValueError(f"{expected_type} receipt {field} mismatch")
    if expected_type == "TEST_RECEIPT":
        if receipt.get("status") != "PASS":
            raise ValueError("test receipt is not PASS")
        replay = receipt.get("full_provenance_replay")
        if not isinstance(replay, dict) or replay.get("status") != "PASS":
            raise ValueError("isolated full provenance replay is not PASS")
    if expected_type == "PRO_REVIEW_RECEIPT" and receipt.get("verdict") != "PROCEED":
        raise ValueError("Pro review did not authorize holdout finalization")
    return receipt


def _commit_final_report(
    *,
    root: Path,
    experiment_id: str,
    opened: dict[str, Any],
    witness: AppendOnlyWitnessLedger,
    finalized_path: Path,
    report: dict[str, Any],
) -> Path:
    if "report_sha256" in report:
        raise ValueError("final report must be unsigned before commit")
    exact_fields = _report_fields_for_kind(report)
    if set(report) != exact_fields - {"report_sha256"}:
        raise ValueError("final report schema does not match its report kind")
    if report.get("schema_version") != HOLDOUT_REPORT_SCHEMA_VERSION:
        raise ValueError("final report schema version mismatch")
    committed = dict(report)
    committed["report_sha256"] = hashlib.sha256(canonical_json(report)).hexdigest()
    _validate_holdout_report_structure(committed)
    report_path = root / "artifacts" / f"holdout-{committed['report_sha256'][:16]}.json"
    write_immutable(report_path, canonical_json(committed) + b"\n")
    _fsync_parent_directory(report_path.parent)
    _fsync_parent_directory(root)
    finalized_at_utc = datetime.now(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
    finalization = commit_finalization(
        witness,
        experiment_id=experiment_id,
        opening_burn_record_sha256=opened["witness_burn_sha256"],
        opened_state_sha256=opened["state_sha256"],
        external_anchor_sha256=opened["external_anchor_sha256"],
        report_sha256=committed["report_sha256"],
        report_status=committed["status"],
        report_kind=committed["report_kind"],
        finalized_at_utc=finalized_at_utc,
    )
    _write_state_exclusive(
        finalized_path,
        {
            "state": "FINALIZED",
            "experiment_id": experiment_id,
            "previous_state_sha256": opened["state_sha256"],
            "report_path": report_path.relative_to(root).as_posix(),
            "report_sha256": committed["report_sha256"],
            "status": committed["status"],
            "finalized_at_utc": finalization.finalized_at_utc,
            "witness_finalization_sequence": finalization.sequence,
            "witness_finalization_sha256": finalization.record_sha256,
        },
    )
    return report_path


def _terminal_liquidation_failure_report(
    *,
    root: Path,
    config: LabConfig,
    selection: dict[str, Any],
    holdout_manifest: dict[str, Any],
    test_receipt: dict[str, Any],
    review_receipt: dict[str, Any],
    opened: dict[str, Any],
    witness: AppendOnlyWitnessLedger,
    finalized_path: Path,
    reason: str,
) -> Path:
    if reason != TerminalLiquidationNotExecutable.code:
        raise ValueError("unexpected terminal-liquidation failure reason")
    report = {
        "schema_version": HOLDOUT_REPORT_SCHEMA_VERSION,
        "report_kind": REPORT_KIND_TERMINAL_LIQUIDATION_FAILURE,
        "status": "NOT_PROVEN",
        "capability": "LIVE_DISABLED",
        "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
        "profit_claim": "NONE",
        "evaluation_label": HOLDOUT_EVALUATION_LABEL,
        "evidence_level": HOLDOUT_EVALUATION_LABEL,
        "experiment_id": selection["experiment_id"],
        "selection_sha256": selection["selection_sha256"],
        "holdout_manifest_sha256": holdout_manifest["manifest_file_sha256"],
        "holdout_data_sha256": holdout_manifest["normalized_sha256"],
        "config_sha256": config.config_sha256,
        "source_tree_sha256": selection["source_tree_sha256"],
        "test_receipt_sha256": test_receipt["receipt_sha256"],
        "review_receipt_sha256": review_receipt["receipt_sha256"],
        "holdout_opened_state_sha256": opened["state_sha256"],
        "opening_commitment_sha256": opened["opening_commitment_sha256"],
        "external_anchor_store_id": opened["external_anchor_store_id"],
        "external_anchor_store_sha256": opened["external_anchor_store_sha256"],
        "external_anchor_sha256": opened["external_anchor_sha256"],
        "witness_store_id": opened["witness_store_id"],
        "witness_header_sha256": opened["witness_header_sha256"],
        "witness_filesystem_device": opened["witness_filesystem_device"],
        "witness_filesystem_inode": opened["witness_filesystem_inode"],
        "witness_burn_sequence": opened["witness_burn_sequence"],
        "witness_burn_sha256": opened["witness_burn_sha256"],
        "selected_params": selection["selected_params"],
        "failure_reason": reason,
        "metrics": None,
        "gates": {"terminal_liquidation_executable": False},
    }
    return _commit_final_report(
        root=root,
        experiment_id=selection["experiment_id"],
        opened=opened,
        witness=witness,
        finalized_path=finalized_path,
        report=report,
    )


def finalize_holdout(
    holdout_manifest_path: str | Path,
    config: LabConfig,
    selection_path: str | Path,
    test_receipt_path: str | Path,
    review_receipt_path: str | Path,
    *,
    anchor_root: str | Path,
    anchor_store_id: str,
    witness_ledger: str | Path,
    witness_store_id: str,
    root: str | Path = ".",
) -> Path:
    _require_config_anchor_store_id(config, anchor_store_id)
    _require_config_witness_store_id(config, witness_store_id)
    root_path = Path(root).resolve()
    selection, _, selection_relative = _load_selection_artifact(
        selection_path, root_path
    )
    if selection.get("status") != "FROZEN_CANDIDATE":
        raise ValueError("selection has no eligible frozen candidate")
    for field in (
        "experiment_id",
        "selection_sha256",
        "config_sha256",
        "source_tree_sha256",
        "preholdout_data_sha256",
        "holdout_commitment_sha256",
    ):
        if not _is_sha256(selection.get(field)):
            raise ValueError(f"selection {field} is malformed")
    if selection["config_sha256"] != config.config_sha256:
        raise ValueError("selection is bound to a different config")
    if selection["source_tree_sha256"] != source_tree_sha256(root_path):
        raise ValueError("source tree changed after candidate selection")
    experiment_dir = root_path / "state" / "experiments" / selection["experiment_id"]
    frozen_path = experiment_dir / "FROZEN.json"
    frozen = _read_canonical_state(
        frozen_path,
        expected_state="FROZEN",
        exact_fields=FROZEN_STATE_FIELDS,
    )
    expected_frozen = _with_state_sha256(
        {
            "state": "FROZEN",
            "experiment_id": selection["experiment_id"],
            "selection_path": selection_relative,
            "selection_sha256": selection["selection_sha256"],
            "config_sha256": config.config_sha256,
            "source_tree_sha256": selection["source_tree_sha256"],
            "preholdout_data_sha256": selection["preholdout_data_sha256"],
            "holdout_commitment_sha256": selection["holdout_commitment_sha256"],
        }
    )
    _require_exact_state_binding(frozen, expected_frozen, "FROZEN")
    metadata = read_manifest_metadata(holdout_manifest_path, root=root_path)
    bind_manifest_to_config(metadata, config, "LOCKED_HOLDOUT")
    exact_manifest = {
        "manifest_path": selection["locked_holdout_manifest_path"],
        "manifest_file_sha256": selection["locked_holdout_manifest_sha256"],
        "partition_descriptor_sha256": selection["locked_partition_descriptor_sha256"],
        "paired_partition_kind": "PREHOLDOUT",
        "paired_partition_descriptor_sha256": selection[
            "preholdout_partition_descriptor_sha256"
        ],
        "lockbox_id": selection["lockbox_id"],
        "normalized_sha256": selection["holdout_commitment_sha256"],
        "preholdout_sha256": selection["preholdout_data_sha256"],
        "parent_manifest_sha256": selection["parent_manifest_sha256"],
    }
    for field, expected in exact_manifest.items():
        if metadata.get(field) != expected:
            raise ValueError(f"holdout manifest {field} mismatch")
    holdout_start = parse_utc_ms(config.splits["validation_end_utc_exclusive"])
    holdout_end = parse_utc_ms(config.splits["locked_holdout_end_utc_exclusive"])
    selected = _params(selection["selected_params"])
    context = [_deserialize_bar(value) for value in selection["warmup_context"]]
    if not context or len(context) > MAX_HOLDOUT_CONTEXT_HOURS:
        raise ValueError("frozen holdout context must contain at most 720 hours")
    if len(context) != int(selection["warmup_context_hours"]):
        raise ValueError("frozen holdout context length mismatch")
    if len(context) < _strategy_warmup_hours(selected):
        raise ValueError("frozen holdout context is shorter than selected warmup")
    if any(
        right.open_time_ms - left.open_time_ms != HOUR_MS
        for left, right in itertools.pairwise(context)
    ):
        raise ValueError("frozen holdout context is not hourly contiguous")
    if context[-1].open_time_ms + HOUR_MS != holdout_start:
        raise ValueError("frozen warmup context does not meet holdout boundary")
    grid = config.strategy_grid()
    expected_neighbors = [
        candidate.as_dict()
        for candidate in grid
        if candidate.as_dict() != selection["selected_params"]
        and _is_adjacent(selection["selected_params"], candidate.as_dict(), grid)
    ]
    frozen_neighbors = selection.get("selected_parameter_neighbors")
    if frozen_neighbors != expected_neighbors:
        raise ValueError("frozen selected-candidate adjacency set mismatch")
    if len({canonical_json(value) for value in frozen_neighbors}) != len(frozen_neighbors):
        raise ValueError("frozen selected-candidate adjacency set contains duplicates")
    test_receipt = _require_receipt(test_receipt_path, selection, "TEST_RECEIPT", root_path)
    review_receipt = _require_receipt(
        review_receipt_path,
        selection,
        "PRO_REVIEW_RECEIPT",
        root_path,
    )
    opened_path = experiment_dir / "HOLDOUT_OPENED.json"
    finalized_path = experiment_dir / "FINALIZED.json"
    anchor_store = verify_anchor_store(
        anchor_root,
        repository_root=root_path,
        expected_store_id=anchor_store_id,
        expected_store_sha256=str(config.raw["anchor"]["store_sha256"]),
    )
    witness = _verify_configured_witness(
        witness_ledger,
        witness_store_id=witness_store_id,
        config=config,
        root=root_path,
    )
    try:
        existing_anchor = read_holdout_opened_anchor(
            anchor_store,
            selection["experiment_id"],
        )
    except ExternalAnchorNotFound:
        existing_anchor = None
    existing_burn = witness.burn_for(selection["experiment_id"])
    existing_finalization = witness.finalization_for(selection["experiment_id"])

    opened_present = _path_entry_exists(opened_path)
    finalized_present = _path_entry_exists(finalized_path)
    if opened_present:
        existing_opened = _read_canonical_state(
            opened_path,
            expected_state="HOLDOUT_OPENED",
            exact_fields=OPENED_STATE_FIELDS,
        )
        if existing_burn is None:
            raise RuntimeError("LOCAL_HOLDOUT_OPENED_WITHOUT_WITNESS_BURN_NOT_RETRYABLE")
        if existing_anchor is None:
            raise RuntimeError("LOCAL_HOLDOUT_OPENED_WITHOUT_EXTERNAL_ANCHOR_NOT_RETRYABLE")
        expected_opening_commitment = _opening_commitment_sha256(
            selection=selection,
            frozen=frozen,
            metadata=metadata,
            test_receipt=test_receipt,
            review_receipt=review_receipt,
            config=config,
            anchor_store_id=anchor_store.store_id,
            anchor_store_sha256=anchor_store.store_sha256,
            witness=witness,
            opened_at_utc=existing_opened["opened_at_utc"],
        )
        expected_opened_base = _build_opened_state_base(
            selection=selection,
            frozen=frozen,
            metadata=metadata,
            test_receipt=test_receipt,
            review_receipt=review_receipt,
            config=config,
            anchor_store_id=anchor_store.store_id,
            anchor_store_sha256=anchor_store.store_sha256,
            witness=witness,
            burn=existing_burn,
            opening_commitment_sha256=expected_opening_commitment,
        )
        _require_exact_state_binding(
            _opened_state_base(existing_opened),
            expected_opened_base,
            "HOLDOUT_OPENED base",
        )
        _require_witness_burn_matches_opened(witness, existing_burn, existing_opened)
        _require_anchor_matches_opened(
            existing_opened,
            existing_anchor,
            anchor_store_id=anchor_store.store_id,
            config_sha256=config.config_sha256,
            source_tree_sha256_value=selection["source_tree_sha256"],
            preholdout_data_sha256=selection["preholdout_data_sha256"],
            holdout_commitment_sha256=selection["holdout_commitment_sha256"],
        )
        if finalized_present:
            if existing_finalization is None:
                raise RuntimeError(
                    "LOCAL_FINALIZED_WITHOUT_WITNESS_FINALIZATION_NOT_RETRYABLE"
                )
            existing_finalized = _read_canonical_state(
                finalized_path,
                expected_state="FINALIZED",
                exact_fields=FINALIZED_STATE_FIELDS,
            )
            if existing_finalized["experiment_id"] != selection["experiment_id"]:
                raise ValueError("FINALIZED state experiment_id mismatch")
            if existing_finalized["previous_state_sha256"] != existing_opened["state_sha256"]:
                raise ValueError("FINALIZED state previous_state_sha256 mismatch")
            report_file, report_relative = _root_relative_file(
                existing_finalized["report_path"], root_path, "finalized report path"
            )
            existing_report = _verified_holdout_report_artifact(report_file)
            if existing_finalized["report_path"] != report_relative:
                raise ValueError("FINALIZED state report_path mismatch")
            if existing_finalized["report_sha256"] != existing_report["report_sha256"]:
                raise ValueError("FINALIZED state report_sha256 mismatch")
            if existing_finalized["status"] != existing_report.get("status"):
                raise ValueError("FINALIZED state status mismatch")
            finalization_expected = {
                "experiment_id": selection["experiment_id"],
                "opening_burn_record_sha256": existing_burn.record_sha256,
                "opened_state_sha256": existing_opened["state_sha256"],
                "external_anchor_sha256": existing_anchor["anchor_sha256"],
                "report_sha256": existing_report["report_sha256"],
                "report_status": existing_report["status"],
                "report_kind": existing_report["report_kind"],
                "finalized_at_utc": existing_finalized["finalized_at_utc"],
                "sequence": existing_finalized["witness_finalization_sequence"],
                "record_sha256": existing_finalized["witness_finalization_sha256"],
            }
            for field, expected in finalization_expected.items():
                if getattr(existing_finalization, field) != expected:
                    raise ValueError(f"witness finalization {field} mismatch")
            expected_report_bindings = {
                "experiment_id": selection["experiment_id"],
                "selection_sha256": selection["selection_sha256"],
                "holdout_manifest_sha256": metadata["manifest_file_sha256"],
                "holdout_data_sha256": selection["holdout_commitment_sha256"],
                "config_sha256": config.config_sha256,
                "source_tree_sha256": selection["source_tree_sha256"],
                "test_receipt_sha256": test_receipt["receipt_sha256"],
                "review_receipt_sha256": review_receipt["receipt_sha256"],
                "holdout_opened_state_sha256": existing_opened["state_sha256"],
                "opening_commitment_sha256": existing_opened[
                    "opening_commitment_sha256"
                ],
                "external_anchor_store_id": anchor_store.store_id,
                "external_anchor_store_sha256": anchor_store.store_sha256,
                "external_anchor_sha256": existing_anchor["anchor_sha256"],
                "witness_store_id": witness.store_id,
                "witness_header_sha256": witness.header_sha256,
                "witness_filesystem_device": witness.filesystem_device,
                "witness_filesystem_inode": witness.filesystem_inode,
                "witness_burn_sequence": existing_burn.sequence,
                "witness_burn_sha256": existing_burn.record_sha256,
                "evaluation_label": HOLDOUT_EVALUATION_LABEL,
                "evidence_level": HOLDOUT_EVALUATION_LABEL,
                "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
                "capability": "LIVE_DISABLED",
                "profit_claim": "NONE",
            }
            for field, expected in expected_report_bindings.items():
                if existing_report.get(field) != expected:
                    raise ValueError(f"finalized report {field} mismatch")
        elif existing_finalization is not None:
            raise RuntimeError(
                "WITNESS_FINALIZED_WITHOUT_LOCAL_FINALIZED_NOT_RETRYABLE"
            )
        raise RuntimeError("HOLDOUT_ALREADY_OPENED_NOT_RETRYABLE")
    if existing_burn is not None:
        raise RuntimeError("WITNESS_HOLDOUT_OPENED_WITHOUT_LOCAL_STATE_NOT_RETRYABLE")
    if existing_anchor is not None:
        raise RuntimeError("EXTERNAL_HOLDOUT_OPENED_WITHOUT_LOCAL_STATE_NOT_RETRYABLE")
    if finalized_present:
        raise ValueError("FINALIZED state exists without HOLDOUT_OPENED")
    assert_holdout_unopened(anchor_store, selection["experiment_id"])
    assert_unburned(
        witness,
        selection["experiment_id"],
        lockbox_id=selection["lockbox_id"],
        holdout_commitment_sha256=selection["holdout_commitment_sha256"],
    )
    opened_at_utc = datetime.now(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
    opening_commitment_sha256 = _opening_commitment_sha256(
        selection=selection,
        frozen=frozen,
        metadata=metadata,
        test_receipt=test_receipt,
        review_receipt=review_receipt,
        config=config,
        anchor_store_id=anchor_store.store_id,
        anchor_store_sha256=anchor_store.store_sha256,
        witness=witness,
        opened_at_utc=opened_at_utc,
    )
    burn = burn_opening(
        witness,
        experiment_id=selection["experiment_id"],
        lockbox_id=selection["lockbox_id"],
        locked_holdout_manifest_sha256=metadata["manifest_file_sha256"],
        holdout_commitment_sha256=selection["holdout_commitment_sha256"],
        anchor_store_id=anchor_store.store_id,
        anchor_store_sha256=anchor_store.store_sha256,
        opening_commitment_sha256=opening_commitment_sha256,
        burned_at_utc=opened_at_utc,
    )
    opened_base = _build_opened_state_base(
        selection=selection,
        frozen=frozen,
        metadata=metadata,
        test_receipt=test_receipt,
        review_receipt=review_receipt,
        config=config,
        anchor_store_id=anchor_store.store_id,
        anchor_store_sha256=anchor_store.store_sha256,
        witness=witness,
        burn=burn,
        opening_commitment_sha256=opening_commitment_sha256,
    )
    external_anchor = commit_holdout_opened_anchor(
        anchor_store,
        opened_state_base=opened_base,
        config_sha256=config.config_sha256,
        source_tree_sha256=selection["source_tree_sha256"],
        preholdout_data_sha256=selection["preholdout_data_sha256"],
        holdout_commitment_sha256=selection["holdout_commitment_sha256"],
    )
    _write_state_exclusive(
        opened_path,
        {
            **opened_base,
            "external_anchor_sha256": external_anchor["anchor_sha256"],
        },
    )
    opened = _read_canonical_state(
        opened_path,
        expected_state="HOLDOUT_OPENED",
        exact_fields=OPENED_STATE_FIELDS,
    )
    external_anchor = read_holdout_opened_anchor(
        anchor_store,
        selection["experiment_id"],
    )
    _require_anchor_matches_opened(
        opened,
        external_anchor,
        anchor_store_id=anchor_store.store_id,
        config_sha256=config.config_sha256,
        source_tree_sha256_value=selection["source_tree_sha256"],
        preholdout_data_sha256=selection["preholdout_data_sha256"],
        holdout_commitment_sha256=selection["holdout_commitment_sha256"],
    )
    witness = _verify_configured_witness(
        witness_ledger,
        witness_store_id=witness_store_id,
        config=config,
        root=root_path,
    )
    committed_burn = witness.burn_for(selection["experiment_id"])
    if committed_burn is None:
        raise RuntimeError("WITNESS_BURN_MISSING_AFTER_OPENING")
    _require_witness_burn_matches_opened(witness, committed_burn, opened)

    holdout_bars, holdout_manifest = load_bars_from_manifest(
        holdout_manifest_path,
        root=root_path,
        expected_kind="LOCKED_HOLDOUT",
        allow_locked_data=True,
    )
    bind_manifest_to_config(holdout_manifest, config, "LOCKED_HOLDOUT")
    _require_post_open_holdout_manifest_match(metadata, holdout_manifest, selection)
    bars = context + holdout_bars
    try:
        base = _window_result(bars, selected, _assumptions(config), holdout_start, holdout_end)
        required_multiplier = float(config.acceptance["minimum_positive_cost_stress_multiplier"])
        required_stress = _window_result(
            bars, selected, _assumptions(config, required_multiplier), holdout_start, holdout_end
        )
        stress_3x = _window_result(
            bars, selected, _assumptions(config, 3.0), holdout_start, holdout_end
        )
        latency_delay = int(config.raw["diagnostics"]["latency_stress_delay_bars"])
        latency_stress = _window_result(
            bars,
            selected,
            _assumptions(config, signal_delay_bars=latency_delay),
            holdout_start,
            holdout_end,
        )
        holdout_neighbors = [
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
            for value in frozen_neighbors
        ]
    except TerminalLiquidationNotExecutable as exc:
        return _terminal_liquidation_failure_report(
            root=root_path,
            config=config,
            selection=selection,
            holdout_manifest=holdout_manifest,
            test_receipt=test_receipt,
            review_receipt=review_receipt,
            opened=opened,
            witness=witness,
            finalized_path=finalized_path,
            reason=exc.code,
        )
    holdout_neighbor_positive_fraction = (
        sum(item["metrics"]["total_return"] > 0 for item in holdout_neighbors)
        / len(holdout_neighbors)
        if holdout_neighbors
        else 0.0
    )
    try:
        benchmark = _apply_exact_window_duration(
            buy_and_hold_metrics(holdout_bars, _assumptions(config)),
            holdout_start,
            holdout_end,
        )
    except TerminalLiquidationNotExecutable as exc:
        return _terminal_liquidation_failure_report(
            root=root_path,
            config=config,
            selection=selection,
            holdout_manifest=holdout_manifest,
            test_receipt=test_receipt,
            review_receipt=review_receipt,
            opened=opened,
            witness=witness,
            finalized_path=finalized_path,
            reason=exc.code,
        )
    acceptance = config.acceptance
    selected_preholdout = next(
        item for item in selection["candidates"] if item["params"] == selection["selected_params"]
    )
    gates = {
        "holdout_sharpe": base["annualized_sharpe_daily"]
        >= float(acceptance["minimum_holdout_sharpe"]),
        "holdout_calmar": _finite(base["calmar"])
        and float(base["calmar"]) >= float(acceptance["minimum_holdout_calmar"]),
        "holdout_drawdown": base["maximum_drawdown"]
        >= -float(acceptance["maximum_holdout_drawdown"]),
        "completed_round_trips": base["completed_round_trips"]
        >= int(acceptance["minimum_completed_round_trips"]),
        "required_cost_stress_return_positive": required_stress["total_return"] > 0,
        "walk_forward_positive_fraction": selected_preholdout["positive_fold_fraction"]
        >= float(config.raw["selection"]["minimum_positive_fold_fraction"]),
        "holdout_parameter_neighbors_positive": holdout_neighbor_positive_fraction
        >= float(acceptance["minimum_positive_parameter_neighbors_fraction"]),
        "mark_to_market_profit_concentration": base[
            "maximum_positive_quarter_mark_to_market_profit_concentration"
        ]
        <= float(acceptance["maximum_single_quarter_profit_concentration"]),
        "source_bound_tests": True,
        "independent_pro_review": True,
    }
    status = "BACKTEST_CANDIDATE" if all(gates.values()) else "NOT_PROVEN"
    gap_events = [
        item
        for item in holdout_manifest.get("declared_source_anomalies", [])
        if item.get("type") == "MISSING_HOURLY_BARS"
    ]
    report = {
        "schema_version": HOLDOUT_REPORT_SCHEMA_VERSION,
        "report_kind": REPORT_KIND_LOCKED_OOS_EVALUATION,
        "status": status,
        "capability": "LIVE_DISABLED",
        "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
        "profit_claim": "NONE",
        "evaluation_label": HOLDOUT_EVALUATION_LABEL,
        "evidence_level": HOLDOUT_EVALUATION_LABEL,
        "experiment_id": selection["experiment_id"],
        "selection_sha256": selection["selection_sha256"],
        "holdout_manifest_sha256": holdout_manifest["manifest_file_sha256"],
        "holdout_data_sha256": holdout_manifest["normalized_sha256"],
        "config_sha256": config.config_sha256,
        "source_tree_sha256": selection["source_tree_sha256"],
        "test_receipt_sha256": test_receipt["receipt_sha256"],
        "review_receipt_sha256": review_receipt["receipt_sha256"],
        "holdout_opened_state_sha256": opened["state_sha256"],
        "opening_commitment_sha256": opened["opening_commitment_sha256"],
        "external_anchor_store_id": opened["external_anchor_store_id"],
        "external_anchor_store_sha256": opened["external_anchor_store_sha256"],
        "external_anchor_sha256": opened["external_anchor_sha256"],
        "witness_store_id": opened["witness_store_id"],
        "witness_header_sha256": opened["witness_header_sha256"],
        "witness_filesystem_device": opened["witness_filesystem_device"],
        "witness_filesystem_inode": opened["witness_filesystem_inode"],
        "witness_burn_sequence": opened["witness_burn_sequence"],
        "witness_burn_sha256": opened["witness_burn_sha256"],
        "selected_params": selection["selected_params"],
        "holdout": {
            "start_ms": holdout_start,
            "end_ms_exclusive": holdout_end,
            "account_state": "RESET_TO_INITIAL_CASH",
            "strategy_state": "RESET_FIRST_SIGNAL_FROM_HOLDOUT",
            "indicator_context_hours": _strategy_warmup_hours(selected),
        },
        "metrics": base,
        "benchmark_protocol": BENCHMARK_PROTOCOL,
        "benchmark_buy_and_hold": benchmark,
        "cost_stress": {f"{required_multiplier:g}x": required_stress, "3x": stress_3x},
        "latency_stress": {
            "signal_delay_bars": latency_delay,
            "metrics": latency_stress,
        },
        "preholdout_parameter_neighbor_fraction": selection[
            "preholdout_neighbor_positive_fraction"
        ],
        "holdout_parameter_neighbors": holdout_neighbors,
        "holdout_parameter_neighbor_protocol": {
            "primary_excluded": True,
            "replacement_allowed": False,
            "exact_frozen_neighbor_count": len(frozen_neighbors),
        },
        "holdout_parameter_neighbor_positive_fraction": holdout_neighbor_positive_fraction,
        "holdout_gap_events": len(gap_events),
        "holdout_missing_hours": sum(int(item["missing_bar_count"]) for item in gap_events),
        "gates": gates,
    }
    return _commit_final_report(
        root=root_path,
        experiment_id=selection["experiment_id"],
        opened=opened,
        witness=witness,
        finalized_path=finalized_path,
        report=report,
    )
