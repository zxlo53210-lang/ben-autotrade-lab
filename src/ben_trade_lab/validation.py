from __future__ import annotations

import hashlib
import itertools
import math
import statistics
from collections import OrderedDict, defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import LabConfig, canonical_json, parse_utc_ms
from .data import HOUR_MS, bind_manifest_to_config, load_bars_from_manifest, read_manifest_metadata
from .diagnostics import moving_block_max_sharpe
from .engine import ExecutionAssumptions, run_backtest
from .integrity import source_tree_sha256, verified_hashed_object, write_exclusive, write_immutable
from .metrics import buy_and_hold_metrics, calculate_metrics
from .models import BacktestResult, Bar, StrategyParams
from .strategy import build_targets

PREHOLDOUT_FOLD_COUNT = 9
MAX_HOLDOUT_CONTEXT_HOURS = 720
VALIDATION_METHOD = "CONTINUOUS_NINE_FOLD_VALIDATION_V1"
HOLDOUT_EVALUATION_LABEL = "RETROSPECTIVE_LOCKED_OOS"


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


def _sample_sharpe(returns: Sequence[float]) -> float:
    if len(returns) < 2:
        return 0.0
    deviation = statistics.stdev(returns)
    return 0.0 if deviation == 0 else statistics.mean(returns) / deviation * math.sqrt(365.25)


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
    terminal = float(metrics["terminal_equity"])
    elapsed_years = max(
        (end_ms_exclusive - start_ms) / (1000 * 60 * 60 * 24 * 365.25),
        1.0 / 365.25,
    )
    cagr = (terminal / boundary_equity) ** (1.0 / elapsed_years) - 1.0

    peak = boundary_equity
    maximum_drawdown = 0.0
    for index, point in enumerate(points):
        value = terminal if index == len(points) - 1 else point.equity
        peak = max(peak, value)
        maximum_drawdown = min(maximum_drawdown, value / peak - 1.0)
    calmar = cagr / abs(maximum_drawdown) if maximum_drawdown < 0 else None

    daily: OrderedDict[str, tuple[datetime, float]] = OrderedDict()
    for index, point in enumerate(points):
        moment = datetime.fromtimestamp(point.open_time_ms / 1000, tz=UTC)
        value = terminal if index == len(points) - 1 else point.equity
        daily[moment.date().isoformat()] = (moment, value)
    daily_returns: list[float] = []
    quarterly_pnl: dict[str, float] = defaultdict(float)
    previous_equity = boundary_equity
    for moment, equity in daily.values():
        daily_returns.append(equity / previous_equity - 1.0)
        quarter = (moment.month - 1) // 3 + 1
        quarterly_pnl[f"{moment.year}-Q{quarter}"] += equity - previous_equity
        previous_equity = equity
    sharpe = _sample_sharpe(daily_returns)
    downside = [min(0.0, value) for value in daily_returns]
    downside_deviation = statistics.stdev(downside) if len(downside) >= 2 else 0.0
    sortino = (
        statistics.mean(daily_returns) / downside_deviation * math.sqrt(365.25)
        if downside_deviation > 0
        else 0.0
    )
    positive_quarters = [value for value in quarterly_pnl.values() if value > 0]
    concentration = max(positive_quarters) / sum(positive_quarters) if positive_quarters else 1.0
    metrics.update(
        {
            "elapsed_hours": (end_ms_exclusive - start_ms) / HOUR_MS,
            "total_return": terminal / boundary_equity - 1.0,
            "cagr": cagr,
            "annualized_sharpe_daily": sharpe,
            "annualized_sortino_daily": sortino,
            "maximum_drawdown": maximum_drawdown,
            "calmar": calmar,
            "maximum_positive_quarter_mark_to_market_profit_concentration": concentration,
        }
    )
    return metrics


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
    terminal_value = float(metrics["terminal_equity"])
    daily: OrderedDict[str, float] = OrderedDict()
    for index, point in enumerate(result.equity):
        moment = datetime.fromtimestamp(point.open_time_ms / 1000, tz=UTC)
        value = terminal_value if index == len(result.equity) - 1 else point.equity
        daily[moment.date().isoformat()] = value
    previous = result.initial_cash
    returns: list[dict[str, Any]] = []
    for date_utc, equity in daily.items():
        returns.append({"date_utc": date_utc, "return": equity / previous - 1.0})
        previous = equity
    return returns


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
    identity = {
        "method_version": VALIDATION_METHOD,
        "config_sha256": config.config_sha256,
        "source_tree_sha256": source_sha,
        "lockbox_id": manifest["lockbox_id"],
        "preholdout_data_sha256": manifest["normalized_sha256"],
        "holdout_commitment_sha256": manifest["holdout_commitment_sha256"],
        "selected_params": selected_params,
    }
    experiment_id = hashlib.sha256(canonical_json(identity)).hexdigest()
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
        "schema_version": "1.1.0",
        "status": "FROZEN_CANDIDATE" if selected_item else "NO_ELIGIBLE_CANDIDATE",
        "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
        "method": VALIDATION_METHOD,
        "experiment_id": experiment_id,
        "preholdout_manifest_sha256": manifest["manifest_file_sha256"],
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
    write_immutable(
        root_path / "state" / "experiments" / experiment_id / "FROZEN.json",
        canonical_json(frozen) + b"\n",
    )
    return path


def _require_receipt(
    path: str | Path, selection: dict[str, Any], expected_type: str
) -> dict[str, Any]:
    receipt = verified_hashed_object(path, "receipt_sha256")
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
    if expected_type == "TEST_RECEIPT" and receipt.get("status") != "PASS":
        raise ValueError("test receipt is not PASS")
    if expected_type == "PRO_REVIEW_RECEIPT" and receipt.get("verdict") != "PROCEED":
        raise ValueError("Pro review did not authorize holdout finalization")
    return receipt


def finalize_holdout(
    holdout_manifest_path: str | Path,
    config: LabConfig,
    selection_path: str | Path,
    test_receipt_path: str | Path,
    review_receipt_path: str | Path,
    root: str | Path = ".",
) -> Path:
    root_path = Path(root).resolve()
    selection = verified_hashed_object(selection_path, "selection_sha256")
    if selection.get("status") != "FROZEN_CANDIDATE":
        raise ValueError("selection has no eligible frozen candidate")
    if selection["config_sha256"] != config.config_sha256:
        raise ValueError("selection is bound to a different config")
    if selection["source_tree_sha256"] != source_tree_sha256(root_path):
        raise ValueError("source tree changed after candidate selection")
    metadata = read_manifest_metadata(holdout_manifest_path, root=root_path)
    bind_manifest_to_config(metadata, config, "LOCKED_HOLDOUT")
    exact_manifest = {
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
    test_receipt = _require_receipt(test_receipt_path, selection, "TEST_RECEIPT")
    review_receipt = _require_receipt(review_receipt_path, selection, "PRO_REVIEW_RECEIPT")
    experiment_dir = root_path / "state" / "experiments" / selection["experiment_id"]
    if not (experiment_dir / "FROZEN.json").exists():
        raise RuntimeError("experiment has no FROZEN receipt")
    opened_path = experiment_dir / "HOLDOUT_OPENED.json"
    finalized_path = experiment_dir / "FINALIZED.json"
    if opened_path.exists() or finalized_path.exists():
        raise RuntimeError("HOLDOUT_ALREADY_OPENED_NOT_RETRYABLE")
    opened = {
        "state": "HOLDOUT_OPENED",
        "experiment_id": selection["experiment_id"],
        "selection_sha256": selection["selection_sha256"],
        "holdout_manifest_sha256": metadata["manifest_file_sha256"],
        "test_receipt_sha256": test_receipt["receipt_sha256"],
        "review_receipt_sha256": review_receipt["receipt_sha256"],
        "opened_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    write_exclusive(opened_path, opened)

    holdout_bars, holdout_manifest = load_bars_from_manifest(holdout_manifest_path, root=root_path)
    bind_manifest_to_config(holdout_manifest, config, "LOCKED_HOLDOUT")
    bars = context + holdout_bars
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
    holdout_neighbor_positive_fraction = (
        sum(item["metrics"]["total_return"] > 0 for item in holdout_neighbors)
        / len(holdout_neighbors)
        if holdout_neighbors
        else 0.0
    )
    benchmark = _apply_exact_window_duration(
        buy_and_hold_metrics(holdout_bars, _assumptions(config)),
        holdout_start,
        holdout_end,
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
        "schema_version": "1.1.0",
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
        "selected_params": selection["selected_params"],
        "holdout": {
            "start_ms": holdout_start,
            "end_ms_exclusive": holdout_end,
            "account_state": "RESET_TO_INITIAL_CASH",
            "strategy_state": "RESET_FIRST_SIGNAL_FROM_HOLDOUT",
            "indicator_context_hours": _strategy_warmup_hours(selected),
        },
        "metrics": base,
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
    report["report_sha256"] = hashlib.sha256(canonical_json(report)).hexdigest()
    report_path = root_path / "artifacts" / f"holdout-{report['report_sha256'][:16]}.json"
    write_immutable(report_path, canonical_json(report) + b"\n")
    finalized = {
        "state": "FINALIZED",
        "experiment_id": selection["experiment_id"],
        "report_path": report_path.relative_to(root_path).as_posix(),
        "report_sha256": report["report_sha256"],
        "status": status,
    }
    write_exclusive(finalized_path, finalized)
    return report_path
