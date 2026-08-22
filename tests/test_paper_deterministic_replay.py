from __future__ import annotations

import copy
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ben_trade_lab.config import parse_utc_ms
from ben_trade_lab.models import StrategyParams
from ben_trade_lab.paper import (
    _require_recomputed_locked_report,
    _require_recomputed_selection_aggregates,
)

HOLDOUT_START_UTC = "2024-08-01T00:00:00Z"
HOLDOUT_END_UTC = "2024-08-01T04:00:00Z"


def _assumption_marker(
    _config: object,
    multiplier: float = 1.0,
    *,
    signal_delay_bars: int | None = None,
) -> dict[str, int | float]:
    return {
        "cost_multiplier": float(multiplier),
        "signal_delay_bars": 1 if signal_delay_bars is None else signal_delay_bars,
    }


def _window_marker(
    _bars: object,
    params: dict[str, str],
    assumptions: dict[str, int | float],
    start_ms: int,
    end_ms_exclusive: int,
) -> dict[str, object]:
    multiplier = float(assumptions["cost_multiplier"])
    delay = int(assumptions["signal_delay_bars"])
    name = params["name"]
    return {
        "scenario": f"{name}|cost={multiplier:g}|delay={delay}",
        "total_return": 0.25 - multiplier * 0.02 - delay * 0.01,
        "start_ms": start_ms,
        "end_ms_exclusive": end_ms_exclusive,
    }


def _benchmark_marker(
    _bars: object,
    assumptions: dict[str, int | float],
) -> dict[str, object]:
    return {
        "scenario": "benchmark",
        "cost_multiplier": assumptions["cost_multiplier"],
    }


def _duration_marker(
    metrics: dict[str, object],
    start_ms: int,
    end_ms_exclusive: int,
) -> dict[str, object]:
    return {**metrics, "start_ms": start_ms, "end_ms_exclusive": end_ms_exclusive}


def _locked_replay_fixture() -> tuple[
    object,
    dict[str, object],
    dict[str, object],
    dict[str, object],
    list[str],
]:
    config = SimpleNamespace(
        splits={
            "validation_end_utc_exclusive": HOLDOUT_START_UTC,
            "locked_holdout_end_utc_exclusive": HOLDOUT_END_UTC,
        },
        acceptance={"minimum_positive_cost_stress_multiplier": 2.0},
        raw={"diagnostics": {"latency_stress_delay_bars": 2}},
    )
    locked_metadata: dict[str, object] = {
        "kind": "LOCKED_HOLDOUT",
        "declared_source_anomalies": [
            {"type": "MISSING_HOURLY_BARS", "missing_bar_count": 2},
            {"type": "OFFICIAL_ZERO_VOLUME", "missing_bar_count": 0},
        ],
    }
    holdout_bars = ["holdout-0", "holdout-1", "holdout-2", "holdout-3"]
    selection: dict[str, object] = {
        "locked_holdout_manifest_path": "synthetic/locked.json",
        "warmup_context": [{"name": "warmup"}],
        "selected_params": {"name": "primary"},
        "selected_parameter_neighbors": [
            {"name": "neighbor-left"},
            {"name": "neighbor-right"},
        ],
    }
    start_ms = parse_utc_ms(HOLDOUT_START_UTC)
    end_ms = parse_utc_ms(HOLDOUT_END_UTC)
    bars = ["warmup"] + holdout_bars
    base_assumptions = _assumption_marker(config)
    selected = selection["selected_params"]
    neighbors = selection["selected_parameter_neighbors"]
    assert isinstance(selected, dict)
    assert isinstance(neighbors, list)
    expected_neighbors = [
        {
            "params": params,
            "metrics": _window_marker(
                bars,
                params,
                base_assumptions,
                start_ms,
                end_ms,
            ),
        }
        for params in neighbors
    ]
    report: dict[str, object] = {
        "metrics": _window_marker(
            bars,
            selected,
            base_assumptions,
            start_ms,
            end_ms,
        ),
        "cost_stress": {
            "2x": _window_marker(
                bars,
                selected,
                _assumption_marker(config, 2.0),
                start_ms,
                end_ms,
            ),
            "3x": _window_marker(
                bars,
                selected,
                _assumption_marker(config, 3.0),
                start_ms,
                end_ms,
            ),
        },
        "latency_stress": {
            "signal_delay_bars": 2,
            "metrics": _window_marker(
                bars,
                selected,
                _assumption_marker(config, signal_delay_bars=2),
                start_ms,
                end_ms,
            ),
        },
        "holdout_parameter_neighbors": expected_neighbors,
        "holdout_parameter_neighbor_positive_fraction": 1.0,
        "benchmark_buy_and_hold": _duration_marker(
            _benchmark_marker(holdout_bars, base_assumptions),
            start_ms,
            end_ms,
        ),
        "holdout_gap_events": 1,
        "holdout_missing_hours": 2,
    }
    return config, selection, report, locked_metadata, holdout_bars


def _selection_candidate(params: StrategyParams, score: float) -> dict[str, object]:
    folds = [
        {
            "fold_number": fold_number,
            "start_ms": fold_number * 100,
            "end_ms_exclusive": (fold_number + 1) * 100,
            "account_boundary": "CONTINUOUS_NO_RESET",
            "metrics": {
                "total_return": score / 10.0,
                "annualized_sharpe_daily": score + 0.5,
                "calmar": score,
                "completed_round_trips": 3,
                "exposure_fraction": 0.5,
            },
        }
        for fold_number in range(1, 10)
    ]
    return {
        "params": params.as_dict(),
        "eligible": True,
        "folds": folds,
        "median_calmar": score,
        "median_sharpe": score + 0.5,
        "median_total_return": score / 10.0,
        "positive_fold_fraction": 1.0,
        "continuous_daily_returns": [],
    }


def _selection_replay_fixture() -> tuple[object, dict[str, object]]:
    grid = (
        StrategyParams(10, 5, 30, 20, 0.30, 0.10),
        StrategyParams(20, 5, 30, 20, 0.30, 0.10),
        StrategyParams(30, 5, 30, 20, 0.30, 0.10),
    )
    candidates = [
        _selection_candidate(grid[0], 1.0),
        _selection_candidate(grid[1], 3.0),
        _selection_candidate(grid[2], 2.0),
    ]
    config = SimpleNamespace(
        raw={
            "selection": {
                "maximum_trials": 3,
                "expected_fold_count": 9,
                "minimum_fold_completed_round_trips": 2,
                "minimum_fold_exposure_fraction": 0.05,
                "maximum_fold_exposure_fraction": 0.95,
                "minimum_positive_fold_fraction": 0.75,
            }
        },
        strategy_grid=lambda: grid,
    )
    selection: dict[str, object] = {
        "trial_count": 3,
        "fold_count": 9,
        "candidates": candidates,
        "selected_params": grid[1].as_dict(),
        "selected_parameter_neighbors": [grid[0].as_dict(), grid[2].as_dict()],
        "preholdout_neighbor_positive_fraction": 1.0,
    }
    return config, selection


class PaperDeterministicReplayTests(unittest.TestCase):
    def test_locked_replay_rejects_base_metrics_copied_into_stress_scenarios(
        self,
    ) -> None:
        config, selection, honest_report, locked_metadata, holdout_bars = (
            _locked_replay_fixture()
        )
        root = Path("synthetic-paper-replay-root").resolve()
        with (
            patch(
                "ben_trade_lab.paper.load_bars_from_manifest",
                return_value=(holdout_bars, copy.deepcopy(locked_metadata)),
            ),
            patch("ben_trade_lab.paper.bind_manifest_to_config") as bind_manifest,
            patch(
                "ben_trade_lab.paper._deserialize_bar",
                side_effect=lambda _value: "warmup",
            ),
            patch("ben_trade_lab.paper._params", side_effect=lambda value: value),
            patch(
                "ben_trade_lab.paper._assumptions",
                side_effect=_assumption_marker,
            ),
            patch(
                "ben_trade_lab.paper._window_result",
                side_effect=_window_marker,
            ),
            patch(
                "ben_trade_lab.paper.buy_and_hold_metrics",
                side_effect=_benchmark_marker,
            ),
            patch(
                "ben_trade_lab.paper._apply_exact_window_duration",
                side_effect=_duration_marker,
            ),
        ):
            _require_recomputed_locked_report(
                root,
                honest_report,
                selection,
                config,
                locked_metadata,
            )
            cases = (
                ("2x_cost", "cost_stress"),
                ("3x_cost", "cost_stress"),
                ("two_bar_latency", "latency_stress"),
            )
            for case, field in cases:
                with self.subTest(case=case):
                    forged = copy.deepcopy(honest_report)
                    if case == "2x_cost":
                        forged["cost_stress"]["2x"] = copy.deepcopy(
                            forged["metrics"]
                        )
                    elif case == "3x_cost":
                        forged["cost_stress"]["3x"] = copy.deepcopy(
                            forged["metrics"]
                        )
                    else:
                        forged["latency_stress"]["metrics"] = copy.deepcopy(
                            forged["metrics"]
                        )
                    with self.assertRaisesRegex(
                        ValueError,
                        rf"paper report {field} differs from deterministic replay",
                    ):
                        _require_recomputed_locked_report(
                            root,
                            forged,
                            selection,
                            config,
                            locked_metadata,
                        )

        self.assertGreaterEqual(bind_manifest.call_count, 4)

    def test_selection_replay_rejects_forged_aggregates_candidates_and_neighbors(
        self,
    ) -> None:
        config, honest_selection = _selection_replay_fixture()
        with patch(
            "ben_trade_lab.paper._validate_performance_metrics",
            side_effect=lambda value, _label: value,
        ):
            _require_recomputed_selection_aggregates(honest_selection, config)
            cases = (
                ("trial_aggregate", "paper selection trial count mismatch"),
                ("candidate_aggregate", "paper selection candidate median_calmar mismatch"),
                ("candidate_order", "paper selection candidate grid/order mismatch"),
                ("selected_candidate", "paper selection primary candidate mismatch"),
                ("neighbor_set", "paper selection neighbor set mismatch"),
                ("neighbor_fraction", "paper selection neighbor fraction mismatch"),
            )
            for case, message in cases:
                with self.subTest(case=case):
                    forged = copy.deepcopy(honest_selection)
                    if case == "trial_aggregate":
                        forged["trial_count"] = 4
                    elif case == "candidate_aggregate":
                        forged["candidates"][1]["median_calmar"] = 999.0
                    elif case == "candidate_order":
                        forged["candidates"][0], forged["candidates"][1] = (
                            forged["candidates"][1],
                            forged["candidates"][0],
                        )
                    elif case == "selected_candidate":
                        forged["selected_params"] = forged["candidates"][0]["params"]
                    elif case == "neighbor_set":
                        forged["selected_parameter_neighbors"] = forged[
                            "selected_parameter_neighbors"
                        ][:1]
                    else:
                        forged["preholdout_neighbor_positive_fraction"] = 0.0
                    with self.assertRaisesRegex(ValueError, message):
                        _require_recomputed_selection_aggregates(forged, config)


if __name__ == "__main__":
    unittest.main()
