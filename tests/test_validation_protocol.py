from __future__ import annotations

import unittest
from datetime import UTC, datetime
from itertools import pairwise
from types import SimpleNamespace
from unittest.mock import patch

from ben_trade_lab.config import canonical_json
from ben_trade_lab.engine import ExecutionAssumptions
from ben_trade_lab.models import Bar, StrategyParams
from ben_trade_lab.strategy import build_targets
from ben_trade_lab.validation import (
    HOLDOUT_EVALUATION_LABEL,
    _assumptions,
    _continuous_fold_results,
    _folds,
    _preholdout_candidate_eligible,
    _select_primary_candidate,
    _window_result,
)

HOUR_MS = 3_600_000


def _utc_ms(value: str) -> int:
    return int(datetime.fromisoformat(value).astimezone(UTC).timestamp() * 1000)


def _bar(
    index: int,
    price: float = 100.0,
    *,
    volume: float = 10.0,
    trade_count: int = 1,
    synthetic: bool = False,
) -> Bar:
    return Bar(
        open_time_ms=index * HOUR_MS,
        open=price,
        high=price + 1.0,
        low=price - 1.0,
        close=price,
        volume=volume,
        close_time_ms=(index + 1) * HOUR_MS - 1,
        synthetic=synthetic,
        trade_count=trade_count,
    )


def _params(lookback: int = 1) -> StrategyParams:
    return StrategyParams(
        entry_lookback=lookback,
        exit_lookback=lookback,
        trend_lookback=lookback,
        volatility_lookback=lookback,
        target_annualized_volatility=0.30,
        volatility_floor=0.10,
    )


class FoldProtocolTests(unittest.TestCase):
    def test_exact_nine_six_month_folds_are_frozen(self) -> None:
        start = _utc_ms("2020-02-01T00:00:00Z")
        end = _utc_ms("2024-08-01T00:00:00Z")
        folds = _folds(start, end, 6)

        self.assertEqual(len(folds), 9)
        self.assertEqual(folds[0], (start, _utc_ms("2020-08-01T00:00:00Z")))
        self.assertEqual(
            folds[-1],
            (_utc_ms("2024-02-01T00:00:00Z"), end),
        )
        self.assertTrue(all(left[1] == right[0] for left, right in pairwise(folds)))

    def test_fold_builder_rejects_truncation_or_a_tenth_fold(self) -> None:
        start = _utc_ms("2020-02-01T00:00:00Z")
        with self.assertRaises(ValueError):
            _folds(start, _utc_ms("2024-07-01T00:00:00Z"), 6)
        with self.assertRaises(ValueError):
            _folds(start, _utc_ms("2025-02-01T00:00:00Z"), 6)

    def test_account_and_strategy_are_continuous_at_fold_boundary(self) -> None:
        prices = [100.0, 100.0, 100.0, 100.0, 110.0, 120.0, 130.0, 130.0]
        bars = [_bar(index, price) for index, price in enumerate(prices)]
        folds = [(2 * HOUR_MS, 5 * HOUR_MS), (5 * HOUR_MS, 8 * HOUR_MS)]
        assumptions = ExecutionAssumptions(1_000.0, 10.0, 5.0)
        # One warmup target followed by six scoring targets. The position opens
        # in Fold 1, crosses the boundary, and closes in Fold 2.
        targets = [0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0]

        with patch("ben_trade_lab.validation.build_targets", return_value=targets) as build:
            results = _continuous_fold_results(bars, _params(), assumptions, folds)

        build.assert_called_once()
        self.assertEqual(build.call_args.kwargs["evaluation_start_index"], 1)
        self.assertEqual(results[0]["account_boundary"]["source"], "FOLD1_INITIALIZATION")
        self.assertEqual(
            results[1]["account_boundary"]["source"],
            "CONTINUOUS_PREVIOUS_BAR_CLOSE",
        )
        self.assertGreater(results[1]["account_boundary"]["quantity"], 0.0)
        self.assertAlmostEqual(
            results[1]["metrics"]["initial_cash"],
            results[0]["metrics"]["mark_to_market_terminal_equity"],
        )
        self.assertEqual(results[1]["metrics"]["completed_round_trips"], 1)
        self.assertEqual(results[0]["metrics"]["start_open_time_ms"], 2 * HOUR_MS)
        self.assertEqual(results[0]["metrics"]["end_open_time_ms"], 4 * HOUR_MS)
        self.assertEqual(results[1]["metrics"]["start_open_time_ms"], 5 * HOUR_MS)
        self.assertEqual(results[1]["metrics"]["end_open_time_ms"], 7 * HOUR_MS)


class HoldoutProtocolTests(unittest.TestCase):
    def test_assumptions_freeze_base_delay_and_allow_named_stress_delay(self) -> None:
        config = SimpleNamespace(
            execution={
                "initial_cash": 1_000.0,
                "fee_bps_per_side": 10.0,
                "slippage_bps_per_side": 5.0,
                "maximum_gross_exposure": 1.0,
                "signal_fill_delay_bars": 1,
            }
        )
        self.assertEqual(_assumptions(config).signal_delay_bars, 1)
        self.assertEqual(
            _assumptions(config, signal_delay_bars=2).signal_delay_bars,
            2,
        )

    def test_exact_warmup_is_sufficient_but_truncation_fails(self) -> None:
        assumptions = ExecutionAssumptions(1_000.0, 10.0, 5.0)
        complete = [_bar(index) for index in range(5)]
        metrics = _window_result(
            complete,
            _params(2),
            assumptions,
            2 * HOUR_MS,
            5 * HOUR_MS,
        )
        self.assertEqual(metrics["equity_points"], 3)

        truncated = complete[1:]
        with self.assertRaisesRegex(ValueError, "lacks the frozen indicator warmup"):
            _window_result(
                truncated,
                _params(2),
                assumptions,
                2 * HOUR_MS,
                5 * HOUR_MS,
            )

    def test_context_is_capped_and_first_signal_belongs_to_holdout(self) -> None:
        assumptions = ExecutionAssumptions(1_000.0, 10.0, 5.0)
        bars = [_bar(index) for index in range(4)]
        with patch(
            "ben_trade_lab.validation.build_targets",
            return_value=[1.0, 1.0, 1.0, 1.0],
        ) as build:
            metrics = _window_result(
                bars,
                _params(2),
                assumptions,
                2 * HOUR_MS,
                4 * HOUR_MS,
            )
        self.assertEqual(build.call_args.kwargs["evaluation_start_index"], 2)
        self.assertEqual(metrics["fill_count"], 1)
        self.assertEqual(metrics["completed_round_trips"], 0)

        oversized = [_bar(index) for index in range(723)]
        with self.assertRaisesRegex(ValueError, "720-hour context cap"):
            _window_result(
                oversized,
                _params(721),
                assumptions,
                721 * HOUR_MS,
                723 * HOUR_MS,
            )

    def test_720_hour_context_does_not_backfill_ineligible_warmup_rows(self) -> None:
        params = _params(720)
        bars = [_bar(index, 100.0 + 2.0 * index) for index in range(724)]
        bars[100] = _bar(100, 1_000_000.0, synthetic=True)
        bars[200] = _bar(200, 1_000_000.0, volume=0.0)
        bars[300] = _bar(300, 1_000_000.0, trade_count=0)
        captured: dict[str, object] = {}

        def capture_targets(
            history: list[Bar],
            supplied_params: StrategyParams,
            *,
            evaluation_start_index: int,
        ) -> list[float]:
            captured["history"] = history
            captured["evaluation_start_index"] = evaluation_start_index
            targets = build_targets(
                history,
                supplied_params,
                evaluation_start_index=evaluation_start_index,
            )
            captured["targets"] = targets
            return targets

        with patch("ben_trade_lab.validation.build_targets", side_effect=capture_targets):
            metrics = _window_result(
                bars,
                params,
                ExecutionAssumptions(1_000.0, 10.0, 5.0),
                720 * HOUR_MS,
                724 * HOUR_MS,
            )

        self.assertEqual(len(captured["history"]), 724)
        self.assertEqual(captured["evaluation_start_index"], 720)
        targets = captured["targets"]
        assert isinstance(targets, list)
        self.assertEqual(targets[720:723], [0.0, 0.0, 0.0])
        self.assertGreater(targets[723], 0.0)
        self.assertEqual(metrics["equity_points"], 4)

    def test_retrospective_oos_label_is_explicit(self) -> None:
        self.assertEqual(HOLDOUT_EVALUATION_LABEL, "RETROSPECTIVE_LOCKED_OOS")


class SelectionDeterminismTests(unittest.TestCase):
    @staticmethod
    def _candidate(entry: int, *, eligible: bool = True) -> dict[str, object]:
        return {
            "params": {"entry_lookback": entry},
            "eligible": eligible,
            "median_calmar": 1.0,
            "positive_fold_fraction": 1.0,
            "median_sharpe": 1.0,
        }

    def test_tie_break_is_unique_and_order_independent(self) -> None:
        candidates = [self._candidate(72), self._candidate(168)]
        expected = min(candidates, key=lambda item: canonical_json(item["params"]))
        self.assertIs(_select_primary_candidate(candidates), expected)
        self.assertIs(_select_primary_candidate(list(reversed(candidates))), expected)

    def test_no_eligible_candidate_stops_selection(self) -> None:
        self.assertIsNone(_select_primary_candidate([self._candidate(72, eligible=False)]))

    def test_known_positive_fold_failure_cannot_be_selected_or_open_oos(self) -> None:
        folds = [
            {
                "metrics": {
                    "total_return": 0.10 if index < 6 else -0.01,
                    "calmar": 1.0,
                    "completed_round_trips": 3,
                    "exposure_fraction": 0.40,
                }
            }
            for index in range(9)
        ]
        eligible = _preholdout_candidate_eligible(
            folds,
            minimum_trips=2,
            minimum_exposure=0.05,
            maximum_exposure=0.95,
            minimum_positive_fraction=0.75,
        )
        candidate = self._candidate(72, eligible=eligible)
        self.assertFalse(eligible)
        self.assertIsNone(_select_primary_candidate([candidate]))

        folds[6]["metrics"]["total_return"] = 0.10
        self.assertTrue(
            _preholdout_candidate_eligible(
                folds,
                minimum_trips=2,
                minimum_exposure=0.05,
                maximum_exposure=0.95,
                minimum_positive_fraction=0.75,
            )
        )


if __name__ == "__main__":
    unittest.main()
