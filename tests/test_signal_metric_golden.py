from __future__ import annotations

import math
import statistics
import unittest
from datetime import UTC, datetime

from ben_trade_lab.engine import ExecutionAssumptions, run_backtest
from ben_trade_lab.metrics import calculate_metrics
from ben_trade_lab.models import BacktestResult, Bar, EquityPoint, StrategyParams
from ben_trade_lab.strategy import (
    HOURS_PER_YEAR,
    _rolling_mean,
    _rolling_previous_extreme,
    _rolling_realized_volatility,
    build_targets,
)

HOUR_MS = 3_600_000


def _bar(
    index: int,
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float = 1.0,
) -> Bar:
    return Bar(
        open_time_ms=index * HOUR_MS,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        close_time_ms=(index + 1) * HOUR_MS - 1,
    )


def _utc_ms(year: int, month: int, day: int, hour: int = 23) -> int:
    return int(datetime(year, month, day, hour, tzinfo=UTC).timestamp() * 1000)


def _cash_result(points: list[tuple[int, float]], initial_cash: float = 1_000.0) -> BacktestResult:
    return BacktestResult(
        equity=tuple(
            EquityPoint(
                open_time_ms=timestamp,
                cash=value,
                quantity=0.0,
                close=1.0,
                equity=value,
            )
            for timestamp, value in points
        ),
        fills=(),
        initial_cash=initial_cash,
        fee_bps_per_side=10.0,
        slippage_bps_per_side=5.0,
    )


class SignalSemanticsGoldenTests(unittest.TestCase):
    def test_previous_donchian_excludes_t_and_sma_includes_t(self) -> None:
        values = [100.0, 102.0, 101.0, 104.0]
        self.assertEqual(
            _rolling_previous_extreme(values, 2, maximum=True),
            [None, None, 102.0, 102.0],
        )
        self.assertEqual(
            _rolling_previous_extreme(values, 2, maximum=False),
            [None, None, 100.0, 101.0],
        )
        self.assertEqual(_rolling_mean([100.0, 102.0, 106.0], 2), [None, 101.0, 104.0])

    def test_sample_log_volatility_has_exact_lookback_and_no_future_input(self) -> None:
        closes = [100.0, 101.0, 103.0, 106.0]
        observed = _rolling_realized_volatility(closes, 2)
        first_two_returns = [math.log(101.0 / 100.0), math.log(103.0 / 101.0)]
        expected_at_two = statistics.stdev(first_two_returns) * math.sqrt(HOURS_PER_YEAR)

        # Two real hourly returns require three closes.  There must be no
        # artificial zero return in the first complete window.
        self.assertIsNone(observed[0])
        self.assertIsNone(observed[1])
        self.assertAlmostEqual(observed[2], expected_at_two, places=14)

        poisoned = _rolling_realized_volatility([100.0, 101.0, 103.0, 10_600.0], 2)
        self.assertEqual(observed[:3], poisoned[:3])

    def test_strict_breakout_fixed_entry_exposure_and_exit_priority(self) -> None:
        bars = [
            _bar(0, open_=100.0, high=101.0, low=99.0, close=100.0),
            _bar(1, open_=101.0, high=102.0, low=100.0, close=101.0),
            # Equal to the prior Donchian high: strict entry must remain flat.
            _bar(2, open_=102.0, high=102.0, low=101.0, close=102.0),
            # Current high is deliberately 104.  Entry proves channel excludes t.
            _bar(3, open_=104.0, high=104.0, low=103.0, close=104.0),
            _bar(4, open_=105.0, high=106.0, low=105.0, close=105.0),
            # Equal to both prior low and inclusive two-close SMA: strict exit holds.
            _bar(5, open_=105.0, high=106.0, low=105.0, close=105.0),
            # Existing-long exit is processed without same-close re-entry.
            _bar(6, open_=104.0, high=105.0, low=104.0, close=104.0),
        ]
        params = StrategyParams(
            entry_lookback=2,
            exit_lookback=1,
            trend_lookback=2,
            volatility_lookback=2,
            target_annualized_volatility=0.30,
            volatility_floor=0.10,
        )
        targets = build_targets(bars, params)
        entry_returns = [math.log(102.0 / 101.0), math.log(104.0 / 102.0)]
        entry_volatility = statistics.stdev(entry_returns) * math.sqrt(HOURS_PER_YEAR)
        expected_exposure = min(1.0, 0.30 / max(entry_volatility, 0.10))

        self.assertEqual(targets[:3], [0.0, 0.0, 0.0])
        self.assertAlmostEqual(targets[3], expected_exposure, places=14)
        self.assertEqual(targets[4], targets[3])
        self.assertEqual(targets[5], targets[3])
        self.assertEqual(targets[6], 0.0)


class ExecutionCostGoldenTests(unittest.TestCase):
    @staticmethod
    def _round_trip(multiplier: float):
        bars = [
            _bar(0, open_=100.0, high=100.0, low=100.0, close=100.0),
            _bar(1, open_=100.0, high=100.0, low=100.0, close=100.0),
            _bar(2, open_=110.0, high=110.0, low=110.0, close=110.0),
            _bar(3, open_=120.0, high=120.0, low=120.0, close=120.0),
        ]
        assumptions = ExecutionAssumptions(
            initial_cash=1_000.0,
            fee_bps_per_side=10.0 * multiplier,
            slippage_bps_per_side=5.0 * multiplier,
        )
        return run_backtest(bars, [1.0, 1.0, 0.0, 0.0], assumptions), assumptions

    def test_base_two_x_and_three_x_costs_match_hand_calculation(self) -> None:
        terminals: list[float] = []
        for multiplier in (1.0, 2.0, 3.0):
            with self.subTest(multiplier=multiplier):
                result, assumptions = self._round_trip(multiplier)
                fee_rate = assumptions.fee_bps_per_side / 10_000.0
                slip_rate = assumptions.slippage_bps_per_side / 10_000.0
                expected_buy = 100.0 * (1.0 + slip_rate)
                expected_quantity = 1_000.0 / (expected_buy * (1.0 + fee_rate))
                expected_sell = 120.0 * (1.0 - slip_rate)
                expected_terminal = expected_quantity * expected_sell * (1.0 - fee_rate)

                self.assertEqual([fill.side for fill in result.fills], ["BUY", "SELL"])
                self.assertEqual(
                    [(fill.source_signal_index, fill.fill_index) for fill in result.fills],
                    [(0, 1), (2, 3)],
                )
                self.assertAlmostEqual(result.fills[0].execution_price, expected_buy, places=14)
                self.assertAlmostEqual(result.fills[0].quantity, expected_quantity, places=14)
                self.assertAlmostEqual(result.fills[1].execution_price, expected_sell, places=14)
                self.assertGreater(result.fills[0].execution_price, result.fills[0].reference_price)
                self.assertLess(result.fills[1].execution_price, result.fills[1].reference_price)
                self.assertAlmostEqual(result.equity[-1].equity, expected_terminal, places=12)
                self.assertAlmostEqual(
                    calculate_metrics(result)["terminal_equity"], expected_terminal, places=12
                )
                self.assertEqual(result.completed_round_trips, 1)
                terminals.append(expected_terminal)
        self.assertGreater(terminals[0], terminals[1])
        self.assertGreater(terminals[1], terminals[2])

    def test_terminal_open_position_is_liquidated_only_for_performance(self) -> None:
        bars = [
            _bar(0, open_=100.0, high=100.0, low=100.0, close=100.0),
            _bar(1, open_=100.0, high=100.0, low=100.0, close=100.0),
            _bar(2, open_=110.0, high=110.0, low=110.0, close=110.0),
        ]
        assumptions = ExecutionAssumptions(1_000.0, 10.0, 5.0)
        result = run_backtest(bars, [1.0, 1.0, 1.0], assumptions)
        metrics = calculate_metrics(result)
        quantity = result.fills[0].quantity
        expected_mark = quantity * 110.0
        expected_liquidation = quantity * 110.0 * (1.0 - 0.0005) * (1.0 - 0.001)

        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.completed_round_trips, 0)
        self.assertAlmostEqual(metrics["mark_to_market_terminal_equity"], expected_mark)
        self.assertAlmostEqual(metrics["terminal_liquidation_value"], expected_liquidation)
        self.assertAlmostEqual(
            metrics["terminal_liquidation_cost"], expected_mark - expected_liquidation
        )
        self.assertTrue(metrics["terminal_liquidation_applied"])


class MetricGoldenTests(unittest.TestCase):
    def test_utc_daily_sharpe_and_hourly_drawdown_use_different_resolutions(self) -> None:
        result = _cash_result(
            [
                (_utc_ms(2026, 1, 1, 0), 1_000.0),
                (_utc_ms(2026, 1, 1, 23), 1_000.0),
                (_utc_ms(2026, 1, 2, 12), 1_200.0),
                (_utc_ms(2026, 1, 2, 23), 1_100.0),
                (_utc_ms(2026, 1, 3, 12), 600.0),
                (_utc_ms(2026, 1, 3, 23), 990.0),
                (_utc_ms(2026, 1, 4, 23), 1_089.0),
            ]
        )
        metrics = calculate_metrics(result)
        daily_returns = [0.10, -0.10, 0.10]
        expected_sharpe = (
            statistics.mean(daily_returns) / statistics.stdev(daily_returns) * math.sqrt(365.25)
        )
        self.assertAlmostEqual(metrics["annualized_sharpe_daily"], expected_sharpe, places=12)
        self.assertAlmostEqual(metrics["maximum_drawdown"], -0.50, places=14)

    def test_positive_quarter_profit_concentration_is_exact(self) -> None:
        result = _cash_result(
            [
                (_utc_ms(2025, 12, 31), 1_000.0),
                (_utc_ms(2026, 3, 31), 1_100.0),
                (_utc_ms(2026, 6, 30), 1_300.0),
                (_utc_ms(2026, 9, 30), 1_250.0),
                (_utc_ms(2026, 12, 31), 1_350.0),
            ]
        )
        metrics = calculate_metrics(result)
        # Positive quarter PnL is +100, +200, +100; max/sum = 1/2.
        self.assertAlmostEqual(
            metrics["maximum_positive_quarter_mark_to_market_profit_concentration"],
            0.5,
            places=14,
        )

    def test_zero_variance_and_zero_drawdown_are_finite_fail_safe_values(self) -> None:
        result = _cash_result(
            [
                (_utc_ms(2026, 1, 1), 1_000.0),
                (_utc_ms(2026, 1, 2), 1_000.0),
                (_utc_ms(2026, 1, 3), 1_000.0),
                (_utc_ms(2026, 1, 4), 1_000.0),
            ]
        )
        metrics = calculate_metrics(result)
        self.assertEqual(metrics["annualized_sharpe_daily"], 0.0)
        self.assertEqual(metrics["annualized_sortino_daily"], 0.0)
        self.assertEqual(metrics["maximum_drawdown"], 0.0)
        self.assertIsNone(metrics["calmar"])
        self.assertEqual(metrics["total_return"], 0.0)
        self.assertEqual(
            metrics["maximum_positive_quarter_mark_to_market_profit_concentration"], 1.0
        )


if __name__ == "__main__":
    unittest.main()
