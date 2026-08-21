from __future__ import annotations

import unittest

from ben_trade_lab.engine import ExecutionAssumptions, run_backtest
from ben_trade_lab.metrics import TerminalLiquidationNotExecutable, calculate_metrics
from ben_trade_lab.models import Bar


def _bar(
    index: int,
    *,
    price: float = 100.0,
    volume: float = 10.0,
    synthetic: bool = False,
    trade_count: int = 1,
) -> Bar:
    return Bar(
        open_time_ms=index * 3_600_000,
        open=price,
        high=price + 1.0,
        low=price - 1.0,
        close=price,
        volume=volume,
        close_time_ms=(index + 1) * 3_600_000 - 1,
        synthetic=synthetic,
        trade_count=trade_count,
    )


class ExecutionEdgeCaseTests(unittest.TestCase):
    def test_zero_volume_defers_and_preserves_original_signal(self) -> None:
        bars = [_bar(0), _bar(1, volume=0.0), _bar(2), _bar(3)]
        result = run_backtest(
            bars,
            [1.0, 1.0, 1.0, 1.0],
            ExecutionAssumptions(1_000.0, 10.0, 5.0),
        )
        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].side, "BUY")
        self.assertEqual(result.fills[0].source_signal_index, 0)
        self.assertEqual(result.fills[0].fill_index, 2)

    def test_synthetic_bar_defers_and_never_fills(self) -> None:
        bars = [_bar(0), _bar(1, synthetic=True), _bar(2), _bar(3)]
        result = run_backtest(
            bars,
            [1.0, 1.0, 1.0, 1.0],
            ExecutionAssumptions(1_000.0, 10.0, 5.0),
        )
        self.assertEqual(
            [(fill.source_signal_index, fill.fill_index) for fill in result.fills], [(0, 2)]
        )
        self.assertFalse(bars[result.fills[0].fill_index].synthetic)

    def test_ineligible_target_cannot_cancel_older_eligible_entry(self) -> None:
        bars = [_bar(0), _bar(1, volume=0.0), _bar(2), _bar(3)]
        result = run_backtest(
            bars,
            [1.0, 0.0, 0.0, 0.0],
            ExecutionAssumptions(1_000.0, 10.0, 5.0),
        )
        self.assertEqual(
            [(fill.source_signal_index, fill.fill_index) for fill in result.fills],
            [(0, 2), (2, 3)],
        )

    def test_last_bar_signal_has_no_same_bar_fill(self) -> None:
        bars = [_bar(0), _bar(1), _bar(2)]
        result = run_backtest(
            bars,
            [0.0, 0.0, 1.0],
            ExecutionAssumptions(1_000.0, 10.0, 5.0),
        )
        self.assertEqual(result.fills, ())

    def test_two_bar_latency_stress_fills_no_earlier_than_t_plus_two(self) -> None:
        bars = [_bar(index) for index in range(5)]
        result = run_backtest(
            bars,
            [1.0, 1.0, 1.0, 1.0, 1.0],
            ExecutionAssumptions(1_000.0, 10.0, 5.0, signal_delay_bars=2),
        )
        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].source_signal_index, 0)
        self.assertEqual(result.fills[0].fill_index, 2)

    def test_two_bar_latency_backlog_allows_later_eligible_cancel_before_fill(self) -> None:
        bars = [_bar(0), _bar(1), _bar(2, volume=0.0), _bar(3)]
        result = run_backtest(
            bars,
            [0.25, 0.0, 1.0, 1.0],
            ExecutionAssumptions(1_000.0, 10.0, 5.0, signal_delay_bars=2),
        )
        self.assertEqual(result.fills, ())

    def test_two_bar_latency_backlog_preserves_original_target_without_resize(self) -> None:
        bars = [_bar(0), _bar(1), _bar(2, volume=0.0), _bar(3)]
        assumptions = ExecutionAssumptions(
            1_000.0,
            10.0,
            5.0,
            signal_delay_bars=2,
        )
        result = run_backtest(bars, [0.25, 0.75, 0.0, 0.0], assumptions)

        self.assertEqual(len(result.fills), 1)
        fill = result.fills[0]
        self.assertEqual((fill.source_signal_index, fill.fill_index), (0, 3))
        expected_quantity = 250.0 / (100.0 * 1.0005 * 1.001)
        self.assertAlmostEqual(fill.quantity, expected_quantity)

    def test_positive_target_changes_do_not_add_or_rebalance(self) -> None:
        bars = [_bar(index) for index in range(5)]
        result = run_backtest(
            bars,
            [0.25, 0.75, 0.5, 0.9, 0.0],
            ExecutionAssumptions(1_000.0, 10.0, 5.0),
        )
        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].source_signal_index, 0)
        self.assertEqual(result.fills[0].fill_index, 1)

    def test_terminal_liquidation_is_costed_but_not_a_round_trip(self) -> None:
        bars = [_bar(0), _bar(1), _bar(2, price=110.0)]
        assumptions = ExecutionAssumptions(1_000.0, 10.0, 5.0)
        result = run_backtest(bars, [1.0, 1.0, 1.0], assumptions)
        metrics = calculate_metrics(result)

        terminal = result.equity[-1]
        expected_execution_price = terminal.close * (1.0 - 5.0 / 10_000.0)
        expected_gross = terminal.quantity * expected_execution_price
        expected_fee = expected_gross * (10.0 / 10_000.0)
        expected_value = terminal.cash + expected_gross - expected_fee

        self.assertEqual(result.completed_round_trips, 0)
        self.assertEqual(len(result.fills), 1)
        self.assertTrue(metrics["terminal_liquidation_applied"])
        self.assertTrue(metrics["terminal_liquidation_executable"])
        self.assertTrue(metrics["terminal_state_eligible"])
        self.assertAlmostEqual(metrics["terminal_liquidation_value"], expected_value)
        self.assertAlmostEqual(metrics["terminal_equity"], expected_value)
        self.assertAlmostEqual(
            metrics["terminal_liquidation_cost"], terminal.equity - expected_value
        )
        self.assertAlmostEqual(metrics["mark_to_market_terminal_equity"], terminal.equity)
        self.assertGreater(metrics["mark_to_market_terminal_equity"], metrics["terminal_equity"])

    def test_terminal_liquidation_rejects_every_ineligible_final_bar(self) -> None:
        cases = {
            "synthetic": {"synthetic": True},
            "zero_volume": {"volume": 0.0},
            "zero_trade": {"trade_count": 0},
        }
        for name, final_bar_overrides in cases.items():
            with self.subTest(name=name):
                bars = [_bar(0), _bar(1), _bar(2, price=110.0, **final_bar_overrides)]
                result = run_backtest(
                    bars,
                    [1.0, 1.0, 1.0],
                    ExecutionAssumptions(1_000.0, 10.0, 5.0),
                )
                self.assertGreater(result.equity[-1].quantity, 0.0)
                self.assertFalse(result.equity[-1].state_eligible)
                with self.assertRaisesRegex(
                    TerminalLiquidationNotExecutable,
                    TerminalLiquidationNotExecutable.code,
                ):
                    calculate_metrics(result)

    def test_double_cost_scenario_reduces_realizable_terminal_value(self) -> None:
        bars = [_bar(0), _bar(1), _bar(2, price=105.0), _bar(3, price=110.0)]
        targets = [1.0, 1.0, 1.0, 1.0]
        base = calculate_metrics(
            run_backtest(bars, targets, ExecutionAssumptions(1_000.0, 10.0, 5.0))
        )
        doubled = calculate_metrics(
            run_backtest(bars, targets, ExecutionAssumptions(1_000.0, 20.0, 10.0))
        )
        self.assertLess(doubled["terminal_liquidation_value"], base["terminal_liquidation_value"])
        self.assertGreater(doubled["terminal_liquidation_cost"], base["terminal_liquidation_cost"])


if __name__ == "__main__":
    unittest.main()
