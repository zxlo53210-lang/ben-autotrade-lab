from __future__ import annotations

import copy
import io
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from ben_trade_lab.cli import main
from ben_trade_lab.config import _validate, load_config
from ben_trade_lab.engine import ExecutionAssumptions, run_backtest
from ben_trade_lab.models import Bar, ExecutionMode, StrategyParams
from ben_trade_lab.strategy import build_targets

ROOT = Path(__file__).resolve().parents[1]


def bars(count: int = 120) -> list[Bar]:
    result: list[Bar] = []
    for index in range(count):
        price = 100.0 + index * 0.2 + (index % 7) * 0.1
        result.append(
            Bar(
                open_time_ms=index * 3_600_000,
                open=price,
                high=price + 1.0,
                low=price - 1.0,
                close=price + 0.25,
                volume=10.0,
                close_time_ms=(index + 1) * 3_600_000 - 1,
            )
        )
    return result


class PolicyTests(unittest.TestCase):
    def test_live_mode_is_unrepresentable(self) -> None:
        with self.assertRaises(ValueError):
            ExecutionMode("LIVE")

    def test_live_cli_fails_closed(self) -> None:
        stream = io.StringIO()
        with redirect_stderr(stream):
            code = main(["--live"])
        self.assertEqual(code, 2)
        self.assertIn("LIVE_EXECUTION_UNAVAILABLE", stream.getvalue())

    def test_config_is_zero_authority(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        self.assertFalse(config.execution["allow_short"])
        self.assertFalse(config.execution["allow_leverage"])
        self.assertEqual(config.market["source_base_url"], "https://data-api.binance.vision")
        self.assertLessEqual(len(config.strategy_grid()), config.raw["selection"]["maximum_trials"])

    def test_frozen_contract_rejects_predeclared_gate_or_cost_mutation(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        changed_gate = copy.deepcopy(config.raw)
        changed_gate["acceptance"]["minimum_holdout_sharpe"] = 0.1
        with self.assertRaisesRegex(ValueError, "acceptance gates are frozen"):
            _validate(changed_gate)

        changed_cost = copy.deepcopy(config.raw)
        changed_cost["execution"]["fee_bps_per_side"] = 0.0
        with self.assertRaisesRegex(ValueError, "cost.*assumptions are frozen"):
            _validate(changed_cost)

        changed_grid = copy.deepcopy(config.raw)
        changed_grid["strategy"]["entry_lookbacks"] = [24, 48, 72]
        with self.assertRaisesRegex(ValueError, "parameter grid are frozen"):
            _validate(changed_grid)


class StrategyAndAccountingTests(unittest.TestCase):
    def test_future_poisoning_does_not_change_past_targets(self) -> None:
        original = bars()
        params = StrategyParams(8, 4, 10, 10, 0.3, 0.1)
        baseline = build_targets(original, params)
        poisoned = list(original)
        future = poisoned[80]
        poisoned[80] = Bar(
            open_time_ms=future.open_time_ms,
            open=future.open * 20,
            high=future.high * 20,
            low=future.low * 20,
            close=future.close * 20,
            volume=future.volume,
            close_time_ms=future.close_time_ms,
        )
        changed = build_targets(poisoned, params)
        self.assertEqual(baseline[:80], changed[:80])

    def test_close_signal_fills_at_next_bar_open(self) -> None:
        sample = bars(5)
        result = run_backtest(
            sample,
            [1.0, 1.0, 0.0, 0.0, 0.0],
            ExecutionAssumptions(1000.0, 10.0, 5.0),
        )
        self.assertEqual(result.fills[0].side, "BUY")
        self.assertEqual(result.fills[0].source_signal_index, 0)
        self.assertEqual(result.fills[0].fill_index, 1)
        self.assertEqual(result.fills[1].side, "SELL")
        self.assertEqual(result.fills[1].source_signal_index, 2)
        self.assertEqual(result.fills[1].fill_index, 3)

    def test_cash_position_and_equity_reconcile(self) -> None:
        sample = bars(30)
        targets = [0.0] * 3 + [0.8] * 10 + [0.0] * 17
        result = run_backtest(
            sample,
            targets,
            ExecutionAssumptions(1000.0, 10.0, 5.0),
        )
        for point in result.equity:
            self.assertGreaterEqual(point.cash, 0.0)
            self.assertGreaterEqual(point.quantity, 0.0)
            self.assertAlmostEqual(
                point.equity, point.cash + point.quantity * point.close, places=9
            )
            self.assertLessEqual(point.quantity * point.close / point.equity, 1.0 + 1e-12)
        self.assertGreater(sum(fill.fee for fill in result.fills), 0.0)


if __name__ == "__main__":
    unittest.main()
