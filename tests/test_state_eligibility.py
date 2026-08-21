from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ben_trade_lab.data import KLINE_COLUMNS, _expand_gaps_carry_forward, load_bars_from_manifest
from ben_trade_lab.engine import ExecutionAssumptions, run_backtest
from ben_trade_lab.models import Bar, StrategyParams
from ben_trade_lab.strategy import build_targets

HOUR_MS = 3_600_000


def _bar(
    index: int,
    *,
    price: float = 100.0,
    high: float | None = None,
    low: float | None = None,
    volume: float = 10.0,
    trade_count: int = 1,
    synthetic: bool = False,
) -> Bar:
    return Bar(
        open_time_ms=index * HOUR_MS,
        open=price,
        high=price + 1.0 if high is None else high,
        low=price - 1.0 if low is None else low,
        close=price,
        volume=volume,
        close_time_ms=(index + 1) * HOUR_MS - 1,
        synthetic=synthetic,
        trade_count=trade_count,
    )


class StateEligibilityTests(unittest.TestCase):
    def test_contract_requires_official_volume_and_trade_count(self) -> None:
        self.assertTrue(_bar(0).official)
        self.assertTrue(_bar(0).state_eligible)
        self.assertFalse(_bar(0, volume=0.0).state_eligible)
        self.assertFalse(_bar(0, trade_count=0).state_eligible)
        self.assertFalse(_bar(0, synthetic=True).official)
        self.assertFalse(_bar(0, synthetic=True).state_eligible)

    def test_ineligible_bars_do_not_age_or_poison_strategy_buffers(self) -> None:
        eligible = [
            _bar(0, price=100.0),
            _bar(1, price=101.0),
            _bar(2, price=103.0),
            _bar(3, price=105.0),
            _bar(4, price=102.0),
            _bar(5, price=99.0),
            _bar(6, price=106.0),
        ]
        params = StrategyParams(2, 2, 2, 2, 0.3, 0.1)
        baseline = build_targets(eligible, params)

        mixed = [
            _bar(0, price=100.0),
            _bar(1, price=10_000.0, low=0.01, volume=0.0),
            _bar(2, price=101.0),
            _bar(3, price=0.01, high=100_000.0, low=0.001, trade_count=0),
            _bar(4, price=103.0),
            _bar(5, price=50_000.0, low=0.001, synthetic=True),
            _bar(6, price=105.0),
            _bar(7, price=102.0),
            _bar(8, price=99.0),
            _bar(9, price=106.0),
        ]
        mixed_targets = build_targets(mixed, params)
        eligible_targets = [
            target for bar, target in zip(mixed, mixed_targets, strict=True) if bar.state_eligible
        ]

        self.assertEqual(eligible_targets, baseline)
        for index, bar in enumerate(mixed):
            if index > 0 and not bar.state_eligible:
                self.assertEqual(mixed_targets[index], mixed_targets[index - 1])

    def test_ineligible_bars_cannot_generate_cancel_resize_or_fill(self) -> None:
        bars = [
            _bar(0, price=100.0),
            _bar(1, price=1.0, volume=0.0),
            _bar(2, price=1_000.0, trade_count=0),
            _bar(3, price=50.0, synthetic=True),
            _bar(4, price=120.0),
        ]
        result = run_backtest(
            bars,
            [0.25, 0.0, 1.0, 0.0, 0.0],
            ExecutionAssumptions(1_000.0, 10.0, 5.0),
        )

        self.assertEqual(len(result.fills), 1)
        fill = result.fills[0]
        self.assertEqual((fill.source_signal_index, fill.fill_index), (0, 4))
        expected_quantity = 250.0 / (120.0 * 1.0005 * 1.001)
        self.assertAlmostEqual(fill.quantity, expected_quantity)
        self.assertTrue(result.equity[4].state_eligible)
        self.assertTrue(all(not point.state_eligible for point in result.equity[1:4]))

        no_eligible_entry = run_backtest(
            bars,
            [0.0, 1.0, 1.0, 1.0, 0.0],
            ExecutionAssumptions(1_000.0, 10.0, 5.0),
        )
        self.assertEqual(no_eligible_entry.fills, ())

    def test_csv_trade_count_reaches_runtime_and_gap_bar_is_zero_trade(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            csv_path = root / "data" / "normalized" / "bars.csv"
            csv_path.parent.mkdir(parents=True)
            row = (
                "0,100,101,99,100,10,3599999,1000,7,5,500,0"
            )
            csv_path.write_bytes(
                (",".join(KLINE_COLUMNS) + "\n" + row + "\n").encode("utf-8")
            )
            manifest = {
                "normalized_path": "data/normalized/bars.csv",
                "normalized_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
                "gap_policy": "REJECT",
            }
            with patch("ben_trade_lab.data.verify_manifest", return_value=manifest):
                bars, _ = load_bars_from_manifest(root / "manifest.json", root=root)

        self.assertEqual(bars[0].trade_count, 7)
        self.assertTrue(bars[0].state_eligible)

        expanded = _expand_gaps_carry_forward([bars[0], _bar(2)])
        self.assertTrue(expanded[1].synthetic)
        self.assertEqual(expanded[1].trade_count, 0)
        self.assertFalse(expanded[1].state_eligible)


if __name__ == "__main__":
    unittest.main()
