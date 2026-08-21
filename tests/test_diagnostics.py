from __future__ import annotations

import unittest

from ben_trade_lab.diagnostics import moving_block_max_sharpe


class MovingBlockDiagnosticTests(unittest.TestCase):
    def test_is_deterministic_and_family_wise(self) -> None:
        series = [
            [0.001 * ((index % 7) - 2) for index in range(70)],
            [0.001 * ((index % 5) - 1) for index in range(70)],
            [0.001 * ((index % 9) - 3) for index in range(70)],
        ]
        arguments = {
            "selected_index": 1,
            "resamples": 100,
            "block_length_days": 7,
            "seed_hex": "a" * 64,
        }
        first = moving_block_max_sharpe(series, **arguments)
        second = moving_block_max_sharpe(series, **arguments)
        self.assertEqual(first, second)
        self.assertEqual(first["candidate_count"], 3)
        self.assertGreaterEqual(first["family_wise_null_p_value"], 1 / 101)
        self.assertLessEqual(first["family_wise_null_p_value"], 1.0)

    def test_mismatched_histories_fail_closed(self) -> None:
        with self.assertRaises(ValueError):
            moving_block_max_sharpe(
                [[0.0, 0.1], [0.0]],
                selected_index=0,
                resamples=100,
                block_length_days=1,
                seed_hex="b" * 64,
            )


if __name__ == "__main__":
    unittest.main()
