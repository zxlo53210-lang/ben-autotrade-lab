from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ben_trade_lab.config import canonical_json, load_config, parse_utc_ms
from ben_trade_lab.engine import ExecutionAssumptions
from ben_trade_lab.integrity import source_tree_sha256, verified_hashed_object
from ben_trade_lab.models import Bar, StrategyParams
from ben_trade_lab.validation import (
    _is_adjacent,
    _window_result,
    finalize_holdout,
)

ROOT = Path(__file__).resolve().parents[1]
HOUR_MS = 3_600_000


def _write_hashed(path: Path, value: dict[str, object], hash_field: str) -> str:
    unsigned = {key: item for key, item in value.items() if key != hash_field}
    value = dict(unsigned)
    value[hash_field] = hashlib.sha256(canonical_json(unsigned)).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json(value) + b"\n")
    return str(value[hash_field])


def _serialized_bar(open_time_ms: int) -> dict[str, object]:
    return {
        "open_time_ms": open_time_ms,
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
        "volume": 10.0,
        "close_time_ms": open_time_ms + HOUR_MS - 1,
        "synthetic": False,
    }


def _metric(total_return: float = 0.10) -> dict[str, object]:
    return {
        "initial_cash": 10_000.0,
        "terminal_equity": 10_000.0 * (1.0 + total_return),
        "total_return": total_return,
        "cagr": 0.10,
        "annualized_sharpe_daily": 1.25,
        "annualized_sortino_daily": 1.50,
        "maximum_drawdown": -0.10,
        "calmar": 1.0,
        "completed_round_trips": 40,
        "fill_count": 80,
        "exposure_fraction": 0.40,
        "total_fees": 10.0,
        "maximum_positive_quarter_mark_to_market_profit_concentration": 0.40,
        "start_open_time_ms": 0,
        "end_open_time_ms": HOUR_MS,
        "equity_points": 2,
    }


class _Harness(SimpleNamespace):
    pass


class HoldoutOneShotTests(unittest.TestCase):
    @staticmethod
    def _build_harness(root: Path) -> _Harness:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        grid = config.strategy_grid()
        selected = next(
            params
            for params in grid
            if params.entry_lookback == 168
            and params.exit_lookback == 72
            and params.trend_lookback == 720
        )
        selected_value = selected.as_dict()
        neighbors = [
            params.as_dict()
            for params in grid
            if params.as_dict() != selected_value
            and _is_adjacent(selected_value, params.as_dict(), grid)
        ]
        experiment_id = "e" * 64
        preholdout_sha = "a" * 64
        holdout_sha = "b" * 64
        parent_sha = "c" * 64
        source_sha = source_tree_sha256(root)
        holdout_start = parse_utc_ms(config.splits["validation_end_utc_exclusive"])
        context = [_serialized_bar(holdout_start - (720 - index) * HOUR_MS) for index in range(720)]
        selection: dict[str, object] = {
            "status": "FROZEN_CANDIDATE",
            "experiment_id": experiment_id,
            "config_sha256": config.config_sha256,
            "source_tree_sha256": source_sha,
            "lockbox_id": "LOCKBOX_TEST_ONLY",
            "preholdout_data_sha256": preholdout_sha,
            "holdout_commitment_sha256": holdout_sha,
            "parent_manifest_sha256": parent_sha,
            "selected_params": selected_value,
            "selected_parameter_neighbors": neighbors,
            "preholdout_neighbor_positive_fraction": 0.80,
            "warmup_context_hours": len(context),
            "warmup_context": context,
            "candidates": [
                {
                    "params": selected_value,
                    "positive_fold_fraction": 8 / 9,
                }
            ],
        }
        selection_path = root / "inputs" / "selection.json"
        _write_hashed(selection_path, selection, "selection_sha256")
        selection = json.loads(selection_path.read_text(encoding="utf-8"))

        receipt_common: dict[str, object] = {
            "experiment_id": experiment_id,
            "selection_sha256": selection["selection_sha256"],
            "config_sha256": config.config_sha256,
            "source_tree_sha256": source_sha,
            "preholdout_data_sha256": preholdout_sha,
            "holdout_commitment_sha256": holdout_sha,
        }
        artifacts = root / "artifacts"
        test_output = b"Ran 1 tests in 0.001s\n\nOK\n"
        test_output_sha = hashlib.sha256(test_output).hexdigest()
        test_output_path = artifacts / f"test-log-{test_output_sha[:16]}.txt"
        test_output_path.parent.mkdir(parents=True, exist_ok=True)
        test_output_path.write_bytes(test_output)
        test_receipt: dict[str, object] = {
            "type": "TEST_RECEIPT",
            "status": "PASS",
            "normalized_output_path": test_output_path.relative_to(root).as_posix(),
            "normalized_output_sha256": test_output_sha,
            "test_count": 1,
            "return_code": 0,
            "environment_policy": "ALLOWLIST_NO_CREDENTIAL_ENV",
            "source_tree_sha256_before": source_sha,
            "source_tree_sha256_after": source_sha,
            "command": [
                "python",
                "-P",
                "-m",
                "unittest",
                "discover",
                "-s",
                "tests",
                "-v",
            ],
            **receipt_common,
        }
        test_receipt_sha = hashlib.sha256(canonical_json(test_receipt)).hexdigest()
        test_receipt_path = artifacts / f"test-receipt-{test_receipt_sha[:16]}.json"
        _write_hashed(test_receipt_path, test_receipt, "receipt_sha256")

        review_path = root / "docs" / "reviews" / "pro-round-test.md"
        review_path.parent.mkdir(parents=True, exist_ok=True)
        review_path.write_text("# Sanitized test-only review\n", encoding="utf-8")
        review_sha = hashlib.sha256(review_path.read_bytes()).hexdigest()
        review_receipt: dict[str, object] = {
            "type": "PRO_REVIEW_RECEIPT",
            "verdict": "PROCEED",
            "reviewer": "ChatGPT Pro",
            "visible_model_label": "GPT-5.6 Sol",
            "visible_reasoning_label": "Pro",
            "sanitized_review_path": review_path.relative_to(root).as_posix(),
            "sanitized_review_sha256": review_sha,
            **receipt_common,
        }
        review_receipt_sha = hashlib.sha256(canonical_json(review_receipt)).hexdigest()
        review_receipt_path = artifacts / f"pro-review-receipt-{review_receipt_sha[:16]}.json"
        _write_hashed(review_receipt_path, review_receipt, "receipt_sha256")
        experiment_dir = root / "state" / "experiments" / experiment_id
        experiment_dir.mkdir(parents=True)
        (experiment_dir / "FROZEN.json").write_text("{}\n", encoding="utf-8")

        metadata: dict[str, object] = {
            "lockbox_id": "LOCKBOX_TEST_ONLY",
            "normalized_sha256": holdout_sha,
            "preholdout_sha256": preholdout_sha,
            "parent_manifest_sha256": parent_sha,
            "manifest_file_sha256": "d" * 64,
            "declared_source_anomalies": [],
        }
        holdout_bars = [
            Bar(
                open_time_ms=holdout_start + index * HOUR_MS,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.0,
                volume=10.0,
                close_time_ms=holdout_start + (index + 1) * HOUR_MS - 1,
            )
            for index in range(2)
        ]
        return _Harness(
            root=root,
            config=config,
            selected=selected_value,
            neighbors=neighbors,
            experiment_dir=experiment_dir,
            opened_path=experiment_dir / "HOLDOUT_OPENED.json",
            finalized_path=experiment_dir / "FINALIZED.json",
            selection_path=selection_path,
            test_receipt_path=test_receipt_path,
            review_receipt_path=review_receipt_path,
            holdout_manifest_path=root / "inputs" / "never-read-holdout-manifest.json",
            metadata=metadata,
            holdout_bars=holdout_bars,
        )

    @staticmethod
    def _call(
        harness: _Harness,
        *,
        metadata: dict[str, object] | None = None,
        load_side_effect: object | None = None,
        metric_side_effect: object | None = None,
        benchmark_side_effect: object | None = None,
    ) -> tuple[Path, Mock, Mock]:
        supplied_metadata = harness.metadata if metadata is None else metadata
        load_value = (harness.holdout_bars, supplied_metadata)
        metric_effect = metric_side_effect if metric_side_effect is not None else _metric()
        with (
            patch(
                "ben_trade_lab.validation.read_manifest_metadata",
                return_value=supplied_metadata,
            ),
            patch("ben_trade_lab.validation.bind_manifest_to_config"),
            patch(
                "ben_trade_lab.validation.load_bars_from_manifest",
                return_value=load_value if load_side_effect is None else None,
                side_effect=load_side_effect,
            ) as load_mock,
            patch(
                "ben_trade_lab.validation._window_result",
                return_value=metric_effect if metric_side_effect is None else None,
                side_effect=metric_side_effect,
            ) as metric_mock,
            patch(
                "ben_trade_lab.validation.buy_and_hold_metrics",
                return_value=_metric() if benchmark_side_effect is None else None,
                side_effect=benchmark_side_effect,
            ),
        ):
            result = finalize_holdout(
                harness.holdout_manifest_path,
                harness.config,
                harness.selection_path,
                harness.test_receipt_path,
                harness.review_receipt_path,
                root=harness.root,
            )
        return result, load_mock, metric_mock

    def test_open_receipt_precedes_load_and_all_performance_then_chains_report(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._build_harness(Path(temporary))
            calls: list[tuple[str, dict[str, object] | None]] = []

            def load_side_effect(*_args: object, **_kwargs: object) -> object:
                self.assertTrue(harness.opened_path.exists())
                calls.append(("load", None))
                return harness.holdout_bars, harness.metadata

            def metric_side_effect(
                _bars: object,
                params: StrategyParams,
                _assumptions: object,
                _start: int,
                _end: int,
            ) -> dict[str, object]:
                self.assertTrue(harness.opened_path.exists())
                calls.append(("metric", params.as_dict()))
                return _metric()

            def benchmark_side_effect(*_args: object, **_kwargs: object) -> object:
                self.assertTrue(harness.opened_path.exists())
                calls.append(("benchmark", None))
                return _metric()

            self.assertFalse(harness.holdout_manifest_path.exists())
            report_path, load_mock, metric_mock = self._call(
                harness,
                load_side_effect=load_side_effect,
                metric_side_effect=metric_side_effect,
                benchmark_side_effect=benchmark_side_effect,
            )

            self.assertEqual(calls[0][0], "load")
            self.assertEqual(load_mock.call_count, 1)
            self.assertEqual(metric_mock.call_count, 4 + len(harness.neighbors))
            primary_calls = [value for kind, value in calls if kind == "metric"][:4]
            neighbor_calls = [value for kind, value in calls if kind == "metric"][4:]
            self.assertEqual(primary_calls, [harness.selected] * 4)
            self.assertEqual(neighbor_calls, harness.neighbors)
            self.assertNotIn(harness.selected, neighbor_calls)
            self.assertEqual(len(neighbor_calls), len({canonical_json(x) for x in neighbor_calls}))
            self.assertEqual(calls[-1][0], "benchmark")

            report = verified_hashed_object(report_path, "report_sha256")
            self.assertEqual(report["evidence_level"], "RETROSPECTIVE_LOCKED_OOS")
            holdout_hours = (
                parse_utc_ms(harness.config.splits["locked_holdout_end_utc_exclusive"])
                - parse_utc_ms(harness.config.splits["validation_end_utc_exclusive"])
            ) / HOUR_MS
            self.assertEqual(
                report["benchmark_buy_and_hold"]["elapsed_hours"],
                holdout_hours,
            )
            self.assertEqual(
                [item["params"] for item in report["holdout_parameter_neighbors"]],
                harness.neighbors,
            )
            protocol = report["holdout_parameter_neighbor_protocol"]
            self.assertTrue(protocol["primary_excluded"])
            self.assertFalse(protocol["replacement_allowed"])
            self.assertEqual(protocol["exact_frozen_neighbor_count"], len(harness.neighbors))

            finalized = json.loads(harness.finalized_path.read_text(encoding="utf-8"))
            self.assertEqual(finalized["report_sha256"], report["report_sha256"])
            self.assertEqual(
                finalized["report_path"],
                report_path.relative_to(harness.root).as_posix(),
            )
            self.assertEqual(finalized["experiment_id"], "e" * 64)
            self.assertEqual(finalized["status"], report["status"])

    def test_load_or_metric_interruption_burns_the_one_shot_and_blocks_retry(self) -> None:
        for stage in ("load", "metric"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temporary:
                harness = self._build_harness(Path(temporary))

                def fail_load(
                    *_args: object,
                    _opened_path: Path = harness.opened_path,
                    **_kwargs: object,
                ) -> object:
                    self.assertTrue(_opened_path.exists())
                    raise RuntimeError("simulated load interruption")

                def fail_metric(
                    *_args: object,
                    _opened_path: Path = harness.opened_path,
                    **_kwargs: object,
                ) -> object:
                    self.assertTrue(_opened_path.exists())
                    raise RuntimeError("simulated metric interruption")

                with self.assertRaisesRegex(RuntimeError, "simulated"):
                    self._call(
                        harness,
                        load_side_effect=fail_load if stage == "load" else None,
                        metric_side_effect=fail_metric if stage == "metric" else None,
                    )
                self.assertTrue(harness.opened_path.exists())
                self.assertFalse(harness.finalized_path.exists())

                retry_load = Mock(return_value=(harness.holdout_bars, harness.metadata))
                with self.assertRaisesRegex(RuntimeError, "HOLDOUT_ALREADY_OPENED_NOT_RETRYABLE"):
                    self._call(harness, load_side_effect=retry_load)
                retry_load.assert_not_called()

    def test_all_selection_manifest_and_receipt_mismatches_precede_open(self) -> None:
        mismatch_cases = (
            "selection_hash",
            "selection_binding",
            "selection_no_eligible",
            "manifest_binding",
            "test_receipt_binding",
            "review_receipt_verdict",
        )
        for case in mismatch_cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                harness = self._build_harness(Path(temporary))
                metadata = dict(harness.metadata)
                if case == "selection_hash":
                    value = json.loads(harness.selection_path.read_text(encoding="utf-8"))
                    value["lockbox_id"] = "TAMPERED_WITHOUT_REHASH"
                    harness.selection_path.write_text(json.dumps(value), encoding="utf-8")
                elif case == "selection_binding":
                    value = json.loads(harness.selection_path.read_text(encoding="utf-8"))
                    value["config_sha256"] = "f" * 64
                    _write_hashed(harness.selection_path, value, "selection_sha256")
                elif case == "selection_no_eligible":
                    value = json.loads(harness.selection_path.read_text(encoding="utf-8"))
                    value["status"] = "NO_ELIGIBLE_CANDIDATE"
                    value["selected_params"] = None
                    _write_hashed(harness.selection_path, value, "selection_sha256")
                elif case == "manifest_binding":
                    metadata["normalized_sha256"] = "f" * 64
                elif case == "test_receipt_binding":
                    value = json.loads(harness.test_receipt_path.read_text(encoding="utf-8"))
                    value["config_sha256"] = "f" * 64
                    _write_hashed(harness.test_receipt_path, value, "receipt_sha256")
                elif case == "review_receipt_verdict":
                    value = json.loads(harness.review_receipt_path.read_text(encoding="utf-8"))
                    value["verdict"] = "BLOCKED"
                    _write_hashed(harness.review_receipt_path, value, "receipt_sha256")

                load_mock = Mock(return_value=(harness.holdout_bars, metadata))
                with self.assertRaises((ValueError, RuntimeError)):
                    self._call(
                        harness,
                        metadata=metadata,
                        load_side_effect=load_mock,
                    )
                self.assertFalse(harness.opened_path.exists())
                load_mock.assert_not_called()

    def test_exact_window_cagr_includes_the_last_bars_full_hour(self) -> None:
        bars = [
            Bar(index * HOUR_MS, 100.0, 101.0, 99.0, 100.0, 10.0, (index + 1) * HOUR_MS - 1)
            for index in range(49)
        ]
        params = StrategyParams(1, 1, 1, 1, 0.30, 0.10)
        raw_metrics = _metric(0.000001)
        with (
            patch(
                "ben_trade_lab.validation.build_targets",
                return_value=[0.0] * len(bars),
            ),
            patch("ben_trade_lab.validation.calculate_metrics", return_value=raw_metrics),
        ):
            metrics = _window_result(
                bars,
                params,
                ExecutionAssumptions(1_000.0, 10.0, 5.0),
                HOUR_MS,
                49 * HOUR_MS,
            )

        expected = (1.000001) ** (365.25 * 24.0 / 48.0) - 1.0
        one_hour_bug = (1.000001) ** (365.25 * 24.0 / 47.0) - 1.0
        self.assertEqual(metrics["elapsed_hours"], 48.0)
        self.assertAlmostEqual(metrics["cagr"], expected, places=12)
        self.assertAlmostEqual(metrics["calmar"], expected / 0.10, places=12)
        self.assertNotAlmostEqual(metrics["cagr"], one_hour_bug, places=6)


if __name__ == "__main__":
    unittest.main()
