from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ben_trade_lab.anchor import (
    ExternalAnchorCorruption,
    commit_holdout_opened_anchor,
    initialize_anchor_store,
    read_holdout_opened_anchor,
)
from ben_trade_lab.config import canonical_json, load_config, parse_utc_ms
from ben_trade_lab.engine import ExecutionAssumptions
from ben_trade_lab.integrity import (
    FULL_PROVENANCE_REPLAY_TEST_ID,
    full_provenance_replay_evidence,
    source_tree_sha256,
    verified_hashed_object,
)
from ben_trade_lab.metrics import TerminalLiquidationNotExecutable
from ben_trade_lab.models import Bar, StrategyParams
from ben_trade_lab.validation import (
    BENCHMARK_PROTOCOL,
    REPORT_KIND_LOCKED_OOS_EVALUATION,
    REPORT_KIND_TERMINAL_LIQUIDATION_FAILURE,
    VALIDATION_METHOD,
    _experiment_id,
    _is_adjacent,
    _path_entry_exists,
    _verified_holdout_report_artifact,
    _window_result,
    _write_state_exclusive,
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
        "trade_count": 1,
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
    def _build_harness(sandbox: Path) -> _Harness:
        root = sandbox / "repository"
        root.mkdir(parents=True)
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        anchor_root = sandbox / "external-anchor"
        anchor_store_id = str(config.raw["anchor"]["store_id"])
        anchor_store = initialize_anchor_store(
            anchor_root,
            repository_root=root,
            store_id=anchor_store_id,
            created_at_utc="2026-08-21T00:00:00.000000Z",
        )
        # This isolated fixture creates its own descriptor. Bind every
        # downstream artifact to that exact descriptor rather than the real
        # release anchor configured for production.
        config.raw["anchor"]["store_sha256"] = anchor_store.store_sha256
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
        preholdout_sha = "a" * 64
        holdout_sha = "b" * 64
        parent_sha = "c" * 64
        preholdout_manifest_path = "data/manifests/preholdout-test.json"
        preholdout_manifest_sha = "f" * 64
        preholdout_descriptor_sha = "6" * 64
        locked_manifest_path = "data/manifests/locked-never-read-test.json"
        locked_manifest_sha = "d" * 64
        locked_descriptor_sha = "7" * 64
        source_sha = source_tree_sha256(root)
        experiment_id = _experiment_id(
            method_version=VALIDATION_METHOD,
            config_sha256=config.config_sha256,
            source_tree_sha256_value=source_sha,
            lockbox_id="LOCKBOX_TEST_ONLY",
            preholdout_data_sha256=preholdout_sha,
            holdout_commitment_sha256=holdout_sha,
            selected_params=selected_value,
        )
        holdout_start = parse_utc_ms(config.splits["validation_end_utc_exclusive"])
        context = [_serialized_bar(holdout_start - (720 - index) * HOUR_MS) for index in range(720)]
        selection: dict[str, object] = {
            "schema_version": "1.2.0",
            "status": "FROZEN_CANDIDATE",
            "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
            "method": "CONTINUOUS_NINE_FOLD_VALIDATION_V2",
            "experiment_id": experiment_id,
            "preholdout_manifest_path": preholdout_manifest_path,
            "preholdout_manifest_sha256": preholdout_manifest_sha,
            "preholdout_partition_descriptor_sha256": preholdout_descriptor_sha,
            "locked_holdout_manifest_path": locked_manifest_path,
            "locked_holdout_manifest_sha256": locked_manifest_sha,
            "locked_partition_descriptor_sha256": locked_descriptor_sha,
            "config_sha256": config.config_sha256,
            "source_tree_sha256": source_sha,
            "lockbox_id": "LOCKBOX_TEST_ONLY",
            "preholdout_data_sha256": preholdout_sha,
            "holdout_commitment_sha256": holdout_sha,
            "parent_manifest_sha256": parent_sha,
            "trial_count": 16,
            "fold_count": 9,
            "fold_protocol": {
                "scoring_start_ms": 0,
                "scoring_end_ms_exclusive": holdout_start,
                "fold_months": 6,
                "strategy_state": "CONTINUOUS_ACROSS_ALL_FOLDS",
                "account_state": "CONTINUOUS_CASH_RESET_AT_FOLD1_ONLY",
                "indicator_initialization": "PRE_FOLD1_HISTORY_ONLY",
                "terminal_valuation": (
                    "COSTED_HYPOTHETICAL_LIQUIDATION_PER_FOLD_METRIC_WITHOUT_ACCOUNT_RESET"
                ),
            },
            "selected_params": selected_value,
            "selection_objective": "MEDIAN_WALK_FORWARD_CALMAR",
            "selection_bias_diagnostic": None,
            "parameter_adjacency_edges": [],
            "selected_parameter_neighbors": neighbors,
            "preholdout_neighbor_count": len(neighbors),
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
        selection_sha = hashlib.sha256(canonical_json(selection)).hexdigest()
        selection_path = root / "artifacts" / f"selection-{selection_sha[:16]}.json"
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
        replay_method = FULL_PROVENANCE_REPLAY_TEST_ID.rsplit(".", 1)[-1]
        test_output = (
            f"{replay_method} ({FULL_PROVENANCE_REPLAY_TEST_ID}) ... ok\n\n"
            "Ran 1 tests\n\nOK\n"
        ).encode()
        test_output_sha = hashlib.sha256(test_output).hexdigest()
        test_output_path = artifacts / f"test-log-{test_output_sha[:16]}.txt"
        test_output_path.parent.mkdir(parents=True, exist_ok=True)
        test_output_path.write_bytes(test_output)
        test_receipt: dict[str, object] = {
            "schema_version": "1.1.0",
            "type": "TEST_RECEIPT",
            "status": "PASS",
            "runner": "CPython 3.12",
            "normalized_output_path": test_output_path.relative_to(root).as_posix(),
            "normalized_output_sha256": test_output_sha,
            "test_count": 1,
            "return_code": 0,
            "environment_policy": "ALLOWLIST_NO_CREDENTIAL_ENV",
            "full_provenance_replay": full_provenance_replay_evidence(
                test_output.decode("utf-8")
            ),
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
        review_path.write_text(
            "# Sanitized test-only review\n\nPROCEED\n\n",
            encoding="utf-8",
        )
        review_sha = hashlib.sha256(review_path.read_bytes()).hexdigest()
        review_receipt: dict[str, object] = {
            "schema_version": "1.0.0",
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
        frozen_path = experiment_dir / "FROZEN.json"
        _write_hashed(
            frozen_path,
            {
                "state": "FROZEN",
                "experiment_id": experiment_id,
                "selection_path": selection_path.relative_to(root).as_posix(),
                "selection_sha256": selection["selection_sha256"],
                "config_sha256": config.config_sha256,
                "source_tree_sha256": source_sha,
                "preholdout_data_sha256": preholdout_sha,
                "holdout_commitment_sha256": holdout_sha,
            },
            "state_sha256",
        )

        metadata: dict[str, object] = {
            "manifest_path": locked_manifest_path,
            "manifest_file_sha256": locked_manifest_sha,
            "partition_descriptor_sha256": locked_descriptor_sha,
            "paired_partition_kind": "PREHOLDOUT",
            "paired_partition_descriptor_sha256": preholdout_descriptor_sha,
            "lockbox_id": "LOCKBOX_TEST_ONLY",
            "normalized_sha256": holdout_sha,
            "preholdout_sha256": preholdout_sha,
            "parent_manifest_sha256": parent_sha,
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
                trade_count=1,
            )
            for index in range(2)
        ]
        return _Harness(
            root=root,
            experiment_id=experiment_id,
            anchor_root=anchor_root,
            anchor_store_id=anchor_store_id,
            anchor_store=anchor_store,
            config=config,
            selected=selected_value,
            neighbors=neighbors,
            experiment_dir=experiment_dir,
            frozen_path=frozen_path,
            opened_path=experiment_dir / "HOLDOUT_OPENED.json",
            finalized_path=experiment_dir / "FINALIZED.json",
            selection_path=selection_path,
            test_receipt_path=test_receipt_path,
            review_receipt_path=review_receipt_path,
            holdout_manifest_path=root / locked_manifest_path,
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
        anchor_store_id: str | None = None,
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
                anchor_root=harness.anchor_root,
                anchor_store_id=(
                    harness.anchor_store_id
                    if anchor_store_id is None
                    else anchor_store_id
                ),
                root=harness.root,
            )
        return result, load_mock, metric_mock

    def test_open_receipt_precedes_load_and_all_performance_then_chains_report(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._build_harness(Path(temporary))
            calls: list[tuple[str, dict[str, object] | None]] = []

            def load_side_effect(*_args: object, **kwargs: object) -> object:
                self.assertTrue(harness.opened_path.exists())
                self.assertIs(kwargs.get("allow_locked_data"), True)
                anchor = read_holdout_opened_anchor(
                    harness.anchor_store, harness.experiment_id
                )
                opened = verified_hashed_object(harness.opened_path, "state_sha256")
                self.assertEqual(opened["opened_at_utc"], anchor["opened_at_utc"])
                self.assertEqual(
                    opened["external_anchor_sha256"],
                    anchor["anchor_sha256"],
                )
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
            self.assertEqual(report["schema_version"], "1.2.0")
            self.assertEqual(report["report_kind"], REPORT_KIND_LOCKED_OOS_EVALUATION)
            self.assertEqual(report["evidence_level"], "RETROSPECTIVE_LOCKED_OOS")
            self.assertEqual(report["benchmark_protocol"], BENCHMARK_PROTOCOL)
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
            self.assertEqual(finalized["experiment_id"], harness.experiment_id)
            self.assertEqual(finalized["status"], report["status"])
            opened = verified_hashed_object(harness.opened_path, "state_sha256")
            external = read_holdout_opened_anchor(
                harness.anchor_store, harness.experiment_id
            )
            self.assertEqual(opened["external_anchor_store_id"], harness.anchor_store_id)
            self.assertEqual(opened["external_anchor_sha256"], external["anchor_sha256"])
            self.assertEqual(opened["opened_at_utc"], external["opened_at_utc"])

    def test_external_anchor_is_durable_before_local_open_and_locked_load(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._build_harness(Path(temporary))
            ordering: list[str] = []

            def commit_first(*args: object, **kwargs: object) -> dict[str, object]:
                self.assertFalse(harness.opened_path.exists())
                ordering.append("external")
                return commit_holdout_opened_anchor(*args, **kwargs)

            def write_local(path: Path, value: dict[str, object]) -> dict[str, object]:
                if path == harness.opened_path:
                    external = read_holdout_opened_anchor(
                        harness.anchor_store, harness.experiment_id
                    )
                    self.assertEqual(external["anchor_store_id"], harness.anchor_store_id)
                    ordering.append("local")
                return _write_state_exclusive(path, value)

            def load_after_both(*_args: object, **_kwargs: object) -> object:
                opened = verified_hashed_object(harness.opened_path, "state_sha256")
                external = read_holdout_opened_anchor(
                    harness.anchor_store, harness.experiment_id
                )
                self.assertEqual(opened["external_anchor_sha256"], external["anchor_sha256"])
                ordering.append("load")
                return harness.holdout_bars, harness.metadata

            with (
                patch(
                    "ben_trade_lab.validation.commit_holdout_opened_anchor",
                    side_effect=commit_first,
                ),
                patch(
                    "ben_trade_lab.validation._write_state_exclusive",
                    side_effect=write_local,
                ),
            ):
                self._call(harness, load_side_effect=load_after_both)
            self.assertEqual(ordering[:3], ["external", "local", "load"])

    def test_path_entry_probe_recognizes_dangling_windows_reparse(self) -> None:
        candidate = Path("simulated-dangling-reparse.json")
        metadata = SimpleNamespace(st_mode=0, st_nlink=1, st_file_attributes=0x400)
        with (
            patch.object(Path, "lstat", return_value=metadata),
            patch.object(Path, "exists", return_value=False),
            patch.object(Path, "is_symlink", return_value=False),
        ):
            self.assertTrue(_path_entry_exists(candidate))

    def test_dangling_reparse_markers_fail_before_external_mutation(self) -> None:
        for marker_name in ("HOLDOUT_OPENED.json", "FINALIZED.json"):
            with self.subTest(marker=marker_name), tempfile.TemporaryDirectory() as temporary:
                harness = self._build_harness(Path(temporary))
                marker = harness.experiment_dir / marker_name
                locked_load = Mock(return_value=(harness.holdout_bars, harness.metadata))

                def simulated_entry(path: Path, expected_marker: Path = marker) -> bool:
                    candidate = Path(path)
                    if candidate == expected_marker:
                        return True
                    try:
                        candidate.lstat()
                    except FileNotFoundError:
                        return False
                    return True

                def simulated_reparse(
                    path: str | Path, expected_marker: Path = marker
                ) -> bool:
                    return Path(path) == expected_marker

                with (
                    patch(
                        "ben_trade_lab.validation._path_entry_exists",
                        side_effect=simulated_entry,
                    ),
                    patch(
                        "ben_trade_lab.validation.is_link_or_reparse",
                        side_effect=simulated_reparse,
                    ),
                    patch(
                        "ben_trade_lab.validation.commit_holdout_opened_anchor"
                    ) as external_commit,
                    self.assertRaises((ValueError, RuntimeError)),
                ):
                    self._call(harness, load_side_effect=locked_load)

                external_commit.assert_not_called()
                locked_load.assert_not_called()
                self.assertFalse(
                    harness.anchor_store.record_path(harness.experiment_id).exists()
                )

    def test_config_store_id_mismatch_fails_before_external_verification_or_load(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._build_harness(Path(temporary))
            load_mock = Mock(return_value=(harness.holdout_bars, harness.metadata))
            with (
                patch("ben_trade_lab.validation.verify_anchor_store") as verify_mock,
                self.assertRaisesRegex(ValueError, "does not match the frozen config"),
            ):
                self._call(
                    harness,
                    load_side_effect=load_mock,
                    anchor_store_id="f" * 64,
                )
            verify_mock.assert_not_called()
            load_mock.assert_not_called()
            self.assertFalse(harness.opened_path.exists())

    def test_failure_after_external_anchor_creation_is_permanently_nonretryable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._build_harness(Path(temporary))
            load_mock = Mock(return_value=(harness.holdout_bars, harness.metadata))

            def fail_local(path: Path, value: dict[str, object]) -> dict[str, object]:
                if path == harness.opened_path:
                    raise RuntimeError("simulated local HOLDOUT_OPENED failure")
                return _write_state_exclusive(path, value)

            with (
                patch(
                    "ben_trade_lab.validation._write_state_exclusive",
                    side_effect=fail_local,
                ),
                self.assertRaisesRegex(RuntimeError, "simulated local HOLDOUT_OPENED failure"),
            ):
                self._call(harness, load_side_effect=load_mock)

            external = read_holdout_opened_anchor(
                harness.anchor_store, harness.experiment_id
            )
            self.assertEqual(external["experiment_id"], harness.experiment_id)
            self.assertFalse(harness.opened_path.exists())
            load_mock.assert_not_called()

            retry_load = Mock(return_value=(harness.holdout_bars, harness.metadata))
            with self.assertRaisesRegex(
                RuntimeError,
                "EXTERNAL_HOLDOUT_OPENED_WITHOUT_LOCAL_STATE_NOT_RETRYABLE",
            ):
                self._call(harness, load_side_effect=retry_load)
            retry_load.assert_not_called()

    def test_partial_external_record_and_local_external_absence_mismatch_fail_closed(self) -> None:
        for case in ("partial_external", "local_without_external"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                harness = self._build_harness(Path(temporary))
                if case == "partial_external":
                    record = harness.anchor_store.record_path(harness.experiment_id)
                    record.parent.mkdir(parents=True)
                    record.write_bytes(b'{"partial":')
                else:
                    frozen = verified_hashed_object(harness.frozen_path, "state_sha256")
                    test_receipt = verified_hashed_object(
                        harness.test_receipt_path,
                        "receipt_sha256",
                    )
                    review_receipt = verified_hashed_object(
                        harness.review_receipt_path,
                        "receipt_sha256",
                    )
                    _write_state_exclusive(
                        harness.opened_path,
                        {
                            "state": "HOLDOUT_OPENED",
                            "experiment_id": harness.experiment_id,
                            "previous_state_sha256": frozen["state_sha256"],
                            "selection_sha256": test_receipt["selection_sha256"],
                            "holdout_manifest_sha256": "d" * 64,
                            "test_receipt_sha256": test_receipt["receipt_sha256"],
                            "review_receipt_sha256": review_receipt["receipt_sha256"],
                            "opened_at_utc": "2026-08-21T00:01:00.000000Z",
                            "external_anchor_store_id": harness.anchor_store_id,
                            "external_anchor_sha256": "f" * 64,
                        },
                    )

                load_mock = Mock(return_value=(harness.holdout_bars, harness.metadata))
                expected = ExternalAnchorCorruption if case == "partial_external" else RuntimeError
                with self.assertRaises(expected):
                    self._call(harness, load_side_effect=load_mock)
                load_mock.assert_not_called()

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
            "selection_experiment_path",
            "selection_no_eligible",
            "manifest_binding",
            "manifest_path",
            "manifest_file_sha",
            "manifest_descriptor",
            "manifest_paired_kind",
            "manifest_paired_descriptor",
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
                elif case == "selection_experiment_path":
                    value = json.loads(harness.selection_path.read_text(encoding="utf-8"))
                    value["experiment_id"] = "../../outside-state"
                    _write_hashed(harness.selection_path, value, "selection_sha256")
                elif case == "selection_no_eligible":
                    value = json.loads(harness.selection_path.read_text(encoding="utf-8"))
                    value["status"] = "NO_ELIGIBLE_CANDIDATE"
                    value["selected_params"] = None
                    _write_hashed(harness.selection_path, value, "selection_sha256")
                elif case == "manifest_binding":
                    metadata["normalized_sha256"] = "f" * 64
                elif case == "manifest_path":
                    metadata["manifest_path"] = "data/manifests/other-locked.json"
                elif case == "manifest_file_sha":
                    metadata["manifest_file_sha256"] = "e" * 64
                elif case == "manifest_descriptor":
                    metadata["partition_descriptor_sha256"] = "e" * 64
                elif case == "manifest_paired_kind":
                    metadata["paired_partition_kind"] = "LOCKED_HOLDOUT"
                elif case == "manifest_paired_descriptor":
                    metadata["paired_partition_descriptor_sha256"] = "e" * 64
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

    def test_skipped_full_provenance_replay_cannot_open_or_load_holdout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._build_harness(Path(temporary))
            method = FULL_PROVENANCE_REPLAY_TEST_ID.rsplit(".", 1)[-1]
            skipped_output = (
                f"{method} ({FULL_PROVENANCE_REPLAY_TEST_ID}) ... "
                "skipped 'complete local snapshot absent'\n\n"
                "Ran 1 tests\n\nOK (skipped=1)\n"
            ).encode()
            output_sha = hashlib.sha256(skipped_output).hexdigest()
            output_path = harness.root / "artifacts" / f"test-log-{output_sha[:16]}.txt"
            output_path.write_bytes(skipped_output)
            receipt = json.loads(harness.test_receipt_path.read_text(encoding="utf-8"))
            receipt["normalized_output_path"] = output_path.relative_to(harness.root).as_posix()
            receipt["normalized_output_sha256"] = output_sha
            receipt["full_provenance_replay"] = full_provenance_replay_evidence(
                skipped_output.decode("utf-8")
            )
            unsigned = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
            receipt_sha = hashlib.sha256(canonical_json(unsigned)).hexdigest()
            receipt_path = (
                harness.root / "artifacts" / f"test-receipt-{receipt_sha[:16]}.json"
            )
            _write_hashed(receipt_path, receipt, "receipt_sha256")
            harness.test_receipt_path = receipt_path

            load_mock = Mock(return_value=(harness.holdout_bars, harness.metadata))
            with self.assertRaisesRegex(ValueError, "full provenance replay is not PASS"):
                self._call(harness, load_side_effect=load_mock)
            self.assertFalse(harness.opened_path.exists())
            load_mock.assert_not_called()

    def test_receipts_require_exact_schema_and_current_repository_root(self) -> None:
        cases = (
            "test_missing_runner",
            "test_extra_field",
            "test_noncanonical",
            "review_missing_schema",
            "review_extra_field",
            "review_noncanonical",
            "external_test_receipt",
            "external_review_receipt",
        )
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                temporary_root = Path(temporary)
                repository = (
                    temporary_root / "repo" if case.startswith("external_") else temporary_root
                )
                harness = self._build_harness(repository)
                is_test = "test" in case
                receipt_path = (
                    harness.test_receipt_path if is_test else harness.review_receipt_path
                )
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))

                if case.endswith("noncanonical"):
                    receipt_path.write_text(
                        json.dumps(receipt, indent=2) + "\n",
                        encoding="utf-8",
                    )
                elif case.startswith("external_"):
                    outside = temporary_root / "outside"
                    evidence_field = (
                        "normalized_output_path" if is_test else "sanitized_review_path"
                    )
                    evidence_relative = Path(str(receipt[evidence_field]))
                    source_evidence = harness.root / evidence_relative
                    outside_evidence = outside / evidence_relative
                    outside_evidence.parent.mkdir(parents=True, exist_ok=True)
                    outside_evidence.write_bytes(source_evidence.read_bytes())
                    outside_receipt = outside / "artifacts" / receipt_path.name
                    outside_receipt.parent.mkdir(parents=True, exist_ok=True)
                    outside_receipt.write_bytes(receipt_path.read_bytes())
                    if is_test:
                        harness.test_receipt_path = outside_receipt
                    else:
                        harness.review_receipt_path = outside_receipt
                else:
                    receipt.pop("receipt_sha256")
                    if case == "test_missing_runner":
                        receipt.pop("runner")
                    elif case == "review_missing_schema":
                        receipt.pop("schema_version")
                    else:
                        receipt["unexpected"] = "not allowed"
                    digest = hashlib.sha256(canonical_json(receipt)).hexdigest()
                    prefix = "test-receipt" if is_test else "pro-review-receipt"
                    rewritten = harness.root / "artifacts" / f"{prefix}-{digest[:16]}.json"
                    _write_hashed(rewritten, receipt, "receipt_sha256")
                    if is_test:
                        harness.test_receipt_path = rewritten
                    else:
                        harness.review_receipt_path = rewritten

                load_mock = Mock(return_value=(harness.holdout_bars, harness.metadata))
                with self.assertRaises((ValueError, RuntimeError)):
                    self._call(harness, load_side_effect=load_mock)
                self.assertFalse(harness.opened_path.exists())
                load_mock.assert_not_called()

    def test_frozen_is_canonical_exact_schema_and_fully_bound_before_load(self) -> None:
        cases = (
            "empty",
            "noncanonical",
            "extra_field",
            "experiment_id",
            "selection_path",
            "selection_sha256",
            "config_sha256",
            "source_tree_sha256",
            "preholdout_data_sha256",
            "holdout_commitment_sha256",
        )
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                harness = self._build_harness(Path(temporary))
                frozen = json.loads(harness.frozen_path.read_text(encoding="utf-8"))
                if case == "empty":
                    harness.frozen_path.write_bytes(b"{}\n")
                elif case == "noncanonical":
                    harness.frozen_path.write_text(
                        json.dumps(frozen, indent=2) + "\n", encoding="utf-8"
                    )
                else:
                    if case == "extra_field":
                        frozen["unexpected"] = "not-allowed"
                    elif case == "experiment_id":
                        frozen[case] = "f" * 64
                    elif case == "selection_path":
                        frozen[case] = "inputs/rolled-back-selection.json"
                    else:
                        frozen[case] = "f" * 64
                    _write_hashed(harness.frozen_path, frozen, "state_sha256")

                load_mock = Mock(return_value=(harness.holdout_bars, harness.metadata))
                with self.assertRaises((ValueError, RuntimeError)):
                    self._call(harness, load_side_effect=load_mock)
                load_mock.assert_not_called()
                self.assertFalse(harness.opened_path.exists())

    def test_state_receipts_form_a_verified_hash_chain(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._build_harness(Path(temporary))
            report_path, _, _ = self._call(harness)

            frozen = verified_hashed_object(harness.frozen_path, "state_sha256")
            opened = verified_hashed_object(harness.opened_path, "state_sha256")
            finalized = verified_hashed_object(harness.finalized_path, "state_sha256")
            report = verified_hashed_object(report_path, "report_sha256")

            self.assertEqual(opened["previous_state_sha256"], frozen["state_sha256"])
            self.assertEqual(opened["selection_sha256"], frozen["selection_sha256"])
            self.assertEqual(opened["holdout_manifest_sha256"], "d" * 64)
            test_receipt = verified_hashed_object(harness.test_receipt_path, "receipt_sha256")
            review_receipt = verified_hashed_object(harness.review_receipt_path, "receipt_sha256")
            self.assertEqual(opened["test_receipt_sha256"], test_receipt["receipt_sha256"])
            self.assertEqual(opened["review_receipt_sha256"], review_receipt["receipt_sha256"])
            self.assertEqual(finalized["previous_state_sha256"], opened["state_sha256"])
            self.assertEqual(finalized["report_sha256"], report["report_sha256"])

    def test_opened_and_finalized_tampering_fail_before_reload(self) -> None:
        for state_name, rehash in (
            ("HOLDOUT_OPENED", False),
            ("HOLDOUT_OPENED", True),
            ("FINALIZED", False),
            ("FINALIZED", True),
        ):
            with (
                self.subTest(state=state_name, rehash=rehash),
                tempfile.TemporaryDirectory() as temporary,
            ):
                harness = self._build_harness(Path(temporary))
                if state_name == "HOLDOUT_OPENED":
                    with self.assertRaisesRegex(RuntimeError, "simulated load interruption"):
                        self._call(
                            harness,
                            load_side_effect=RuntimeError("simulated load interruption"),
                        )
                    state_path = harness.opened_path
                else:
                    self._call(harness)
                    state_path = harness.finalized_path
                state = json.loads(state_path.read_text(encoding="utf-8"))
                state["previous_state_sha256"] = "f" * 64
                if rehash:
                    _write_hashed(state_path, state, "state_sha256")
                else:
                    state_path.write_bytes(canonical_json(state) + b"\n")

                load_mock = Mock(return_value=(harness.holdout_bars, harness.metadata))
                with self.assertRaises((ValueError, RuntimeError)):
                    self._call(harness, load_side_effect=load_mock)
                load_mock.assert_not_called()

    def test_parent_directory_sync_failure_consumes_marker_before_load(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._build_harness(Path(temporary))
            load_mock = Mock(return_value=(harness.holdout_bars, harness.metadata))
            with (
                patch(
                    "ben_trade_lab.validation._fsync_parent_directory",
                    side_effect=RuntimeError("simulated directory fsync failure"),
                ),
                self.assertRaisesRegex(RuntimeError, "simulated directory fsync failure"),
            ):
                self._call(harness, load_side_effect=load_mock)
            self.assertTrue(harness.opened_path.exists())
            load_mock.assert_not_called()

    def test_report_written_before_finalized_failure_remains_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._build_harness(Path(temporary))

            def fail_finalized(path: Path, value: dict[str, object]) -> dict[str, object]:
                if path.name == "FINALIZED.json":
                    raise RuntimeError("simulated FINALIZED durability failure")
                return _write_state_exclusive(path, value)

            with (
                patch(
                    "ben_trade_lab.validation._write_state_exclusive",
                    side_effect=fail_finalized,
                ),
                self.assertRaisesRegex(RuntimeError, "FINALIZED durability failure"),
            ):
                self._call(harness)

            reports = list((harness.root / "artifacts").glob("holdout-*.json"))
            self.assertEqual(len(reports), 1)
            self.assertTrue(harness.opened_path.exists())
            self.assertFalse(harness.finalized_path.exists())

            retry_load = Mock(return_value=(harness.holdout_bars, harness.metadata))
            with self.assertRaisesRegex(RuntimeError, "HOLDOUT_ALREADY_OPENED_NOT_RETRYABLE"):
                self._call(harness, load_side_effect=retry_load)
            retry_load.assert_not_called()

    def test_terminal_liquidation_failure_finalizes_not_proven_without_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._build_harness(Path(temporary))
            report_path, load_mock, _ = self._call(
                harness,
                metric_side_effect=TerminalLiquidationNotExecutable("terminal ineligible"),
            )
            self.assertEqual(load_mock.call_count, 1)
            report = _verified_holdout_report_artifact(report_path, root=harness.root)
            self.assertEqual(report["status"], "NOT_PROVEN")
            self.assertEqual(
                report["report_kind"],
                REPORT_KIND_TERMINAL_LIQUIDATION_FAILURE,
            )
            self.assertEqual(
                report["failure_reason"],
                TerminalLiquidationNotExecutable.code,
            )
            finalized = verified_hashed_object(harness.finalized_path, "state_sha256")
            opened = verified_hashed_object(harness.opened_path, "state_sha256")
            self.assertEqual(finalized["previous_state_sha256"], opened["state_sha256"])
            self.assertEqual(finalized["report_sha256"], report["report_sha256"])

            retry_load = Mock(return_value=(harness.holdout_bars, harness.metadata))
            with self.assertRaisesRegex(RuntimeError, "HOLDOUT_ALREADY_OPENED_NOT_RETRYABLE"):
                self._call(harness, load_side_effect=retry_load)
            retry_load.assert_not_called()

    def test_gate_failed_evaluation_keeps_full_schema_and_retry_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._build_harness(Path(temporary))

            def gate_failed_metric(*_args: object, **_kwargs: object) -> dict[str, object]:
                value = _metric()
                value["annualized_sharpe_daily"] = 0.79
                return value

            report_path, load_mock, _ = self._call(
                harness,
                metric_side_effect=gate_failed_metric,
            )
            self.assertEqual(load_mock.call_count, 1)
            report = _verified_holdout_report_artifact(report_path, root=harness.root)
            self.assertEqual(report["status"], "NOT_PROVEN")
            self.assertEqual(report["report_kind"], REPORT_KIND_LOCKED_OOS_EVALUATION)
            self.assertFalse(report["gates"]["holdout_sharpe"])
            self.assertIn("holdout", report)
            self.assertIn("metrics", report)
            self.assertIn("benchmark_buy_and_hold", report)
            self.assertIn("cost_stress", report)
            self.assertIn("latency_stress", report)
            self.assertNotIn("failure_reason", report)

            finalized = verified_hashed_object(harness.finalized_path, "state_sha256")
            self.assertEqual(finalized["status"], "NOT_PROVEN")
            self.assertEqual(finalized["report_sha256"], report["report_sha256"])

            retry_load = Mock(return_value=(harness.holdout_bars, harness.metadata))
            with self.assertRaisesRegex(RuntimeError, "HOLDOUT_ALREADY_OPENED_NOT_RETRYABLE"):
                self._call(harness, load_side_effect=retry_load)
            retry_load.assert_not_called()

    def test_exact_window_cagr_includes_the_last_bars_full_hour(self) -> None:
        bars = [
            Bar(
                index * HOUR_MS,
                100.0,
                101.0,
                99.0,
                100.0,
                10.0,
                (index + 1) * HOUR_MS - 1,
                trade_count=1,
            )
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
