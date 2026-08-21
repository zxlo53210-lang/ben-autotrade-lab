from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from ben_trade_lab.config import canonical_json, load_config
from ben_trade_lab.integrity import source_tree_sha256, write_immutable
from ben_trade_lab.paper import (
    REQUIRED_HOLDOUT_GATES,
    _head_path,
    _single_writer,
    initialize_paper,
    stop_paper,
    verify_journal,
)

ROOT = Path(__file__).resolve().parents[1]


def _self_hashed(value: dict[str, object], field: str) -> dict[str, object]:
    result = dict(value)
    result[field] = hashlib.sha256(canonical_json(result)).hexdigest()
    return result


def _write_json(path: Path, value: dict[str, object]) -> None:
    write_immutable(path, canonical_json(value) + b"\n")


def _complete_provenance(root: Path):
    config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
    artifacts = root / "artifacts"
    review_path = root / "docs" / "reviews" / "pro.md"
    review_path.parent.mkdir(parents=True)
    review_payload = b"Sanitized independent review: PROCEED.\n"
    review_path.write_bytes(review_payload)

    experiment_id = "1" * 64
    preholdout_sha = "2" * 64
    holdout_sha = "3" * 64
    holdout_manifest_sha = "4" * 64
    source_sha = source_tree_sha256(root)
    selected_params = {
        "entry_lookback": 72,
        "exit_lookback": 24,
        "trend_lookback": 336,
        "volatility_lookback": 720,
        "target_annualized_volatility": 0.3,
        "volatility_floor": 0.1,
    }
    selection = _self_hashed(
        {
            "schema_version": "1.1.0",
            "status": "FROZEN_CANDIDATE",
            "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
            "experiment_id": experiment_id,
            "config_sha256": config.config_sha256,
            "source_tree_sha256": source_sha,
            "preholdout_data_sha256": preholdout_sha,
            "holdout_commitment_sha256": holdout_sha,
            "selected_params": selected_params,
        },
        "selection_sha256",
    )
    selection_path = artifacts / f"selection-{selection['selection_sha256'][:16]}.json"
    _write_json(selection_path, selection)

    binding = {
        "experiment_id": experiment_id,
        "selection_sha256": selection["selection_sha256"],
        "config_sha256": config.config_sha256,
        "source_tree_sha256": source_sha,
        "preholdout_data_sha256": preholdout_sha,
        "holdout_commitment_sha256": holdout_sha,
    }
    test_log = b"test_example (fixture) ... ok\n\nRan 1 tests\n\nOK\n"
    test_log_sha = hashlib.sha256(test_log).hexdigest()
    test_log_path = artifacts / f"test-log-{test_log_sha[:16]}.txt"
    write_immutable(test_log_path, test_log)
    test_receipt = _self_hashed(
        {
            "schema_version": "1.0.0",
            "type": "TEST_RECEIPT",
            "status": "PASS",
            **binding,
            "runner": "CPython 3.12",
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
            "return_code": 0,
            "test_count": 1,
            "environment_policy": "ALLOWLIST_NO_CREDENTIAL_ENV",
            "source_tree_sha256_before": source_sha,
            "source_tree_sha256_after": source_sha,
            "normalized_output_sha256": test_log_sha,
            "normalized_output_path": test_log_path.relative_to(root).as_posix(),
        },
        "receipt_sha256",
    )
    test_path = artifacts / f"test-receipt-{test_receipt['receipt_sha256'][:16]}.json"
    _write_json(test_path, test_receipt)

    review_receipt = _self_hashed(
        {
            "schema_version": "1.0.0",
            "type": "PRO_REVIEW_RECEIPT",
            "verdict": "PROCEED",
            **binding,
            "reviewer": "ChatGPT Pro",
            "visible_model_label": "GPT-5.6 Sol",
            "visible_reasoning_label": "Pro",
            "sanitized_review_path": review_path.relative_to(root).as_posix(),
            "sanitized_review_sha256": hashlib.sha256(review_payload).hexdigest(),
        },
        "receipt_sha256",
    )
    review_receipt_path = (
        artifacts / f"pro-review-receipt-{review_receipt['receipt_sha256'][:16]}.json"
    )
    _write_json(review_receipt_path, review_receipt)

    report = _self_hashed(
        {
            "schema_version": "1.1.0",
            "status": "BACKTEST_CANDIDATE",
            "capability": "LIVE_DISABLED",
            "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
            "profit_claim": "NONE",
            "evaluation_label": "RETROSPECTIVE_LOCKED_OOS",
            "evidence_level": "RETROSPECTIVE_LOCKED_OOS",
            "experiment_id": experiment_id,
            "selection_sha256": selection["selection_sha256"],
            "holdout_manifest_sha256": holdout_manifest_sha,
            "holdout_data_sha256": holdout_sha,
            "config_sha256": config.config_sha256,
            "source_tree_sha256": source_sha,
            "test_receipt_sha256": test_receipt["receipt_sha256"],
            "review_receipt_sha256": review_receipt["receipt_sha256"],
            "selected_params": selected_params,
            "gates": {gate: True for gate in REQUIRED_HOLDOUT_GATES},
        },
        "report_sha256",
    )
    report_path = artifacts / f"holdout-{report['report_sha256'][:16]}.json"
    _write_json(report_path, report)

    experiment = root / "state" / "experiments" / experiment_id
    _write_json(
        experiment / "FROZEN.json",
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
    )
    _write_json(
        experiment / "HOLDOUT_OPENED.json",
        {
            "state": "HOLDOUT_OPENED",
            "experiment_id": experiment_id,
            "selection_sha256": selection["selection_sha256"],
            "holdout_manifest_sha256": holdout_manifest_sha,
            "test_receipt_sha256": test_receipt["receipt_sha256"],
            "review_receipt_sha256": review_receipt["receipt_sha256"],
            "opened_at_utc": "2026-08-21T00:00:00Z",
        },
    )
    _write_json(
        experiment / "FINALIZED.json",
        {
            "state": "FINALIZED",
            "experiment_id": experiment_id,
            "report_path": report_path.relative_to(root).as_posix(),
            "report_sha256": report["report_sha256"],
            "status": "BACKTEST_CANDIDATE",
        },
    )
    return config, report_path


class PaperAuthorizationTests(unittest.TestCase):
    def test_nonfinite_capital_fails_before_report_processing(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for capital in (float("nan"), float("inf"), float("-inf")):
                with self.subTest(capital=capital), self.assertRaises(ValueError):
                    initialize_paper(root, root / "missing.json", config, capital)

    def test_minimal_self_hashed_candidate_is_rejected(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = _self_hashed(
                {
                    "status": "BACKTEST_CANDIDATE",
                    "capability": "LIVE_DISABLED",
                    "config_sha256": config.config_sha256,
                    "source_tree_sha256": source_tree_sha256(root),
                    "selection_sha256": "a" * 64,
                },
                "report_sha256",
            )
            path = root / "artifacts" / f"holdout-{report['report_sha256'][:16]}.json"
            _write_json(path, report)
            with self.assertRaises(ValueError):
                initialize_paper(root, path, config, 1_000.0)

    def test_complete_mutually_bound_finalize_chain_can_initialize(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, report = _complete_provenance(root)
            initialized = initialize_paper(root, report, config, 1_000.0)
            self.assertEqual(initialized["status"], "PAPER_INITIALIZED")
            journal = root / "state" / "paper" / "journal.jsonl"
            events = verify_journal(journal)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["payload"]["live_execution"], "UNAVAILABLE")

    def test_consistently_rolled_back_head_still_fails_against_event_commitments(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, report = _complete_provenance(root)
            initialize_paper(root, report, config, 1_000.0)
            stop_paper(root)
            journal = root / "state" / "paper" / "journal.jsonl"
            first_line = journal.read_bytes().splitlines(keepends=True)[0]
            first_event = json.loads(first_line)
            journal.write_bytes(first_line)
            _head_path(journal).write_bytes(
                canonical_json({"event_count": 1, "event_hash": first_event["event_hash"]}) + b"\n"
            )
            with self.assertRaises(RuntimeError):
                verify_journal(journal)

    def test_single_writer_lock_rejects_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            journal = Path(temporary) / "journal.jsonl"
            with (
                _single_writer(journal),
                self.assertRaises(FileExistsError),
                _single_writer(journal),
            ):
                self.fail("overlapping writer unexpectedly acquired the lock")


if __name__ == "__main__":
    unittest.main()
