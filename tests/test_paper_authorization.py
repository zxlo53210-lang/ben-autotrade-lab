from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ben_trade_lab.anchor import (
    commit_holdout_opened_anchor,
    initialize_anchor_store,
)
from ben_trade_lab.config import CANONICAL_ANCHOR_STORE_ID, canonical_json, load_config
from ben_trade_lab.integrity import (
    FULL_PROVENANCE_REPLAY_TEST_ID,
    full_provenance_replay_evidence,
    source_tree_sha256,
    write_immutable,
)
from ben_trade_lab.paper import (
    REQUIRED_HOLDOUT_GATES,
    _append_event,
    _commit_directory,
    _head_path,
    _single_writer,
    initialize_paper,
    stop_paper,
    verify_journal,
)
from ben_trade_lab.validation import (
    BENCHMARK_PROTOCOL,
    VALIDATION_METHOD,
    _experiment_id,
)

ROOT = Path(__file__).resolve().parents[1]
ANCHOR_STORE_ID = CANONICAL_ANCHOR_STORE_ID


def _self_hashed(value: dict[str, object], field: str) -> dict[str, object]:
    result = dict(value)
    result[field] = hashlib.sha256(canonical_json(result)).hexdigest()
    return result


def _write_json(path: Path, value: dict[str, object]) -> None:
    write_immutable(path, canonical_json(value) + b"\n")


def _complete_provenance(
    root: Path,
    *,
    anchor_root: Path,
    anchor_store_id: str = ANCHOR_STORE_ID,
    replay_outcome: str = "ok",
):
    root.mkdir(parents=True, exist_ok=True)
    config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
    artifacts = root / "artifacts"
    review_path = root / "docs" / "reviews" / "pro.md"
    review_path.parent.mkdir(parents=True)
    review_payload = b"Sanitized independent review.\n\nPROCEED\n\n"
    review_path.write_bytes(review_payload)

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
    experiment_id = _experiment_id(
        method_version=VALIDATION_METHOD,
        config_sha256=config.config_sha256,
        source_tree_sha256_value=source_sha,
        lockbox_id="LOCKBOX_TEST_ONLY",
        preholdout_data_sha256=preholdout_sha,
        holdout_commitment_sha256=holdout_sha,
        selected_params=selected_params,
    )
    selection = _self_hashed(
        {
            "schema_version": "1.2.0",
            "status": "FROZEN_CANDIDATE",
            "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
            "method": "CONTINUOUS_NINE_FOLD_VALIDATION_V2",
            "experiment_id": experiment_id,
            "preholdout_manifest_path": "data/manifests/preholdout-test.json",
            "preholdout_manifest_sha256": "5" * 64,
            "preholdout_partition_descriptor_sha256": "7" * 64,
            "locked_holdout_manifest_path": "data/manifests/locked-test.json",
            "locked_holdout_manifest_sha256": holdout_manifest_sha,
            "locked_partition_descriptor_sha256": "8" * 64,
            "config_sha256": config.config_sha256,
            "source_tree_sha256": source_sha,
            "preholdout_data_sha256": preholdout_sha,
            "holdout_commitment_sha256": holdout_sha,
            "parent_manifest_sha256": "6" * 64,
            "lockbox_id": "LOCKBOX_TEST_ONLY",
            "trial_count": 16,
            "fold_count": 9,
            "fold_protocol": {},
            "selected_params": selected_params,
            "selection_objective": "MEDIAN_WALK_FORWARD_CALMAR",
            "selection_bias_diagnostic": None,
            "parameter_adjacency_edges": [],
            "selected_parameter_neighbors": [],
            "preholdout_neighbor_count": 0,
            "preholdout_neighbor_positive_fraction": 0.0,
            "warmup_context_hours": 0,
            "warmup_context": [],
            "candidates": [],
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
    replay_method = FULL_PROVENANCE_REPLAY_TEST_ID.rsplit(".", 1)[-1]
    test_log = (
        f"{replay_method} ({FULL_PROVENANCE_REPLAY_TEST_ID}) ... {replay_outcome}\n\n"
        "Ran 1 tests\n\nOK\n"
    ).encode()
    test_log_sha = hashlib.sha256(test_log).hexdigest()
    test_log_path = artifacts / f"test-log-{test_log_sha[:16]}.txt"
    write_immutable(test_log_path, test_log)
    test_receipt = _self_hashed(
        {
            "schema_version": "1.1.0",
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
            "full_provenance_replay": full_provenance_replay_evidence(
                test_log.decode("utf-8")
            ),
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
            "schema_version": "1.2.0",
            "status": "BACKTEST_CANDIDATE",
            "capability": "LIVE_DISABLED",
            "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
            "profit_claim": "NONE",
            "evaluation_label": "RETROSPECTIVE_LOCKED_OOS",
            "evidence_level": "RETROSPECTIVE_LOCKED_OOS",
            "report_kind": "LOCKED_OOS_EVALUATION",
            "experiment_id": experiment_id,
            "selection_sha256": selection["selection_sha256"],
            "holdout_manifest_sha256": holdout_manifest_sha,
            "holdout_data_sha256": holdout_sha,
            "config_sha256": config.config_sha256,
            "source_tree_sha256": source_sha,
            "test_receipt_sha256": test_receipt["receipt_sha256"],
            "review_receipt_sha256": review_receipt["receipt_sha256"],
            "selected_params": selected_params,
            "holdout": {},
            "metrics": {},
            "benchmark_protocol": BENCHMARK_PROTOCOL,
            "benchmark_buy_and_hold": {},
            "cost_stress": {},
            "latency_stress": {},
            "preholdout_parameter_neighbor_fraction": 0.0,
            "holdout_parameter_neighbors": [],
            "holdout_parameter_neighbor_protocol": {},
            "holdout_parameter_neighbor_positive_fraction": 0.0,
            "holdout_gap_events": 0,
            "holdout_missing_hours": 0,
            "gates": {gate: True for gate in REQUIRED_HOLDOUT_GATES},
        },
        "report_sha256",
    )
    report_path = artifacts / f"holdout-{report['report_sha256'][:16]}.json"
    _write_json(report_path, report)

    experiment = root / "state" / "experiments" / experiment_id
    frozen = _self_hashed(
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
    _write_json(
        experiment / "FROZEN.json",
        frozen,
    )
    opened_base = {
        "state": "HOLDOUT_OPENED",
        "experiment_id": experiment_id,
        "previous_state_sha256": frozen["state_sha256"],
        "selection_sha256": selection["selection_sha256"],
        "holdout_manifest_sha256": holdout_manifest_sha,
        "test_receipt_sha256": test_receipt["receipt_sha256"],
        "review_receipt_sha256": review_receipt["receipt_sha256"],
        "opened_at_utc": "2026-08-21T00:00:00.000000Z",
    }
    anchor_store = initialize_anchor_store(
        anchor_root,
        repository_root=root,
        store_id=anchor_store_id,
        created_at_utc="2026-08-21T15:56:48.648419Z",
    )
    external_anchor = commit_holdout_opened_anchor(
        anchor_store,
        opened_state_base=opened_base,
        config_sha256=config.config_sha256,
        source_tree_sha256=source_sha,
        preholdout_data_sha256=preholdout_sha,
        holdout_commitment_sha256=holdout_sha,
    )
    opened = _self_hashed(
        {
            **opened_base,
            "external_anchor_store_id": anchor_store_id,
            "external_anchor_sha256": external_anchor["anchor_sha256"],
        },
        "state_sha256",
    )
    _write_json(
        experiment / "HOLDOUT_OPENED.json",
        opened,
    )
    finalized = _self_hashed(
        {
            "state": "FINALIZED",
            "experiment_id": experiment_id,
            "previous_state_sha256": opened["state_sha256"],
            "report_path": report_path.relative_to(root).as_posix(),
            "report_sha256": report["report_sha256"],
            "status": "BACKTEST_CANDIDATE",
        },
        "state_sha256",
    )
    _write_json(
        experiment / "FINALIZED.json",
        finalized,
    )
    return config, report_path


class PaperAuthorizationTests(unittest.TestCase):
    def test_nonfinite_capital_fails_before_report_processing(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for capital in (float("nan"), float("inf"), float("-inf")):
                with self.subTest(capital=capital), self.assertRaises(ValueError):
                    initialize_paper(
                        root,
                        root / "missing.json",
                        config,
                        capital,
                        anchor_root=root / "unused-anchor",
                        anchor_store_id=ANCHOR_STORE_ID,
                    )

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
                initialize_paper(
                    root,
                    path,
                    config,
                    1_000.0,
                    anchor_root=root / "unused-anchor",
                    anchor_store_id=ANCHOR_STORE_ID,
                )

    def test_complete_mutually_bound_finalize_chain_can_initialize(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            sandbox = Path(temporary)
            root = sandbox / "repository"
            anchor_root = sandbox / "external-anchor"
            config, report = _complete_provenance(root, anchor_root=anchor_root)
            initialized = initialize_paper(
                root,
                report,
                config,
                1_000.0,
                anchor_root=anchor_root,
                anchor_store_id=ANCHOR_STORE_ID,
            )
            self.assertEqual(initialized["status"], "PAPER_INITIALIZED")
            journal = root / "state" / "paper" / "journal.jsonl"
            events = verify_journal(journal)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["payload"]["live_execution"], "UNAVAILABLE")

    def test_paper_rejects_noncanonical_unhashed_or_rechained_state(self) -> None:
        cases = (
            "missing_frozen_hash",
            "rehashed_frozen_extra_field",
            "wrong_opened_previous_hash",
            "wrong_finalized_previous_hash",
            "noncanonical_finalized",
        )
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                sandbox = Path(temporary)
                root = sandbox / "repository"
                anchor_root = sandbox / "external-anchor"
                config, report = _complete_provenance(root, anchor_root=anchor_root)
                experiment_id = json.loads(report.read_text(encoding="utf-8"))[
                    "experiment_id"
                ]
                experiment = root / "state" / "experiments" / experiment_id
                state_name = (
                    "FROZEN.json"
                    if "frozen" in case
                    else "HOLDOUT_OPENED.json"
                    if "opened" in case
                    else "FINALIZED.json"
                )
                state_path = experiment / state_name
                state = json.loads(state_path.read_text(encoding="utf-8"))
                if case == "missing_frozen_hash":
                    state.pop("state_sha256")
                    state_path.write_bytes(canonical_json(state) + b"\n")
                elif case == "noncanonical_finalized":
                    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
                else:
                    state.pop("state_sha256")
                    if case == "rehashed_frozen_extra_field":
                        state["unexpected"] = "not allowed"
                    else:
                        state["previous_state_sha256"] = "f" * 64
                    state_path.write_bytes(
                        canonical_json(_self_hashed(state, "state_sha256")) + b"\n"
                    )

                with self.assertRaises((ValueError, RuntimeError, TypeError)):
                    initialize_paper(
                        root,
                        report,
                        config,
                        1_000.0,
                        anchor_root=anchor_root,
                        anchor_store_id=ANCHOR_STORE_ID,
                    )
                self.assertFalse((root / "state" / "paper" / "journal.jsonl").exists())

    def test_paper_rejects_nonpassing_full_provenance_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            sandbox = Path(temporary)
            root = sandbox / "repository"
            anchor_root = sandbox / "external-anchor"
            config, report = _complete_provenance(
                root,
                anchor_root=anchor_root,
                replay_outcome="skipped 'snapshot absent'",
            )
            with self.assertRaisesRegex(ValueError, "full provenance replay is not PASS"):
                initialize_paper(
                    root,
                    report,
                    config,
                    1_000.0,
                    anchor_root=anchor_root,
                    anchor_store_id=ANCHOR_STORE_ID,
                )
            self.assertFalse((root / "state" / "paper" / "journal.jsonl").exists())

    def test_paper_revalidates_external_anchor_and_local_binding(self) -> None:
        cases = (
            "tampered_external",
            "missing_external",
            "rehashed_local_anchor_reference",
            "wrong_supplied_store_id",
        )
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                sandbox = Path(temporary)
                root = sandbox / "repository"
                anchor_root = sandbox / "external-anchor"
                config, report = _complete_provenance(root, anchor_root=anchor_root)
                supplied_store_id = ANCHOR_STORE_ID
                record = next(anchor_root.rglob("000000-HOLDOUT_OPENED.json"))

                if case == "tampered_external":
                    external = json.loads(record.read_text(encoding="utf-8"))
                    external["config_sha256"] = "f" * 64
                    record.write_bytes(canonical_json(external) + b"\n")
                elif case == "missing_external":
                    record.unlink()
                elif case == "rehashed_local_anchor_reference":
                    opened_path = (
                        root
                        / "state"
                        / "experiments"
                        / json.loads(report.read_text(encoding="utf-8"))["experiment_id"]
                        / "HOLDOUT_OPENED.json"
                    )
                    opened = json.loads(opened_path.read_text(encoding="utf-8"))
                    opened.pop("state_sha256")
                    opened["external_anchor_sha256"] = "f" * 64
                    opened_path.write_bytes(
                        canonical_json(_self_hashed(opened, "state_sha256")) + b"\n"
                    )
                else:
                    supplied_store_id = "b" * 64

                with self.assertRaises((ValueError, RuntimeError, TypeError)):
                    initialize_paper(
                        root,
                        report,
                        config,
                        1_000.0,
                        anchor_root=anchor_root,
                        anchor_store_id=supplied_store_id,
                    )
                self.assertFalse((root / "state" / "paper" / "journal.jsonl").exists())

    def test_consistently_rolled_back_head_still_fails_against_event_commitments(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            sandbox = Path(temporary)
            root = sandbox / "repository"
            anchor_root = sandbox / "external-anchor"
            config, report = _complete_provenance(root, anchor_root=anchor_root)
            initialize_paper(
                root,
                report,
                config,
                1_000.0,
                anchor_root=anchor_root,
                anchor_store_id=ANCHOR_STORE_ID,
            )
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

    def test_journal_head_and_commitment_hardlinks_are_rejected(self) -> None:
        for target_kind in ("journal", "head", "commitment"):
            with self.subTest(target_kind=target_kind), tempfile.TemporaryDirectory() as temporary:
                base = Path(temporary)
                journal = base / "repository" / "state" / "paper" / "journal.jsonl"
                _append_event(journal, {"type": "PAPER_INITIALIZED"})
                if target_kind == "journal":
                    target = journal
                elif target_kind == "head":
                    target = _head_path(journal)
                else:
                    target = next(_commit_directory(journal).iterdir())
                outside = base / f"outside-{target_kind}"
                try:
                    os.link(target, outside)
                except OSError as exc:
                    self.skipTest(f"hardlinks unavailable: {exc}")
                self.assertGreater(target.stat().st_nlink, 1)
                with self.assertRaisesRegex(RuntimeError, "hard link"):
                    verify_journal(journal)

    def test_commitment_store_reparse_or_junction_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            journal = Path(temporary) / "state" / "paper" / "journal.jsonl"
            _append_event(journal, {"type": "PAPER_INITIALIZED"})
            commitment_store = _commit_directory(journal)

            def simulated_reparse(path: str | Path) -> bool:
                return Path(path) == commitment_store

            with (
                patch(
                    "ben_trade_lab.paper.is_link_or_reparse",
                    side_effect=simulated_reparse,
                ),
                self.assertRaisesRegex(RuntimeError, "plain directory"),
            ):
                verify_journal(journal)


if __name__ == "__main__":
    unittest.main()
