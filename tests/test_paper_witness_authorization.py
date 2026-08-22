from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from test_paper_authorization import (
    ANCHOR_STORE_ID,
    _complete_provenance,
    _rewrite_report_chain,
    _witness_path,
    initialize_paper,
)

from ben_trade_lab.config import canonical_json


class PaperWitnessAuthorizationTests(unittest.TestCase):
    def test_rehashed_report_and_rechained_local_finalized_cannot_replace_commitment(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            sandbox = Path(temporary)
            root = sandbox / "repository"
            anchor_root = sandbox / "external-anchor"
            config, report_path = _complete_provenance(root, anchor_root=anchor_root)

            def change_semantically_valid_sortino(report: dict[str, object]) -> None:
                metrics = report["metrics"]
                assert isinstance(metrics, dict)
                metrics["annualized_sortino_daily"] = (
                    float(metrics["annualized_sortino_daily"]) + 0.01
                )

            rewritten = _rewrite_report_chain(
                root,
                report_path,
                change_semantically_valid_sortino,
            )

            with self.assertRaisesRegex(
                ValueError,
                "paper witness finalization report_sha256 mismatch",
            ):
                initialize_paper(
                    root,
                    rewritten,
                    config,
                    1_000.0,
                    anchor_root=anchor_root,
                    anchor_store_id=ANCHOR_STORE_ID,
                )
            self.assertFalse((root / "state" / "paper" / "journal.jsonl").exists())

    def test_missing_truncated_or_tampered_witness_fails_before_journal_mutation(
        self,
    ) -> None:
        expected_errors = {
            "missing": "WITNESS_OR_REPOSITORY_MISSING",
            "truncated": "WITNESS_LEDGER_TRUNCATED",
            "tampered": "WITNESS_FINALIZATION_RECORD_SHA256_MISMATCH",
        }
        for case, expected_error in expected_errors.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                sandbox = Path(temporary)
                root = sandbox / "repository"
                anchor_root = sandbox / "external-anchor"
                config, report_path = _complete_provenance(root, anchor_root=anchor_root)
                witness_path = _witness_path(root)

                if case == "missing":
                    witness_path.unlink()
                elif case == "truncated":
                    witness_path.write_bytes(witness_path.read_bytes()[:-1])
                else:
                    records = [
                        json.loads(line)
                        for line in witness_path.read_text(encoding="utf-8").splitlines()
                    ]
                    records[-1]["report_status"] = "NOT_PROVEN"
                    witness_path.write_bytes(
                        b"".join(canonical_json(record) + b"\n" for record in records)
                    )

                with self.assertRaisesRegex(RuntimeError, expected_error):
                    initialize_paper(
                        root,
                        report_path,
                        config,
                        1_000.0,
                        anchor_root=anchor_root,
                        anchor_store_id=ANCHOR_STORE_ID,
                    )
                self.assertFalse(
                    (root / "state" / "paper" / "journal.jsonl").exists()
                )

    def test_supplied_witness_store_identity_mismatch_fails_before_journal_mutation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            sandbox = Path(temporary)
            root = sandbox / "repository"
            anchor_root = sandbox / "external-anchor"
            config, report_path = _complete_provenance(root, anchor_root=anchor_root)

            with self.assertRaisesRegex(
                ValueError,
                "append-only witness store id does not match the frozen config",
            ):
                initialize_paper(
                    root,
                    report_path,
                    config,
                    1_000.0,
                    anchor_root=anchor_root,
                    anchor_store_id=ANCHOR_STORE_ID,
                    witness_store_id="f" * 64,
                )
            self.assertFalse((root / "state" / "paper" / "journal.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
