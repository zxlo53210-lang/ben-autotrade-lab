from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import unittest
from contextlib import ExitStack, nullcontext
from pathlib import Path
from unittest.mock import patch

from ben_trade_lab.config import canonical_json
from ben_trade_lab.witness import (
    BURN_FIELDS,
    FINALIZATION_FIELDS,
    FS_APPEND_FL,
    GENESIS_RECORD_SHA256,
    HEADER_FIELDS,
    WITNESS_BURN_EVENT,
    WITNESS_BURN_TYPE,
    WITNESS_FINALIZATION_EVENT,
    WITNESS_FINALIZATION_TYPE,
    WITNESS_LEDGER_TYPE,
    WITNESS_POLICY,
    WITNESS_SCHEMA_VERSION,
    WitnessAlreadyBurned,
    WitnessAlreadyFinalized,
    WitnessCorruption,
    WitnessPathError,
    WitnessPlatformError,
    assert_unburned,
    assert_unfinalized,
    burn_opening,
    commit_finalization,
    verify_witness_ledger,
)


class AppendOnlyWitnessLedgerTests(unittest.TestCase):
    STORE_ID = "a" * 64
    EXPERIMENT_ID = "e" * 64
    ANCHOR_STORE_ID = "b" * 64
    ANCHOR_STORE_SHA256 = "c" * 64
    OPENING_COMMITMENT_SHA256 = "d" * 64
    LOCKBOX_ID = "1" * 64
    LOCKED_HOLDOUT_MANIFEST_SHA256 = "2" * 64
    HOLDOUT_COMMITMENT_SHA256 = "3" * 64
    OPENED_STATE_SHA256 = "4" * 64
    EXTERNAL_ANCHOR_SHA256 = "5" * 64
    REPORT_SHA256 = "6" * 64
    CREATED_AT = "2026-08-22T01:00:00.000000Z"
    BURNED_AT = "2026-08-22T01:01:00.000000Z"
    FINALIZED_AT = "2026-08-22T01:02:00.000000Z"

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.repository = self.base / "repository"
        self.repository.mkdir()
        witness_root = self.base / "witness"
        witness_root.mkdir()
        self.ledger_path = witness_root / "holdout-opening-burn.jsonl"
        self.ledger_path.touch()
        metadata = self.ledger_path.stat()
        self.device = int(metadata.st_dev)
        self.inode = int(metadata.st_ino)
        self.header = self._self_hashed(
            {
                "schema_version": WITNESS_SCHEMA_VERSION,
                "type": WITNESS_LEDGER_TYPE,
                "store_id": self.STORE_ID,
                "created_at_utc": self.CREATED_AT,
                "policy": WITNESS_POLICY,
                "filesystem_device": self.device,
                "filesystem_inode": self.inode,
                "sequence": 0,
                "previous_record_sha256": GENESIS_RECORD_SHA256,
                "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
                "capability": "LIVE_DISABLED",
            }
        )
        self.header_sha256 = str(self.header["record_sha256"])
        self.ledger_path.write_bytes(canonical_json(self.header) + b"\n")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _self_hashed(unsigned: dict[str, object]) -> dict[str, object]:
        value = dict(unsigned)
        value["record_sha256"] = hashlib.sha256(canonical_json(value)).hexdigest()
        return value

    def _runtime(self, *, inode_flags: int = FS_APPEND_FL) -> ExitStack:
        stack = ExitStack()
        stack.enter_context(
            patch("ben_trade_lab.witness._require_supported_platform", return_value=None)
        )
        stack.enter_context(
            patch("ben_trade_lab.witness._read_inode_flags", return_value=inode_flags)
        )
        stack.enter_context(
            patch(
                "ben_trade_lab.witness._shared_lock",
                side_effect=lambda _descriptor: nullcontext(),
            )
        )
        stack.enter_context(
            patch(
                "ben_trade_lab.witness._exclusive_lock",
                side_effect=lambda _descriptor: nullcontext(),
            )
        )
        stack.enter_context(patch("ben_trade_lab.witness.fsync_directory"))
        return stack

    def _verify(self):
        return verify_witness_ledger(
            self.ledger_path,
            repository_root=self.repository,
            expected_store_id=self.STORE_ID,
            expected_header_sha256=self.header_sha256,
            expected_device=self.device,
            expected_inode=self.inode,
        )

    def _burn_value(
        self,
        *,
        previous: str,
        sequence: int,
        experiment_id: str | None = None,
        lockbox_id: str | None = None,
        locked_holdout_manifest_sha256: str | None = None,
        holdout_commitment_sha256: str | None = None,
    ) -> dict[str, object]:
        return self._self_hashed(
            {
                "schema_version": WITNESS_SCHEMA_VERSION,
                "type": WITNESS_BURN_TYPE,
                "event": WITNESS_BURN_EVENT,
                "store_id": self.STORE_ID,
                "sequence": sequence,
                "previous_record_sha256": previous,
                "experiment_id": self.EXPERIMENT_ID
                if experiment_id is None
                else experiment_id,
                "lockbox_id": self.LOCKBOX_ID
                if lockbox_id is None
                else lockbox_id,
                "locked_holdout_manifest_sha256": (
                    self.LOCKED_HOLDOUT_MANIFEST_SHA256
                    if locked_holdout_manifest_sha256 is None
                    else locked_holdout_manifest_sha256
                ),
                "holdout_commitment_sha256": (
                    self.HOLDOUT_COMMITMENT_SHA256
                    if holdout_commitment_sha256 is None
                    else holdout_commitment_sha256
                ),
                "burned_at_utc": self.BURNED_AT,
                "anchor_store_id": self.ANCHOR_STORE_ID,
                "anchor_store_sha256": self.ANCHOR_STORE_SHA256,
                "opening_commitment_sha256": self.OPENING_COMMITMENT_SHA256,
                "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
                "capability": "LIVE_DISABLED",
            }
        )

    def _burn(
        self,
        ledger,
        *,
        experiment_id: str | None = None,
        lockbox_id: str | None = None,
        locked_holdout_manifest_sha256: str | None = None,
        holdout_commitment_sha256: str | None = None,
    ):
        return burn_opening(
            ledger,
            experiment_id=self.EXPERIMENT_ID
            if experiment_id is None
            else experiment_id,
            lockbox_id=self.LOCKBOX_ID if lockbox_id is None else lockbox_id,
            locked_holdout_manifest_sha256=(
                self.LOCKED_HOLDOUT_MANIFEST_SHA256
                if locked_holdout_manifest_sha256 is None
                else locked_holdout_manifest_sha256
            ),
            holdout_commitment_sha256=(
                self.HOLDOUT_COMMITMENT_SHA256
                if holdout_commitment_sha256 is None
                else holdout_commitment_sha256
            ),
            anchor_store_id=self.ANCHOR_STORE_ID,
            anchor_store_sha256=self.ANCHOR_STORE_SHA256,
            opening_commitment_sha256=self.OPENING_COMMITMENT_SHA256,
            burned_at_utc=self.BURNED_AT,
        )

    def _finalization_value(
        self,
        *,
        previous: str,
        sequence: int,
        opening_burn_record_sha256: str,
        experiment_id: str | None = None,
        opened_state_sha256: str | None = None,
        external_anchor_sha256: str | None = None,
        report_sha256: str | None = None,
    ) -> dict[str, object]:
        return self._self_hashed(
            {
                "schema_version": WITNESS_SCHEMA_VERSION,
                "type": WITNESS_FINALIZATION_TYPE,
                "event": WITNESS_FINALIZATION_EVENT,
                "store_id": self.STORE_ID,
                "sequence": sequence,
                "previous_record_sha256": previous,
                "experiment_id": self.EXPERIMENT_ID
                if experiment_id is None
                else experiment_id,
                "finalized_at_utc": self.FINALIZED_AT,
                "opening_burn_record_sha256": opening_burn_record_sha256,
                "opened_state_sha256": self.OPENED_STATE_SHA256
                if opened_state_sha256 is None
                else opened_state_sha256,
                "external_anchor_sha256": self.EXTERNAL_ANCHOR_SHA256
                if external_anchor_sha256 is None
                else external_anchor_sha256,
                "report_sha256": self.REPORT_SHA256
                if report_sha256 is None
                else report_sha256,
                "report_status": "BACKTEST_CANDIDATE",
                "report_kind": "LOCKED_OOS_EVALUATION",
                "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
                "capability": "LIVE_DISABLED",
            }
        )

    def _finalize(
        self,
        ledger,
        *,
        opening_burn_record_sha256: str,
        experiment_id: str | None = None,
        report_sha256: str | None = None,
    ):
        return commit_finalization(
            ledger,
            experiment_id=self.EXPERIMENT_ID
            if experiment_id is None
            else experiment_id,
            opening_burn_record_sha256=opening_burn_record_sha256,
            opened_state_sha256=self.OPENED_STATE_SHA256,
            external_anchor_sha256=self.EXTERNAL_ANCHOR_SHA256,
            report_sha256=self.REPORT_SHA256
            if report_sha256 is None
            else report_sha256,
            report_status="BACKTEST_CANDIDATE",
            report_kind="LOCKED_OOS_EVALUATION",
            finalized_at_utc=self.FINALIZED_AT,
        )

    def test_runtime_fails_closed_without_linux_append_only_support(self) -> None:
        with (
            patch(
                "ben_trade_lab.witness._require_supported_platform",
                side_effect=WitnessPlatformError("WITNESS_REQUIRES_LINUX_FS_APPEND_FL"),
            ),
            self.assertRaisesRegex(WitnessPlatformError, "LINUX_FS_APPEND_FL"),
        ):
            self._verify()

    def test_verifies_exact_header_inode_and_append_only_flag(self) -> None:
        with self._runtime():
            ledger = self._verify()
        self.assertEqual(set(self.header), HEADER_FIELDS)
        self.assertEqual(ledger.store_id, self.STORE_ID)
        self.assertEqual(ledger.header_sha256, self.header_sha256)
        self.assertEqual(ledger.filesystem_device, self.device)
        self.assertEqual(ledger.filesystem_inode, self.inode)
        self.assertEqual(ledger.head_sha256, self.header_sha256)
        self.assertEqual(ledger.burns, ())
        self.assertEqual(ledger.finalizations, ())

        with self._runtime(inode_flags=0), self.assertRaisesRegex(
            WitnessPlatformError, "FS_APPEND_FL_REQUIRED"
        ):
            self._verify()

    def test_witness_must_be_outside_repository_and_single_link(self) -> None:
        inside = self.repository / "witness.jsonl"
        inside.write_bytes(self.ledger_path.read_bytes())
        inside_metadata = inside.stat()
        with self._runtime(), self.assertRaisesRegex(WitnessPathError, "OUTSIDE_REPOSITORY"):
            verify_witness_ledger(
                inside,
                repository_root=self.repository,
                expected_store_id=self.STORE_ID,
                expected_header_sha256=self.header_sha256,
                expected_device=int(inside_metadata.st_dev),
                expected_inode=int(inside_metadata.st_ino),
            )

        hardlink = self.base / "witness-hardlink.jsonl"
        try:
            os.link(self.ledger_path, hardlink)
        except OSError as exc:
            self.skipTest(f"hardlinks unavailable: {exc}")
        with self._runtime(), self.assertRaisesRegex(WitnessCorruption, "HARDLINK_REJECTED"):
            self._verify()

    def test_exact_preuse_copy_cannot_replace_the_pinned_inode(self) -> None:
        preuse = self.ledger_path.read_bytes()
        displaced = self.base / "displaced-ledger.jsonl"
        self.ledger_path.replace(displaced)
        self.ledger_path.write_bytes(preuse)
        self.assertNotEqual(int(self.ledger_path.stat().st_ino), self.inode)
        with self._runtime(), self.assertRaisesRegex(
            WitnessCorruption, "FILESYSTEM_INODE_MISMATCH"
        ):
            self._verify()

    def test_burn_is_canonical_chained_locked_o_append_and_not_retryable(self) -> None:
        real_open = os.open
        with (
            self._runtime() as runtime,
            patch("ben_trade_lab.witness.os.open", wraps=real_open) as open_mock,
            patch("ben_trade_lab.witness.os.fsync", wraps=os.fsync) as fsync_mock,
        ):
            exclusive_lock = runtime.enter_context(
                patch(
                    "ben_trade_lab.witness._exclusive_lock",
                    side_effect=lambda _descriptor: nullcontext(),
                )
            )
            ledger = self._verify()
            burn = self._burn(ledger)
            current = self._verify()
            with self.assertRaisesRegex(WitnessAlreadyBurned, "NOT_RETRYABLE"):
                assert_unburned(
                    current,
                    self.EXPERIMENT_ID,
                    lockbox_id=self.LOCKBOX_ID,
                    holdout_commitment_sha256=self.HOLDOUT_COMMITMENT_SHA256,
                )
            with self.assertRaisesRegex(WitnessAlreadyBurned, "NOT_RETRYABLE"):
                self._burn(current)

        lines = self.ledger_path.read_bytes().splitlines()
        self.assertEqual(len(lines), 2)
        value = json.loads(lines[1])
        self.assertEqual(set(value), BURN_FIELDS)
        self.assertEqual(lines[1], canonical_json(value))
        self.assertEqual(value["previous_record_sha256"], self.header_sha256)
        self.assertEqual(value["lockbox_id"], self.LOCKBOX_ID)
        self.assertEqual(
            value["locked_holdout_manifest_sha256"],
            self.LOCKED_HOLDOUT_MANIFEST_SHA256,
        )
        self.assertEqual(
            value["holdout_commitment_sha256"], self.HOLDOUT_COMMITMENT_SHA256
        )
        self.assertEqual(value["record_sha256"], burn.record_sha256)
        self.assertEqual(current.head_sha256, burn.record_sha256)
        self.assertGreaterEqual(exclusive_lock.call_count, 1)
        self.assertGreaterEqual(fsync_mock.call_count, 1)
        writable_flags = [
            call.args[1]
            for call in open_mock.call_args_list
            if len(call.args) >= 2 and call.args[1] & os.O_RDWR
        ]
        self.assertTrue(writable_flags)
        self.assertTrue(all(flags & os.O_APPEND for flags in writable_flags))

    def test_lockbox_and_holdout_commitment_are_global_one_shot_allocations(
        self,
    ) -> None:
        second_experiment = "f" * 64
        second_lockbox = "7" * 64
        second_holdout_commitment = "8" * 64
        cases = (
            (
                "lockbox",
                self.LOCKBOX_ID,
                second_holdout_commitment,
            ),
            (
                "holdout_commitment",
                second_lockbox,
                self.HOLDOUT_COMMITMENT_SHA256,
            ),
        )
        with self._runtime():
            ledger = self._verify()
            first = self._burn(ledger)
            current = self._verify()
            self.assertEqual(current.burn_for(self.EXPERIMENT_ID), first)

            for label, lockbox_id, holdout_commitment_sha256 in cases:
                with self.subTest(label=label):
                    with self.assertRaisesRegex(
                        WitnessAlreadyBurned, "NOT_RETRYABLE"
                    ):
                        assert_unburned(
                            current,
                            second_experiment,
                            lockbox_id=lockbox_id,
                            holdout_commitment_sha256=(
                                holdout_commitment_sha256
                            ),
                        )
                    with self.assertRaisesRegex(
                        WitnessAlreadyBurned, "NOT_RETRYABLE"
                    ):
                        self._burn(
                            current,
                            experiment_id=second_experiment,
                            lockbox_id=lockbox_id,
                            holdout_commitment_sha256=(
                                holdout_commitment_sha256
                            ),
                        )

            unchanged = self._verify()
            self.assertEqual(unchanged.burns, (first,))

    def test_parser_rejects_global_duplicate_lockbox_and_holdout_commitment(
        self,
    ) -> None:
        second_experiment = "f" * 64
        cases = (
            (
                "lockbox",
                self.LOCKBOX_ID,
                "8" * 64,
                "WITNESS_DUPLICATE_LOCKBOX_BURN",
            ),
            (
                "holdout_commitment",
                "7" * 64,
                self.HOLDOUT_COMMITMENT_SHA256,
                "WITNESS_DUPLICATE_HOLDOUT_COMMITMENT_BURN",
            ),
        )
        first = self._burn_value(previous=self.header_sha256, sequence=1)
        for label, lockbox_id, holdout_commitment_sha256, message in cases:
            with self.subTest(label=label):
                second = self._burn_value(
                    previous=str(first["record_sha256"]),
                    sequence=2,
                    experiment_id=second_experiment,
                    lockbox_id=lockbox_id,
                    holdout_commitment_sha256=holdout_commitment_sha256,
                )
                self.ledger_path.write_bytes(
                    b"\n".join(
                        (
                            canonical_json(self.header),
                            canonical_json(first),
                            canonical_json(second),
                        )
                    )
                    + b"\n"
                )
                with self._runtime(), self.assertRaisesRegex(
                    WitnessCorruption, message
                ):
                    self._verify()

    def test_finalization_is_canonical_chained_o_append_and_not_retryable(
        self,
    ) -> None:
        real_open = os.open
        with (
            self._runtime(),
            patch("ben_trade_lab.witness.os.open", wraps=real_open) as open_mock,
            patch("ben_trade_lab.witness.os.fsync", wraps=os.fsync) as fsync_mock,
        ):
            ledger = self._verify()
            opening = self._burn(ledger)
            opened = self._verify()
            assert_unfinalized(opened, self.EXPERIMENT_ID)
            finalization = self._finalize(
                opened,
                opening_burn_record_sha256=opening.record_sha256,
            )
            current = self._verify()
            with self.assertRaisesRegex(WitnessAlreadyFinalized, "NOT_RETRYABLE"):
                assert_unfinalized(current, self.EXPERIMENT_ID)
            with self.assertRaisesRegex(WitnessAlreadyFinalized, "NOT_RETRYABLE"):
                self._finalize(
                    current,
                    opening_burn_record_sha256=opening.record_sha256,
                )

        lines = self.ledger_path.read_bytes().splitlines()
        self.assertEqual(len(lines), 3)
        value = json.loads(lines[2])
        self.assertEqual(set(value), FINALIZATION_FIELDS)
        self.assertEqual(lines[2], canonical_json(value))
        self.assertEqual(value["sequence"], 2)
        self.assertEqual(value["previous_record_sha256"], opening.record_sha256)
        self.assertEqual(
            value["opening_burn_record_sha256"], opening.record_sha256
        )
        self.assertEqual(value["record_sha256"], finalization.record_sha256)
        self.assertEqual(current.burns, (opening,))
        self.assertEqual(current.finalizations, (finalization,))
        self.assertEqual(current.head_sha256, finalization.record_sha256)
        self.assertGreaterEqual(fsync_mock.call_count, 2)
        writable_flags = [
            call.args[1]
            for call in open_mock.call_args_list
            if len(call.args) >= 2 and call.args[1] & os.O_RDWR
        ]
        self.assertGreaterEqual(len(writable_flags), 2)
        self.assertTrue(all(flags & os.O_APPEND for flags in writable_flags))

    def test_finalization_tampering_fails_closed(self) -> None:
        with self._runtime():
            ledger = self._verify()
            opening = self._burn(ledger)
            opened = self._verify()
            self._finalize(
                opened,
                opening_burn_record_sha256=opening.record_sha256,
            )
            legitimate_lines = self.ledger_path.read_bytes().splitlines()

        cases = (
            ("report_hash", "WITNESS_FINALIZATION_RECORD_SHA256_MISMATCH"),
            ("opening_link", "WITNESS_FINALIZATION_OPENING_BURN_MISMATCH"),
            ("chain_link", "WITNESS_RECORD_PREVIOUS_HASH_MISMATCH"),
            ("duplicate", "WITNESS_DUPLICATE_EXPERIMENT_FINALIZATION"),
        )
        for case, message in cases:
            with self.subTest(case=case):
                header, burn_line, finalization_line = legitimate_lines
                finalization = json.loads(finalization_line)
                if case == "report_hash":
                    finalization["report_sha256"] = "7" * 64
                    payload_lines = (
                        header,
                        burn_line,
                        canonical_json(finalization),
                    )
                elif case == "opening_link":
                    finalization["opening_burn_record_sha256"] = "8" * 64
                    finalization = self._self_hashed(
                        {
                            key: value
                            for key, value in finalization.items()
                            if key != "record_sha256"
                        }
                    )
                    payload_lines = (
                        header,
                        burn_line,
                        canonical_json(finalization),
                    )
                elif case == "chain_link":
                    finalization["previous_record_sha256"] = "9" * 64
                    finalization = self._self_hashed(
                        {
                            key: value
                            for key, value in finalization.items()
                            if key != "record_sha256"
                        }
                    )
                    payload_lines = (
                        header,
                        burn_line,
                        canonical_json(finalization),
                    )
                else:
                    duplicate = self._finalization_value(
                        previous=str(finalization["record_sha256"]),
                        sequence=3,
                        opening_burn_record_sha256=opening.record_sha256,
                    )
                    payload_lines = (
                        header,
                        burn_line,
                        finalization_line,
                        canonical_json(duplicate),
                    )
                self.ledger_path.write_bytes(b"\n".join(payload_lines) + b"\n")
                with self._runtime(), self.assertRaisesRegex(
                    WitnessCorruption, message
                ):
                    self._verify()

    def test_parser_rejects_terminal_failure_with_candidate_status(self) -> None:
        burn = self._burn_value(previous=self.header_sha256, sequence=1)
        finalization = self._finalization_value(
            previous=str(burn["record_sha256"]),
            sequence=2,
            opening_burn_record_sha256=str(burn["record_sha256"]),
        )
        finalization.pop("record_sha256")
        finalization["report_kind"] = "TERMINAL_LIQUIDATION_FAILURE"
        finalization = self._self_hashed(finalization)
        self.ledger_path.write_bytes(
            canonical_json(self.header)
            + b"\n"
            + canonical_json(burn)
            + b"\n"
            + canonical_json(finalization)
            + b"\n"
        )

        with self._runtime(), self.assertRaisesRegex(
            WitnessCorruption,
            "REPORT_KIND_STATUS_MISMATCH",
        ):
            self._verify()

    def test_truncation_noncanonical_chain_and_duplicate_burn_fail_closed(self) -> None:
        cases = ("truncated", "noncanonical", "wrong_previous", "duplicate")
        for case in cases:
            with self.subTest(case=case):
                first = self._burn_value(
                    previous=self.header_sha256,
                    sequence=1,
                )
                if case == "truncated":
                    payload = canonical_json(self.header) + b"\n" + canonical_json(first)
                elif case == "noncanonical":
                    payload = canonical_json(self.header) + b"\n" + json.dumps(
                        first, indent=2
                    ).encode() + b"\n"
                elif case == "wrong_previous":
                    wrong = self._burn_value(previous="f" * 64, sequence=1)
                    payload = (
                        canonical_json(self.header)
                        + b"\n"
                        + canonical_json(wrong)
                        + b"\n"
                    )
                else:
                    second = self._burn_value(
                        previous=str(first["record_sha256"]),
                        sequence=2,
                    )
                    payload = b"\n".join(
                        (canonical_json(self.header), canonical_json(first), canonical_json(second))
                    ) + b"\n"
                self.ledger_path.write_bytes(payload)
                with self._runtime(), self.assertRaises(WitnessCorruption):
                    self._verify()

    def test_partial_append_permanently_corrupts_instead_of_retrying(self) -> None:
        with self._runtime():
            ledger = self._verify()
            real_write = os.write

            def partial_write(descriptor: int, payload: bytes) -> int:
                partial = payload[:17]
                real_write(descriptor, partial)
                return len(partial)

            with (
                patch("ben_trade_lab.witness.os.write", side_effect=partial_write),
                self.assertRaisesRegex(WitnessCorruption, "PARTIAL_WRITE_BURNED"),
            ):
                self._burn(ledger)
            with self.assertRaisesRegex(WitnessCorruption, "TRUNCATED"):
                self._verify()

    def test_copying_the_repo_and_primary_store_cannot_erase_witness_burn(self) -> None:
        primary = self.base / "primary-anchor"
        primary.mkdir()
        (primary / "ANCHOR_STORE.json").write_text("pre-use", encoding="utf-8")
        repository_snapshot = self.base / "repository-snapshot"
        primary_snapshot = self.base / "primary-snapshot"
        shutil.copytree(self.repository, repository_snapshot)
        shutil.copytree(primary, primary_snapshot)

        with self._runtime():
            ledger = self._verify()
            self._burn(ledger)
            # Simulate a byte-for-byte repository + primary-anchor rollback.
            # The independently pinned append-only inode is deliberately not a
            # child of either rollback target.
            shutil.rmtree(self.repository)
            shutil.copytree(repository_snapshot, self.repository)
            shutil.rmtree(primary)
            shutil.copytree(primary_snapshot, primary)
            current = self._verify()
            with self.assertRaisesRegex(WitnessAlreadyBurned, "NOT_RETRYABLE"):
                assert_unburned(
                    current,
                    self.EXPERIMENT_ID,
                    lockbox_id=self.LOCKBOX_ID,
                    holdout_commitment_sha256=self.HOLDOUT_COMMITMENT_SHA256,
                )


if __name__ == "__main__":
    unittest.main()
