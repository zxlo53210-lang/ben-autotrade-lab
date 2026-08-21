from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import ben_trade_lab.anchor as anchor_module
from ben_trade_lab.anchor import (
    ANCHOR_FIELDS,
    ANCHOR_RECORD_NAME,
    STORE_FIELDS,
    ExternalAnchorAlreadyInitialized,
    ExternalAnchorAlreadyOpened,
    ExternalAnchorCorruption,
    ExternalAnchorPathError,
    assert_holdout_unopened,
    commit_holdout_opened_anchor,
    initialize_anchor_store,
    read_holdout_opened_anchor,
    verify_anchor_store,
)
from ben_trade_lab.config import canonical_json


class ExternalAnchorStoreTests(unittest.TestCase):
    STORE_ID = "a" * 64
    EXPERIMENT_ID = "e" * 64
    CREATED_AT = "2026-08-21T12:00:00.000000Z"
    OPENED_AT = "2026-08-21T12:01:00.000000Z"

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.repository = self.base / "repository"
        self.repository.mkdir()
        self.anchor_root = self.base / "external-anchors"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _initialize(self):
        return initialize_anchor_store(
            self.anchor_root,
            repository_root=self.repository,
            store_id=self.STORE_ID,
            created_at_utc=self.CREATED_AT,
        )

    def _opened_base(self) -> dict[str, object]:
        return {
            "state": "HOLDOUT_OPENED",
            "experiment_id": self.EXPERIMENT_ID,
            "previous_state_sha256": "1" * 64,
            "selection_sha256": "2" * 64,
            "holdout_manifest_sha256": "3" * 64,
            "test_receipt_sha256": "4" * 64,
            "review_receipt_sha256": "5" * 64,
            "opened_at_utc": self.OPENED_AT,
        }

    def _commit(self, store):
        return commit_holdout_opened_anchor(
            store,
            opened_state_base=self._opened_base(),
            config_sha256="6" * 64,
            source_tree_sha256="7" * 64,
            preholdout_data_sha256="8" * 64,
            holdout_commitment_sha256="9" * 64,
        )

    def test_initialize_and_verify_exact_canonical_store(self) -> None:
        store = self._initialize()
        descriptor_path = self.anchor_root / "ANCHOR_STORE.json"
        payload = descriptor_path.read_bytes()
        descriptor = json.loads(payload)

        self.assertEqual(set(descriptor), STORE_FIELDS)
        self.assertEqual(payload, canonical_json(descriptor) + b"\n")
        unsigned = {
            key: value for key, value in descriptor.items() if key != "store_sha256"
        }
        self.assertEqual(
            descriptor["store_sha256"],
            hashlib.sha256(canonical_json(unsigned)).hexdigest(),
        )
        self.assertEqual(store.store_id, self.STORE_ID)
        self.assertEqual(
            verify_anchor_store(
                self.anchor_root,
                repository_root=self.repository,
                expected_store_id=self.STORE_ID,
                expected_store_sha256=store.store_sha256,
            ),
            store,
        )
        with self.assertRaisesRegex(ExternalAnchorCorruption, "STORE_ID_MISMATCH"):
            verify_anchor_store(
                self.anchor_root,
                repository_root=self.repository,
                expected_store_id="b" * 64,
                expected_store_sha256=store.store_sha256,
            )
        with self.assertRaises(ExternalAnchorAlreadyInitialized):
            self._initialize()

        descriptor["policy"] = "TAMPERED"
        descriptor_path.write_bytes(canonical_json(descriptor) + b"\n")
        with self.assertRaisesRegex(ExternalAnchorCorruption, "STORE_SHA256_MISMATCH"):
            verify_anchor_store(
                self.anchor_root,
                repository_root=self.repository,
                expected_store_id=self.STORE_ID,
                expected_store_sha256=store.store_sha256,
            )

    def test_anchor_root_must_be_absolute_disjoint_and_link_free(self) -> None:
        with self.assertRaisesRegex(ExternalAnchorPathError, "MUST_BE_ABSOLUTE"):
            initialize_anchor_store(
                "relative-anchor",
                repository_root=self.repository,
                store_id=self.STORE_ID,
                created_at_utc=self.CREATED_AT,
            )
        with self.assertRaisesRegex(ExternalAnchorPathError, "DISJOINT"):
            initialize_anchor_store(
                self.repository / "anchor",
                repository_root=self.repository,
                store_id=self.STORE_ID,
                created_at_utc=self.CREATED_AT,
            )
        with self.assertRaisesRegex(ExternalAnchorPathError, "DISJOINT"):
            initialize_anchor_store(
                self.base,
                repository_root=self.repository,
                store_id=self.STORE_ID,
                created_at_utc=self.CREATED_AT,
            )

    def test_anchor_root_symlink_is_rejected_when_platform_can_create_one(self) -> None:
        link = self.base / "anchor-link"
        target = self.base / "anchor-target"
        target.mkdir()
        try:
            link.symlink_to(target, target_is_directory=True)
        except OSError as exc:
            self.skipTest(f"directory symlink unavailable: {exc}")
        with self.assertRaisesRegex(ExternalAnchorPathError, "LINK_OR_REPARSE"):
            initialize_anchor_store(
                link,
                repository_root=self.repository,
                store_id=self.STORE_ID,
                created_at_utc=self.CREATED_AT,
            )

    def test_windows_reparse_attribute_is_recognized(self) -> None:
        candidate = self.base / "simulated-reparse"
        metadata = SimpleNamespace(
            st_mode=stat.S_IFDIR,
            st_file_attributes=getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400),
        )
        patches = [
            patch.object(Path, "lstat", return_value=metadata),
            patch.object(Path, "is_symlink", return_value=False),
            patch("ben_trade_lab.anchor.os.name", "nt"),
        ]
        if hasattr(Path, "is_junction"):
            patches.append(patch.object(Path, "is_junction", return_value=False))
        with patches[0], patches[1], patches[2]:
            if len(patches) == 4:
                with patches[3]:
                    self.assertTrue(anchor_module._is_link_or_reparse(candidate))
            else:
                self.assertTrue(anchor_module._is_link_or_reparse(candidate))

    def test_descriptor_and_record_hardlinks_are_rejected(self) -> None:
        store = self._initialize()
        descriptor_link = self.base / "descriptor-hardlink.json"
        try:
            os.link(self.anchor_root / "ANCHOR_STORE.json", descriptor_link)
        except OSError as exc:
            self.skipTest(f"hardlinks unavailable: {exc}")
        with self.assertRaisesRegex(ExternalAnchorCorruption, "HARDLINK_REJECTED"):
            verify_anchor_store(
                self.anchor_root,
                repository_root=self.repository,
                expected_store_id=self.STORE_ID,
                expected_store_sha256=store.store_sha256,
            )
        descriptor_link.unlink()

        anchor = self._commit(store)
        record = store.record_path(self.EXPERIMENT_ID)
        record_link = self.base / "record-hardlink.json"
        os.link(record, record_link)
        with self.assertRaisesRegex(ExternalAnchorCorruption, "HARDLINK_REJECTED"):
            read_holdout_opened_anchor(store, self.EXPERIMENT_ID)
        self.assertEqual(anchor["experiment_id"], self.EXPERIMENT_ID)

    def test_commit_is_canonical_self_hashed_and_exclusive_per_experiment(self) -> None:
        store = self._initialize()
        assert_holdout_unopened(store, self.EXPERIMENT_ID)
        anchor = self._commit(store)
        record_path = store.record_path(self.EXPERIMENT_ID)
        payload = record_path.read_bytes()

        self.assertEqual(record_path.name, ANCHOR_RECORD_NAME)
        self.assertEqual(set(anchor), ANCHOR_FIELDS)
        self.assertEqual(payload, canonical_json(anchor) + b"\n")
        unsigned = {key: value for key, value in anchor.items() if key != "anchor_sha256"}
        self.assertEqual(
            anchor["anchor_sha256"],
            hashlib.sha256(canonical_json(unsigned)).hexdigest(),
        )
        self.assertEqual(
            anchor["opened_state_base_sha256"],
            hashlib.sha256(canonical_json(self._opened_base())).hexdigest(),
        )
        self.assertEqual(read_holdout_opened_anchor(store, self.EXPERIMENT_ID), anchor)
        with self.assertRaisesRegex(ExternalAnchorAlreadyOpened, "NOT_RETRYABLE"):
            self._commit(store)

    def test_partial_or_tampered_record_never_gets_overwritten_or_retried(self) -> None:
        store = self._initialize()
        record = store.record_path(self.EXPERIMENT_ID)
        record.parent.mkdir(parents=True)
        record.write_bytes(b'{"partial":')
        original = record.read_bytes()

        with self.assertRaises(ExternalAnchorCorruption):
            self._commit(store)
        self.assertEqual(record.read_bytes(), original)

        tamper_root = self.base / "tamper-anchors"
        tamper_store = initialize_anchor_store(
            tamper_root,
            repository_root=self.repository,
            store_id="b" * 64,
            created_at_utc=self.CREATED_AT,
        )
        other_id = "f" * 64
        opened = self._opened_base()
        opened["experiment_id"] = other_id
        committed = commit_holdout_opened_anchor(
            tamper_store,
            opened_state_base=opened,
            config_sha256="6" * 64,
            source_tree_sha256="7" * 64,
            preholdout_data_sha256="8" * 64,
            holdout_commitment_sha256="9" * 64,
        )
        other_record = tamper_store.record_path(other_id)
        tampered = dict(committed)
        tampered["selection_sha256"] = "0" * 64
        other_record.write_bytes(canonical_json(tampered) + b"\n")
        with self.assertRaisesRegex(ExternalAnchorCorruption, "ANCHOR_SHA256_MISMATCH"):
            verify_anchor_store(
                tamper_root,
                repository_root=self.repository,
                expected_store_id="b" * 64,
                expected_store_sha256=tamper_store.store_sha256,
            )

    def test_commit_flushes_file_and_every_directory_level(self) -> None:
        store = self._initialize()
        record = store.record_path(self.EXPERIMENT_ID)
        from ben_trade_lab.anchor import fsync_directory as real_fsync_directory

        with (
            patch("ben_trade_lab.anchor.os.fsync", wraps=os.fsync) as file_fsync,
            patch(
                "ben_trade_lab.anchor.fsync_directory",
                wraps=real_fsync_directory,
            ) as directory_fsync,
        ):
            self._commit(store)

        self.assertGreaterEqual(file_fsync.call_count, 1)
        flushed = {Path(call.args[0]).resolve() for call in directory_fsync.call_args_list}
        self.assertTrue(
            {
                record.parent.resolve(),
                record.parent.parent.resolve(),
                (store.root / "records").resolve(),
                store.root.resolve(),
            }.issubset(flushed)
        )

    def test_directory_fsync_failure_after_create_burns_the_experiment(self) -> None:
        store = self._initialize()
        record = store.record_path(self.EXPERIMENT_ID)
        from ben_trade_lab.anchor import fsync_directory as real_fsync_directory

        def fail_after_record_created(directory: str | Path) -> None:
            if record.exists():
                raise RuntimeError("simulated external directory fsync failure")
            real_fsync_directory(directory)

        with (
            patch(
                "ben_trade_lab.anchor.fsync_directory",
                side_effect=fail_after_record_created,
            ),
            self.assertRaisesRegex(RuntimeError, "simulated external directory fsync failure"),
        ):
            self._commit(store)
        self.assertTrue(record.exists())
        with self.assertRaisesRegex(ExternalAnchorAlreadyOpened, "NOT_RETRYABLE"):
            self._commit(store)

    def test_store_can_move_without_changing_id_or_embedding_absolute_path(self) -> None:
        store = self._initialize()
        anchor = self._commit(store)
        moved_root = self.base / "moved-anchor-store"
        shutil.move(str(self.anchor_root), str(moved_root))

        moved = verify_anchor_store(
            moved_root,
            repository_root=self.repository,
            expected_store_id=self.STORE_ID,
            expected_store_sha256=store.store_sha256,
        )
        self.assertEqual(read_holdout_opened_anchor(moved, self.EXPERIMENT_ID), anchor)
        all_payload = b"".join(path.read_bytes() for path in moved_root.rglob("*.json"))
        self.assertNotIn(str(self.anchor_root).encode(), all_payload)
        self.assertNotIn(str(moved_root).encode(), all_payload)

    def test_same_public_id_at_fresh_root_is_not_the_pinned_store_instance(self) -> None:
        canonical = self._initialize()
        duplicate_root = self.base / "duplicate-empty-anchor-store"
        duplicate = initialize_anchor_store(
            duplicate_root,
            repository_root=self.repository,
            store_id=self.STORE_ID,
            created_at_utc="2026-08-21T12:00:01.000000Z",
        )
        self.assertEqual(duplicate.store_id, canonical.store_id)
        self.assertNotEqual(duplicate.store_sha256, canonical.store_sha256)
        with self.assertRaisesRegex(ExternalAnchorCorruption, "DESCRIPTOR_MISMATCH"):
            verify_anchor_store(
                duplicate_root,
                repository_root=self.repository,
                expected_store_id=canonical.store_id,
                expected_store_sha256=canonical.store_sha256,
            )


if __name__ == "__main__":
    unittest.main()
