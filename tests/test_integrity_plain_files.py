from __future__ import annotations

import hashlib
import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ben_trade_lab.config import canonical_json
from ben_trade_lab.integrity import (
    is_link_or_reparse,
    require_plain_regular_single_link,
    resolve_regular_file_inside,
    verified_hashed_object,
    write_exclusive,
    write_immutable,
)
from ben_trade_lab.validation import FROZEN_STATE_FIELDS, _read_canonical_state


class PlainFileBoundaryTests(unittest.TestCase):
    def test_writers_reject_reparse_parent_before_creating_any_entry(self) -> None:
        writers = (
            ("immutable", lambda path: write_immutable(path, b"synthetic\n")),
            ("exclusive", lambda path: write_exclusive(path, {"synthetic": True})),
        )
        for label, writer in writers:
            with self.subTest(writer=label), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                simulated_reparse = root / "artifacts"
                simulated_reparse.mkdir()
                nested = simulated_reparse / "nested"
                destination = nested / "evidence.json"
                expected_parent = simulated_reparse.absolute()

                def reparse_at_parent(
                    path: str | Path, expected: Path = expected_parent
                ) -> bool:
                    candidate = Path(path).absolute()
                    return candidate == expected or is_link_or_reparse(path)

                with (
                    patch(
                        "ben_trade_lab.integrity.is_link_or_reparse",
                        side_effect=reparse_at_parent,
                    ),
                    self.assertRaisesRegex(ValueError, "parent path.*reparse"),
                ):
                    writer(destination)

                self.assertFalse(nested.exists())
                self.assertFalse(destination.exists())

    def test_writer_requires_nearest_existing_parent_to_be_a_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            blocker = root / "artifacts"
            blocker.write_bytes(b"not a directory")
            destination = blocker / "evidence.json"

            with self.assertRaisesRegex(ValueError, "nearest existing parent"):
                write_immutable(destination, b"synthetic\n")
            self.assertEqual(blocker.read_bytes(), b"not a directory")

    def test_hardlinked_artifact_is_rejected_by_every_read_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            root = base / "repository"
            artifact = root / "artifacts" / "sample.json"
            unsigned = {"type": "SYNTHETIC", "value": 1}
            value = {
                **unsigned,
                "object_sha256": hashlib.sha256(canonical_json(unsigned)).hexdigest(),
            }
            payload = canonical_json(value) + b"\n"
            write_immutable(artifact, payload)
            outside = base / "outside-hardlink.json"
            try:
                os.link(artifact, outside)
            except OSError as exc:
                self.skipTest(f"hardlinks unavailable: {exc}")

            self.assertGreater(artifact.stat().st_nlink, 1)
            with self.assertRaisesRegex(ValueError, "hard link"):
                require_plain_regular_single_link(artifact, "artifact")
            with self.assertRaisesRegex(ValueError, "hard link"):
                resolve_regular_file_inside(root, "artifacts/sample.json", "artifacts")
            with self.assertRaisesRegex(ValueError, "hard link"):
                verified_hashed_object(artifact, "object_sha256")
            with self.assertRaisesRegex(ValueError, "hard link"):
                write_immutable(artifact, payload)

    def test_windows_reparse_attribute_is_rejected_even_without_symlink_or_junction(self) -> None:
        candidate = Path("simulated-reparse.json")
        metadata = SimpleNamespace(
            st_mode=stat.S_IFREG,
            st_nlink=1,
            st_file_attributes=getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400),
        )
        with (
            patch.object(Path, "lstat", return_value=metadata),
            patch.object(Path, "is_symlink", return_value=False),
            patch.object(Path, "is_junction", return_value=False),
        ):
            self.assertTrue(is_link_or_reparse(candidate))
            with self.assertRaisesRegex(ValueError, "reparse"):
                require_plain_regular_single_link(candidate, "artifact")

    def test_hardlinked_experiment_state_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            experiment_id = "e" * 64
            unsigned = {
                "state": "FROZEN",
                "experiment_id": experiment_id,
                "selection_path": "artifacts/selection-test.json",
                "selection_sha256": "1" * 64,
                "config_sha256": "2" * 64,
                "source_tree_sha256": "3" * 64,
                "preholdout_data_sha256": "4" * 64,
                "holdout_commitment_sha256": "5" * 64,
            }
            state = {
                **unsigned,
                "state_sha256": hashlib.sha256(canonical_json(unsigned)).hexdigest(),
            }
            path = base / "state" / "experiments" / experiment_id / "FROZEN.json"
            write_immutable(path, canonical_json(state) + b"\n")
            outside = base / "outside-state.json"
            try:
                os.link(path, outside)
            except OSError as exc:
                self.skipTest(f"hardlinks unavailable: {exc}")

            self.assertEqual(set(state), FROZEN_STATE_FIELDS)
            with self.assertRaisesRegex(ValueError, "hard link"):
                _read_canonical_state(
                    path,
                    expected_state="FROZEN",
                    exact_fields=FROZEN_STATE_FIELDS,
                )


if __name__ == "__main__":
    unittest.main()
