from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ben_trade_lab.cli import main
from ben_trade_lab.config import canonical_json
from ben_trade_lab.data import (
    DATA_EXCEPTION_REGISTRY_SHA256,
    HOUR_MS,
    PARTITION_SCHEMA_VERSION,
    DataIntegrityError,
    _assert_registered_full_source_identity,
    _csv_bytes,
    _load_exception_registry,
    _lockbox_id,
    _partition_descriptor_sha256,
    _validate_partition_manifest_schema,
    _validate_registered_exceptions,
    _verify_partition_provenance,
    _write_new_or_same,
    load_bars_from_manifest,
    partition_lockbox,
    verify_manifest,
)
from ben_trade_lab.integrity import source_files, source_tree_sha256
from ben_trade_lab.validation import _load_selection_artifact

ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "configs" / "data_exceptions_v1.json"
FULL_MANIFEST = (
    ROOT
    / "data"
    / "manifests"
    / "BTCUSDT-1h-1502942400000-1785542400000-afaef07eb47c4613-cc3f4f474f84a96f.json"
)
FULL_MANIFEST_SHA256 = "cc3f4f474f84a96f7b9e5cf5d8048537139a028ee08c3f6d36b71a8a8cdb1026"


def full_local_snapshot_available() -> bool:
    """Return true only when every ignored byte needed for raw replay exists."""

    try:
        manifest = json.loads(FULL_MANIFEST.read_text(encoding="utf-8"))
        normalized = ROOT / manifest["normalized_path"]
        raw_paths = [ROOT / batch["raw_path"] for batch in manifest["batches"]]
    except (FileNotFoundError, KeyError, TypeError, json.JSONDecodeError):
        return False
    return normalized.is_file() and all(path.is_file() for path in raw_paths)


def fixture_row(index: int) -> tuple[str, ...]:
    start = index * HOUR_MS
    return (
        str(start),
        "100",
        "102",
        "99",
        "101",
        "1",
        str(start + HOUR_MS - 1),
        "101",
        "1",
        "0.5",
        "50.5",
        "0",
    )


class RegistryContractTests(unittest.TestCase):
    def test_registry_hash_schema_counts_and_gap_geometry_are_exact(self) -> None:
        payload = REGISTRY_PATH.read_bytes()
        self.assertEqual(
            hashlib.sha256(payload).hexdigest(),
            "5d45208bf3ca72d38b92b671694c74921c5a3ee3ddbae850b30ee7893ca0a582",
        )
        registry, digest = _load_exception_registry(ROOT)
        self.assertEqual(digest, DATA_EXCEPTION_REGISTRY_SHA256)
        self.assertFalse(registry["source"]["source_row_modified"])
        close_events = registry["close_time_source_variances"]
        gap_events = registry["missing_hourly_bar_events"]
        self.assertEqual(len(close_events), 14)
        self.assertEqual(len(gap_events), 28)
        self.assertEqual(sum(item["missing_bar_count"] for item in gap_events), 128)
        self.assertEqual(max(item["missing_bar_count"] for item in gap_events), 33)
        self.assertEqual(len({item["canonical_full_row_sha256"] for item in close_events}), 14)
        for item in gap_events:
            self.assertEqual(
                item["next_open_time_ms"] - item["previous_open_time_ms"],
                (item["missing_bar_count"] + 1) * HOUR_MS,
            )


class PublishedProvenanceRootTests(unittest.TestCase):
    def test_price_free_provenance_root_is_exact_and_source_hashed(self) -> None:
        payload = FULL_MANIFEST.read_bytes()
        self.assertEqual(hashlib.sha256(payload).hexdigest(), FULL_MANIFEST_SHA256)
        self.assertIn(FULL_MANIFEST.resolve(), source_files(ROOT))
        manifest = json.loads(payload)
        self.assertEqual(
            set(manifest),
            {
                "authentication",
                "batches",
                "config_sha256",
                "declared_source_anomalies",
                "first_open_ms",
                "gap_policy",
                "http_method",
                "interval",
                "last_open_ms",
                "normalized_path",
                "normalized_sha256",
                "requested_end_ms_exclusive",
                "requested_start_ms",
                "retrieved_at_utc",
                "row_count",
                "schema_version",
                "source",
                "symbol",
                "timezone",
                "validation",
            },
        )
        self.assertEqual(manifest["source"], "https://data-api.binance.vision/api/v3/klines")
        self.assertEqual(manifest["http_method"], "GET")
        self.assertEqual(manifest["authentication"], "NONE")
        self.assertEqual(manifest["symbol"], "BTCUSDT")
        self.assertEqual(manifest["interval"], "1h")
        self.assertEqual(manifest["timezone"], "UTC")
        self.assertEqual(manifest["requested_start_ms"], 1_502_942_400_000)
        self.assertEqual(manifest["requested_end_ms_exclusive"], 1_785_542_400_000)
        self.assertEqual(manifest["first_open_ms"], 1_502_942_400_000)
        self.assertEqual(manifest["last_open_ms"], 1_785_538_800_000)
        self.assertEqual(manifest["row_count"], 78_372)
        self.assertEqual(len(manifest["batches"]), 79)
        self.assertEqual(len(manifest["declared_source_anomalies"]), 42)

        batch_keys = {
            "raw_path",
            "raw_sha256",
            "request_start_ms",
            "response_first_open_ms",
            "response_last_open_ms",
            "row_count",
        }
        prior_last: int | None = None
        for batch in manifest["batches"]:
            self.assertEqual(set(batch), batch_keys)
            self.assertRegex(batch["raw_sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(
                batch["raw_path"],
                (
                    f"data/raw/BTCUSDT-1h-{batch['request_start_ms']}-"
                    f"{batch['raw_sha256'][:16]}.json"
                ),
            )
            self.assertEqual(batch["request_start_ms"], batch["response_first_open_ms"])
            self.assertGreater(batch["row_count"], 0)
            self.assertLessEqual(batch["row_count"], 1_000)
            if prior_last is not None:
                self.assertEqual(batch["request_start_ms"], prior_last + HOUR_MS)
            prior_last = batch["response_last_open_ms"]
        self.assertEqual(sum(batch["row_count"] for batch in manifest["batches"]), 78_372)
        self.assertEqual(prior_last, manifest["last_open_ms"])

        forbidden_price_keys = {"open", "high", "low", "close", "volume", "rows", "data"}

        def assert_price_free(value: object) -> None:
            if isinstance(value, dict):
                self.assertTrue(forbidden_price_keys.isdisjoint(value))
                for child in value.values():
                    assert_price_free(child)
            elif isinstance(value, list):
                for child in value:
                    assert_price_free(child)

        assert_price_free(manifest)
        registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        projected_close_events = [
            {key: value for key, value in item.items() if key != "canonical_full_row_sha256"}
            for item in registry["close_time_source_variances"]
        ]
        expected_anomalies = projected_close_events + registry["missing_hourly_bar_events"]
        canonical_order = lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"))
        self.assertEqual(
            sorted(manifest["declared_source_anomalies"], key=canonical_order),
            sorted(expected_anomalies, key=canonical_order),
        )
        _assert_registered_full_source_identity(manifest, registry, ROOT)

    def test_registry_byte_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "configs" / "data_exceptions_v1.json"
            path.parent.mkdir(parents=True)
            path.write_bytes(REGISTRY_PATH.read_bytes() + b" ")
            with self.assertRaisesRegex(DataIntegrityError, "registry hash mismatch"):
                _load_exception_registry(root)

    def test_registered_close_time_row_hash_detects_source_drift(self) -> None:
        open_time = 1_504_713_600_000
        source_row = (
            "1504713600000",
            "4619.43",
            "4619.43",
            "4619.43",
            "4619.43",
            "0",
            "1504713600000",
            "0",
            "0",
            "0",
            "0",
            "0",
        )
        anomaly = {
            "type": "CLOSE_TIME_SOURCE_VARIANCE",
            "open_time_ms": open_time,
            "observed_close_time_ms": open_time,
            "expected_interval_end_ms": open_time + HOUR_MS - 1,
            "trade_count": 0,
            "zero_volume": True,
        }
        registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        full_sha = registry["source"]["normalized_sha256"]
        self.assertEqual(
            _validate_registered_exceptions(
                [source_row],
                [anomaly],
                start_ms=open_time,
                end_ms_exclusive=open_time + HOUR_MS,
                full_source_sha256=full_sha,
                gap_policy="CARRY_FORWARD_NO_FILL",
                root=ROOT,
                expected_registry_sha256=DATA_EXCEPTION_REGISTRY_SHA256,
            ),
            DATA_EXCEPTION_REGISTRY_SHA256,
        )
        changed = list(source_row)
        changed[4] = "4619.44"
        with self.assertRaisesRegex(DataIntegrityError, "registered exception row changed"):
            _validate_registered_exceptions(
                [tuple(changed)],
                [anomaly],
                start_ms=open_time,
                end_ms_exclusive=open_time + HOUR_MS,
                full_source_sha256=full_sha,
                gap_policy="CARRY_FORWARD_NO_FILL",
                root=ROOT,
                expected_registry_sha256=DATA_EXCEPTION_REGISTRY_SHA256,
            )


class PartitionBindingTests(unittest.TestCase):
    def _pair(self, root: Path) -> dict[str, object]:
        manifests = root / "data" / "manifests"
        normalized = root / "data" / "normalized"
        manifests.mkdir(parents=True)
        normalized.mkdir(parents=True)
        parent_rows = [fixture_row(index) for index in range(4)]
        parent_payload = _csv_bytes(parent_rows)
        parent_csv_path = normalized / "parent.csv"
        parent_csv_path.write_bytes(parent_payload)
        parent_normalized_sha = hashlib.sha256(parent_payload).hexdigest()
        registry_sha = "a" * 64
        parent = {
            "kind": "FULL_SOURCE",
            "normalized_path": "data/normalized/parent.csv",
            "normalized_sha256": parent_normalized_sha,
            "exception_registry_sha256": registry_sha,
            "requested_start_ms": 0,
            "requested_end_ms_exclusive": 4 * HOUR_MS,
            "source": "https://data-api.binance.vision/api/v3/klines",
            "http_method": "GET",
            "authentication": "NONE",
            "symbol": "BTCUSDT",
            "interval": "1h",
            "timezone": "UTC",
            "gap_policy": "CARRY_FORWARD_NO_FILL",
        }
        parent_manifest_payload = canonical_json(parent) + b"\n"
        parent_manifest_sha = hashlib.sha256(parent_manifest_payload).hexdigest()
        parent_relative = f"data/manifests/parent-{parent_manifest_sha[:16]}.json"
        parent_manifest_path = root / parent_relative
        parent_manifest_path.write_bytes(parent_manifest_payload)

        pre_rows = parent_rows[:2]
        locked_rows = parent_rows[2:]
        pre_payload = _csv_bytes(pre_rows)
        locked_payload = _csv_bytes(locked_rows)
        pre_sha = hashlib.sha256(pre_payload).hexdigest()
        locked_sha = hashlib.sha256(locked_payload).hexdigest()
        config_sha = "b" * 64
        lockbox_id = _lockbox_id(
            config_sha256=config_sha,
            exception_registry_sha256=registry_sha,
            parent_normalized_sha256=parent_normalized_sha,
            preholdout_sha256=pre_sha,
            holdout_commitment_sha256=locked_sha,
            holdout_start_ms=2 * HOUR_MS,
            holdout_end_ms_exclusive=4 * HOUR_MS,
        )

        def base(kind: str, rows: list[tuple[str, ...]], payload_sha: str) -> dict[str, object]:
            start_ms = 0 if kind == "PREHOLDOUT" else 2 * HOUR_MS
            end_ms = 2 * HOUR_MS if kind == "PREHOLDOUT" else 4 * HOUR_MS
            return {
                "schema_version": PARTITION_SCHEMA_VERSION,
                "kind": kind,
                "source": parent["source"],
                "http_method": "GET",
                "authentication": "NONE",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "timezone": "UTC",
                "gap_policy": "CARRY_FORWARD_NO_FILL",
                "requested_start_ms": start_ms,
                "requested_end_ms_exclusive": end_ms,
                "first_open_ms": int(rows[0][0]),
                "last_open_ms": int(rows[-1][0]),
                "row_count": len(rows),
                "normalized_path": (
                    f"data/normalized/BTCUSDT-1h-{kind.lower()}-{start_ms}-{end_ms}-"
                    f"{payload_sha[:16]}.csv"
                ),
                "normalized_sha256": payload_sha,
                "config_sha256": config_sha,
                "exception_registry_sha256": registry_sha,
                "validation": "PASS",
                "declared_source_anomalies": [],
                "parent_manifest_path": parent_relative,
                "parent_manifest_sha256": parent_manifest_sha,
                "parent_normalized_sha256": parent_normalized_sha,
                "lockbox_id": lockbox_id,
                "preholdout_sha256": pre_sha,
                "holdout_commitment_sha256": locked_sha,
            }

        pre_base = base("PREHOLDOUT", pre_rows, pre_sha)
        locked_base = base("LOCKED_HOLDOUT", locked_rows, locked_sha)
        pre_descriptor = hashlib.sha256(canonical_json(pre_base)).hexdigest()
        locked_descriptor = hashlib.sha256(canonical_json(locked_base)).hexdigest()
        locked = {
            **locked_base,
            "partition_descriptor_sha256": locked_descriptor,
            "paired_partition_kind": "PREHOLDOUT",
            "paired_partition_descriptor_sha256": pre_descriptor,
        }
        locked_manifest_payload = canonical_json(locked) + b"\n"
        locked_manifest_sha = hashlib.sha256(locked_manifest_payload).hexdigest()
        locked_relative = f"data/manifests/locked-{locked_manifest_sha[:16]}.json"
        locked_path = root / locked_relative
        locked_path.write_bytes(locked_manifest_payload)
        pre = {
            **pre_base,
            "partition_descriptor_sha256": pre_descriptor,
            "paired_partition_kind": "LOCKED_HOLDOUT",
            "paired_partition_descriptor_sha256": locked_descriptor,
            "locked_holdout_manifest_path": locked_relative,
            "locked_holdout_manifest_sha256": locked_manifest_sha,
        }
        pre_manifest_payload = canonical_json(pre) + b"\n"
        pre_manifest_sha = hashlib.sha256(pre_manifest_payload).hexdigest()
        pre_path = manifests / f"pre-{pre_manifest_sha[:16]}.json"
        pre_path.write_bytes(pre_manifest_payload)
        (root / str(pre["normalized_path"])).write_bytes(pre_payload)
        (root / str(locked["normalized_path"])).write_bytes(locked_payload)
        return {
            "parent": {
                **parent,
                "manifest_path": parent_relative,
                "manifest_file_sha256": parent_manifest_sha,
            },
            "parent_rows": parent_rows,
            "parent_payload": parent_payload,
            "parent_csv_path": parent_csv_path,
            "pre": pre,
            "pre_rows": pre_rows,
            "pre_payload": pre_payload,
            "pre_path": pre_path,
            "locked": locked,
            "locked_path": locked_path,
        }

    def test_default_preholdout_verification_uses_metadata_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = self._pair(Path(temporary))
            parent_csv_path = fixture["parent_csv_path"]
            assert isinstance(parent_csv_path, Path)
            parent_csv_path.unlink()
            with patch("ben_trade_lab.data.verify_manifest") as full_replay:
                _verify_partition_provenance(
                    fixture["pre"],
                    fixture["pre_rows"],
                    fixture["pre_payload"],
                    Path(temporary),
                    observed_at_ms=5 * HOUR_MS,
                )
            full_replay.assert_not_called()

    def test_immutable_data_writer_rejects_parent_indirection_before_mkdir(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "redirected" / "data.bin"
            with (
                patch(
                    "ben_trade_lab.data.require_plain_parent_chain_for_create",
                    side_effect=ValueError("synthetic parent reparse"),
                ),
                patch.object(Path, "mkdir") as mkdir,
                self.assertRaisesRegex(DataIntegrityError, "parent path is unsafe"),
            ):
                _write_new_or_same(destination, b"payload")
            mkdir.assert_not_called()

    def test_verify_manifest_preholdout_never_reads_full_or_locked_prices(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._pair(root)
            parent_csv = fixture["parent_csv_path"]
            locked = fixture["locked"]
            assert isinstance(parent_csv, Path) and isinstance(locked, dict)
            locked_csv = root / str(locked["normalized_path"])
            observed_reads: list[Path] = []
            original_read_bytes = Path.read_bytes

            def recording_read_bytes(path: Path) -> bytes:
                observed_reads.append(path.resolve())
                return original_read_bytes(path)

            with (
                patch.object(Path, "read_bytes", recording_read_bytes),
                patch(
                    "ben_trade_lab.data._load_exception_registry",
                    return_value=({}, "a" * 64),
                ),
                patch(
                    "ben_trade_lab.data._validate_registered_exceptions",
                    return_value="a" * 64,
                ),
                patch("ben_trade_lab.data._assert_registered_full_source_identity"),
            ):
                verified = verify_manifest(
                    fixture["pre_path"],
                    root=root,
                    as_of_ms=5 * HOUR_MS,
                )
            self.assertEqual(verified["kind"], "PREHOLDOUT")
            self.assertNotIn(parent_csv.resolve(), observed_reads)
            self.assertNotIn(locked_csv.resolve(), observed_reads)

    def test_loader_rechecks_and_consumes_one_verified_second_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._pair(root)
            pre = fixture["pre"]
            assert isinstance(pre, dict)
            pre_price_path = (root / str(pre["normalized_path"])).resolve()
            normalized_reads = 0
            original_read_bytes = Path.read_bytes

            def recording_read_bytes(path: Path) -> bytes:
                nonlocal normalized_reads
                if path.resolve() == pre_price_path:
                    normalized_reads += 1
                return original_read_bytes(path)

            with (
                patch.object(Path, "read_bytes", recording_read_bytes),
                patch(
                    "ben_trade_lab.data._load_exception_registry",
                    return_value=({}, "a" * 64),
                ),
                patch(
                    "ben_trade_lab.data._validate_registered_exceptions",
                    return_value="a" * 64,
                ),
                patch("ben_trade_lab.data._assert_registered_full_source_identity"),
            ):
                bars, manifest = load_bars_from_manifest(
                    fixture["pre_path"],
                    root=root,
                    expected_kind="PREHOLDOUT",
                )
            self.assertEqual(normalized_reads, 2)
            self.assertEqual(manifest["normalized_sha256"], pre["normalized_sha256"])
            self.assertEqual([bar.close for bar in bars], [101.0, 101.0])

    def test_loader_rejects_normalized_swap_after_manifest_verification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._pair(root)
            pre = fixture["pre"]
            pre_rows = fixture["pre_rows"]
            assert isinstance(pre, dict) and isinstance(pre_rows, list)
            pre_price_path = (root / str(pre["normalized_path"])).resolve()
            changed_rows = list(pre_rows)
            changed_first = list(changed_rows[0])
            changed_first[4] = "100.5"
            changed_rows[0] = tuple(changed_first)
            changed_payload = _csv_bytes(changed_rows)
            original_verify = verify_manifest
            normalized_reads = 0
            original_read_bytes = Path.read_bytes

            def recording_read_bytes(path: Path) -> bytes:
                nonlocal normalized_reads
                if path.resolve() == pre_price_path:
                    normalized_reads += 1
                return original_read_bytes(path)

            def verify_then_swap(*args: object, **kwargs: object) -> dict[str, object]:
                verified = original_verify(*args, **kwargs)
                pre_price_path.write_bytes(changed_payload)
                return verified

            with (
                patch.object(Path, "read_bytes", recording_read_bytes),
                patch("ben_trade_lab.data.verify_manifest", side_effect=verify_then_swap),
                patch(
                    "ben_trade_lab.data._load_exception_registry",
                    return_value=({}, "a" * 64),
                ),
                patch(
                    "ben_trade_lab.data._validate_registered_exceptions",
                    return_value="a" * 64,
                ),
                patch("ben_trade_lab.data._assert_registered_full_source_identity"),
                self.assertRaisesRegex(DataIntegrityError, "changed after manifest verification"),
            ):
                load_bars_from_manifest(
                    fixture["pre_path"],
                    root=root,
                    expected_kind="PREHOLDOUT",
                )
            self.assertEqual(normalized_reads, 2)

    def test_normalized_hardlink_is_rejected_as_data_integrity_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._pair(root)
            pre = fixture["pre"]
            assert isinstance(pre, dict)
            pre_price_path = root / str(pre["normalized_path"])
            alias = root / "synthetic-hardlink.csv"
            try:
                os.link(pre_price_path, alias)
            except OSError as exc:
                self.skipTest(f"hardlinks unavailable: {exc}")
            with self.assertRaisesRegex(DataIntegrityError, "plain single-link"):
                verify_manifest(
                    fixture["pre_path"],
                    root=root,
                    expected_kind="PREHOLDOUT",
                )

    def test_research_select_rejects_full_before_reading_price_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifests = root / "data" / "manifests"
            normalized = root / "data" / "normalized"
            manifests.mkdir(parents=True)
            normalized.mkdir(parents=True)
            full_price_path = normalized / "full-must-not-be-read.csv"
            full_price_path.write_bytes(b"synthetic-price-sentinel\n")
            full = {
                "kind": "FULL_SOURCE",
                "normalized_path": full_price_path.relative_to(root).as_posix(),
            }
            full_payload = canonical_json(full) + b"\n"
            full_sha = hashlib.sha256(full_payload).hexdigest()
            full_path = manifests / f"full-{full_sha[:16]}.json"
            full_path.write_bytes(full_payload)
            observed_reads: list[Path] = []
            original_read_bytes = Path.read_bytes

            def recording_read_bytes(path: Path) -> bytes:
                observed_reads.append(path.resolve())
                return original_read_bytes(path)

            with (
                patch.object(Path, "read_bytes", recording_read_bytes),
                self.assertRaisesRegex(DataIntegrityError, "expected PREHOLDOUT"),
            ):
                main(
                    [
                        "--root",
                        str(root),
                        "--config",
                        str(ROOT / "configs" / "btcusdt_1h.toml"),
                        "research",
                        "select",
                        "--manifest",
                        str(full_path),
                    ]
                )
            with (
                patch.object(Path, "read_bytes", recording_read_bytes),
                self.assertRaisesRegex(DataIntegrityError, "expected PREHOLDOUT"),
            ):
                verify_manifest(
                    full_path,
                    root=root,
                    expected_kind="PREHOLDOUT",
                )
            self.assertNotIn(full_price_path.resolve(), observed_reads)

    def test_research_select_rejects_locked_before_reading_any_price_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._pair(root)
            parent_price_path = fixture["parent_csv_path"]
            locked = fixture["locked"]
            assert isinstance(parent_price_path, Path) and isinstance(locked, dict)
            locked_price_path = root / str(locked["normalized_path"])
            observed_reads: list[Path] = []
            original_read_bytes = Path.read_bytes

            def recording_read_bytes(path: Path) -> bytes:
                observed_reads.append(path.resolve())
                return original_read_bytes(path)

            with (
                patch.object(Path, "read_bytes", recording_read_bytes),
                patch(
                    "ben_trade_lab.data._load_exception_registry",
                    return_value=({}, "a" * 64),
                ),
                patch("ben_trade_lab.data._assert_registered_full_source_identity"),
                self.assertRaisesRegex(DataIntegrityError, "expected PREHOLDOUT"),
            ):
                main(
                    [
                        "--root",
                        str(root),
                        "--config",
                        str(ROOT / "configs" / "btcusdt_1h.toml"),
                        "research",
                        "select",
                        "--manifest",
                        str(fixture["locked_path"]),
                    ]
                )
            self.assertNotIn(parent_price_path.resolve(), observed_reads)
            self.assertNotIn(locked_price_path.resolve(), observed_reads)

    def test_expected_kind_gate_precedes_locked_price_read(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._pair(root)
            parent_price_path = fixture["parent_csv_path"]
            locked = fixture["locked"]
            assert isinstance(parent_price_path, Path) and isinstance(locked, dict)
            locked_price_path = root / str(locked["normalized_path"])
            observed_reads: list[Path] = []
            original_read_bytes = Path.read_bytes

            def recording_read_bytes(path: Path) -> bytes:
                observed_reads.append(path.resolve())
                return original_read_bytes(path)

            with (
                patch.object(Path, "read_bytes", recording_read_bytes),
                self.assertRaisesRegex(DataIntegrityError, "expected PREHOLDOUT"),
            ):
                verify_manifest(
                    fixture["locked_path"],
                    root=root,
                    expected_kind="PREHOLDOUT",
                )
            self.assertNotIn(parent_price_path.resolve(), observed_reads)
            self.assertNotIn(locked_price_path.resolve(), observed_reads)

    def test_full_parent_replay_requires_gate_and_detects_non_slice(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = self._pair(Path(temporary))
            with (
                patch.dict(os.environ, {"BEN_ISOLATED_PROVENANCE_REPLAY": "0"}),
                self.assertRaisesRegex(DataIntegrityError, "ISOLATED_PROVENANCE_MODE"),
            ):
                _verify_partition_provenance(
                    fixture["pre"],
                    fixture["pre_rows"],
                    fixture["pre_payload"],
                    Path(temporary),
                    observed_at_ms=5 * HOUR_MS,
                    replay_full_parent=True,
                )
            changed = list(fixture["pre_rows"])
            mutated = list(changed[1])
            mutated[4] = "100.5"
            changed[1] = tuple(mutated)
            with (
                patch.dict(os.environ, {"BEN_ISOLATED_PROVENANCE_REPLAY": "1"}),
                patch("ben_trade_lab.data.verify_manifest", return_value=fixture["parent"]),
                self.assertRaisesRegex(DataIntegrityError, "exact parent slice"),
            ):
                _verify_partition_provenance(
                    fixture["pre"],
                    changed,
                    _csv_bytes(changed),
                    Path(temporary),
                    observed_at_ms=5 * HOUR_MS,
                    replay_full_parent=True,
                )

    def test_locked_partition_default_fails_before_reading_price_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = self._pair(Path(temporary))
            locked = fixture["locked"]
            assert isinstance(locked, dict)
            locked_data = Path(temporary) / str(locked["normalized_path"])
            locked_data.unlink()
            with self.assertRaisesRegex(DataIntegrityError, "LOCKED_DATA_ACCESS_REQUIRES"):
                verify_manifest(
                    fixture["locked_path"],
                    root=temporary,
                    as_of_ms=5 * HOUR_MS,
                )

    def test_partition_pair_and_schema_are_exact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = self._pair(Path(temporary))
            pre = fixture["pre"]
            locked = fixture["locked"]
            assert isinstance(pre, dict) and isinstance(locked, dict)
            _validate_partition_manifest_schema(pre)
            _validate_partition_manifest_schema(locked)
            self.assertEqual(_partition_descriptor_sha256(pre), pre["partition_descriptor_sha256"])
            self.assertEqual(
                pre["paired_partition_descriptor_sha256"],
                locked["partition_descriptor_sha256"],
            )
            extra = dict(pre)
            extra["unexpected"] = True
            with self.assertRaisesRegex(DataIntegrityError, "schema mismatch"):
                _validate_partition_manifest_schema(extra)
            wrong_pair = dict(pre)
            wrong_pair["paired_partition_kind"] = "PREHOLDOUT"
            with self.assertRaisesRegex(DataIntegrityError, "paired kind mismatch"):
                _validate_partition_manifest_schema(wrong_pair)

    def test_partition_lockbox_writes_non_circular_exact_pair(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            normalized = root / "data" / "normalized"
            normalized.mkdir(parents=True)
            rows = [fixture_row(index) for index in range(4)]
            full_payload = _csv_bytes(rows)
            full_sha = hashlib.sha256(full_payload).hexdigest()
            full_path = normalized / "full.csv"
            full_path.write_bytes(full_payload)
            config_sha = "b" * 64
            registry_sha = "a" * 64
            full = {
                "kind": "FULL_SOURCE",
                "manifest_path": "data/manifests/full-fixture.json",
                "manifest_file_sha256": "c" * 64,
                "normalized_path": full_path.relative_to(root).as_posix(),
                "normalized_sha256": full_sha,
                "exception_registry_sha256": registry_sha,
                "requested_start_ms": 0,
                "requested_end_ms_exclusive": 4 * HOUR_MS,
                "source": "https://data-api.binance.vision/api/v3/klines",
                "http_method": "GET",
                "authentication": "NONE",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "timezone": "UTC",
                "gap_policy": "CARRY_FORWARD_NO_FILL",
            }
            config = SimpleNamespace(
                config_sha256=config_sha,
                market={
                    "source_base_url": "https://data-api.binance.vision",
                    "symbol": "BTCUSDT",
                    "interval": "1h",
                    "timezone": "UTC",
                    "gap_policy": "CARRY_FORWARD_NO_FILL",
                    "exception_registry_sha256": registry_sha,
                    "start_utc": "1970-01-01T00:00:00Z",
                    "end_utc_exclusive": "1970-01-01T04:00:00Z",
                },
                splits={
                    "validation_end_utc_exclusive": "1970-01-01T02:00:00Z",
                    "locked_holdout_end_utc_exclusive": "1970-01-01T04:00:00Z",
                },
            )
            with patch("ben_trade_lab.data.verify_manifest", return_value=full):
                pre_path, locked_path = partition_lockbox("ignored.json", config, root=root)
            pre = json.loads(pre_path.read_text(encoding="utf-8"))
            locked = json.loads(locked_path.read_text(encoding="utf-8"))
            _validate_partition_manifest_schema(pre, payload=pre_path.read_bytes())
            _validate_partition_manifest_schema(locked, payload=locked_path.read_bytes())
            self.assertEqual(pre["paired_partition_kind"], "LOCKED_HOLDOUT")
            self.assertEqual(locked["paired_partition_kind"], "PREHOLDOUT")
            self.assertEqual(
                pre["paired_partition_descriptor_sha256"],
                locked["partition_descriptor_sha256"],
            )
            self.assertEqual(
                locked["paired_partition_descriptor_sha256"],
                pre["partition_descriptor_sha256"],
            )
            self.assertEqual(
                pre["locked_holdout_manifest_path"],
                locked_path.relative_to(root).as_posix(),
            )
            self.assertEqual(
                pre["locked_holdout_manifest_sha256"],
                hashlib.sha256(locked_path.read_bytes()).hexdigest(),
            )


@unittest.skipUnless(
    os.environ.get("BEN_ISOLATED_PROVENANCE_REPLAY") == "1" and full_local_snapshot_available(),
    "requires explicit isolated provenance replay mode and the complete local snapshot",
)
class LocalSnapshotIntegrationTests(unittest.TestCase):
    def test_full_raw_rebuild_emits_only_hashes_and_counters(self) -> None:
        selection_relative = os.environ.get("BEN_AUDIT_SELECTION_PATH")
        expected_selection_sha = os.environ.get("BEN_AUDIT_SELECTION_SHA256")
        if not isinstance(selection_relative, str) or not selection_relative:
            self.fail("AUDIT_SELECTION_PATH_MISSING")
        if not isinstance(expected_selection_sha, str) or not expected_selection_sha:
            self.fail("AUDIT_SELECTION_SHA256_MISSING")
        try:
            selection, selection_path, canonical_relative = _load_selection_artifact(
                selection_relative,
                ROOT,
            )
        except (OSError, TypeError, ValueError):
            raise self.failureException("AUDIT_SELECTION_VALIDATION_FAILED") from None
        if canonical_relative != selection_relative:
            self.fail("AUDIT_SELECTION_PATH_NOT_CANONICAL")
        if selection["selection_sha256"] != expected_selection_sha:
            self.fail("AUDIT_SELECTION_SHA256_MISMATCH")
        if selection_path.parent != (ROOT / "artifacts").resolve():
            self.fail("AUDIT_SELECTION_LOCATION_MISMATCH")
        self.assertEqual(selection["status"], "FROZEN_CANDIDATE")
        self.assertEqual(selection["source_tree_sha256"], source_tree_sha256(ROOT))

        try:
            manifest = verify_manifest(FULL_MANIFEST, root=ROOT)
            preholdout = verify_manifest(
                selection["preholdout_manifest_path"],
                root=ROOT,
                replay_full_parent=True,
            )
            locked = verify_manifest(
                selection["locked_holdout_manifest_path"],
                root=ROOT,
                replay_full_parent=True,
            )
        except (OSError, KeyError, TypeError, ValueError, RuntimeError):
            raise self.failureException("ISOLATED_PROVENANCE_REPLAY_FAILED") from None
        self.assertEqual(manifest["manifest_file_sha256"], FULL_MANIFEST_SHA256)
        self.assertEqual(selection["parent_manifest_sha256"], FULL_MANIFEST_SHA256)
        self.assertEqual(preholdout["kind"], "PREHOLDOUT")
        self.assertEqual(locked["kind"], "LOCKED_HOLDOUT")
        self.assertEqual(
            preholdout["manifest_file_sha256"],
            selection["preholdout_manifest_sha256"],
        )
        self.assertEqual(
            locked["manifest_file_sha256"],
            selection["locked_holdout_manifest_sha256"],
        )
        self.assertEqual(
            preholdout["partition_descriptor_sha256"],
            selection["preholdout_partition_descriptor_sha256"],
        )
        self.assertEqual(
            locked["partition_descriptor_sha256"],
            selection["locked_partition_descriptor_sha256"],
        )
        self.assertEqual(
            preholdout["paired_partition_descriptor_sha256"],
            locked["partition_descriptor_sha256"],
        )
        self.assertEqual(
            locked["paired_partition_descriptor_sha256"],
            preholdout["partition_descriptor_sha256"],
        )
        self.assertEqual(
            preholdout["locked_holdout_manifest_path"],
            selection["locked_holdout_manifest_path"],
        )
        self.assertEqual(
            preholdout["locked_holdout_manifest_sha256"],
            selection["locked_holdout_manifest_sha256"],
        )
        for partition in (preholdout, locked):
            self.assertEqual(partition["parent_manifest_sha256"], FULL_MANIFEST_SHA256)
            self.assertEqual(
                partition["preholdout_sha256"],
                selection["preholdout_data_sha256"],
            )
            self.assertEqual(
                partition["holdout_commitment_sha256"],
                selection["holdout_commitment_sha256"],
            )
            self.assertEqual(partition["lockbox_id"], selection["lockbox_id"])

        registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        self.assertEqual(manifest["exception_registry_sha256"], DATA_EXCEPTION_REGISTRY_SHA256)
        self.assertEqual(manifest["row_count"], 78_372)
        self.assertEqual(manifest["normalized_sha256"], registry["source"]["normalized_sha256"])
        missing_hours = sum(
            (item["next_open_time_ms"] - item["previous_open_time_ms"]) // HOUR_MS - 1
            for item in registry["missing_hourly_bar_events"]
        )
        zero_volume_variances = sum(
            bool(item["zero_volume"]) for item in registry["close_time_source_variances"]
        )
        self.assertEqual(len(registry["missing_hourly_bar_events"]), 28)
        self.assertEqual(missing_hours, 128)
        self.assertEqual(zero_volume_variances, 4)


if __name__ == "__main__":
    unittest.main()
