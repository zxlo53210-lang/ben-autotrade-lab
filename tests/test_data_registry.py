from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ben_trade_lab.data import (
    DATA_EXCEPTION_REGISTRY_SHA256,
    HOUR_MS,
    DataIntegrityError,
    _assert_registered_full_source_identity,
    _csv_bytes,
    _load_exception_registry,
    _lockbox_id,
    _validate_registered_exceptions,
    _verify_partition_provenance,
    load_bars_from_manifest,
)
from ben_trade_lab.integrity import source_files

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
    def test_partition_must_be_exact_canonical_parent_slice(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifests = root / "data" / "manifests"
            normalized = root / "data" / "normalized"
            manifests.mkdir(parents=True)
            normalized.mkdir(parents=True)
            parent_manifest_path = manifests / "parent.json"
            parent_manifest_path.write_bytes(b"parent-manifest-fixture\n")
            parent_manifest_sha = hashlib.sha256(parent_manifest_path.read_bytes()).hexdigest()
            parent_rows = [fixture_row(index) for index in range(4)]
            parent_payload = _csv_bytes(parent_rows)
            parent_csv_path = normalized / "parent.csv"
            parent_csv_path.write_bytes(parent_payload)
            parent_normalized_sha = hashlib.sha256(parent_payload).hexdigest()
            registry_sha = "a" * 64
            parent = {
                "kind": "FULL_SOURCE",
                "manifest_path": "data/manifests/parent.json",
                "manifest_file_sha256": parent_manifest_sha,
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
            partition_rows = parent_rows[:2]
            partition_payload = _csv_bytes(partition_rows)
            preholdout_sha = hashlib.sha256(partition_payload).hexdigest()
            holdout_sha = hashlib.sha256(_csv_bytes(parent_rows[2:])).hexdigest()
            config_sha = "b" * 64
            manifest = {
                "kind": "PREHOLDOUT",
                "source": parent["source"],
                "http_method": "GET",
                "authentication": "NONE",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "timezone": "UTC",
                "gap_policy": "CARRY_FORWARD_NO_FILL",
                "requested_start_ms": 0,
                "requested_end_ms_exclusive": 2 * HOUR_MS,
                "normalized_path": (
                    f"data/normalized/BTCUSDT-1h-preholdout-0-7200000-{preholdout_sha[:16]}.csv"
                ),
                "normalized_sha256": preholdout_sha,
                "config_sha256": config_sha,
                "exception_registry_sha256": registry_sha,
                "parent_manifest_path": "data/manifests/parent.json",
                "parent_manifest_sha256": parent_manifest_sha,
                "parent_normalized_sha256": parent_normalized_sha,
                "preholdout_sha256": preholdout_sha,
                "holdout_commitment_sha256": holdout_sha,
            }
            manifest["lockbox_id"] = _lockbox_id(
                config_sha256=config_sha,
                exception_registry_sha256=registry_sha,
                parent_normalized_sha256=parent_normalized_sha,
                preholdout_sha256=preholdout_sha,
                holdout_commitment_sha256=holdout_sha,
                holdout_start_ms=2 * HOUR_MS,
                holdout_end_ms_exclusive=4 * HOUR_MS,
            )
            with patch("ben_trade_lab.data.verify_manifest", return_value=parent):
                _verify_partition_provenance(
                    manifest,
                    partition_rows,
                    partition_payload,
                    root,
                    observed_at_ms=5 * HOUR_MS,
                )
                changed = list(partition_rows)
                mutated = list(changed[1])
                mutated[4] = "100.5"
                changed[1] = tuple(mutated)
                with self.assertRaisesRegex(DataIntegrityError, "exact parent slice"):
                    _verify_partition_provenance(
                        manifest,
                        changed,
                        _csv_bytes(changed),
                        root,
                        observed_at_ms=5 * HOUR_MS,
                    )


@unittest.skipUnless(
    full_local_snapshot_available(),
    "local raw and normalized full-source snapshot not present",
)
class LocalSnapshotIntegrationTests(unittest.TestCase):
    def test_full_raw_rebuild_registry_rows_gaps_and_flags(self) -> None:
        bars, manifest = load_bars_from_manifest(FULL_MANIFEST, root=ROOT)
        registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        self.assertEqual(manifest["exception_registry_sha256"], DATA_EXCEPTION_REGISTRY_SHA256)
        self.assertEqual(manifest["row_count"], 78_372)
        self.assertEqual(len(bars), 78_500)
        expected_synthetic_times: set[int] = set()
        for gap in registry["missing_hourly_bar_events"]:
            expected_synthetic_times.update(
                range(
                    gap["previous_open_time_ms"] + HOUR_MS,
                    gap["next_open_time_ms"],
                    HOUR_MS,
                )
            )
        synthetic = {bar.open_time_ms: bar for bar in bars if bar.synthetic}
        self.assertEqual(set(synthetic), expected_synthetic_times)
        self.assertEqual(len(synthetic), 128)
        for bar in synthetic.values():
            self.assertEqual(bar.volume, 0.0)
            self.assertEqual(bar.open, bar.close)
            self.assertEqual(bar.high, bar.close)
            self.assertEqual(bar.low, bar.close)
            self.assertEqual(bar.close_time_ms, bar.open_time_ms + HOUR_MS - 1)
        by_open = {bar.open_time_ms: bar for bar in bars}
        zero_volume_exception_times = {
            item["open_time_ms"]
            for item in registry["close_time_source_variances"]
            if item["zero_volume"]
        }
        self.assertEqual(len(zero_volume_exception_times), 4)
        for open_time in zero_volume_exception_times:
            self.assertFalse(by_open[open_time].synthetic)
            self.assertEqual(by_open[open_time].volume, 0.0)


if __name__ == "__main__":
    unittest.main()
