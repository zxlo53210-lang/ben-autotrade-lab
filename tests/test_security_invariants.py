from __future__ import annotations

import ast
import hashlib
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from ben_trade_lab.audit import create_pro_review_receipt, create_test_receipt
from ben_trade_lab.cli import main
from ben_trade_lab.config import canonical_json, load_config
from ben_trade_lab.integrity import (
    FULL_PROVENANCE_REPLAY_TEST_ID,
    full_provenance_replay_evidence,
    source_tree_sha256,
    verified_hashed_object,
    write_immutable,
)
from ben_trade_lab.validation import (
    HOLDOUT_SUCCESS_REPORT_FIELDS,
    REPORT_KIND_LOCKED_OOS_EVALUATION,
    REPORT_KIND_TERMINAL_LIQUIDATION_FAILURE,
    VALIDATION_METHOD,
    _experiment_id,
    _verified_holdout_report_artifact,
    _verified_selection_artifact,
)

ROOT = Path(__file__).resolve().parents[1]
NETWORK_MODULES = {
    "aiohttp",
    "ftplib",
    "http",
    "httpx",
    "requests",
    "smtplib",
    "socket",
    "urllib",
    "websocket",
    "websockets",
}
BROKER_MODULES = {"alpaca_trade_api", "binance", "ccxt", "ib_insync"}
SAFE_ENVIRONMENT_KEYS = {
    "BEN_ISOLATED_PROVENANCE_REPLAY",
    "COMSPEC",
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "WINDIR",
}


def _selection(root: Path) -> Path:
    config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
    source_sha = source_tree_sha256(root)
    experiment_id = _experiment_id(
        method_version=VALIDATION_METHOD,
        config_sha256=config.config_sha256,
        source_tree_sha256_value=source_sha,
        lockbox_id="LOCKBOX_TEST_ONLY",
        preholdout_data_sha256="2" * 64,
        holdout_commitment_sha256="3" * 64,
        selected_params={},
    )
    value = {
        "schema_version": "1.2.0",
        "status": "FROZEN_CANDIDATE",
        "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
        "method": "CONTINUOUS_NINE_FOLD_VALIDATION_V2",
        "experiment_id": experiment_id,
        "preholdout_manifest_path": "data/manifests/preholdout-test.json",
        "preholdout_manifest_sha256": "4" * 64,
        "preholdout_partition_descriptor_sha256": "6" * 64,
        "locked_holdout_manifest_path": "data/manifests/locked-test.json",
        "locked_holdout_manifest_sha256": "7" * 64,
        "locked_partition_descriptor_sha256": "8" * 64,
        "config_sha256": config.config_sha256,
        "source_tree_sha256": source_sha,
        "preholdout_data_sha256": "2" * 64,
        "holdout_commitment_sha256": "3" * 64,
        "parent_manifest_sha256": "5" * 64,
        "lockbox_id": "LOCKBOX_TEST_ONLY",
        "trial_count": 16,
        "fold_count": 9,
        "fold_protocol": {},
        "selected_params": {},
        "selection_objective": "MEDIAN_WALK_FORWARD_CALMAR",
        "selection_bias_diagnostic": None,
        "parameter_adjacency_edges": [],
        "selected_parameter_neighbors": [],
        "preholdout_neighbor_count": 0,
        "preholdout_neighbor_positive_fraction": 0.0,
        "warmup_context_hours": 0,
        "warmup_context": [],
        "candidates": [],
    }
    value["selection_sha256"] = hashlib.sha256(canonical_json(value)).hexdigest()
    path = root / "artifacts" / f"selection-{value['selection_sha256'][:16]}.json"
    write_immutable(path, canonical_json(value) + b"\n")
    return path


class SourceBoundaryTests(unittest.TestCase):
    def test_no_live_order_or_secret_capability_in_source(self) -> None:
        source = "\n".join(
            path.read_text(encoding="utf-8") for path in sorted((ROOT / "src").rglob("*.py"))
        ).casefold()
        forbidden = (
            "/api/v3/account",
            "/api/v3/order",
            "/dapi/",
            "/fapi/",
            "/sapi/",
            "access_token",
            "api_key",
            "api_secret",
            "authorization:",
            "cancel_order(",
            "create_order(",
            "private_key",
            "submit_order(",
        )
        for marker in forbidden:
            self.assertNotIn(marker, source)

    def test_only_data_module_can_import_network_code(self) -> None:
        offenders: list[tuple[str, str]] = []
        broker_imports: list[tuple[str, str]] = []
        for path in sorted((ROOT / "src" / "ben_trade_lab").glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                modules: list[str] = []
                if isinstance(node, ast.Import):
                    modules.extend(alias.name.split(".")[0] for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    modules.append(node.module.split(".")[0])
                for module in modules:
                    if module in BROKER_MODULES:
                        broker_imports.append((path.name, module))
                    if module in NETWORK_MODULES and path.name != "data.py":
                        offenders.append((path.name, module))
        self.assertEqual(broker_imports, [])
        self.assertEqual(offenders, [])

    def test_source_cannot_read_unallowlisted_environment_values(self) -> None:
        offenders: list[tuple[str, str]] = []
        for path in sorted((ROOT / "src" / "ben_trade_lab").glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Attribute):
                    owner = node.value.value
                    if (
                        isinstance(owner, ast.Name)
                        and owner.id == "os"
                        and node.value.attr == "environ"
                    ):
                        offenders.append((path.name, "os.environ[]"))
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    continue
                owner = node.func.value
                if isinstance(owner, ast.Name) and owner.id == "os" and node.func.attr == "getenv":
                    offenders.append((path.name, "os.getenv"))
                if (
                    isinstance(owner, ast.Attribute)
                    and isinstance(owner.value, ast.Name)
                    and owner.value.id == "os"
                    and owner.attr == "environ"
                ):
                    if (
                        node.func.attr != "get"
                        or not node.args
                        or not isinstance(node.args[0], ast.Constant)
                    ):
                        offenders.append((path.name, f"os.environ.{node.func.attr}"))
                    elif node.args[0].value not in SAFE_ENVIRONMENT_KEYS:
                        offenders.append((path.name, f"environment:{node.args[0].value}"))
        self.assertEqual(offenders, [])

    def test_market_data_request_is_explicitly_get_only(self) -> None:
        data_source = (ROOT / "src" / "ben_trade_lab" / "data.py").read_text(encoding="utf-8")
        compact = "".join(data_source.split())
        self.assertIn('method="GET"', compact)
        for forbidden_method in ('method="POST"', 'method="PUT"', 'method="DELETE"'):
            self.assertNotIn(forbidden_method, compact)

    def test_live_cli_spellings_fail_closed_before_parsing(self) -> None:
        for spelling in (
            "LIVE",
            "--live",
            "--live=true",
            "live_trading",
            "live-execution",
            "--mode=LIVE",
            "--execution_mode=live",
        ):
            with self.subTest(spelling=spelling), redirect_stderr(io.StringIO()) as stream:
                self.assertEqual(main([spelling]), 2)
                self.assertIn("LIVE_EXECUTION_UNAVAILABLE", stream.getvalue())


class ReceiptIntegrityTests(unittest.TestCase):
    def test_selection_and_holdout_report_require_exact_v12_schemas(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / "tests").mkdir()
            valid_selection = _selection(root)
            self.assertEqual(
                _verified_selection_artifact(valid_selection)["schema_version"],
                "1.2.0",
            )
            verified_selection = _verified_selection_artifact(valid_selection)
            self.assertEqual(
                verified_selection["preholdout_manifest_path"],
                "data/manifests/preholdout-test.json",
            )
            self.assertEqual(
                verified_selection["locked_holdout_manifest_path"],
                "data/manifests/locked-test.json",
            )

            missing_binding = json.loads(valid_selection.read_text(encoding="utf-8"))
            missing_binding.pop("selection_sha256")
            missing_binding.pop("locked_partition_descriptor_sha256")
            missing_binding_sha = hashlib.sha256(canonical_json(missing_binding)).hexdigest()
            missing_binding["selection_sha256"] = missing_binding_sha
            missing_binding_path = root / f"missing-binding-{missing_binding_sha[:16]}.json"
            write_immutable(
                missing_binding_path,
                canonical_json(missing_binding) + b"\n",
            )
            with self.assertRaisesRegex(ValueError, "schema mismatch"):
                _verified_selection_artifact(missing_binding_path)

            selection = json.loads(valid_selection.read_text(encoding="utf-8"))
            selection.pop("selection_sha256")
            selection["schema_version"] = "1.1.0"
            old_selection_sha = hashlib.sha256(canonical_json(selection)).hexdigest()
            old_selection = root / f"old-selection-{old_selection_sha[:16]}.json"
            selection["selection_sha256"] = old_selection_sha
            write_immutable(old_selection, canonical_json(selection) + b"\n")
            with self.assertRaisesRegex(ValueError, "schema version mismatch"):
                _verified_selection_artifact(old_selection)

            forged = json.loads(valid_selection.read_text(encoding="utf-8"))
            forged.pop("selection_sha256")
            forged["experiment_id"] = "f" * 64
            forged_sha = hashlib.sha256(canonical_json(forged)).hexdigest()
            forged["selection_sha256"] = forged_sha
            forged_path = root / f"forged-selection-{forged_sha[:16]}.json"
            write_immutable(forged_path, canonical_json(forged) + b"\n")
            with self.assertRaisesRegex(ValueError, "experiment identity mismatch"):
                _verified_selection_artifact(forged_path)

            report = {
                field: "0" * 64 if field.endswith("_sha256") else None
                for field in HOLDOUT_SUCCESS_REPORT_FIELDS
                if field != "report_sha256"
            }
            report.update(
                {
                    "schema_version": "1.2.0",
                    "report_kind": REPORT_KIND_LOCKED_OOS_EVALUATION,
                    "status": "BACKTEST_CANDIDATE",
                }
            )
            report_sha = hashlib.sha256(canonical_json(report)).hexdigest()
            report["report_sha256"] = report_sha
            report_path = root / f"holdout-{report_sha[:16]}.json"
            write_immutable(report_path, canonical_json(report) + b"\n")
            self.assertEqual(
                _verified_holdout_report_artifact(report_path)["schema_version"],
                "1.2.0",
            )

            gate_failed = dict(report)
            gate_failed.pop("report_sha256")
            gate_failed["status"] = "NOT_PROVEN"
            gate_failed_sha = hashlib.sha256(canonical_json(gate_failed)).hexdigest()
            gate_failed["report_sha256"] = gate_failed_sha
            gate_failed_path = root / f"holdout-{gate_failed_sha[:16]}.json"
            write_immutable(gate_failed_path, canonical_json(gate_failed) + b"\n")
            verified_gate_failed = _verified_holdout_report_artifact(gate_failed_path)
            self.assertEqual(verified_gate_failed["status"], "NOT_PROVEN")
            self.assertEqual(
                verified_gate_failed["report_kind"],
                REPORT_KIND_LOCKED_OOS_EVALUATION,
            )

            wrong_kind = dict(gate_failed)
            wrong_kind.pop("report_sha256")
            wrong_kind["report_kind"] = REPORT_KIND_TERMINAL_LIQUIDATION_FAILURE
            wrong_kind_sha = hashlib.sha256(canonical_json(wrong_kind)).hexdigest()
            wrong_kind["report_sha256"] = wrong_kind_sha
            wrong_kind_path = root / f"holdout-{wrong_kind_sha[:16]}.json"
            write_immutable(wrong_kind_path, canonical_json(wrong_kind) + b"\n")
            with self.assertRaisesRegex(ValueError, "schema mismatch"):
                _verified_holdout_report_artifact(wrong_kind_path)

            report.pop("report_sha256")
            report["unexpected"] = True
            extra_sha = hashlib.sha256(canonical_json(report)).hexdigest()
            report["report_sha256"] = extra_sha
            extra_report = root / f"holdout-{extra_sha[:16]}.json"
            write_immutable(extra_report, canonical_json(report) + b"\n")
            with self.assertRaisesRegex(ValueError, "schema mismatch"):
                _verified_holdout_report_artifact(extra_report)

    def test_source_tree_fingerprint_is_deterministic(self) -> None:
        self.assertEqual(source_tree_sha256(ROOT), source_tree_sha256(ROOT))

    def test_immutable_write_refuses_different_existing_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "artifact.bin"
            write_immutable(path, b"first")
            write_immutable(path, b"first")
            with self.assertRaises(RuntimeError):
                write_immutable(path, b"second")
            self.assertEqual(path.read_bytes(), b"first")

    def test_test_receipt_binds_log_and_strips_credential_environment(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / "tests").mkdir()
            (root / "tests" / "test_environment.py").write_text(
                "import os\n"
                "import unittest\n\n"
                "class EnvironmentTest(unittest.TestCase):\n"
                "    def test_secret_is_absent(self):\n"
                "        self.assertNotIn('BEN_TEST_SECRET', os.environ)\n\n"
                "    def test_local_path_is_redacted(self):\n"
                "        local_path = os.path.join(os.environ['TEMP'], 'private')\n"
                "        self.skipTest(repr(local_path))\n",
                encoding="utf-8",
            )
            (root / "tests" / "test_data_registry.py").write_text(
                "import json\n"
                "import os\n"
                "from pathlib import Path\n"
                "import unittest\n\n"
                "class LocalSnapshotIntegrationTests(unittest.TestCase):\n"
                "    def test_full_raw_rebuild_emits_only_hashes_and_counters(self):\n"
                "        self.assertEqual(\n"
                "            os.environ.get('BEN_ISOLATED_PROVENANCE_REPLAY'), '1'\n"
                "        )\n"
                "        selection = os.environ.get('BEN_AUDIT_SELECTION_PATH', '')\n"
                "        self.assertTrue(selection.startswith('artifacts/selection-'))\n"
                "        self.assertTrue(selection.endswith('.json'))\n"
                "        expected_sha = os.environ.get('BEN_AUDIT_SELECTION_SHA256', '')\n"
                "        self.assertEqual(len(expected_sha), 64)\n"
                "        payload = json.loads(Path(selection).read_text(encoding='utf-8'))\n"
                "        self.assertEqual(payload['selection_sha256'], expected_sha)\n",
                encoding="utf-8",
            )
            selection = _selection(root)
            try:
                with patch.dict(os.environ, {"BEN_TEST_SECRET": "must-not-propagate"}):
                    receipt_path = create_test_receipt(root, selection, config)
            except RuntimeError as exc:
                logs = list((root / "artifacts").glob("test-log-*.txt"))
                evidence = logs[0].read_text(encoding="utf-8") if logs else "NO_LOG"
                self.fail(f"{exc}\n{evidence}")
            receipt = verified_hashed_object(receipt_path, "receipt_sha256")
            self.assertEqual(receipt["status"], "PASS")
            self.assertEqual(receipt["environment_policy"], "ALLOWLIST_NO_CREDENTIAL_ENV")
            self.assertEqual(receipt["full_provenance_replay"]["status"], "PASS")

            log = root / receipt["normalized_output_path"]
            log_text = log.read_text(encoding="utf-8")
            for key in ("TEMP", "TMP"):
                if value := os.environ.get(key):
                    self.assertNotIn(value, log_text)
                    self.assertNotIn(repr(value)[1:-1], log_text)
            self.assertIn("<REDACTED_LOCAL_PATH>", log_text)
            log.write_bytes(log.read_bytes() + b"tampered\n")
            with self.assertRaises(ValueError):
                verified_hashed_object(receipt_path, "receipt_sha256")

    def test_full_provenance_replay_evidence_distinguishes_all_fail_closed_states(self) -> None:
        method = FULL_PROVENANCE_REPLAY_TEST_ID.rsplit(".", 1)[-1]
        prefix = f"{method} ({FULL_PROVENANCE_REPLAY_TEST_ID}) ... "
        self.assertEqual(full_provenance_replay_evidence(prefix + "ok\n")["status"], "PASS")
        self.assertEqual(
            full_provenance_replay_evidence(prefix + "skipped 'snapshot absent'\n")["status"],
            "SKIPPED",
        )
        self.assertEqual(full_provenance_replay_evidence("Ran 1 tests\n")["status"], "ABSENT")
        self.assertEqual(
            full_provenance_replay_evidence(prefix + "ERROR\n")["status"], "FAIL"
        )
        self.assertEqual(
            full_provenance_replay_evidence(prefix + "ok\n" + prefix + "ok\n")["status"],
            "AMBIGUOUS",
        )

    def test_test_receipt_records_skipped_and_absent_provenance_replay(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        scenarios = {
            "SKIPPED": (
                "import unittest\n\n"
                "class LocalSnapshotIntegrationTests(unittest.TestCase):\n"
                "    @unittest.skip('synthetic snapshot absent')\n"
                "    def test_full_raw_rebuild_emits_only_hashes_and_counters(self):\n"
                "        self.fail('must remain skipped')\n"
            ),
            "ABSENT": (
                "import unittest\n\n"
                "class OtherTests(unittest.TestCase):\n"
                "    def test_unrelated(self):\n"
                "        self.assertTrue(True)\n"
            ),
        }
        for expected_status, source in scenarios.items():
            with (
                self.subTest(expected_status=expected_status),
                tempfile.TemporaryDirectory() as temp,
            ):
                root = Path(temp)
                (root / "src").mkdir()
                (root / "tests").mkdir()
                (root / "tests" / "test_data_registry.py").write_text(
                    source, encoding="utf-8"
                )
                selection = _selection(root)
                receipt_path = create_test_receipt(root, selection, config)
                receipt = verified_hashed_object(receipt_path, "receipt_sha256")
                self.assertEqual(receipt["status"], "PASS")
                self.assertEqual(
                    receipt["full_provenance_replay"]["status"], expected_status
                )

    def test_review_receipt_binds_sanitized_review_and_labels(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / "tests").mkdir()
            review = root / "docs" / "reviews" / "pro.md"
            review.parent.mkdir(parents=True)
            review.write_text(
                "Sanitized review: proceed.\n\nPROCEED\n\n",
                encoding="utf-8",
            )
            selection = _selection(root)
            receipt_path = create_pro_review_receipt(
                root,
                selection,
                config,
                review,
                "PROCEED",
                "GPT-5.6 Sol",
                "Pro",
            )
            receipt = verified_hashed_object(receipt_path, "receipt_sha256")
            self.assertEqual(receipt["verdict"], "PROCEED")

            mismatched_receipt = dict(receipt)
            mismatched_receipt.pop("receipt_sha256")
            mismatched_receipt["verdict"] = "BLOCKED"
            mismatched_sha = hashlib.sha256(canonical_json(mismatched_receipt)).hexdigest()
            mismatched_receipt["receipt_sha256"] = mismatched_sha
            mismatched_path = (
                root / "artifacts" / f"pro-review-receipt-{mismatched_sha[:16]}.json"
            )
            write_immutable(
                mismatched_path,
                canonical_json(mismatched_receipt) + b"\n",
            )
            with self.assertRaisesRegex(ValueError, "verdict does not match"):
                verified_hashed_object(mismatched_path, "receipt_sha256")

            review.write_text("replaced", encoding="utf-8")
            with self.assertRaises(ValueError):
                verified_hashed_object(receipt_path, "receipt_sha256")

    def test_review_receipt_creation_rejects_terminal_verdict_mismatch(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / "tests").mkdir()
            review = root / "docs" / "reviews" / "pro.md"
            review.parent.mkdir(parents=True)
            review.write_text("Sanitized findings.\n\nBLOCKED\n", encoding="utf-8")
            selection = _selection(root)

            with self.assertRaisesRegex(ValueError, "final non-blank line"):
                create_pro_review_receipt(
                    root,
                    selection,
                    config,
                    review,
                    "PROCEED",
                    "GPT-5.6 Sol",
                    "Pro",
                )
            self.assertEqual(list((root / "artifacts").glob("pro-review-receipt-*.json")), [])


if __name__ == "__main__":
    unittest.main()
