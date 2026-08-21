from __future__ import annotations

import ast
import hashlib
import io
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
    source_tree_sha256,
    verified_hashed_object,
    write_immutable,
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
    value = {
        "status": "FROZEN_CANDIDATE",
        "authority": "RESEARCH_ONLY_ZERO_LIVE_AUTHORITY",
        "experiment_id": "1" * 64,
        "config_sha256": config.config_sha256,
        "source_tree_sha256": source_tree_sha256(root),
        "preholdout_data_sha256": "2" * 64,
        "holdout_commitment_sha256": "3" * 64,
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
                "        self.assertNotIn('BEN_TEST_SECRET', os.environ)\n",
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

            log = root / receipt["normalized_output_path"]
            log.write_bytes(log.read_bytes() + b"tampered\n")
            with self.assertRaises(ValueError):
                verified_hashed_object(receipt_path, "receipt_sha256")

    def test_review_receipt_binds_sanitized_review_and_labels(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / "tests").mkdir()
            review = root / "docs" / "reviews" / "pro.md"
            review.parent.mkdir(parents=True)
            review.write_text("Sanitized review: proceed.\n", encoding="utf-8")
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
            review.write_text("replaced", encoding="utf-8")
            with self.assertRaises(ValueError):
                verified_hashed_object(receipt_path, "receipt_sha256")


if __name__ == "__main__":
    unittest.main()
