from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from test_paper_authorization import _complete_provenance

from ben_trade_lab.config import canonical_json, load_config
from ben_trade_lab.data import (
    DataIntegrityError,
    _expand_gaps_carry_forward,
    _market_data_get,
    _validate_rows,
)
from ben_trade_lab.integrity import source_tree_sha256
from ben_trade_lab.models import Bar
from ben_trade_lab.paper import initialize_paper, paper_status, stop_paper, verify_journal

ROOT = Path(__file__).resolve().parents[1]


def row(index: int) -> tuple[str, ...]:
    start = index * 3_600_000
    return (
        str(start),
        "100",
        "102",
        "99",
        "101",
        "1",
        str(start + 3_600_000 - 1),
        "101",
        "1",
        "0.5",
        "50.5",
        "0",
    )


class DataTests(unittest.TestCase):
    def test_contiguous_rows_pass(self) -> None:
        rows = [row(index) for index in range(3)]
        self.assertEqual(_validate_rows(rows, 0, 3 * 3_600_000), rows)

    def test_gap_fails_closed(self) -> None:
        with self.assertRaises(DataIntegrityError):
            _validate_rows([row(0), row(2)], 0, 3 * 3_600_000)

    def test_predeclared_gap_is_ledgered_and_expanded_without_fill_bar(self) -> None:
        ledger: list[dict[str, object]] = []
        _validate_rows(
            [row(0), row(2)],
            0,
            3 * 3_600_000,
            anomaly_sink=ledger,
            allow_declared_gaps=True,
        )
        self.assertEqual(ledger[0]["type"], "MISSING_HOURLY_BARS")
        official = [
            Bar(0, 100, 101, 99, 100, 1, 3_599_999),
            Bar(7_200_000, 110, 111, 109, 110, 1, 10_799_999),
        ]
        expanded = _expand_gaps_carry_forward(official)
        self.assertEqual(len(expanded), 3)
        self.assertFalse(expanded[0].synthetic)
        self.assertTrue(expanded[1].synthetic)
        self.assertEqual(expanded[1].close, 100)
        self.assertEqual(expanded[1].volume, 0.0)
        self.assertEqual(expanded[1].open, expanded[1].close)
        self.assertEqual(expanded[1].high, expanded[1].close)
        self.assertEqual(expanded[1].low, expanded[1].close)
        self.assertEqual(expanded[1].close_time_ms, 7_199_999)
        self.assertFalse(expanded[2].synthetic)

    def test_official_zero_volume_bar_is_not_relabelled_synthetic(self) -> None:
        official = [Bar(0, 100, 100, 100, 100, 0, 3_599_999)]
        expanded = _expand_gaps_carry_forward(official)
        self.assertEqual(expanded, official)
        self.assertFalse(expanded[0].synthetic)
        self.assertEqual(expanded[0].volume, 0)

    def test_in_interval_source_close_time_variance_is_declared(self) -> None:
        anomalous = list(row(0))
        anomalous[6] = anomalous[0]
        ledger: list[dict[str, object]] = []
        _validate_rows([tuple(anomalous)], 0, 3_600_000, anomaly_sink=ledger)
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger[0]["type"], "CLOSE_TIME_SOURCE_VARIANCE")

    def test_out_of_interval_close_time_fails_closed(self) -> None:
        invalid = list(row(0))
        invalid[6] = str(3_600_000)
        with self.assertRaises(DataIntegrityError):
            _validate_rows([tuple(invalid)], 0, 3_600_000)

    def test_non_allowlisted_market_data_url_is_rejected_before_network(self) -> None:
        with self.assertRaises(ValueError):
            _market_data_get("https://example.com/api/v3/klines")


class PaperJournalTests(unittest.TestCase):
    def _report(self, root: Path, config, status: str = "BACKTEST_CANDIDATE") -> Path:
        if status == "BACKTEST_CANDIDATE":
            provenance_config, report = _complete_provenance(root)
            self.assertEqual(provenance_config.config_sha256, config.config_sha256)
            return report
        value = {
            "status": status,
            "capability": "LIVE_DISABLED",
            "config_sha256": config.config_sha256,
            "source_tree_sha256": source_tree_sha256(root),
            "selection_sha256": "a" * 64,
        }
        value["report_sha256"] = hashlib.sha256(canonical_json(value)).hexdigest()
        path = root / "artifacts" / f"holdout-{value['report_sha256'][:16]}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical_json(value) + b"\n")
        return path

    def test_journal_is_hash_chained_and_stoppable(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = self._report(root, config)
            initialized = initialize_paper(root, report, config, 1000.0)
            self.assertEqual(initialized["status"], "PAPER_INITIALIZED")
            self.assertEqual(paper_status(root)["event_count"], 1)
            stopped = stop_paper(root)
            self.assertTrue(stopped["stopped"])
            self.assertEqual(stopped["event_count"], 2)
            journal = root / "state" / "paper" / "journal.jsonl"
            self.assertEqual(len(verify_journal(journal)), 2)

    def test_not_proven_report_cannot_initialize_paper(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = self._report(root, config, status="NOT_PROVEN")
            with self.assertRaises(ValueError):
                initialize_paper(root, report, config, 1000.0)

    def test_valid_prefix_truncation_is_detected_by_head_commitment(self) -> None:
        config = load_config(ROOT / "configs" / "btcusdt_1h.toml")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = self._report(root, config)
            initialize_paper(root, report, config, 1000.0)
            stop_paper(root)
            journal = root / "state" / "paper" / "journal.jsonl"
            first = journal.read_bytes().splitlines(keepends=True)[0]
            journal.write_bytes(first)
            with self.assertRaises(RuntimeError):
                verify_journal(journal)

    def test_tampered_journal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            journal = Path(temporary) / "journal.jsonl"
            journal.write_text(
                '{"event_hash":"bad","payload":{},"previous_hash":"'
                + "0" * 64
                + '","sequence":0,"timestamp_utc":"2026-01-01T00:00:00Z"}\n',
                encoding="utf-8",
            )
            with self.assertRaises(RuntimeError):
                verify_journal(journal)


if __name__ == "__main__":
    unittest.main()
