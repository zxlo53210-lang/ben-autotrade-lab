from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import urllib.parse
import urllib.request
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .config import ALLOWED_MARKET_DATA_URL, LabConfig, canonical_json, parse_utc_ms
from .integrity import (
    require_plain_parent_chain_for_create,
    require_plain_regular_single_link,
)
from .models import Bar

HOUR_MS = 3_600_000
DATA_EXCEPTION_REGISTRY_PATH = "configs/data_exceptions_v1.json"
DATA_EXCEPTION_REGISTRY_SHA256 = "5d45208bf3ca72d38b92b671694c74921c5a3ee3ddbae850b30ee7893ca0a582"
PARTITION_SCHEMA_VERSION = "1.2.0"
PARTITION_BASE_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "source",
        "http_method",
        "authentication",
        "symbol",
        "interval",
        "timezone",
        "gap_policy",
        "requested_start_ms",
        "requested_end_ms_exclusive",
        "first_open_ms",
        "last_open_ms",
        "row_count",
        "normalized_path",
        "normalized_sha256",
        "config_sha256",
        "exception_registry_sha256",
        "validation",
        "declared_source_anomalies",
        "parent_manifest_path",
        "parent_manifest_sha256",
        "parent_normalized_sha256",
        "lockbox_id",
        "preholdout_sha256",
        "holdout_commitment_sha256",
    }
)
PARTITION_LINK_FIELDS = frozenset(
    {
        "partition_descriptor_sha256",
        "paired_partition_kind",
        "paired_partition_descriptor_sha256",
    }
)
PREHOLDOUT_LOCKED_MANIFEST_FIELDS = frozenset(
    {
        "locked_holdout_manifest_path",
        "locked_holdout_manifest_sha256",
    }
)
KLINE_COLUMNS = (
    "open_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time_ms",
    "quote_asset_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
)


class DataIntegrityError(RuntimeError):
    pass


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise DataIntegrityError(f"market data redirect rejected: HTTP {code}")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _isolated_provenance_replay_enabled() -> bool:
    """Read the one non-credential environment gate used by provenance replay."""

    return os.environ.get("BEN_ISOLATED_PROVENANCE_REPLAY") == "1"


def _require_plain_data_file(path: Path, label: str) -> Path:
    """Translate the shared plain-file boundary into the data error domain."""

    try:
        return require_plain_regular_single_link(path, label)
    except (OSError, ValueError) as exc:
        raise DataIntegrityError(f"{label} must be a stable plain single-link file") from exc


def _read_plain_data_bytes(path: Path, label: str) -> bytes:
    """Read one plain file and retain only bytes observed inside two link checks."""

    _require_plain_data_file(path, label)
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise DataIntegrityError(f"{label} is unavailable") from exc
    _require_plain_data_file(path, label)
    return payload


def _canonical_decimal(value: Any) -> str:
    try:
        decimal = Decimal(str(value))
    except InvalidOperation as exc:
        raise DataIntegrityError(f"invalid decimal: {value!r}") from exc
    if not decimal.is_finite():
        raise DataIntegrityError(f"non-finite decimal: {value!r}")
    if decimal == 0:
        return "0"
    rendered = format(decimal.normalize(), "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def _canonical_int(value: Any) -> str:
    try:
        decimal = Decimal(str(value))
    except InvalidOperation as exc:
        raise DataIntegrityError(f"invalid integer: {value!r}") from exc
    if not decimal.is_finite() or decimal != decimal.to_integral_value():
        raise DataIntegrityError(f"non-integral integer field: {value!r}")
    return str(int(decimal))


def _write_new_or_same(path: Path, payload: bytes) -> None:
    try:
        require_plain_parent_chain_for_create(path, "immutable data file")
    except (OSError, ValueError) as exc:
        raise DataIntegrityError("immutable data parent path is unsafe") from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        require_plain_parent_chain_for_create(path, "immutable data file")
    except (OSError, ValueError) as exc:
        raise DataIntegrityError("immutable data parent path is unsafe") from exc
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        if _read_plain_data_bytes(path, "immutable data file") != payload:
            raise DataIntegrityError(f"immutable file collision: {path}")
    _require_plain_data_file(path, "immutable data file")


def _market_data_get(url: str, timeout_seconds: float = 30.0) -> bytes:
    parsed = urllib.parse.urlsplit(url)
    allowlisted = urllib.parse.urlsplit(ALLOWED_MARKET_DATA_URL)
    if (
        parsed.scheme != "https"
        or parsed.netloc != allowlisted.netloc
        or parsed.path != "/api/v3/klines"
    ):
        raise ValueError("market data URL is not allowlisted")
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    if set(query) != {"symbol", "interval", "startTime", "endTime", "limit", "timeZone"}:
        raise ValueError("market data query contract mismatch")
    if query["symbol"] != ["BTCUSDT"] or query["interval"] != ["1h"]:
        raise ValueError("market data symbol or interval is not allowlisted")
    if query["limit"] != ["1000"] or query["timeZone"] != ["0"]:
        raise ValueError("market data pagination or timezone contract mismatch")
    for field in ("startTime", "endTime"):
        if len(query[field]) != 1 or not query[field][0].isdigit():
            raise ValueError(f"market data {field} must be one millisecond integer")
    request = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": "ben-autotrade-lab/0.2",
            "X-MBX-TIME-UNIT": "millisecond",
        },
    )
    opener = urllib.request.build_opener(_RejectRedirects())
    with opener.open(request, timeout=timeout_seconds) as response:
        if response.status != 200:
            raise RuntimeError(f"market data request failed with HTTP {response.status}")
        final = urllib.parse.urlsplit(response.geturl())
        if final.scheme != "https" or final.netloc != allowlisted.netloc:
            raise DataIntegrityError("market data response escaped the allowlisted host")
        return response.read()


def _parse_batch(payload: bytes) -> list[list[Any]]:
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataIntegrityError("market data response is not valid JSON") from exc
    if not isinstance(decoded, list):
        raise DataIntegrityError("market data response must be a list")
    rows: list[list[Any]] = []
    for item in decoded:
        if not isinstance(item, list) or len(item) != 12:
            raise DataIntegrityError("invalid kline row shape")
        rows.append(item)
    return rows


def _normalized_row(item: list[Any]) -> tuple[str, ...]:
    return (
        _canonical_int(item[0]),
        _canonical_decimal(item[1]),
        _canonical_decimal(item[2]),
        _canonical_decimal(item[3]),
        _canonical_decimal(item[4]),
        _canonical_decimal(item[5]),
        _canonical_int(item[6]),
        _canonical_decimal(item[7]),
        _canonical_int(item[8]),
        _canonical_decimal(item[9]),
        _canonical_decimal(item[10]),
        _canonical_decimal(item[11]),
    )


def _validate_rows(
    rows: Iterable[tuple[str, ...]],
    start_ms: int,
    end_ms_exclusive: int,
    anomaly_sink: list[dict[str, Any]] | None = None,
    allow_declared_gaps: bool = False,
) -> list[tuple[str, ...]]:
    accepted = list(rows)
    if not accepted:
        raise DataIntegrityError("dataset is empty")
    previous: int | None = None
    missing_bar_count = 0
    for row in accepted:
        open_time = int(row[0])
        close_time = int(row[6])
        open_, high, low, close, volume = (Decimal(row[index]) for index in range(1, 6))
        quote_volume = Decimal(row[7])
        trade_count = int(row[8])
        taker_base = Decimal(row[9])
        taker_quote = Decimal(row[10])
        if open_time % HOUR_MS != 0:
            raise DataIntegrityError(f"bar is not hour-aligned: {open_time}")
        expected_close = open_time + HOUR_MS - 1
        if not open_time <= close_time <= expected_close:
            raise DataIntegrityError(f"close time lies outside its bar: {open_time}")
        if close_time != expected_close and anomaly_sink is not None:
            anomaly_sink.append(
                {
                    "type": "CLOSE_TIME_SOURCE_VARIANCE",
                    "open_time_ms": open_time,
                    "observed_close_time_ms": close_time,
                    "expected_interval_end_ms": expected_close,
                    "trade_count": trade_count,
                    "zero_volume": volume == 0,
                }
            )
        if not start_ms <= open_time < end_ms_exclusive:
            raise DataIntegrityError(f"bar lies outside requested interval: {open_time}")
        if previous is not None and open_time != previous + HOUR_MS:
            difference = open_time - previous
            if difference <= 0 or difference % HOUR_MS != 0:
                raise DataIntegrityError(f"duplicate, disorder, or misaligned gap at: {open_time}")
            if not allow_declared_gaps:
                raise DataIntegrityError(f"undeclared gap before: {open_time}")
            missing = difference // HOUR_MS - 1
            missing_bar_count += missing
            if anomaly_sink is not None:
                anomaly_sink.append(
                    {
                        "type": "MISSING_HOURLY_BARS",
                        "previous_open_time_ms": previous,
                        "next_open_time_ms": open_time,
                        "missing_bar_count": missing,
                    }
                )
        if low > min(open_, close) or high < max(open_, close) or low > high:
            raise DataIntegrityError(f"invalid OHLC relationship at: {open_time}")
        if (
            min(open_, high, low, close) <= 0
            or min(volume, quote_volume, taker_base, taker_quote) < 0
            or trade_count < 0
        ):
            raise DataIntegrityError(f"invalid price or volume at: {open_time}")
        previous = open_time
    if int(accepted[0][0]) != start_ms:
        raise DataIntegrityError("first bar does not match requested start")
    if int(accepted[-1][0]) != end_ms_exclusive - HOUR_MS:
        raise DataIntegrityError("last bar does not match requested end")
    expected = (end_ms_exclusive - start_ms) // HOUR_MS - missing_bar_count
    if len(accepted) != expected:
        raise DataIntegrityError(f"expected {expected} bars, received {len(accepted)}")
    return accepted


def _csv_bytes(rows: list[tuple[str, ...]]) -> bytes:
    lines = [",".join(KLINE_COLUMNS)]
    lines.extend(",".join(row) for row in rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


def _require_sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise DataIntegrityError(f"data exception registry {field} is not a SHA-256")
    return value


def _validate_exception_registry_structure(registry: dict[str, Any]) -> None:
    if registry.get("schema_version") != "1.0.0":
        raise DataIntegrityError("unexpected data exception registry schema")
    if registry.get("registry_name") != "BTCUSDT_1H_DATA_EXCEPTIONS_V1":
        raise DataIntegrityError("unexpected data exception registry")
    source = registry.get("source")
    if not isinstance(source, dict) or source.get("source_row_modified") is not False:
        raise DataIntegrityError("data exception registry must preserve source rows")
    manifest_sha = _require_sha256(source.get("manifest_sha256"), "manifest_sha256")
    normalized_sha = _require_sha256(source.get("normalized_sha256"), "normalized_sha256")
    manifest_path = source.get("manifest_path")
    normalized_path = source.get("normalized_path")
    if (
        not isinstance(manifest_path, str)
        or not manifest_path.startswith("data/manifests/")
        or not manifest_path.endswith(f"-{manifest_sha[:16]}.json")
    ):
        raise DataIntegrityError("data exception registry manifest path is not bound")
    if (
        not isinstance(normalized_path, str)
        or not normalized_path.startswith("data/normalized/")
        or not normalized_path.endswith(f"-{normalized_sha[:16]}.csv")
    ):
        raise DataIntegrityError("data exception registry normalized path is not bound")
    canonical = registry.get("canonical_full_row_hash")
    if not isinstance(canonical, dict):
        raise DataIntegrityError("data exception registry row hash contract is missing")
    if (
        canonical.get("algorithm") != "SHA-256"
        or canonical.get("encoding") != "UTF-8"
        or canonical.get("column_order") != list(KLINE_COLUMNS)
        or canonical.get("serialization")
        != (
            "comma-join the 12 normalized field values in column_order with no "
            "quoting and no line terminator"
        )
    ):
        raise DataIntegrityError("data exception registry row hash contract changed")
    policy = registry.get("policy")
    if policy != {"name": "CARRY_FORWARD_NO_FILL", "version": "1.0.0"}:
        raise DataIntegrityError("data exception registry policy mismatch")
    close_events = registry.get("close_time_source_variances")
    gap_events = registry.get("missing_hourly_bar_events")
    if not isinstance(close_events, list) or not isinstance(gap_events, list):
        raise DataIntegrityError("data exception registry event arrays are missing")
    if len(close_events) != 14 or len(gap_events) != 28:
        raise DataIntegrityError("data exception registry event count mismatch")
    previous_close_open = -1
    for event in close_events:
        if not isinstance(event, dict) or event.get("type") != "CLOSE_TIME_SOURCE_VARIANCE":
            raise DataIntegrityError("invalid registered close-time event")
        required = {
            "type",
            "open_time_ms",
            "observed_close_time_ms",
            "expected_interval_end_ms",
            "trade_count",
            "zero_volume",
            "canonical_full_row_sha256",
        }
        if set(event) != required:
            raise DataIntegrityError("registered close-time event schema mismatch")
        try:
            open_time = int(event["open_time_ms"])
            observed_close = int(event["observed_close_time_ms"])
            expected_close = int(event["expected_interval_end_ms"])
            trade_count = int(event["trade_count"])
        except (TypeError, ValueError) as exc:
            raise DataIntegrityError("registered close-time event field is invalid") from exc
        if (
            open_time <= previous_close_open
            or open_time % HOUR_MS != 0
            or expected_close != open_time + HOUR_MS - 1
            or not open_time <= observed_close < expected_close
            or trade_count < 0
            or type(event.get("zero_volume")) is not bool
        ):
            raise DataIntegrityError("registered close-time event is inconsistent")
        _require_sha256(event.get("canonical_full_row_sha256"), "canonical_full_row_sha256")
        previous_close_open = open_time
    previous_gap_open = -1
    missing_total = 0
    maximum_missing = 0
    for event in gap_events:
        if not isinstance(event, dict) or event.get("type") != "MISSING_HOURLY_BARS":
            raise DataIntegrityError("invalid registered gap event")
        required = {
            "type",
            "previous_open_time_ms",
            "next_open_time_ms",
            "missing_bar_count",
        }
        if set(event) != required:
            raise DataIntegrityError("registered gap event schema mismatch")
        try:
            previous_open = int(event["previous_open_time_ms"])
            next_open = int(event["next_open_time_ms"])
            missing = int(event["missing_bar_count"])
        except (TypeError, ValueError) as exc:
            raise DataIntegrityError("registered gap event field is invalid") from exc
        if (
            previous_open <= previous_gap_open
            or previous_open % HOUR_MS != 0
            or next_open % HOUR_MS != 0
            or missing <= 0
            or next_open - previous_open != (missing + 1) * HOUR_MS
        ):
            raise DataIntegrityError("registered gap event is inconsistent")
        previous_gap_open = previous_open
        missing_total += missing
        maximum_missing = max(maximum_missing, missing)
    expected_totals = {
        "close_time_source_variance_events": 14,
        "missing_hourly_bar_events": 28,
        "missing_hourly_bars": 128,
        "maximum_missing_hourly_bars_in_one_event": 33,
    }
    if registry.get("totals") != expected_totals or missing_total != 128 or maximum_missing != 33:
        raise DataIntegrityError("data exception registry totals mismatch")


def _load_exception_registry(
    root: Path, *, expected_sha256: str | None = None
) -> tuple[dict[str, Any], str]:
    unresolved = root / DATA_EXCEPTION_REGISTRY_PATH
    path = unresolved.resolve()
    try:
        path.relative_to((root / "configs").resolve())
    except ValueError as exc:
        raise DataIntegrityError("data exception registry escaped configs") from exc
    _require_plain_data_file(unresolved, "data exception registry")
    if unresolved.resolve() != path:
        raise DataIntegrityError("data exception registry changed during path validation")
    payload = _read_plain_data_bytes(path, "data exception registry")
    registry_sha = _sha256(payload)
    if registry_sha != DATA_EXCEPTION_REGISTRY_SHA256:
        raise DataIntegrityError("data exception registry hash mismatch")
    if expected_sha256 is not None and registry_sha != expected_sha256:
        raise DataIntegrityError("data exception registry commitment mismatch")
    try:
        registry = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataIntegrityError("data exception registry is invalid") from exc
    if not isinstance(registry, dict):
        raise DataIntegrityError("data exception registry must be an object")
    _validate_exception_registry_structure(registry)
    return registry, registry_sha


def _validate_registered_exceptions(
    rows: list[tuple[str, ...]],
    anomalies: list[dict[str, Any]],
    *,
    start_ms: int,
    end_ms_exclusive: int,
    full_source_sha256: str,
    gap_policy: str,
    root: Path,
    expected_registry_sha256: str | None = None,
) -> str:
    registry, registry_sha = _load_exception_registry(
        root, expected_sha256=expected_registry_sha256
    )
    if registry["source"]["normalized_sha256"] != full_source_sha256:
        raise DataIntegrityError("SOURCE_DRIFT: dataset is not the registered source snapshot")
    policy = registry["policy"]
    if policy.get("name") != gap_policy or policy.get("version") != "1.0.0":
        raise DataIntegrityError("data exception policy mismatch")
    expected_close = [
        item
        for item in registry["close_time_source_variances"]
        if start_ms <= int(item["open_time_ms"]) < end_ms_exclusive
    ]
    expected_gaps = [
        item
        for item in registry["missing_hourly_bar_events"]
        if start_ms <= int(item["previous_open_time_ms"])
        and int(item["next_open_time_ms"]) < end_ms_exclusive
    ]
    actual_close = [item for item in anomalies if item.get("type") == "CLOSE_TIME_SOURCE_VARIANCE"]
    actual_gaps = [item for item in anomalies if item.get("type") == "MISSING_HOURLY_BARS"]
    expected_close_fields = [
        {
            key: item[key]
            for key in (
                "type",
                "open_time_ms",
                "observed_close_time_ms",
                "expected_interval_end_ms",
                "trade_count",
                "zero_volume",
            )
        }
        for item in expected_close
    ]
    if actual_close != expected_close_fields or actual_gaps != expected_gaps:
        raise DataIntegrityError("UNREGISTERED_DATA_EXCEPTION")
    row_by_open = {int(row[0]): row for row in rows}
    for exception in expected_close:
        row = row_by_open.get(int(exception["open_time_ms"]))
        if row is None:
            raise DataIntegrityError("registered close-time exception row is missing")
        row_sha = _sha256(",".join(row).encode("utf-8"))
        if row_sha != exception["canonical_full_row_sha256"]:
            raise DataIntegrityError("SOURCE_DRIFT: registered exception row changed")
    return registry_sha


def fetch_klines(config: LabConfig, root: str | Path = ".") -> Path:
    root_path = Path(root).resolve()
    market = config.market
    start_ms = parse_utc_ms(market["start_utc"])
    end_ms = parse_utc_ms(market["end_utc_exclusive"])
    completed_boundary_ms = int(datetime.now(UTC).timestamp() * 1000) // HOUR_MS * HOUR_MS
    if end_ms > completed_boundary_ms:
        raise DataIntegrityError("requested interval includes an incomplete hourly bar")
    cursor = start_ms
    normalized: list[tuple[str, ...]] = []
    batches: list[dict[str, Any]] = []
    raw_dir = root_path / "data" / "raw"

    while cursor < end_ms:
        query = urllib.parse.urlencode(
            {
                "symbol": market["symbol"],
                "interval": market["interval"],
                "startTime": cursor,
                "endTime": end_ms - 1,
                "limit": 1000,
                "timeZone": "0",
            }
        )
        url = f"{market['source_base_url']}/api/v3/klines?{query}"
        payload = _market_data_get(url)
        payload_sha = _sha256(payload)
        batch_path = (
            raw_dir / f"{market['symbol']}-{market['interval']}-{cursor}-{payload_sha[:16]}.json"
        )
        _write_new_or_same(batch_path, payload)
        batch = _parse_batch(payload)
        if not batch:
            raise DataIntegrityError(f"no data returned at cursor {cursor}")
        for item in batch:
            open_time = int(_canonical_int(item[0]))
            if open_time >= end_ms:
                continue
            if open_time < cursor:
                raise DataIntegrityError("API returned a row before the cursor")
            normalized.append(_normalized_row(item))
        last_open = int(_canonical_int(batch[-1][0]))
        if last_open < cursor:
            raise DataIntegrityError("market data cursor did not advance")
        batches.append(
            {
                "request_start_ms": cursor,
                "response_first_open_ms": int(batch[0][0]),
                "response_last_open_ms": last_open,
                "row_count": len(batch),
                "raw_path": batch_path.relative_to(root_path).as_posix(),
                "raw_sha256": payload_sha,
            }
        )
        cursor = last_open + HOUR_MS

    anomalies: list[dict[str, Any]] = []
    gap_policy = market["gap_policy"]
    rows = _validate_rows(
        normalized,
        start_ms,
        end_ms,
        anomaly_sink=anomalies,
        allow_declared_gaps=gap_policy == "CARRY_FORWARD_NO_FILL",
    )
    csv_payload = _csv_bytes(rows)
    csv_sha = _sha256(csv_payload)
    exception_registry_sha = _validate_registered_exceptions(
        rows,
        anomalies,
        start_ms=start_ms,
        end_ms_exclusive=end_ms,
        full_source_sha256=csv_sha,
        gap_policy=gap_policy,
        root=root_path,
        expected_registry_sha256=str(market["exception_registry_sha256"]),
    )
    manifest_dir = root_path / "data" / "manifests"
    existing_pattern = f"{market['symbol']}-{market['interval']}-{start_ms}-{end_ms}-*.json"
    for existing_path in manifest_dir.glob(existing_pattern):
        existing = json.loads(existing_path.read_text(encoding="utf-8"))
        if existing.get("kind", "FULL_SOURCE") != "FULL_SOURCE":
            continue
        if existing.get("normalized_sha256") != csv_sha:
            raise DataIntegrityError(
                "SOURCE_DRIFT: public history differs from the frozen full-source snapshot"
            )
    stem = f"{market['symbol']}-{market['interval']}-{start_ms}-{end_ms}-{csv_sha[:16]}"
    normalized_path = root_path / "data" / "normalized" / f"{stem}.csv"
    _write_new_or_same(normalized_path, csv_payload)

    retrieved_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    manifest = {
        "schema_version": "1.0.0",
        "kind": "FULL_SOURCE",
        "source": f"{market['source_base_url']}/api/v3/klines",
        "http_method": "GET",
        "authentication": "NONE",
        "symbol": market["symbol"],
        "interval": market["interval"],
        "timezone": "UTC",
        "gap_policy": gap_policy,
        "requested_start_ms": start_ms,
        "requested_end_ms_exclusive": end_ms,
        "first_open_ms": int(rows[0][0]),
        "last_open_ms": int(rows[-1][0]),
        "row_count": len(rows),
        "normalized_path": normalized_path.relative_to(root_path).as_posix(),
        "normalized_sha256": csv_sha,
        "config_sha256": config.config_sha256,
        "exception_registry_sha256": exception_registry_sha,
        "retrieved_at_utc": retrieved_at,
        "validation": "PASS",
        "declared_source_anomalies": anomalies,
        "batches": batches,
    }
    registry, _ = _load_exception_registry(
        root_path,
        expected_sha256=exception_registry_sha,
    )
    _assert_registered_full_source_identity(manifest, registry, root_path)
    manifest_payload = canonical_json(manifest) + b"\n"
    manifest_sha = _sha256(manifest_payload)
    manifest_path = root_path / "data" / "manifests" / f"{stem}-{manifest_sha[:16]}.json"
    _write_new_or_same(manifest_path, manifest_payload)
    return manifest_path


def _resolve_inside(root: Path, relative: str, allowed: Path) -> Path:
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(allowed.resolve())
    except ValueError as exc:
        raise DataIntegrityError(f"path escapes allowed data directory: {relative}") from exc
    return candidate


def _resolve_plain_file_inside(
    root: Path, relative: str, allowed: Path, label: str
) -> Path:
    unresolved = root / relative
    resolved = _resolve_inside(root, relative, allowed)
    _require_plain_data_file(unresolved, label)
    if unresolved.resolve() != resolved:
        raise DataIntegrityError(f"{label} changed during path validation")
    return resolved


def _manifest_file(path: str | Path, root: Path) -> tuple[Path, bytes, dict[str, Any], str]:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = root / candidate
    manifest_path = candidate.resolve()
    try:
        manifest_path.relative_to((root / "data" / "manifests").resolve())
    except ValueError as exc:
        raise DataIntegrityError("manifest must be inside data/manifests") from exc
    _require_plain_data_file(candidate, "manifest")
    if candidate.resolve() != manifest_path:
        raise DataIntegrityError("manifest changed during path validation")
    payload = _read_plain_data_bytes(manifest_path, "manifest")
    digest = _sha256(payload)
    match = re.search(r"-([0-9a-f]{16})\.json$", manifest_path.name)
    if match is None or not digest.startswith(match.group(1)):
        raise DataIntegrityError("manifest filename is not content-addressed")
    try:
        manifest = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataIntegrityError("manifest is not valid UTF-8 JSON") from exc
    if not isinstance(manifest, dict):
        raise DataIntegrityError("manifest must be an object")
    return manifest_path, payload, manifest, digest


def _assert_registered_full_source_identity(
    manifest: dict[str, Any], registry: dict[str, Any], root: Path
) -> None:
    source = registry["source"]
    _, _, registered, registered_sha = _manifest_file(source["manifest_path"], root)
    if registered_sha != source["manifest_sha256"]:
        raise DataIntegrityError("registered source manifest hash mismatch")
    immutable_fields = (
        "source",
        "http_method",
        "authentication",
        "symbol",
        "interval",
        "timezone",
        "gap_policy",
        "requested_start_ms",
        "requested_end_ms_exclusive",
        "first_open_ms",
        "last_open_ms",
        "row_count",
        "normalized_path",
        "normalized_sha256",
        "validation",
        "declared_source_anomalies",
        "batches",
    )
    for field in immutable_fields:
        if manifest.get(field) != registered.get(field):
            raise DataIntegrityError(f"SOURCE_DRIFT: full-source {field} changed")
    if (
        registered.get("normalized_path") != source["normalized_path"]
        or registered.get("normalized_sha256") != source["normalized_sha256"]
    ):
        raise DataIntegrityError("registered source manifest dataset binding mismatch")


def _rows_from_csv(payload: bytes) -> list[tuple[str, ...]]:
    try:
        lines = payload.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise DataIntegrityError("normalized dataset is not UTF-8") from exc
    reader = csv.DictReader(lines)
    if tuple(reader.fieldnames or ()) != KLINE_COLUMNS:
        raise DataIntegrityError("normalized dataset schema mismatch")
    rows: list[tuple[str, ...]] = []
    for item in reader:
        if set(item) != set(KLINE_COLUMNS) or None in item:
            raise DataIntegrityError("normalized dataset row schema mismatch")
        rows.append(tuple(item[column] for column in KLINE_COLUMNS))
    return rows


def _verify_full_raw_provenance(
    manifest: dict[str, Any],
    rows: list[tuple[str, ...]],
    normalized_payload: bytes,
    root: Path,
) -> None:
    batches = manifest.get("batches")
    if not isinstance(batches, list) or not batches:
        raise DataIntegrityError("full-source manifest has no raw batches")
    start_ms = int(manifest["requested_start_ms"])
    end_ms = int(manifest["requested_end_ms_exclusive"])
    expected_request_start = start_ms
    rebuilt: list[tuple[str, ...]] = []
    for batch in batches:
        if not isinstance(batch, dict):
            raise DataIntegrityError("raw batch metadata must be an object")
        if int(batch["request_start_ms"]) != expected_request_start:
            raise DataIntegrityError("raw batch request sequence is discontinuous")
        raw_path = _resolve_plain_file_inside(
            root,
            str(batch["raw_path"]),
            root / "data" / "raw",
            "raw market-data batch",
        )
        raw_payload = _read_plain_data_bytes(raw_path, "raw market-data batch")
        raw_sha = _sha256(raw_payload)
        if raw_sha != batch["raw_sha256"]:
            raise DataIntegrityError(f"raw batch hash mismatch: {raw_path}")
        expected_raw_name = (
            f"{manifest['symbol']}-{manifest['interval']}-"
            f"{expected_request_start}-{raw_sha[:16]}.json"
        )
        if raw_path.name != expected_raw_name:
            raise DataIntegrityError("raw batch filename is not exactly content-addressed")
        decoded = _parse_batch(raw_payload)
        if len(decoded) != int(batch["row_count"]):
            raise DataIntegrityError("raw batch row_count mismatch")
        if not decoded:
            raise DataIntegrityError("raw batch is empty")
        first_open = int(_canonical_int(decoded[0][0]))
        last_open = int(_canonical_int(decoded[-1][0]))
        if first_open != int(batch["response_first_open_ms"]):
            raise DataIntegrityError("raw batch first-open metadata mismatch")
        if last_open != int(batch["response_last_open_ms"]):
            raise DataIntegrityError("raw batch last-open metadata mismatch")
        for item in decoded:
            open_time = int(_canonical_int(item[0]))
            if not expected_request_start <= open_time < end_ms:
                raise DataIntegrityError("raw batch row lies outside its request range")
            rebuilt.append(_normalized_row(item))
        expected_request_start = last_open + HOUR_MS
    if expected_request_start != end_ms:
        raise DataIntegrityError("raw batch sequence does not reach the declared end")
    if rebuilt != rows or _csv_bytes(rebuilt) != normalized_payload:
        raise DataIntegrityError("normalized dataset does not reconstruct from raw batches")


def _lockbox_id(
    *,
    config_sha256: str,
    exception_registry_sha256: str,
    parent_normalized_sha256: str,
    preholdout_sha256: str,
    holdout_commitment_sha256: str,
    holdout_start_ms: int,
    holdout_end_ms_exclusive: int,
) -> str:
    return _sha256(
        canonical_json(
            {
                "config_sha256": config_sha256,
                "exception_registry_sha256": exception_registry_sha256,
                "parent_normalized_sha256": parent_normalized_sha256,
                "preholdout_sha256": preholdout_sha256,
                "holdout_commitment_sha256": holdout_commitment_sha256,
                "holdout_start_ms": holdout_start_ms,
                "holdout_end_ms_exclusive": holdout_end_ms_exclusive,
            }
        )
    )


def _partition_exact_fields(kind: str) -> frozenset[str]:
    if kind == "PREHOLDOUT":
        return PARTITION_BASE_FIELDS | PARTITION_LINK_FIELDS | PREHOLDOUT_LOCKED_MANIFEST_FIELDS
    if kind == "LOCKED_HOLDOUT":
        return PARTITION_BASE_FIELDS | PARTITION_LINK_FIELDS
    raise DataIntegrityError("partition provenance requires a partition manifest")


def _partition_descriptor_sha256(manifest: dict[str, Any]) -> str:
    try:
        base = {field: manifest[field] for field in PARTITION_BASE_FIELDS}
    except KeyError as exc:
        raise DataIntegrityError(f"partition descriptor field is missing: {exc.args[0]}") from exc
    return _sha256(canonical_json(base))


def _validate_partition_manifest_schema(
    manifest: dict[str, Any], *, payload: bytes | None = None
) -> None:
    kind = manifest.get("kind")
    if not isinstance(kind, str):
        raise DataIntegrityError("partition kind is missing")
    if set(manifest) != _partition_exact_fields(kind):
        raise DataIntegrityError("partition manifest schema mismatch")
    if manifest.get("schema_version") != PARTITION_SCHEMA_VERSION:
        raise DataIntegrityError("partition manifest schema version mismatch")
    if payload is not None and payload != canonical_json(manifest) + b"\n":
        raise DataIntegrityError("partition manifest is not canonical JSON")
    descriptor = manifest.get("partition_descriptor_sha256")
    paired_descriptor = manifest.get("paired_partition_descriptor_sha256")
    for field, value in (
        ("partition_descriptor_sha256", descriptor),
        ("paired_partition_descriptor_sha256", paired_descriptor),
    ):
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise DataIntegrityError(f"partition {field} is malformed")
    if descriptor != _partition_descriptor_sha256(manifest):
        raise DataIntegrityError("partition descriptor commitment mismatch")
    if descriptor == paired_descriptor:
        raise DataIntegrityError("partition descriptor cannot pair with itself")
    expected_pair = "LOCKED_HOLDOUT" if kind == "PREHOLDOUT" else "PREHOLDOUT"
    if manifest.get("paired_partition_kind") != expected_pair:
        raise DataIntegrityError("partition paired kind mismatch")
    if kind == "PREHOLDOUT":
        locked_path = manifest.get("locked_holdout_manifest_path")
        if (
            not isinstance(locked_path, str)
            or not locked_path
            or "\\" in locked_path
            or Path(locked_path).is_absolute()
            or ".." in Path(locked_path).parts
        ):
            raise DataIntegrityError("paired locked manifest path is malformed")
        locked_sha = manifest.get("locked_holdout_manifest_sha256")
        if not isinstance(locked_sha, str) or re.fullmatch(r"[0-9a-f]{64}", locked_sha) is None:
            raise DataIntegrityError("paired locked manifest hash is malformed")


def _partition_parent_metadata(
    manifest: dict[str, Any],
    root: Path,
    *,
    observed_at_ms: int,
    registry: dict[str, Any] | None = None,
) -> tuple[Path, dict[str, Any], str]:
    parent_relative = manifest.get("parent_manifest_path")
    if not isinstance(parent_relative, str):
        raise DataIntegrityError("partition parent manifest path is missing")
    parent_path = _resolve_plain_file_inside(
        root,
        parent_relative,
        root / "data" / "manifests",
        "partition parent manifest",
    )
    parent_path, _, parent, parent_sha = _manifest_file(parent_path, root)
    if parent_sha != manifest.get("parent_manifest_sha256"):
        raise DataIntegrityError("parent manifest commitment mismatch")
    if parent_path.relative_to(root).as_posix() != parent_relative:
        raise DataIntegrityError("partition parent path is not canonical")
    if parent.get("kind", "FULL_SOURCE") != "FULL_SOURCE":
        raise DataIntegrityError("partition parent is not a full-source manifest")
    if registry is not None:
        _assert_registered_full_source_identity(parent, registry, root)
    completed_boundary_ms = observed_at_ms // HOUR_MS * HOUR_MS
    if int(parent["requested_end_ms_exclusive"]) > completed_boundary_ms:
        raise DataIntegrityError("partition parent includes an incomplete hourly bar")
    if parent.get("normalized_sha256") != manifest.get("parent_normalized_sha256"):
        raise DataIntegrityError("partition parent dataset commitment mismatch")
    parent_registry = parent.get("exception_registry_sha256")
    if (
        parent_registry is not None
        and parent_registry != manifest.get("exception_registry_sha256")
    ):
        raise DataIntegrityError("partition registry differs from its parent")
    for field in (
        "source",
        "http_method",
        "authentication",
        "symbol",
        "interval",
        "timezone",
        "gap_policy",
    ):
        if manifest.get(field) != parent.get(field):
            raise DataIntegrityError(f"partition {field} differs from its parent")
    return parent_path, parent, parent_sha


def _verify_partition_commitments(manifest: dict[str, Any], parent: dict[str, Any]) -> None:
    kind = str(manifest["kind"])
    start_ms = int(manifest["requested_start_ms"])
    end_ms = int(manifest["requested_end_ms_exclusive"])
    parent_start = int(parent["requested_start_ms"])
    parent_end = int(parent["requested_end_ms_exclusive"])
    expected_normalized_path = (
        f"data/normalized/{manifest['symbol']}-{manifest['interval']}-"
        f"{kind.lower()}-{start_ms}-{end_ms}-"
        f"{str(manifest['normalized_sha256'])[:16]}.csv"
    )
    if manifest.get("normalized_path") != expected_normalized_path:
        raise DataIntegrityError("partition normalized path is not content-addressed")
    if kind == "PREHOLDOUT":
        if start_ms != parent_start or not start_ms < end_ms < parent_end:
            raise DataIntegrityError("preholdout boundaries do not partition the parent")
        holdout_start_ms = end_ms
    else:
        if end_ms != parent_end or not parent_start < start_ms < end_ms:
            raise DataIntegrityError("holdout boundaries do not partition the parent")
        holdout_start_ms = start_ms
    config_sha = manifest.get("config_sha256")
    registry_sha = manifest.get("exception_registry_sha256")
    parent_normalized_sha = manifest.get("parent_normalized_sha256")
    preholdout_sha = manifest.get("preholdout_sha256")
    holdout_sha = manifest.get("holdout_commitment_sha256")
    for field, value in (
        ("config_sha256", config_sha),
        ("exception_registry_sha256", registry_sha),
        ("parent_normalized_sha256", parent_normalized_sha),
        ("preholdout_sha256", preholdout_sha),
        ("holdout_commitment_sha256", holdout_sha),
    ):
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise DataIntegrityError(f"partition {field} is malformed")
    own_commitment = preholdout_sha if kind == "PREHOLDOUT" else holdout_sha
    if own_commitment != manifest.get("normalized_sha256"):
        raise DataIntegrityError("partition self-commitment mismatch")
    expected_lockbox_id = _lockbox_id(
        config_sha256=str(config_sha),
        exception_registry_sha256=str(registry_sha),
        parent_normalized_sha256=str(parent_normalized_sha),
        preholdout_sha256=str(preholdout_sha),
        holdout_commitment_sha256=str(holdout_sha),
        holdout_start_ms=holdout_start_ms,
        holdout_end_ms_exclusive=parent_end,
    )
    if manifest.get("lockbox_id") != expected_lockbox_id:
        raise DataIntegrityError("partition lockbox commitment mismatch")


def _verify_preholdout_locked_pair(
    preholdout: dict[str, Any], parent: dict[str, Any], root: Path
) -> dict[str, Any]:
    locked_relative = str(preholdout["locked_holdout_manifest_path"])
    locked_path = _resolve_plain_file_inside(
        root,
        locked_relative,
        root / "data" / "manifests",
        "paired locked manifest",
    )
    locked_path, payload, locked, locked_sha = _manifest_file(locked_path, root)
    if locked_path.relative_to(root).as_posix() != locked_relative:
        raise DataIntegrityError("paired locked manifest path is not canonical")
    if locked_sha != preholdout.get("locked_holdout_manifest_sha256"):
        raise DataIntegrityError("paired locked manifest file commitment mismatch")
    _validate_partition_manifest_schema(locked, payload=payload)
    if locked.get("kind") != "LOCKED_HOLDOUT":
        raise DataIntegrityError("paired manifest is not LOCKED_HOLDOUT")
    _verify_partition_commitments(locked, parent)
    if preholdout.get("paired_partition_descriptor_sha256") != locked.get(
        "partition_descriptor_sha256"
    ):
        raise DataIntegrityError("preholdout locked descriptor commitment mismatch")
    if locked.get("paired_partition_descriptor_sha256") != preholdout.get(
        "partition_descriptor_sha256"
    ):
        raise DataIntegrityError("locked preholdout descriptor commitment mismatch")
    common_fields = (
        "source",
        "http_method",
        "authentication",
        "symbol",
        "interval",
        "timezone",
        "gap_policy",
        "config_sha256",
        "exception_registry_sha256",
        "parent_manifest_path",
        "parent_manifest_sha256",
        "parent_normalized_sha256",
        "lockbox_id",
        "preholdout_sha256",
        "holdout_commitment_sha256",
    )
    for field in common_fields:
        if preholdout.get(field) != locked.get(field):
            raise DataIntegrityError(f"paired partition {field} mismatch")
    if int(preholdout["requested_end_ms_exclusive"]) != int(locked["requested_start_ms"]):
        raise DataIntegrityError("paired partition boundary mismatch")
    return locked


def _verify_partition_metadata(
    manifest: dict[str, Any],
    root: Path,
    *,
    observed_at_ms: int,
    manifest_payload: bytes | None = None,
    registry: dict[str, Any] | None = None,
) -> tuple[Path, dict[str, Any]]:
    _validate_partition_manifest_schema(manifest, payload=manifest_payload)
    parent_path, parent, _ = _partition_parent_metadata(
        manifest,
        root,
        observed_at_ms=observed_at_ms,
        registry=registry,
    )
    _verify_partition_commitments(manifest, parent)
    if manifest.get("kind") == "PREHOLDOUT":
        _verify_preholdout_locked_pair(manifest, parent, root)
    return parent_path, parent


def _verify_partition_provenance(
    manifest: dict[str, Any],
    rows: list[tuple[str, ...]],
    normalized_payload: bytes,
    root: Path,
    *,
    observed_at_ms: int,
    replay_full_parent: bool = False,
    manifest_payload: bytes | None = None,
    registry: dict[str, Any] | None = None,
) -> None:
    if replay_full_parent and not _isolated_provenance_replay_enabled():
        raise DataIntegrityError("FULL_PARENT_REPLAY_REQUIRES_ISOLATED_PROVENANCE_MODE")
    parent_path, parent = _verify_partition_metadata(
        manifest,
        root,
        observed_at_ms=observed_at_ms,
        manifest_payload=manifest_payload,
        registry=registry,
    )
    if not replay_full_parent:
        return
    verified_parent = verify_manifest(parent_path, root=root, as_of_ms=observed_at_ms)
    if verified_parent.get("manifest_file_sha256") != manifest.get("parent_manifest_sha256"):
        raise DataIntegrityError("verified parent manifest hash mismatch")
    parent_normalized_path = _resolve_plain_file_inside(
        root,
        str(parent["normalized_path"]),
        root / "data" / "normalized",
        "full-parent normalized dataset",
    )
    parent_payload = _read_plain_data_bytes(
        parent_normalized_path, "full-parent normalized dataset"
    )
    if _sha256(parent_payload) != manifest.get("parent_normalized_sha256"):
        raise DataIntegrityError("partition parent normalized bytes changed")
    parent_rows = _rows_from_csv(parent_payload)
    if parent_payload != _csv_bytes(parent_rows):
        raise DataIntegrityError("partition parent normalized CSV is not canonical")
    start_ms = int(manifest["requested_start_ms"])
    end_ms = int(manifest["requested_end_ms_exclusive"])
    expected_rows = [row for row in parent_rows if start_ms <= int(row[0]) < end_ms]
    if rows != expected_rows or normalized_payload != _csv_bytes(expected_rows):
        raise DataIntegrityError("partition bytes are not the exact parent slice")


def verify_manifest(
    path: str | Path,
    root: str | Path = ".",
    *,
    as_of_ms: int | None = None,
    expected_kind: str | None = None,
    replay_full_parent: bool = False,
    allow_locked_data: bool = False,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    manifest_path, manifest_payload, manifest, manifest_sha = _manifest_file(path, root_path)
    kind = manifest.get("kind", "FULL_SOURCE")
    if kind not in {"FULL_SOURCE", "PREHOLDOUT", "LOCKED_HOLDOUT"}:
        raise DataIntegrityError(f"unknown manifest kind: {kind}")
    if expected_kind is not None:
        if expected_kind not in {"FULL_SOURCE", "PREHOLDOUT", "LOCKED_HOLDOUT"}:
            raise DataIntegrityError(f"unknown expected manifest kind: {expected_kind}")
        if kind != expected_kind:
            raise DataIntegrityError(f"expected {expected_kind} manifest, received {kind}")
    if replay_full_parent:
        if kind not in {"PREHOLDOUT", "LOCKED_HOLDOUT"}:
            raise DataIntegrityError("FULL_PARENT_REPLAY_REQUIRES_PARTITION_MANIFEST")
        if not _isolated_provenance_replay_enabled():
            raise DataIntegrityError("FULL_PARENT_REPLAY_REQUIRES_ISOLATED_PROVENANCE_MODE")
    if kind in {"PREHOLDOUT", "LOCKED_HOLDOUT"}:
        _validate_partition_manifest_schema(manifest, payload=manifest_payload)
    if kind == "LOCKED_HOLDOUT" and not (allow_locked_data or replay_full_parent):
        raise DataIntegrityError("LOCKED_DATA_ACCESS_REQUIRES_DURABLE_OPEN_OR_ISOLATED_REPLAY")
    normalized_path = _resolve_plain_file_inside(
        root_path,
        str(manifest["normalized_path"]),
        root_path / "data" / "normalized",
        "normalized dataset",
    )
    observed_at = int(datetime.now(UTC).timestamp() * 1000) if as_of_ms is None else int(as_of_ms)
    completed_boundary_ms = observed_at // HOUR_MS * HOUR_MS
    if int(manifest["requested_end_ms_exclusive"]) > completed_boundary_ms:
        raise DataIntegrityError("manifest includes an incomplete hourly bar")
    payload = _read_plain_data_bytes(normalized_path, "normalized dataset")
    if _sha256(payload) != manifest.get("normalized_sha256"):
        raise DataIntegrityError("normalized dataset hash mismatch")
    rows = _rows_from_csv(payload)
    if payload != _csv_bytes(rows):
        raise DataIntegrityError("normalized dataset is not canonical CSV")
    anomalies: list[dict[str, Any]] = []
    _validate_rows(
        rows,
        int(manifest["requested_start_ms"]),
        int(manifest["requested_end_ms_exclusive"]),
        anomaly_sink=anomalies,
        allow_declared_gaps=manifest.get("gap_policy") == "CARRY_FORWARD_NO_FILL",
    )
    if manifest.get("validation") != "PASS":
        raise DataIntegrityError("manifest validation status is not PASS")
    if len(rows) != int(manifest.get("row_count", -1)):
        raise DataIntegrityError("manifest row_count mismatch")
    if int(rows[0][0]) != int(manifest.get("first_open_ms", -1)):
        raise DataIntegrityError("manifest first_open_ms mismatch")
    if int(rows[-1][0]) != int(manifest.get("last_open_ms", -1)):
        raise DataIntegrityError("manifest last_open_ms mismatch")
    if anomalies != manifest.get("declared_source_anomalies", []):
        raise DataIntegrityError("declared source anomaly ledger mismatch")
    declared_registry_sha = manifest.get("exception_registry_sha256")
    expected_registry_sha = (
        declared_registry_sha if isinstance(declared_registry_sha, str) else None
    )
    registry, registry_sha = _load_exception_registry(
        root_path, expected_sha256=expected_registry_sha
    )
    manifest_relative = manifest_path.relative_to(root_path).as_posix()
    if declared_registry_sha is None:
        source = registry["source"]
        if (
            kind != "FULL_SOURCE"
            or manifest_sha != source["manifest_sha256"]
            or manifest_relative != source["manifest_path"]
        ):
            raise DataIntegrityError("manifest is missing its exception registry commitment")
    elif declared_registry_sha != registry_sha:
        raise DataIntegrityError("exception registry commitment mismatch")
    if kind == "FULL_SOURCE" and (
        manifest.get("normalized_path") != registry["source"]["normalized_path"]
        or manifest.get("normalized_sha256") != registry["source"]["normalized_sha256"]
    ):
        raise DataIntegrityError("SOURCE_DRIFT: full-source snapshot differs from registry")
    full_source_sha = (
        str(manifest["normalized_sha256"])
        if kind == "FULL_SOURCE"
        else str(manifest["parent_normalized_sha256"])
    )
    registry_sha = _validate_registered_exceptions(
        rows,
        anomalies,
        start_ms=int(manifest["requested_start_ms"]),
        end_ms_exclusive=int(manifest["requested_end_ms_exclusive"]),
        full_source_sha256=full_source_sha,
        gap_policy=str(manifest.get("gap_policy")),
        root=root_path,
        expected_registry_sha256=registry_sha,
    )
    if kind == "FULL_SOURCE":
        _assert_registered_full_source_identity(manifest, registry, root_path)
        _verify_full_raw_provenance(manifest, rows, payload, root_path)
    else:
        _verify_partition_provenance(
            manifest,
            rows,
            payload,
            root_path,
            observed_at_ms=observed_at,
            replay_full_parent=replay_full_parent,
            manifest_payload=manifest_payload,
            registry=registry,
        )
    verified = dict(manifest)
    verified["manifest_path"] = manifest_relative
    verified["manifest_file_sha256"] = manifest_sha
    verified["exception_registry_sha256"] = registry_sha
    return verified


def read_manifest_metadata(path: str | Path, root: str | Path = ".") -> dict[str, Any]:
    root_path = Path(root).resolve()
    manifest_path, payload, manifest, manifest_sha = _manifest_file(path, root_path)
    kind = manifest.get("kind", "FULL_SOURCE")
    if kind in {"PREHOLDOUT", "LOCKED_HOLDOUT"}:
        observed_at = int(datetime.now(UTC).timestamp() * 1000)
        declared_registry_sha = manifest.get("exception_registry_sha256")
        expected_registry_sha = (
            declared_registry_sha if isinstance(declared_registry_sha, str) else None
        )
        registry, _ = _load_exception_registry(
            root_path, expected_sha256=expected_registry_sha
        )
        _verify_partition_metadata(
            manifest,
            root_path,
            observed_at_ms=observed_at,
            manifest_payload=payload,
            registry=registry,
        )
    value = dict(manifest)
    value["manifest_path"] = manifest_path.relative_to(root_path).as_posix()
    value["manifest_file_sha256"] = manifest_sha
    return value


def bind_manifest_to_config(
    manifest: dict[str, Any], config: LabConfig, expected_kind: str
) -> None:
    kind = manifest.get("kind", "FULL_SOURCE")
    if kind != expected_kind:
        raise DataIntegrityError(f"expected {expected_kind} manifest, received {kind}")
    market = config.market
    expected_source = f"{market['source_base_url']}/api/v3/klines"
    exact = {
        "source": expected_source,
        "http_method": "GET",
        "authentication": "NONE",
        "symbol": market["symbol"],
        "interval": market["interval"],
        "timezone": market["timezone"],
        "gap_policy": market["gap_policy"],
    }
    for field, expected in exact.items():
        if manifest.get(field) != expected:
            raise DataIntegrityError(f"manifest {field} is not bound to config")
    if manifest.get("exception_registry_sha256") != market["exception_registry_sha256"]:
        raise DataIntegrityError("manifest exception registry is not bound to config")
    full_start = parse_utc_ms(market["start_utc"])
    holdout_start = parse_utc_ms(config.splits["validation_end_utc_exclusive"])
    full_end = parse_utc_ms(market["end_utc_exclusive"])
    ranges = {
        "FULL_SOURCE": (full_start, full_end),
        "PREHOLDOUT": (full_start, holdout_start),
        "LOCKED_HOLDOUT": (holdout_start, full_end),
    }
    start_ms, end_ms = ranges[expected_kind]
    if int(manifest.get("requested_start_ms", -1)) != start_ms:
        raise DataIntegrityError("manifest start boundary is not bound to config")
    if int(manifest.get("requested_end_ms_exclusive", -1)) != end_ms:
        raise DataIntegrityError("manifest end boundary is not bound to config")
    if expected_kind != "FULL_SOURCE" and manifest.get("config_sha256") != config.config_sha256:
        raise DataIntegrityError("partition manifest config hash mismatch")


def _write_partition_manifest(root: Path, stem: str, manifest: dict[str, Any]) -> Path:
    payload = canonical_json(manifest) + b"\n"
    digest = _sha256(payload)
    path = root / "data" / "manifests" / f"{stem}-{digest[:16]}.json"
    _write_new_or_same(path, payload)
    return path


def partition_lockbox(
    full_manifest_path: str | Path, config: LabConfig, root: str | Path = "."
) -> tuple[Path, Path]:
    root_path = Path(root).resolve()
    full = verify_manifest(full_manifest_path, root=root_path)
    bind_manifest_to_config(full, config, "FULL_SOURCE")
    normalized_path = _resolve_plain_file_inside(
        root_path,
        str(full["normalized_path"]),
        root_path / "data" / "normalized",
        "full-source normalized dataset",
    )
    full_payload = _read_plain_data_bytes(
        normalized_path, "full-source normalized dataset"
    )
    if _sha256(full_payload) != full["normalized_sha256"]:
        raise DataIntegrityError("full-source dataset changed after manifest verification")
    rows = _rows_from_csv(full_payload)
    if full_payload != _csv_bytes(rows):
        raise DataIntegrityError("full-source normalized CSV is not canonical")
    holdout_start = parse_utc_ms(config.splits["validation_end_utc_exclusive"])
    holdout_end = parse_utc_ms(config.splits["locked_holdout_end_utc_exclusive"])
    pre_rows = [row for row in rows if int(row[0]) < holdout_start]
    holdout_rows = [row for row in rows if holdout_start <= int(row[0]) < holdout_end]
    partitions: list[tuple[str, list[tuple[str, ...]], int, int]] = [
        ("PREHOLDOUT", pre_rows, parse_utc_ms(config.market["start_utc"]), holdout_start),
        ("LOCKED_HOLDOUT", holdout_rows, holdout_start, holdout_end),
    ]
    prepared: dict[str, dict[str, Any]] = {}
    for kind, selected_rows, start_ms, end_ms in partitions:
        anomalies: list[dict[str, Any]] = []
        _validate_rows(
            selected_rows,
            start_ms,
            end_ms,
            anomaly_sink=anomalies,
            allow_declared_gaps=config.market["gap_policy"] == "CARRY_FORWARD_NO_FILL",
        )
        csv_payload = _csv_bytes(selected_rows)
        csv_sha = _sha256(csv_payload)
        stem = f"{config.market['symbol']}-{config.market['interval']}-{kind.lower()}-{start_ms}-{end_ms}-{csv_sha[:16]}"
        csv_path = root_path / "data" / "normalized" / f"{stem}.csv"
        _write_new_or_same(csv_path, csv_payload)
        prepared[kind] = {
            "rows": selected_rows,
            "anomalies": anomalies,
            "csv_sha": csv_sha,
            "csv_path": csv_path,
            "stem": stem,
            "start_ms": start_ms,
            "end_ms": end_ms,
        }
    lockbox_id = _lockbox_id(
        config_sha256=config.config_sha256,
        exception_registry_sha256=full["exception_registry_sha256"],
        parent_normalized_sha256=full["normalized_sha256"],
        preholdout_sha256=prepared["PREHOLDOUT"]["csv_sha"],
        holdout_commitment_sha256=prepared["LOCKED_HOLDOUT"]["csv_sha"],
        holdout_start_ms=holdout_start,
        holdout_end_ms_exclusive=holdout_end,
    )
    base_manifests: dict[str, dict[str, Any]] = {}
    for kind in ("PREHOLDOUT", "LOCKED_HOLDOUT"):
        item = prepared[kind]
        base_manifests[kind] = {
            "schema_version": PARTITION_SCHEMA_VERSION,
            "kind": kind,
            "source": full["source"],
            "http_method": "GET",
            "authentication": "NONE",
            "symbol": config.market["symbol"],
            "interval": config.market["interval"],
            "timezone": "UTC",
            "gap_policy": config.market["gap_policy"],
            "requested_start_ms": item["start_ms"],
            "requested_end_ms_exclusive": item["end_ms"],
            "first_open_ms": int(item["rows"][0][0]),
            "last_open_ms": int(item["rows"][-1][0]),
            "row_count": len(item["rows"]),
            "normalized_path": item["csv_path"].relative_to(root_path).as_posix(),
            "normalized_sha256": item["csv_sha"],
            "config_sha256": config.config_sha256,
            "exception_registry_sha256": full["exception_registry_sha256"],
            "validation": "PASS",
            "declared_source_anomalies": item["anomalies"],
            "parent_manifest_path": full["manifest_path"],
            "parent_manifest_sha256": full["manifest_file_sha256"],
            "parent_normalized_sha256": full["normalized_sha256"],
            "lockbox_id": lockbox_id,
            "preholdout_sha256": prepared["PREHOLDOUT"]["csv_sha"],
            "holdout_commitment_sha256": prepared["LOCKED_HOLDOUT"]["csv_sha"],
        }
    descriptors = {
        kind: _sha256(canonical_json(base_manifests[kind]))
        for kind in ("PREHOLDOUT", "LOCKED_HOLDOUT")
    }
    locked_manifest = {
        **base_manifests["LOCKED_HOLDOUT"],
        "partition_descriptor_sha256": descriptors["LOCKED_HOLDOUT"],
        "paired_partition_kind": "PREHOLDOUT",
        "paired_partition_descriptor_sha256": descriptors["PREHOLDOUT"],
    }
    _validate_partition_manifest_schema(locked_manifest)
    locked_path = _write_partition_manifest(
        root_path,
        str(prepared["LOCKED_HOLDOUT"]["stem"]),
        locked_manifest,
    )
    locked_manifest_sha = _sha256(
        _read_plain_data_bytes(locked_path, "locked partition manifest")
    )
    preholdout_manifest = {
        **base_manifests["PREHOLDOUT"],
        "partition_descriptor_sha256": descriptors["PREHOLDOUT"],
        "paired_partition_kind": "LOCKED_HOLDOUT",
        "paired_partition_descriptor_sha256": descriptors["LOCKED_HOLDOUT"],
        "locked_holdout_manifest_path": locked_path.relative_to(root_path).as_posix(),
        "locked_holdout_manifest_sha256": locked_manifest_sha,
    }
    _validate_partition_manifest_schema(preholdout_manifest)
    preholdout_path = _write_partition_manifest(
        root_path,
        str(prepared["PREHOLDOUT"]["stem"]),
        preholdout_manifest,
    )
    return preholdout_path, locked_path


def load_bars_from_manifest(
    path: str | Path,
    root: str | Path = ".",
    *,
    expected_kind: str | None = None,
    replay_full_parent: bool = False,
    allow_locked_data: bool = False,
) -> tuple[list[Bar], dict[str, Any]]:
    root_path = Path(root).resolve()
    manifest = verify_manifest(
        path,
        root=root_path,
        expected_kind=expected_kind,
        replay_full_parent=replay_full_parent,
        allow_locked_data=allow_locked_data,
    )
    normalized_path = _resolve_plain_file_inside(
        root_path,
        str(manifest["normalized_path"]),
        root_path / "data" / "normalized",
        "normalized dataset",
    )
    payload = _read_plain_data_bytes(normalized_path, "normalized dataset")
    if _sha256(payload) != manifest["normalized_sha256"]:
        raise DataIntegrityError("normalized dataset changed after manifest verification")
    rows = _rows_from_csv(payload)
    if payload != _csv_bytes(rows):
        raise DataIntegrityError("normalized dataset changed to non-canonical CSV")
    bars = [
        Bar(
            open_time_ms=int(row[0]),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            close_time_ms=int(row[6]),
            trade_count=int(row[8]),
        )
        for row in rows
    ]
    if manifest.get("gap_policy") == "CARRY_FORWARD_NO_FILL":
        bars = _expand_gaps_carry_forward(bars)
    return bars, manifest


def _expand_gaps_carry_forward(bars: list[Bar]) -> list[Bar]:
    if not bars:
        return []
    if any(bar.synthetic for bar in bars):
        raise DataIntegrityError("source bars must not already be marked synthetic")
    expanded = [bars[0]]
    for current in bars[1:]:
        previous = expanded[-1]
        cursor = previous.open_time_ms + HOUR_MS
        last_official_close = previous.close
        while cursor < current.open_time_ms:
            expanded.append(
                Bar(
                    open_time_ms=cursor,
                    open=last_official_close,
                    high=last_official_close,
                    low=last_official_close,
                    close=last_official_close,
                    volume=0.0,
                    close_time_ms=cursor + HOUR_MS - 1,
                    synthetic=True,
                    trade_count=0,
                )
            )
            cursor += HOUR_MS
        expanded.append(current)
    return expanded
