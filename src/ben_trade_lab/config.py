from __future__ import annotations

import hashlib
import itertools
import json
import math
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import StrategyParams

ALLOWED_MARKET_DATA_URL = "https://data-api.binance.vision"
CANONICAL_ANCHOR_STORE_ID = (
    "64acdb6e068cd88f1db2045ebf0dd2f6d93c6710b338f2b4e4898c927e4ef47e"
)
CANONICAL_ANCHOR_STORE_SHA256 = (
    "0f12006fa9f7d5137faa77f694b59bce4afe34899f93ca9785d1ab923ab1a995"
)
CANONICAL_ANCHOR_POLICY = "CREATE_ONLY_PER_EXPERIMENT_HASH_CHAIN_V1"
CANONICAL_WITNESS_STORE_ID = (
    "3e19a9e67f76ed22f2345d202f607d006ac512f24f7ff96e85822133870dcca9"
)
CANONICAL_WITNESS_HEADER_SHA256 = (
    "b28626b92481c06f86f6757113f89b123f2eac170ab0387f70567e2f67a0da2c"
)
CANONICAL_WITNESS_FILESYSTEM_DEVICE = 2096
CANONICAL_WITNESS_FILESYSTEM_INODE = 2434
CANONICAL_WITNESS_POLICY = "LINUX_FS_APPEND_FL_ONE_SHOT_BURN_LEDGER_V1"


def parse_utc_ms(value: str) -> int:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"timestamp must be explicit UTC: {value}")
    return int(parsed.timestamp() * 1000)


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _strict_frozen_equal(actual: object, expected: object) -> bool:
    """Compare frozen config values without Python's bool/int coercions."""

    if type(actual) is not type(expected):
        return False
    if isinstance(expected, float) and isinstance(actual, float):
        return math.isfinite(actual) and actual == expected
    if isinstance(expected, list) and isinstance(actual, list):
        return len(actual) == len(expected) and all(
            _strict_frozen_equal(left, right)
            for left, right in zip(actual, expected, strict=True)
        )
    return actual == expected


@dataclass(frozen=True, slots=True)
class LabConfig:
    raw: dict[str, Any]
    path: Path

    @property
    def market(self) -> dict[str, Any]:
        return self.raw["market"]

    @property
    def execution(self) -> dict[str, Any]:
        return self.raw["execution"]

    @property
    def splits(self) -> dict[str, Any]:
        return self.raw["splits"]

    @property
    def acceptance(self) -> dict[str, Any]:
        return self.raw["acceptance"]

    @property
    def anchor(self) -> dict[str, Any]:
        return self.raw["anchor"]

    @property
    def witness(self) -> dict[str, Any]:
        return self.raw["witness"]

    @property
    def config_sha256(self) -> str:
        return sha256_bytes(canonical_json(self.raw))

    def strategy_grid(self) -> tuple[StrategyParams, ...]:
        strategy = self.raw["strategy"]
        grid = tuple(
            StrategyParams(
                entry_lookback=entry,
                exit_lookback=exit_,
                trend_lookback=trend,
                volatility_lookback=int(strategy["volatility_lookback"]),
                target_annualized_volatility=float(strategy["target_annualized_volatility"]),
                volatility_floor=float(strategy["volatility_floor"]),
            )
            for entry, exit_, trend in itertools.product(
                strategy["entry_lookbacks"],
                strategy["exit_lookbacks"],
                strategy["trend_lookbacks"],
            )
            if exit_ <= entry
        )
        maximum = int(self.raw["selection"]["maximum_trials"])
        if len(grid) != maximum:
            raise ValueError(
                f"strategy grid has {len(grid)} trials; frozen budget requires {maximum}"
            )
        return grid


def load_config(path: str | Path) -> LabConfig:
    resolved = Path(path).resolve()
    with resolved.open("rb") as handle:
        raw = tomllib.load(handle)
    _validate(raw)
    config = LabConfig(raw=raw, path=resolved)
    registry = (resolved.parent.parent / raw["market"]["exception_registry_path"]).resolve()
    try:
        registry.relative_to((resolved.parent.parent / "configs").resolve())
    except ValueError as exc:
        raise ValueError("exception registry must remain inside configs") from exc
    if sha256_bytes(registry.read_bytes()) != raw["market"]["exception_registry_sha256"]:
        raise ValueError("exception registry hash mismatch")
    config.strategy_grid()
    return config


def _validate(raw: dict[str, Any]) -> None:
    required = {
        "project",
        "market",
        "splits",
        "execution",
        "strategy",
        "selection",
        "acceptance",
        "diagnostics",
        "anchor",
        "witness",
        "paper",
    }
    missing = required.difference(raw)
    if missing:
        raise ValueError(f"missing config sections: {sorted(missing)}")
    project = raw["project"]
    if project.get("contract_version") != "1.2.0":
        raise ValueError("this source tree implements only frozen contract v1.2.0")
    if project.get("status") != "RESEARCH_NOT_YET_VALIDATED":
        raise ValueError("the config may not pre-claim a validated research status")
    market = raw["market"]
    if market.get("venue") != "BINANCE_SPOT":
        raise ValueError("v1.2 is fixed to Binance spot market data")
    if market.get("source_base_url") != ALLOWED_MARKET_DATA_URL:
        raise ValueError("only the Binance market-data-only host is allowed")
    if market.get("symbol") != "BTCUSDT" or market.get("interval") != "1h":
        raise ValueError("v1 is fixed to BTCUSDT spot 1h")
    if market.get("timezone") != "UTC":
        raise ValueError("v1 requires UTC bars")
    if market.get("gap_policy") != "CARRY_FORWARD_NO_FILL":
        raise ValueError("v1 requires the audited carry-forward/no-fill gap policy")
    if (
        market.get("start_utc") != "2017-08-17T04:00:00Z"
        or market.get("end_utc_exclusive") != "2026-08-01T00:00:00Z"
    ):
        raise ValueError("v1.2 source interval is frozen")
    registry_sha = market.get("exception_registry_sha256")
    if not isinstance(registry_sha, str) or len(registry_sha) != 64:
        raise ValueError("exception registry SHA-256 is missing")
    execution = raw["execution"]
    exact_execution = {
        "initial_cash": 10_000.0,
        "fee_bps_per_side": 10.0,
        "slippage_bps_per_side": 5.0,
        "maximum_gross_exposure": 1.0,
        "signal_fill_delay_bars": 1,
    }
    if any(
        not _strict_frozen_equal(execution.get(key), value)
        for key, value in exact_execution.items()
    ):
        raise ValueError("v1.2 cash, cost, exposure, and base-latency assumptions are frozen")
    if execution.get("allow_short") is not False or execution.get("allow_leverage") is not False:
        raise ValueError("shorting and leverage must remain disabled")
    if execution.get("signal_fill_delay_bars") < 1:
        raise ValueError("signals must fill at least one bar later")
    if (
        execution.get("fill_eligibility")
        != "OFFICIAL_POSITIVE_VOLUME_AND_TRADE_COUNT"
    ):
        raise ValueError("fills require an official positive-volume positive-trade bar")
    if (
        execution.get("unfilled_intent_policy")
        != "DEFER_THROUGH_INELIGIBLE_UNTIL_ELIGIBLE_FILL_OR_ELIGIBLE_CANCEL"
    ):
        raise ValueError("unfilled intent policy is not the frozen v1.2 policy")
    if (
        execution.get("terminal_valuation")
        != "LIQUIDATE_AT_FINAL_ELIGIBLE_CLOSE_WITH_COSTS_ELSE_NOT_PROVEN"
    ):
        raise ValueError("terminal valuation must fail closed when liquidation is ineligible")
    exposure = execution["maximum_gross_exposure"]
    if not 0.0 < exposure <= 1.0:
        raise ValueError("gross exposure must be within (0, 1]")
    start = parse_utc_ms(market["start_utc"])
    end = parse_utc_ms(market["end_utc_exclusive"])
    dev = parse_utc_ms(raw["splits"]["development_end_utc_exclusive"])
    val = parse_utc_ms(raw["splits"]["validation_end_utc_exclusive"])
    holdout = parse_utc_ms(raw["splits"]["locked_holdout_end_utc_exclusive"])
    if not start < dev < val < holdout == end:
        raise ValueError("chronological boundaries are inconsistent")
    if (
        raw["splits"].get("development_end_utc_exclusive") != "2022-01-01T00:00:00Z"
        or raw["splits"].get("validation_end_utc_exclusive") != "2024-08-01T00:00:00Z"
        or raw["splits"].get("locked_holdout_end_utc_exclusive") != "2026-08-01T00:00:00Z"
    ):
        raise ValueError("v1.2 chronology is frozen")
    strategy = raw["strategy"]
    exact_strategy = {
        "family": "DONCHIAN_TREND_VOL_TARGET",
        "entry_lookbacks": [72, 168, 336],
        "exit_lookbacks": [24, 72, 168],
        "trend_lookbacks": [336, 720],
        "volatility_lookback": 720,
        "target_annualized_volatility": 0.30,
        "volatility_floor": 0.10,
    }
    if any(
        not _strict_frozen_equal(strategy.get(key), value)
        for key, value in exact_strategy.items()
    ):
        raise ValueError("v1.2 strategy family and parameter grid are frozen")
    selection = raw["selection"]
    if selection.get("objective") != "MEDIAN_WALK_FORWARD_CALMAR":
        raise ValueError("v1.2 uses the frozen median walk-forward Calmar objective")
    if not _strict_frozen_equal(selection.get("maximum_trials"), 16):
        raise ValueError("v1.2 trial budget is exactly 16 candidates")
    if not _strict_frozen_equal(selection.get("expected_fold_count"), 9):
        raise ValueError("v1.2 requires exactly nine scoring folds")
    scoring_start = parse_utc_ms(selection["scoring_start_utc"])
    if scoring_start != parse_utc_ms("2020-02-01T00:00:00Z"):
        raise ValueError("v1.2 scoring start is frozen at 2020-02-01 UTC")
    if not _strict_frozen_equal(selection.get("minimum_fold_months"), 6):
        raise ValueError("v1.2 uses exact six-month folds")
    if not _strict_frozen_equal(
        selection.get("minimum_fold_completed_round_trips"), 2
    ):
        raise ValueError("v1.2 fold round-trip floor is frozen at two")
    minimum_exposure = selection.get("minimum_fold_exposure_fraction")
    maximum_exposure = selection.get("maximum_fold_exposure_fraction")
    if not _strict_frozen_equal(minimum_exposure, 0.05) or not _strict_frozen_equal(
        maximum_exposure, 0.95
    ):
        raise ValueError("v1.2 fold exposure bounds are frozen")
    positive_fraction = selection.get("minimum_positive_fold_fraction")
    if not _strict_frozen_equal(positive_fraction, 0.75):
        raise ValueError("v1.2 positive-fold threshold is frozen")
    acceptance = raw["acceptance"]
    exact_acceptance = {
        "minimum_holdout_sharpe": 0.80,
        "minimum_holdout_calmar": 0.75,
        "maximum_holdout_drawdown": 0.25,
        "minimum_completed_round_trips": 30,
        "minimum_positive_cost_stress_multiplier": 2.0,
        "minimum_positive_parameter_neighbors_fraction": 0.70,
        "maximum_single_quarter_profit_concentration": 0.50,
    }
    if any(
        not _strict_frozen_equal(acceptance.get(key), value)
        for key, value in exact_acceptance.items()
    ):
        raise ValueError("v1.2 acceptance gates are frozen")
    diagnostics = raw["diagnostics"]
    if not _strict_frozen_equal(
        diagnostics.get("moving_block_bootstrap_resamples"), 2000
    ):
        raise ValueError("v1.2 bootstrap resample count is frozen at 2000")
    if not _strict_frozen_equal(diagnostics.get("moving_block_length_days"), 7):
        raise ValueError("v1.2 moving-block length is frozen at seven days")
    if not _strict_frozen_equal(diagnostics.get("latency_stress_delay_bars"), 2):
        raise ValueError("v1.2 latency stress is frozen at two bars")
    if diagnostics.get("seed_policy") != "SHA256_EXPERIMENT_ID":
        raise ValueError("diagnostic seed policy mismatch")
    anchor = raw["anchor"]
    if anchor.get("store_id") != CANONICAL_ANCHOR_STORE_ID:
        raise ValueError("v1.2 external anchor store identity is frozen")
    if anchor.get("store_sha256") != CANONICAL_ANCHOR_STORE_SHA256:
        raise ValueError("v1.2 external anchor store descriptor is frozen")
    if anchor.get("policy") != CANONICAL_ANCHOR_POLICY:
        raise ValueError("v1.2 external anchor policy is frozen")
    witness = raw["witness"]
    if not isinstance(witness, dict) or set(witness) != {
        "store_id",
        "header_sha256",
        "filesystem_device",
        "filesystem_inode",
        "policy",
    }:
        raise ValueError("v1.2 append-only witness schema is frozen")
    if witness.get("store_id") != CANONICAL_WITNESS_STORE_ID:
        raise ValueError("v1.2 append-only witness store identity is frozen")
    if witness.get("header_sha256") != CANONICAL_WITNESS_HEADER_SHA256:
        raise ValueError("v1.2 append-only witness header is frozen")
    device = witness.get("filesystem_device")
    if (
        not isinstance(device, int)
        or isinstance(device, bool)
        or device != CANONICAL_WITNESS_FILESYSTEM_DEVICE
    ):
        raise ValueError("v1.2 append-only witness filesystem device is frozen")
    inode = witness.get("filesystem_inode")
    if (
        not isinstance(inode, int)
        or isinstance(inode, bool)
        or inode != CANONICAL_WITNESS_FILESYSTEM_INODE
    ):
        raise ValueError("v1.2 append-only witness inode is frozen")
    if witness.get("policy") != CANONICAL_WITNESS_POLICY:
        raise ValueError("v1.2 append-only witness policy is frozen")
    paper = raw["paper"]
    exact_paper = {
        "minimum_calendar_days": 180,
        "minimum_completed_round_trips": 30,
        "maximum_tracking_error_bps": 10.0,
    }
    if any(
        not _strict_frozen_equal(paper.get(key), value)
        for key, value in exact_paper.items()
    ):
        raise ValueError("v1.2 paper-validation gates are frozen")
