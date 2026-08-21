from __future__ import annotations

import hashlib
import itertools
import json
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import StrategyParams

ALLOWED_MARKET_DATA_URL = "https://data-api.binance.vision"


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
        "paper",
    }
    missing = required.difference(raw)
    if missing:
        raise ValueError(f"missing config sections: {sorted(missing)}")
    project = raw["project"]
    if project.get("contract_version") != "1.1.0":
        raise ValueError("this source tree implements only frozen contract v1.1.0")
    if project.get("status") != "RESEARCH_NOT_YET_VALIDATED":
        raise ValueError("the config may not pre-claim a validated research status")
    market = raw["market"]
    if market.get("venue") != "BINANCE_SPOT":
        raise ValueError("v1.1 is fixed to Binance spot market data")
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
        raise ValueError("v1.1 source interval is frozen")
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
    if any(float(execution.get(key, -1.0)) != value for key, value in exact_execution.items()):
        raise ValueError("v1.1 cash, cost, exposure, and base-latency assumptions are frozen")
    if execution.get("allow_short") is not False or execution.get("allow_leverage") is not False:
        raise ValueError("shorting and leverage must remain disabled")
    if int(execution.get("signal_fill_delay_bars", 0)) < 1:
        raise ValueError("signals must fill at least one bar later")
    if execution.get("fill_eligibility") != "OFFICIAL_POSITIVE_VOLUME_ONLY":
        raise ValueError("fills require an official positive-volume bar")
    if execution.get("unfilled_intent_policy") != "DEFER_UNTIL_NEXT_ELIGIBLE_OR_CANCEL":
        raise ValueError("unfilled intent policy is not the frozen v1.1 policy")
    if execution.get("terminal_valuation") != "LIQUIDATE_AT_FINAL_CLOSE_WITH_COSTS":
        raise ValueError("terminal valuation must include liquidation costs")
    exposure = float(execution.get("maximum_gross_exposure", 0.0))
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
        raise ValueError("v1.1 chronology is frozen")
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
    if any(strategy.get(key) != value for key, value in exact_strategy.items()):
        raise ValueError("v1.1 strategy family and parameter grid are frozen")
    selection = raw["selection"]
    if selection.get("objective") != "MEDIAN_WALK_FORWARD_CALMAR":
        raise ValueError("v1.1 uses the frozen median walk-forward Calmar objective")
    if int(selection.get("maximum_trials", 0)) != 16:
        raise ValueError("v1.1 trial budget is exactly 16 candidates")
    if int(selection.get("expected_fold_count", 0)) != 9:
        raise ValueError("v1.1 requires exactly nine scoring folds")
    scoring_start = parse_utc_ms(selection["scoring_start_utc"])
    if scoring_start != parse_utc_ms("2020-02-01T00:00:00Z"):
        raise ValueError("v1.1 scoring start is frozen at 2020-02-01 UTC")
    if int(selection.get("minimum_fold_months", 0)) != 6:
        raise ValueError("v1.1 uses exact six-month folds")
    if int(selection.get("minimum_fold_completed_round_trips", -1)) != 2:
        raise ValueError("v1.1 fold round-trip floor is frozen at two")
    minimum_exposure = float(selection.get("minimum_fold_exposure_fraction", -1.0))
    maximum_exposure = float(selection.get("maximum_fold_exposure_fraction", -1.0))
    if minimum_exposure != 0.05 or maximum_exposure != 0.95:
        raise ValueError("v1.1 fold exposure bounds are frozen")
    positive_fraction = float(selection.get("minimum_positive_fold_fraction", -1.0))
    if positive_fraction != 0.75:
        raise ValueError("v1.1 positive-fold threshold is frozen")
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
    if any(float(acceptance.get(key, -1.0)) != value for key, value in exact_acceptance.items()):
        raise ValueError("v1.1 acceptance gates are frozen")
    diagnostics = raw["diagnostics"]
    if int(diagnostics.get("moving_block_bootstrap_resamples", 0)) != 2000:
        raise ValueError("v1.1 bootstrap resample count is frozen at 2000")
    if int(diagnostics.get("moving_block_length_days", 0)) != 7:
        raise ValueError("v1.1 moving-block length is frozen at seven days")
    if int(diagnostics.get("latency_stress_delay_bars", 0)) != 2:
        raise ValueError("v1.1 latency stress is frozen at two bars")
    if diagnostics.get("seed_policy") != "SHA256_EXPERIMENT_ID":
        raise ValueError("diagnostic seed policy mismatch")
    paper = raw["paper"]
    exact_paper = {
        "minimum_calendar_days": 180,
        "minimum_completed_round_trips": 30,
        "maximum_tracking_error_bps": 10.0,
    }
    if any(float(paper.get(key, -1.0)) != value for key, value in exact_paper.items()):
        raise ValueError("v1.1 paper-validation gates are frozen")
