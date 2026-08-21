from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class ExecutionMode(StrEnum):
    BACKTEST = "BACKTEST"
    PAPER = "PAPER"


@dataclass(frozen=True, slots=True)
class Bar:
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time_ms: int
    synthetic: bool = False


@dataclass(frozen=True, slots=True)
class StrategyParams:
    entry_lookback: int
    exit_lookback: int
    trend_lookback: int
    volatility_lookback: int
    target_annualized_volatility: float
    volatility_floor: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "entry_lookback": self.entry_lookback,
            "exit_lookback": self.exit_lookback,
            "trend_lookback": self.trend_lookback,
            "volatility_lookback": self.volatility_lookback,
            "target_annualized_volatility": self.target_annualized_volatility,
            "volatility_floor": self.volatility_floor,
        }


@dataclass(frozen=True, slots=True)
class Fill:
    side: str
    source_signal_index: int
    fill_index: int
    open_time_ms: int
    quantity: float
    reference_price: float
    execution_price: float
    fee: float


@dataclass(frozen=True, slots=True)
class EquityPoint:
    open_time_ms: int
    cash: float
    quantity: float
    close: float
    equity: float


@dataclass(frozen=True, slots=True)
class BacktestResult:
    equity: tuple[EquityPoint, ...]
    fills: tuple[Fill, ...]
    initial_cash: float
    fee_bps_per_side: float
    slippage_bps_per_side: float

    @property
    def completed_round_trips(self) -> int:
        return sum(1 for fill in self.fills if fill.side == "SELL")
