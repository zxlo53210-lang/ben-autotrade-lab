from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .models import BacktestResult, Bar, EquityPoint, Fill


@dataclass(frozen=True, slots=True)
class ExecutionAssumptions:
    initial_cash: float
    fee_bps_per_side: float
    slippage_bps_per_side: float
    maximum_gross_exposure: float = 1.0
    signal_delay_bars: int = 1

    def __post_init__(self) -> None:
        if self.initial_cash <= 0:
            raise ValueError("initial cash must be positive")
        if self.fee_bps_per_side < 0 or self.slippage_bps_per_side < 0:
            raise ValueError("costs cannot be negative")
        if not 0 < self.maximum_gross_exposure <= 1:
            raise ValueError("maximum gross exposure must be within (0, 1]")
        if self.signal_delay_bars < 1:
            raise ValueError("signal delay must be at least one bar")


@dataclass(frozen=True, slots=True)
class _PendingIntent:
    """A regime change waiting for the next executable bar.

    The original signal index and target are deliberately immutable.  Later
    signals may cancel the intent, but cannot silently move its timestamp or
    resize it while it is waiting for liquidity.
    """

    target: float
    source_signal_index: int


def run_backtest(
    bars: Sequence[Bar], targets: Sequence[float], assumptions: ExecutionAssumptions
) -> BacktestResult:
    if len(bars) != len(targets):
        raise ValueError("bars and targets must have the same length")
    if not bars:
        raise ValueError("backtest requires at least one bar")
    for target in targets:
        if not 0.0 <= target <= assumptions.maximum_gross_exposure:
            raise ValueError(f"invalid target exposure: {target}")

    fee_rate = assumptions.fee_bps_per_side / 10_000.0
    slippage_rate = assumptions.slippage_bps_per_side / 10_000.0
    cash = assumptions.initial_cash
    quantity = 0.0
    fills: list[Fill] = []
    equity: list[EquityPoint] = []
    pending: _PendingIntent | None = None

    for index, bar in enumerate(bars):
        # A close-derived target becomes actionable only at the following bar.
        # If an older intent is still pending, the newest closed-bar signal can
        # cancel it by returning to the portfolio's current regime.  Otherwise
        # the original intent (including its source index and size) survives.
        if index >= assumptions.signal_delay_bars:
            signal_index = index - assumptions.signal_delay_bars
            newest_target = targets[signal_index]
            current_is_long = quantity > 0.0
            newest_is_long = newest_target > 0.0
            if pending is not None and newest_is_long == current_is_long:
                pending = None
            elif pending is None and newest_is_long != current_is_long:
                pending = _PendingIntent(
                    target=newest_target,
                    source_signal_index=signal_index,
                )

        executable = not bar.synthetic and bar.volume > 0.0
        if executable and pending is not None and pending.target > 0.0 and quantity == 0.0:
            execution_price = bar.open * (1.0 + slippage_rate)
            budget = cash * pending.target
            bought = budget / (execution_price * (1.0 + fee_rate))
            gross = bought * execution_price
            fee = gross * fee_rate
            cash -= gross + fee
            if cash < -1e-8:
                raise ArithmeticError("buy produced negative cash")
            cash = max(0.0, cash)
            quantity = bought
            fills.append(
                Fill(
                    side="BUY",
                    source_signal_index=pending.source_signal_index,
                    fill_index=index,
                    open_time_ms=bar.open_time_ms,
                    quantity=bought,
                    reference_price=bar.open,
                    execution_price=execution_price,
                    fee=fee,
                )
            )
            pending = None
        elif executable and pending is not None and pending.target == 0.0 and quantity > 0.0:
            execution_price = bar.open * (1.0 - slippage_rate)
            sold = quantity
            gross = sold * execution_price
            fee = gross * fee_rate
            cash += gross - fee
            quantity = 0.0
            fills.append(
                Fill(
                    side="SELL",
                    source_signal_index=pending.source_signal_index,
                    fill_index=index,
                    open_time_ms=bar.open_time_ms,
                    quantity=sold,
                    reference_price=bar.open,
                    execution_price=execution_price,
                    fee=fee,
                )
            )
            pending = None

        marked_equity = cash + quantity * bar.close
        if cash < -1e-8 or quantity < -1e-12 or marked_equity < -1e-8:
            raise ArithmeticError("portfolio invariant violated")
        equity.append(
            EquityPoint(
                open_time_ms=bar.open_time_ms,
                cash=cash,
                quantity=quantity,
                close=bar.close,
                equity=marked_equity,
            )
        )

    return BacktestResult(
        equity=tuple(equity),
        fills=tuple(fills),
        initial_cash=assumptions.initial_cash,
        fee_bps_per_side=assumptions.fee_bps_per_side,
        slippage_bps_per_side=assumptions.slippage_bps_per_side,
    )
