from __future__ import annotations

import math
import statistics
from collections import OrderedDict, defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from .engine import ExecutionAssumptions, run_backtest
from .models import BacktestResult, Bar

HOUR_MS = 3_600_000
MILLISECONDS_PER_YEAR = 1000 * 60 * 60 * 24 * 365.25


def _daily_equity(
    result: BacktestResult, *, terminal_value: float | None = None
) -> list[tuple[datetime, float]]:
    daily: OrderedDict[str, tuple[datetime, float]] = OrderedDict()
    last_index = len(result.equity) - 1
    for index, point in enumerate(result.equity):
        moment = datetime.fromtimestamp(point.open_time_ms / 1000, tz=UTC)
        value = (
            terminal_value if index == last_index and terminal_value is not None else point.equity
        )
        daily[moment.date().isoformat()] = (moment, value)
    return list(daily.values())


def _sample_sharpe(returns: Sequence[float]) -> float:
    if len(returns) < 2:
        return 0.0
    deviation = statistics.stdev(returns)
    return 0.0 if deviation == 0 else statistics.mean(returns) / deviation * math.sqrt(365.25)


def calculate_metrics(result: BacktestResult) -> dict[str, Any]:
    if not result.equity:
        raise ValueError("cannot calculate metrics for an empty result")
    terminal_point = result.equity[-1]
    mark_to_market_terminal = terminal_point.equity
    fee_rate = result.fee_bps_per_side / 10_000.0
    slippage_rate = result.slippage_bps_per_side / 10_000.0
    liquidation_execution_price = terminal_point.close * (1.0 - slippage_rate)
    liquidation_gross = terminal_point.quantity * liquidation_execution_price
    liquidation_fee = liquidation_gross * fee_rate
    liquidation_value = terminal_point.cash + liquidation_gross - liquidation_fee
    liquidation_slippage_cost = terminal_point.quantity * terminal_point.close - liquidation_gross
    liquidation_cost = mark_to_market_terminal - liquidation_value

    # Performance statistics are realizable terminal wealth statistics.  The
    # stored BacktestResult remains a mark-to-market diagnostic and no
    # synthetic SELL is added to fills or completed round trips.
    curve = [point.equity for point in result.equity]
    curve[-1] = liquidation_value
    terminal = liquidation_value
    total_return = terminal / result.initial_cash - 1.0
    # Each equity point represents the close of one complete hourly interval.
    # The first-to-last open-time difference omits the final bar unless one
    # additional hour is included.
    elapsed_years = max(
        (result.equity[-1].open_time_ms - result.equity[0].open_time_ms + HOUR_MS)
        / MILLISECONDS_PER_YEAR,
        1.0 / (365.25 * 24.0),
    )
    cagr = (terminal / result.initial_cash) ** (1.0 / elapsed_years) - 1.0

    peak = curve[0]
    maximum_drawdown = 0.0
    for value in curve:
        peak = max(peak, value)
        maximum_drawdown = min(maximum_drawdown, value / peak - 1.0)
    calmar = cagr / abs(maximum_drawdown) if maximum_drawdown < 0 else None

    daily = _daily_equity(result, terminal_value=liquidation_value)
    daily_returns = [daily[index][1] / daily[index - 1][1] - 1.0 for index in range(1, len(daily))]
    sharpe = _sample_sharpe(daily_returns)
    downside = [min(0.0, value) for value in daily_returns]
    downside_deviation = statistics.stdev(downside) if len(downside) >= 2 else 0.0
    sortino = (
        statistics.mean(daily_returns) / downside_deviation * math.sqrt(365.25)
        if downside_deviation > 0
        else 0.0
    )

    quarterly_pnl: dict[str, float] = defaultdict(float)
    for index in range(1, len(daily)):
        moment = daily[index][0]
        quarter = (moment.month - 1) // 3 + 1
        key = f"{moment.year}-Q{quarter}"
        quarterly_pnl[key] += daily[index][1] - daily[index - 1][1]
    positive_quarters = [value for value in quarterly_pnl.values() if value > 0]
    concentration = max(positive_quarters) / sum(positive_quarters) if positive_quarters else 1.0

    exposure = sum(1 for point in result.equity if point.quantity > 0) / len(result.equity)
    fees = sum(fill.fee for fill in result.fills)
    return {
        "initial_cash": result.initial_cash,
        "terminal_equity": terminal,
        "mark_to_market_terminal_equity": mark_to_market_terminal,
        "mark_to_market_total_return": mark_to_market_terminal / result.initial_cash - 1.0,
        "terminal_liquidation_applied": terminal_point.quantity > 0.0,
        "terminal_liquidation_value": liquidation_value,
        "terminal_liquidation_cost": liquidation_cost,
        "terminal_liquidation_slippage_cost": liquidation_slippage_cost,
        "terminal_liquidation_fee": liquidation_fee,
        "terminal_liquidation_reference_price": terminal_point.close,
        "terminal_liquidation_execution_price": liquidation_execution_price,
        "terminal_open_quantity": terminal_point.quantity,
        "total_return": total_return,
        "cagr": cagr,
        "annualized_sharpe_daily": sharpe,
        "annualized_sortino_daily": sortino,
        "maximum_drawdown": maximum_drawdown,
        "calmar": calmar,
        "completed_round_trips": result.completed_round_trips,
        "fill_count": len(result.fills),
        "exposure_fraction": exposure,
        "total_fees": fees,
        "performance_total_fees_including_terminal_liquidation": fees + liquidation_fee,
        "maximum_positive_quarter_mark_to_market_profit_concentration": concentration,
        "start_open_time_ms": result.equity[0].open_time_ms,
        "end_open_time_ms": result.equity[-1].open_time_ms,
        "equity_points": len(result.equity),
    }


def buy_and_hold_metrics(bars: Sequence[Bar], assumptions: ExecutionAssumptions) -> dict[str, Any]:
    if len(bars) < 2:
        raise ValueError("benchmark requires at least two bars")
    targets = [1.0] * len(bars)
    return calculate_metrics(run_backtest(bars, targets, assumptions))
