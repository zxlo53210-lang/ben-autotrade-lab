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


class TerminalLiquidationNotExecutable(RuntimeError):
    """The frozen terminal valuation cannot be executed on the final bar."""

    code = "TERMINAL_LIQUIDATION_NOT_EXECUTABLE"


def boundary_aware_utc_daily_metrics(
    result: BacktestResult,
    *,
    boundary_equity: float | None = None,
    terminal_value: float | None = None,
) -> dict[str, Any]:
    """Return the one canonical UTC daily path for every evaluation scorer.

    The boundary is the wealth immediately before the first evaluated bar.
    Including it makes the first UTC day a real observation rather than
    silently starting at the second daily endpoint.  ``terminal_value``
    replaces the final mark on the final day so liquidation costs flow through
    daily returns and quarterly PnL exactly once.
    """

    if not result.equity:
        raise ValueError("cannot calculate UTC daily metrics for an empty result")
    boundary = result.initial_cash if boundary_equity is None else boundary_equity
    if boundary <= 0:
        raise ValueError("UTC daily metric boundary equity must be positive")

    daily: OrderedDict[str, tuple[datetime, float]] = OrderedDict()
    last_index = len(result.equity) - 1
    for index, point in enumerate(result.equity):
        moment = datetime.fromtimestamp(point.open_time_ms / 1000, tz=UTC)
        value = (
            terminal_value if index == last_index and terminal_value is not None else point.equity
        )
        if value < 0:
            raise ValueError("UTC daily metric endpoint equity cannot be negative")
        daily[moment.date().isoformat()] = (moment, value)

    path: list[dict[str, Any]] = []
    quarterly_pnl: dict[str, float] = defaultdict(float)
    previous_equity = boundary
    for moment, equity in daily.values():
        if previous_equity <= 0:
            raise ValueError("UTC daily return has a non-positive prior boundary")
        daily_return = equity / previous_equity - 1.0
        pnl = equity - previous_equity
        quarter = f"{moment.year}-Q{(moment.month - 1) // 3 + 1}"
        quarterly_pnl[quarter] += pnl
        path.append(
            {
                "date_utc": moment.date().isoformat(),
                "end_open_time_ms": int(moment.timestamp() * 1000),
                "end_equity": equity,
                "return": daily_return,
                "pnl": pnl,
                "quarter_utc": quarter,
            }
        )
        previous_equity = equity

    daily_returns = [float(point["return"]) for point in path]
    sharpe = _sample_sharpe(daily_returns)
    downside = [min(0.0, value) for value in daily_returns]
    downside_deviation = statistics.stdev(downside) if len(downside) >= 2 else 0.0
    sortino = (
        statistics.mean(daily_returns) / downside_deviation * math.sqrt(365.25)
        if downside_deviation > 0
        else 0.0
    )
    positive_quarters = [value for value in quarterly_pnl.values() if value > 0]
    concentration = max(positive_quarters) / sum(positive_quarters) if positive_quarters else 1.0
    return {
        "path": tuple(path),
        "annualized_sharpe_daily": sharpe,
        "annualized_sortino_daily": sortino,
        "quarterly_pnl": dict(quarterly_pnl),
        "maximum_positive_quarter_mark_to_market_profit_concentration": concentration,
    }


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
    terminal_liquidation_required = terminal_point.quantity > 0.0
    terminal_state_eligible = bool(getattr(terminal_point, "state_eligible", False))
    if terminal_liquidation_required and not terminal_state_eligible:
        raise TerminalLiquidationNotExecutable(
            "TERMINAL_LIQUIDATION_NOT_EXECUTABLE: final bar is not an official "
            "positive-volume positive-trade bar"
        )
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

    peak = result.initial_cash
    maximum_drawdown = 0.0
    for value in curve:
        peak = max(peak, value)
        maximum_drawdown = min(maximum_drawdown, value / peak - 1.0)
    calmar = cagr / abs(maximum_drawdown) if maximum_drawdown < 0 else None

    daily = boundary_aware_utc_daily_metrics(
        result,
        boundary_equity=result.initial_cash,
        terminal_value=liquidation_value,
    )

    exposure = sum(1 for point in result.equity if point.quantity > 0) / len(result.equity)
    fees = sum(fill.fee for fill in result.fills)
    return {
        "initial_cash": result.initial_cash,
        "terminal_equity": terminal,
        "mark_to_market_terminal_equity": mark_to_market_terminal,
        "mark_to_market_total_return": mark_to_market_terminal / result.initial_cash - 1.0,
        "terminal_liquidation_applied": terminal_liquidation_required,
        "terminal_liquidation_executable": (
            not terminal_liquidation_required or terminal_state_eligible
        ),
        "terminal_state_eligible": terminal_state_eligible,
        "terminal_liquidation_value": liquidation_value,
        "terminal_liquidation_cost": liquidation_cost,
        "terminal_liquidation_slippage_cost": liquidation_slippage_cost,
        "terminal_liquidation_fee": liquidation_fee,
        "terminal_liquidation_reference_price": terminal_point.close,
        "terminal_liquidation_execution_price": liquidation_execution_price,
        "terminal_open_quantity": terminal_point.quantity,
        "total_return": total_return,
        "cagr": cagr,
        "annualized_sharpe_daily": daily["annualized_sharpe_daily"],
        "annualized_sortino_daily": daily["annualized_sortino_daily"],
        "maximum_drawdown": maximum_drawdown,
        "calmar": calmar,
        "completed_round_trips": result.completed_round_trips,
        "fill_count": len(result.fills),
        "exposure_fraction": exposure,
        "total_fees": fees,
        "performance_total_fees_including_terminal_liquidation": fees + liquidation_fee,
        "maximum_positive_quarter_mark_to_market_profit_concentration": daily[
            "maximum_positive_quarter_mark_to_market_profit_concentration"
        ],
        "start_open_time_ms": result.equity[0].open_time_ms,
        "end_open_time_ms": result.equity[-1].open_time_ms,
        "equity_points": len(result.equity),
    }


def buy_and_hold_metrics(bars: Sequence[Bar], assumptions: ExecutionAssumptions) -> dict[str, Any]:
    if len(bars) < 2:
        raise ValueError("benchmark requires at least two bars")
    targets = [1.0] * len(bars)
    return calculate_metrics(run_backtest(bars, targets, assumptions))
