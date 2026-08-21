from __future__ import annotations

import math
from collections import deque
from collections.abc import Sequence

from .models import Bar, StrategyParams

HOURS_PER_YEAR = 365.25 * 24.0


def _rolling_previous_extreme(
    values: Sequence[float], lookback: int, maximum: bool
) -> list[float | None]:
    if lookback < 1:
        raise ValueError("lookback must be positive")
    result: list[float | None] = [None] * len(values)
    queue: deque[int] = deque()
    for index, value in enumerate(values):
        while queue and queue[0] < index - lookback:
            queue.popleft()
        if index >= lookback and queue:
            result[index] = values[queue[0]]
        if maximum:
            while queue and values[queue[-1]] <= value:
                queue.pop()
        else:
            while queue and values[queue[-1]] >= value:
                queue.pop()
        queue.append(index)
    return result


def _rolling_mean(values: Sequence[float], lookback: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    total = 0.0
    for index, value in enumerate(values):
        total += value
        if index >= lookback:
            total -= values[index - lookback]
        if index + 1 >= lookback:
            result[index] = total / lookback
    return result


def _rolling_realized_volatility(closes: Sequence[float], lookback: int) -> list[float | None]:
    if lookback < 2:
        raise ValueError("volatility lookback must contain at least two returns")
    result: list[float | None] = [None] * len(closes)
    returns = [math.log(closes[index] / closes[index - 1]) for index in range(1, len(closes))]
    total = 0.0
    total_sq = 0.0
    for return_index, value in enumerate(returns):
        total += value
        total_sq += value * value
        if return_index >= lookback:
            old = returns[return_index - lookback]
            total -= old
            total_sq -= old * old
        if return_index + 1 >= lookback:
            variance = max(0.0, (total_sq - total * total / lookback) / (lookback - 1))
            # Return index 0 is close[1] / close[0], so a window ending at
            # return_index belongs to the following close-bar index.
            result[return_index + 1] = math.sqrt(variance * HOURS_PER_YEAR)
    return result


def build_targets(
    bars: Sequence[Bar],
    params: StrategyParams,
    *,
    evaluation_start_index: int = 0,
) -> list[float]:
    if not bars:
        return []
    if not 0 <= evaluation_start_index < len(bars):
        raise ValueError("evaluation_start_index lies outside bars")
    # Indicator time advances only on official, observed-liquidity bars.  In
    # particular, a synthetic, zero-volume, or zero-trade bar must neither
    # enter a rolling buffer nor evict an older eligible observation from one.
    eligible_indices = [index for index, bar in enumerate(bars) if bar.state_eligible]
    eligible_bars = [bars[index] for index in eligible_indices]
    highs = [bar.high for bar in eligible_bars]
    lows = [bar.low for bar in eligible_bars]
    closes = [bar.close for bar in eligible_bars]
    entry_levels = _rolling_previous_extreme(highs, params.entry_lookback, maximum=True)
    exit_levels = _rolling_previous_extreme(lows, params.exit_lookback, maximum=False)
    trend_levels = _rolling_mean(closes, params.trend_lookback)
    volatilities = _rolling_realized_volatility(closes, params.volatility_lookback)
    eligible_ordinal = {
        bar_index: ordinal for ordinal, bar_index in enumerate(eligible_indices)
    }

    targets: list[float] = []
    is_long = False
    entry_exposure = 0.0
    for index, bar in enumerate(bars):
        if index < evaluation_start_index:
            targets.append(0.0)
            continue
        if not bar.state_eligible:
            targets.append(entry_exposure if is_long else 0.0)
            continue
        ordinal = eligible_ordinal[index]
        entry = entry_levels[ordinal]
        exit_ = exit_levels[ordinal]
        trend = trend_levels[ordinal]
        realized_volatility = volatilities[ordinal]
        ready = None not in (entry, exit_, trend, realized_volatility)
        if not ready:
            targets.append(0.0)
            continue
        assert entry is not None
        assert exit_ is not None
        assert trend is not None
        assert realized_volatility is not None
        if not is_long and bar.close > entry and bar.close > trend:
            denominator = max(realized_volatility, params.volatility_floor)
            entry_exposure = min(1.0, params.target_annualized_volatility / denominator)
            is_long = entry_exposure > 0.0
        elif is_long and (bar.close < exit_ or bar.close < trend):
            is_long = False
            entry_exposure = 0.0
        targets.append(entry_exposure if is_long else 0.0)
    return targets
