from __future__ import annotations

import math
import random
import statistics
from collections.abc import Sequence


def annualized_daily_sharpe(returns: Sequence[float]) -> float:
    if len(returns) < 2:
        return 0.0
    deviation = statistics.stdev(returns)
    if deviation == 0.0:
        return 0.0
    return statistics.mean(returns) / deviation * math.sqrt(365.25)


def moving_block_max_sharpe(
    candidate_daily_returns: Sequence[Sequence[float]],
    *,
    selected_index: int,
    resamples: int,
    block_length_days: int,
    seed_hex: str,
) -> dict[str, float | int | str]:
    """Family-wise max-Sharpe null diagnostic with shared circular blocks.

    Each candidate is centered independently before resampling. Every bootstrap
    draw uses the same day indexes across candidates, preserving their
    cross-strategy dependence. This is a diagnostic, never a rescue gate.
    """

    if len(candidate_daily_returns) < 2:
        raise ValueError("max-stat diagnostic requires at least two candidates")
    if not 0 <= selected_index < len(candidate_daily_returns):
        raise ValueError("selected candidate index is invalid")
    if resamples < 100:
        raise ValueError("at least 100 bootstrap resamples are required")
    lengths = {len(series) for series in candidate_daily_returns}
    if len(lengths) != 1:
        raise ValueError("candidate return histories must have equal length")
    observations = lengths.pop()
    if not 1 < block_length_days < observations:
        raise ValueError("moving-block length is invalid")
    if len(seed_hex) != 64 or any(character not in "0123456789abcdef" for character in seed_hex):
        raise ValueError("seed must be one lowercase SHA-256 hex digest")

    centered = [
        [value - statistics.mean(series) for value in series] for series in candidate_daily_returns
    ]
    observed = annualized_daily_sharpe(candidate_daily_returns[selected_index])
    observed_family_max = max(annualized_daily_sharpe(series) for series in candidate_daily_returns)
    generator = random.Random(int(seed_hex[:16], 16))
    maximum_null_sharpes: list[float] = []
    for _ in range(resamples):
        indexes: list[int] = []
        while len(indexes) < observations:
            start = generator.randrange(observations)
            indexes.extend((start + offset) % observations for offset in range(block_length_days))
        indexes = indexes[:observations]
        maximum_null_sharpes.append(
            max(
                annualized_daily_sharpe([series[index] for index in indexes]) for series in centered
            )
        )
    exceedances = sum(value >= observed for value in maximum_null_sharpes)
    p_value = (exceedances + 1) / (resamples + 1)
    ordered = sorted(maximum_null_sharpes)
    threshold_index = min(resamples - 1, math.ceil(0.95 * resamples) - 1)
    return {
        "method": "SHARED_CIRCULAR_MOVING_BLOCK_MAX_SHARPE_V1",
        "candidate_count": len(candidate_daily_returns),
        "observation_days": observations,
        "resamples": resamples,
        "block_length_days": block_length_days,
        "seed_sha256": seed_hex,
        "selected_annualized_daily_sharpe": observed,
        "observed_family_max_annualized_daily_sharpe": observed_family_max,
        "family_wise_null_p_value": p_value,
        "family_wise_null_95pct_max_sharpe": ordered[threshold_index],
    }
