from __future__ import annotations

import numpy as np
import pandas as pd


def fit_and_predict_rolling_mean(y: pd.Series, horizon: int) -> np.ndarray:
    """
    Baseline: forecast each future day as the mean of the last 7 observed values (or fewer if history is short).
    """
    y = y.dropna()
    if y.empty:
        return np.array([0.0] * horizon)

    window = min(7, len(y))
    pred = float(y.tail(window).mean())
    return np.array([pred] * horizon)
