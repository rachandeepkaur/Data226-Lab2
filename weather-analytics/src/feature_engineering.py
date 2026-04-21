from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


def raw_rows_to_staging_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """Normalize API rows to a DataFrame with DATE as Python date objects for Snowflake binding."""
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    return df
