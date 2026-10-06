"""Exclude known-bad time windows from binning, shared by the L1B/L2 binning scripts."""

import pandas as pd


def load_excluded_times(path, excluded_dt_col="timestamp") -> pd.Series:
    """
    Load a CSV of timestamps to exclude from binning.

    `excluded_dt_col` is the CSV column holding the timestamps (ISO-8601, UTC -
    sub-second precision not required). Returns a sorted, UTC-aware Series of those
    timestamps.
    """
    df = pd.read_csv(path)
    if excluded_dt_col not in df.columns:
        raise ValueError(f"{path} must have a '{excluded_dt_col}' column")
    return (
        pd.to_datetime(df[excluded_dt_col], utc=True)
        .sort_values()
        .reset_index(drop=True)
    )


def filter_excluded_times(
    df, excluded_times, threshold_s, df_dt_col="dt", excluded_dt_col="timestamp"
):
    """
    Drop rows from `df` whose `df_dt_col` falls within +/- `threshold_s` seconds of any
    timestamp in `excluded_times` (as produced by `load_excluded_times`).

    `excluded_dt_col` names the excluded-timestamp column internally during the merge -
    only matters if it collides with an existing column of `df`.

    A single global threshold makes checking only the nearest excluded timestamp per
    row sufficient: if a row is within `threshold_s` of any excluded time, it's within
    `threshold_s` of the nearest one too. Returned rows are sorted by `df_dt_col`.
    """
    if excluded_times is None or len(excluded_times) == 0 or df.empty:
        return df
    excluded_df = pd.DataFrame({excluded_dt_col: excluded_times})
    sorted_df = df.sort_values(df_dt_col)
    merged = pd.merge_asof(
        sorted_df,
        excluded_df,
        left_on=df_dt_col,
        right_on=excluded_dt_col,
        direction="nearest",
        tolerance=pd.Timedelta(seconds=threshold_s),
    )
    keep_mask = merged[excluded_dt_col].isna().to_numpy()
    return sorted_df[keep_mask]
