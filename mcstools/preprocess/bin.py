from functools import cached_property

import numpy as np
import pandas as pd
import xarray as xr
from scipy.stats import binned_statistic_2d


class Bin:
    def __init__(self, start, stop):
        self.start = start
        self.stop = stop

    @property
    def midpoint(self):
        return (self.start + self.stop) / 2

    def __repr__(self):
        return f"Bin({self.start}, {self.stop})"

    def __eq__(self, other):
        if not isinstance(other, Bin):
            return NotImplemented
        return self.start == other.start and self.stop == other.stop


class BinGrid:
    """
    An evenly-spaced sequence of `Bin`s from `start` to `stop` in steps of `size`.

    `name` is both the DataFrame column this grid bins values from and the
    xarray dimension/coordinate name of the resulting binned statistics.
    """

    def __init__(self, start, stop, size, name):
        self.start = start
        self.stop = stop
        self.size = size
        self.name = name
        self.edges = np.arange(self.start, self.stop + self.size, self.size)

    @cached_property
    def bins(self):
        return [Bin(bs, bs + self.size) for bs in self.edges[:-1]]

    @property
    def midpoints(self):
        return np.array([b.midpoint for b in self.bins])

    def __len__(self):
        return len(self.bins)

    def __getitem__(self, i):
        return self.bins[i]

    def find_bin_from_value(self, value):
        idx = np.digitize(value, self.edges) - 1
        if idx < 0 or idx >= len(self):
            return None
        return self.bins[idx]


def compute_bin_stats_2d(
    df: pd.DataFrame,
    value_col,
    lat_bins: BinGrid,
    lon_bins: BinGrid,
    stats=("mean", "median", "std", "count"),
    include_nan_count=True,
) -> xr.Dataset:
    """
    Bin `df[value_col]` by `df[lat_bins.name]`/`df[lon_bins.name]` and compute `stats`
    for each bin, returning an xr.Dataset with dims/coords named after `lat_bins.name`
    and `lon_bins.name`.

    Each entry in `stats` is a stat name or callable accepted by
    `scipy.stats.binned_statistic_2d`'s `statistic` argument.

    If `include_nan_count`, rows where `value_col` is null are excluded from `stats`
    (so they don't pollute e.g. `mean`) but are counted separately per bin into a
    `{value_col}_nan_count` variable.
    """
    lat_col, lon_col = lat_bins.name, lon_bins.name
    not_null_df = df.dropna(subset=[value_col, lat_col, lon_col])
    data_vars = {}
    if not not_null_df.empty:
        for stat in stats:
            stat_name = stat if isinstance(stat, str) else stat.__name__
            result = binned_statistic_2d(
                not_null_df[lat_col],
                not_null_df[lon_col],
                not_null_df[value_col],
                bins=[lat_bins.edges, lon_bins.edges],
                statistic=stat,
            ).statistic
            data_vars[f"{value_col}_{stat_name}"] = ([lat_col, lon_col], result)
    if include_nan_count:
        null_df = df[df[value_col].isnull()]
        if not null_df.empty:
            nan_count = binned_statistic_2d(
                null_df[lat_col],
                null_df[lon_col],
                null_df[value_col],
                bins=[lat_bins.edges, lon_bins.edges],
                statistic="count",
            ).statistic
            data_vars[f"{value_col}_nan_count"] = ([lat_col, lon_col], nan_count)
    return xr.Dataset(
        data_vars=data_vars,
        coords={
            lat_col: lat_bins.midpoints,
            lon_col: lon_bins.midpoints,
        },
    )
