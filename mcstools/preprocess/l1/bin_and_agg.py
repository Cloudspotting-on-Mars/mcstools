"""
Load MCS L1B radiance data in Ls chunks, run it through a view pipeline, and aggregate
into a single xarray Dataset.

Pipeline per (Mars Year, Ls chunk):
  1. Load L1B data in that Ls range.
  2. Run it through `view_pipeline.preprocess` (e.g. `L1BStandardInTrack`), which selects/
     averages limb views and - notably - is what actually adds the `LTST` column used next.
  3. Split into day/night by LTST (`DAY_LTST_RANGE`).
  4. For each non-empty day/night subset, bin radiance columns (`agg_columns`) spatially by
     Scene_lat/Scene_lon.
  5. Concat the day/night subsets along a new `Day` dim, then Ls chunks along `Ls`, then
     Mars Years along `MY`.
"""
from mcstools.mcsfile import L1BFile
from typing import Union
from mcstools.preprocess import L1BStandardInTrack
from joblib import parallel_config, Parallel, delayed
from mcstools import L1BLoader
import click
import os
from mars_time import MarsTime
from mcstools.preprocess.bin import BinGrid, Bin, compute_bin_stats_2d
from mcstools.util.log import setup_logging, logger
from mcstools.util.io import makedirs
import numpy as np
import xarray as xr

MY_DEFAULT = [29, 30, 31, 32, 33, 34, 35, 36]

BIN_CONFIG_DEFAULT = {
    "Ls": BinGrid(0, 360,5, "Ls"),
    "Scene_lat": BinGrid(-90, 90, 15, "Scene_lat"),
    "Scene_lon": BinGrid(-180, 180, 15, "Scene_lon"),
}

DEFAULT_N_JOBS = 72
DEFAULT_PIPELINE = L1BStandardInTrack()
AGG_COLUMNS = L1BFile.radcols
# L1B's LTST column is in hours (0-24), unlike L2's 0-1 fraction-of-day LTST.
DAY_LTST_RANGE = (9, 21)

def p10(x): return np.quantile(x, 0.1)
def p90(x): return np.quantile(x, 0.9)

AGG_STATS = ["min", "max", "mean", "median", "std", "count", p10, p90]

def _compute_stats_for_subset(subdf, agg_columns, lat_bins: BinGrid, lon_bins: BinGrid, stats):
    """Bin `agg_columns` from `subdf` spatially and merge into a single xr.Dataset."""
    rad_ds_list = []
    for rdr_col in agg_columns:
        stat_ds = compute_bin_stats_2d(subdf, rdr_col, lat_bins, lon_bins, stats=stats, include_nan_count=False)
        rad_ds_list.append(stat_ds)
    return xr.merge(rad_ds_list, join="outer", compat="no_conflicts")

def load_and_aggregate_single_ls_bin(
    loader: L1BLoader,
    view_pipeline: Union[L1BStandardInTrack],
    my: int,
    ls_bin: Bin,
    lat_bins: BinGrid,
    lon_bins: BinGrid,
    agg_columns=AGG_COLUMNS,
    stats=AGG_STATS,
    verbose=False,
):
    """
    Load, preprocess, day/night-split, and bin L1B data for a single Ls bin of a single
    Mars Year. Returns an xr.Dataset expanded with MY/Ls/Day dims, or None if no data
    survives loading/preprocessing.
    """
    logger.info(f"Processing MY{my} Ls={ls_bin.midpoint} on PID {os.getpid()}")
    l1b_df = loader.load_ls_range(
        MarsTime.from_solar_longitude(my, ls_bin.start),
        MarsTime.from_solar_longitude(my, ls_bin.stop),
        verbose=verbose,
    )
    if l1b_df.empty:
        return None
    # LTST is added here by view_pipeline.preprocess (not by the loader) - see module docstring.
    l1b_df = view_pipeline.preprocess(l1b_df)
    if l1b_df.empty:
        return None
    day_cond = l1b_df["LTST"].between(*DAY_LTST_RANGE)
    day_ds_list = []
    for day_ind, subdf in zip([1, 0], [l1b_df[day_cond], l1b_df[~day_cond]]):
        if subdf.empty:
            continue
        subset_ds = _compute_stats_for_subset(subdf, agg_columns, lat_bins, lon_bins, stats)
        day_ds_list.append(subset_ds.expand_dims(Day=[day_ind]))
    if len(day_ds_list) == 0:
        return None
    concat_stat_ds = xr.concat(day_ds_list, dim="Day", join="outer", compat="no_conflicts")
    concat_stat_ds = concat_stat_ds.expand_dims(MY=[my], Ls=[ls_bin.midpoint])
    return concat_stat_ds

def main(
    loader: L1BLoader|None = None,
    view_pipeline: Union[L1BStandardInTrack]| None =  DEFAULT_PIPELINE,
    my_list = MY_DEFAULT,
    bin_config = BIN_CONFIG_DEFAULT,
    agg_columns=AGG_COLUMNS,
    stats=AGG_STATS,
    n_jobs = DEFAULT_N_JOBS,
    verbose=True
):
    if loader is None:
        loader = L1BLoader()
    all_my_ds = []
    with parallel_config(n_jobs=n_jobs, verbose=10):
        for my in my_list:
            my_stat_ds_list = Parallel()(
                delayed(load_and_aggregate_single_ls_bin)(
                    loader,
                    view_pipeline,
                    my,
                    bin_config["Ls"][ls_i],
                    bin_config["Scene_lat"],
                    bin_config["Scene_lon"],
                    agg_columns=agg_columns,
                    stats=stats,
                    verbose=verbose,
                ) for ls_i in range(len(bin_config["Ls"]))
            )
            my_stat_ds_list = [ds for ds in my_stat_ds_list if ds is not None]
            if len(my_stat_ds_list) == 0:
                continue
            single_my_ds = xr.concat(my_stat_ds_list, dim="Ls", join="outer", compat="no_conflicts")
            # An Ls chunk with no data at all (e.g. a gap with no files to load) is simply
            # absent above rather than producing a dataset - reindex against every bin's
            # midpoint so those chunks show up as NaN instead of silently vanishing from Ls.
            single_my_ds = single_my_ds.reindex(Ls=bin_config["Ls"].midpoints)
            all_my_ds.append(single_my_ds)
    all_my_ds = xr.concat(all_my_ds, dim="MY", join="outer", compat="no_conflicts")
    # Likewise for a Mars Year with no data in any Ls chunk.
    return all_my_ds.reindex(MY=my_list)


@click.command()
@click.option("--output-path")
def main_cli(output_path):
    results = main()
    if output_path:
        logger.info(f"Saving to {output_path}.")
        makedirs(output_path)
        results.to_netcdf(output_path)

if __name__=="__main__":
    setup_logging()
    main_cli()
