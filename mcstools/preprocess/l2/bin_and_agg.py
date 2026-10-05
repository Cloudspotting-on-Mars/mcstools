"""
Load MCS L2 (DDR1/DDR2) data in Ls chunks, filter and bin it, and aggregate into a
single xarray Dataset.

Pipeline per (Mars Year, Ls chunk):
  1. Load the DDR1 profiles in that Ls range and quality-filter them
     (`FILTER_CONFIG_DEFAULT`).
  2. Split the surviving profiles into day/night by LTST (`DAY_LTST_RANGE`), matching
     L1's Day=1/Night=0 convention.
  3. For each non-empty day/night subset:
     a. Bin DDR1 columns (e.g. Dust_column, T_surf) spatially by Surf_lat/Surf_lon.
     b. Load the matching DDR2 profiles and merge DDR1's profile-level metadata onto
        them via `loader.merge_ddrs` (DDR2 has no Profile_lat/Profile_lon of its own -
        only per-level Lat/Lon - so this is what lets every level of a profile land in
        the same spatial bin instead of drifting across bins as the limb path curves
        with altitude).
     c. Bin DDR2 columns (e.g. Dust, T, Alt) per pressure level by
        Profile_lat/Profile_lon.
  4. Concat the day/night subsets along a new `Day` dim, then Ls chunks along `Ls`,
     then Mars Years along `MY`.
"""

import os
from typing import Dict, List

import click
import xarray as xr
from joblib import Parallel, delayed, parallel_config
from mars_time import MarsTime

from mcstools import L2Loader
from mcstools.preprocess.bin import BinGrid, compute_bin_stats_2d
from mcstools.preprocess.l2.filter_and_bin import filter_ddr1_df_from_config
from mcstools.util.io import makedirs

MY_DEFAULT = list(range(29, 30))
BIN_CONFIG_DEFAULT = {
    "Ls": BinGrid(0, 140, 15, "Ls"),
    "Surf_lat": BinGrid(-90, 90, 5, "Surf_lat"),
    "Surf_lon": BinGrid(-180, 180, 5, "Surf_lon"),
    "Profile_lat": BinGrid(-90, 90, 5, "Profile_lat"),
    "Profile_lon": BinGrid(-180, 180, 5, "Profile_lon"),
}
FILTER_CONFIG_DEFAULT = {
    "Obs_qual": [0, 1, 7, 10, 11, 17],
    "Gqual": [0, 6, 12],
    "1": [0],  # raw MCS record-type/version flag column; 0 = nominal record
}
# DDR1's LTST column is a 0-1 fraction of a sol (not hours) - 9/24 to 21/24 is
# 9am-9pm local time.
DAY_LTST_RANGE = (9 / 24, 21 / 24)
DDR1_AGG_DEFAULT = ["Dust_column", "H2Oice_column", "T_surf"]
DDR1_LAT_BIN_COL = "Surf_lat"
DDR1_LON_BIN_COL = "Surf_lon"
DDR2_AGG_DEFAULT = ["Dust", "H2Oice", "T", "Alt"]
DDR2_LAT_BIN_COL = "Profile_lat"
DDR2_LON_BIN_COL = "Profile_lon"
DEFAULT_N_JOBS = 72


def load_ddr1_ls_chunk(loader, my, ls_bin_start, ls_bin_end):
    """Load DDR1 profiles for a single Mars Year within an Ls range."""
    return loader.load_ls_range(
        MarsTime.from_solar_longitude(my, ls_bin_start),
        MarsTime.from_solar_longitude(my, ls_bin_end),
        ddr="DDR1",
        verbose=False,
    )


def _compute_stats_for_ddr1_subset(
    loader,
    ddr1_subdf,
    ddr1_agg_columns,
    ddr1_lat_bin: BinGrid,
    ddr1_lon_bin: BinGrid,
    ddr2_agg_columns=List[str] | None,
    ddr2_lat_bin=BinGrid | None,
    ddr2_lon_bin=BinGrid | None,
):
    """
    Bin DDR1 columns from `ddr1_subdf`, and (if requested) load the matching DDR2
    profiles, merge DDR1 metadata onto them, and bin DDR2 columns per pressure level.

    `ddr1_subdf` must be non-empty.
    """
    stat_ds_list = []
    for ddr1_col in ddr1_agg_columns:
        stat_ds = compute_bin_stats_2d(ddr1_subdf, ddr1_col, ddr1_lat_bin, ddr1_lon_bin)
        stat_ds_list.append(stat_ds)
    if ddr2_agg_columns is not None:
        ddr2_df = loader.load("DDR2", profiles=ddr1_subdf["Profile_identifier"])
        ddr2_df = loader.merge_ddrs(ddr2_df, ddr1_subdf)
        ddr2_stat_ds_list = []
        for ddr2_col in ddr2_agg_columns:
            level_stat_ds_list = []
            # "level" is just row position (0-104) within a profile's fixed-size DDR2
            # block, not a pressure value - but since the pressure grid is fixed, every
            # profile's Pres at a given level is identical, so it's safe to use as that
            # level's coordinate.
            for plevel, plevel_df in ddr2_df.groupby("level"):
                stat_ds = compute_bin_stats_2d(
                    plevel_df, ddr2_col, ddr2_lat_bin, ddr2_lon_bin
                )
                stat_ds = stat_ds.expand_dims(level=[plevel]).assign_coords(
                    {"Pres": ("level", [plevel_df["Pres"].unique().squeeze()])}
                )
                level_stat_ds_list.append(stat_ds)
            merged_level_stat_ds = xr.concat(
                level_stat_ds_list,
                dim="level",
                join="outer",
                compat="no_conflicts",
            )
            ddr2_stat_ds_list.append(merged_level_stat_ds)
        merged_ddr2_stat_ds = xr.merge(
            ddr2_stat_ds_list, join="outer", compat="no_conflicts"
        )
        stat_ds_list.append(merged_ddr2_stat_ds)
    return xr.merge(stat_ds_list)


def load_and_aggregate_single_ls_chunk(
    loader,
    my,
    ls_bin: BinGrid,
    ls_index,
    filter_config,
    ddr1_agg_columns,
    ddr1_lat_bin: BinGrid,
    ddr1_lon_bin: BinGrid,
    ddr2_agg_columns=List[str] | None,
    ddr2_lat_bin=BinGrid | None,
    ddr2_lon_bin=BinGrid | None,
    verbose=False,
):
    """
    Load, quality-filter, day/night-split, and bin DDR1 (and optionally DDR2) data for
    a single Ls bin of a single Mars Year. Returns an xr.Dataset expanded with
    MY/Ls/Day dims, or None if no profiles survive filtering.
    """
    single_ls_bin = ls_bin[ls_index]
    print(f"Processing MY{my} {single_ls_bin.midpoint} on PID: {os.getpid()}")
    ddr1_df = load_ddr1_ls_chunk(loader, my, single_ls_bin.start, single_ls_bin.stop)
    ddr1_df = filter_ddr1_df_from_config(ddr1_df, filter_config, verbose=verbose)
    if ddr1_df.empty:
        return None
    day_cond = ddr1_df["LTST"].between(*DAY_LTST_RANGE)
    day_ds_list = []
    for day_ind, subdf in zip([1, 0], [ddr1_df[day_cond], ddr1_df[~day_cond]]):
        if subdf.empty:
            # Loading DDR2 for an empty profile list isn't meaningful (and errors), so
            # a day/night subset with no profiles is simply absent from the Day dim
            # here; it's filled back in with NaN by the outer-join concat over Ls/MY
            # chunks that do have both.
            continue
        subset_ds = _compute_stats_for_ddr1_subset(
            loader,
            subdf,
            ddr1_agg_columns,
            ddr1_lat_bin,
            ddr1_lon_bin,
            ddr2_agg_columns=ddr2_agg_columns,
            ddr2_lat_bin=ddr2_lat_bin,
            ddr2_lon_bin=ddr2_lon_bin,
        )
        day_ds_list.append(subset_ds.expand_dims(Day=[day_ind]))
    if len(day_ds_list) == 0:
        return None
    merged_stat_ds = xr.concat(
        day_ds_list, dim="Day", join="outer", compat="no_conflicts"
    )
    merged_stat_ds = merged_stat_ds.expand_dims(MY=[my], Ls=[single_ls_bin.midpoint])
    return merged_stat_ds


def main(
    loader: L2Loader | None = None,
    my_list: List = MY_DEFAULT,
    bin_config: Dict = BIN_CONFIG_DEFAULT,
    filter_config: Dict = FILTER_CONFIG_DEFAULT,
    ddr1_agg_columns: List = DDR1_AGG_DEFAULT,
    ddr1_lat_bin_col: str = DDR1_LAT_BIN_COL,
    ddr1_lon_bin_col: str = DDR1_LON_BIN_COL,
    ddr2_agg_columns: List = DDR2_AGG_DEFAULT,
    ddr2_lat_bin_col: str = DDR2_LAT_BIN_COL,
    ddr2_lon_bin_col: str = DDR2_LON_BIN_COL,
    n_jobs=DEFAULT_N_JOBS,
    verbose=False,
):
    if loader is None:
        loader = L2Loader()
    # Don't load all data at once - break into chunks, aggregate, then piece together
    with parallel_config(backend="loky", n_jobs=n_jobs, verbose=10):
        all_my_ds = []
        for my in my_list:
            merged_stat_ds = Parallel()(
                delayed(load_and_aggregate_single_ls_chunk)(
                    loader,
                    my,
                    bin_config["Ls"],
                    ls_index,
                    filter_config,
                    ddr1_agg_columns,
                    bin_config[ddr1_lat_bin_col],
                    bin_config[ddr1_lon_bin_col],
                    ddr2_agg_columns=ddr2_agg_columns,
                    ddr2_lat_bin=bin_config[ddr2_lat_bin_col],
                    ddr2_lon_bin=bin_config[ddr2_lon_bin_col],
                    verbose=verbose,
                )
                for ls_index in range(len(bin_config["Ls"]))
            )
            if len(merged_stat_ds) == 0:
                continue
            single_my_ds = xr.concat(
                [ds for ds in merged_stat_ds if ds is not None],
                dim="Ls",
                join="outer",
                compat="no_conflicts",
            )
            all_my_ds.append(single_my_ds)
    print("Finished processing.")
    print("Concating all MY Datasets...")
    all_my_ds = xr.concat(all_my_ds, dim="MY", join="outer", compat="no_conflicts")
    print(all_my_ds)
    return all_my_ds


@click.command()
@click.option("--output-path")
def main_cli(output_path):
    results = main()
    makedirs(output_path)
    results.to_netcdf(output_path, engine="netcdf4")


if __name__ == "__main__":
    main_cli()
