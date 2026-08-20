from json import load
import click
import os
from joblib import Parallel, delayed, parallel_config
import pandas as pd
from scipy.stats import binned_statistic_2d
from typing import List, Dict
import xarray as xr
from mars_time import MarsTime

from mcstools.preprocess.l2.filter_and_bin import filter_ddr1_df_from_config, Bin
from mcstools import L2Loader
from mcstools.util.io import load_yaml, makedirs

MY_DEFAULT= list(range(29, 37))
BIN_CONFIG_DEFAULT = {
    "Ls": Bin(0, 140, 3),
    "Surf_lat": Bin(-90, 90, 5),
    "Surf_lon": Bin(-180, 180, 5),
    "Profile_lat": Bin(-90, 90, 5),
    "Profile_lon": Bin(-180, 180, 5)
}
FILTER_CONFIG_DEFAULT = {
    "LTST": (9/24, 21/24),
    "Obs_qual": [0, 1, 7, 10, 11, 17],
    "Gqual": [0, 6, 12],
    "1": [0]
}
DDR1_AGG_DEFAULT = ["Dust_column", "T_surf"]
DDR1_LAT_BIN_COL = "Surf_lat"
DDR1_LON_BIN_COL = "Surf_lon"
DDR2_AGG_DEFAULT = ["Dust", "T", "Alt"]
DDR2_LAT_BIN_COL = "Profile_lat"
DDR2_LON_BIN_COL = "Profile_lon"
DEFAULT_N_JOBS = 72

def load_ddr1_ls_chunk(loader, my, ls_bin_start, ls_bin_end):
    return loader.load_ls_range(
            MarsTime.from_solar_longitude(my, ls_bin_start),
            MarsTime.from_solar_longitude(my, ls_bin_end),
            ddr="DDR1",
            verbose=False
        )

def make_stats_for_single_bin_from_subdf(
    df: pd.DataFrame,
    variable_column: str,
    lat_bin: Bin,
    lon_bin: Bin,
    lat_col: str,
    lon_col: str,
):
    """
    Bin a single DDR1 or DDR2 df by lat/lon and compute statistics for profiles within that bin.
    Ideally filtered to small time window and if DDR2 filtered to single level (if not, will
    compute statistics across all levels). 
    """
    valid_variable_columns = ["Dust_column", "T_surf", "Dust", "T", "H2Oice" "Pres", "Alt"]
    valid_lat_columns = ["Surf_lat", "Profile_lat", "Solar_lat", "Lat"]
    valid_lon_columns = ["Surf_lon", "Profile_lon", "Solar_lon", "Lon"]
    if variable_column not in valid_variable_columns:
        raise ValueError(f"Either {variable_column} not valid L2 column or not yet implemented for aggregating")
    for lc, valid_lc in zip([lat_col, lon_col], [valid_lat_columns, valid_lon_columns]):
        if lc not in valid_lc:
            raise ValueError(f"{lc} not a valid lat/lon col: {valid_lc}")
    not_null_df = df.dropna(subset=variable_column)
    null_df = df[df[variable_column].isnull()]
    if not not_null_df.empty: 
        stat_dict = {
            stat: binned_statistic_2d(
                not_null_df[lat_col], 
                not_null_df[lon_col],
                not_null_df[variable_column], 
                bins=[lat_bin.bins, lon_bin.bins],
                statistic=stat
            ).statistic for stat in ["mean", "median", "std", "count"]
        }
    else: 
        stat_dict = {}
    if not null_df.empty:
        stat_dict["nan_count"] = binned_statistic_2d(
            null_df[lat_col], 
                null_df[lon_col],
                null_df[variable_column], 
                bins=[lat_bin.bins, lon_bin.bins],
                statistic="count"
        ).statistic
    ds = xr.Dataset(
        data_vars={
            f"{variable_column}_{stat}": ([lat_col, lon_col], stat_dict[stat]) for stat in stat_dict.keys()
        },
        coords={
            lat_col: lat_bin.midpoints,
            lon_col: lon_bin.midpoints,
        }
    )
    return ds

def load_and_aggregate_single_ls_chunk(
    loader,
    my,
    ls_bin,
    ls_index,
    filter_config, 
    ddr1_agg_columns, 
    ddr1_lat_bin, 
    ddr1_lon_bin, 
    ddr1_lat_bin_col, 
    ddr1_lon_bin_col,
    ddr2_agg_columns=List[str]|None,
    ddr2_lat_bin=Bin|None,
    ddr2_lon_bin=Bin|None,
    ddr2_lat_bin_col=str|None,
    ddr2_lon_bin_col=str|None,
    verbose=False
):
    print(f"Processing MY{my} {ls_bin.midpoints[ls_index]} on PID: {os.getpid()}")
    ddr1_df = load_ddr1_ls_chunk(loader, my, ls_bin.bins[ls_index], ls_bin.bins[ls_index+1])
    ddr1_df = filter_ddr1_df_from_config(ddr1_df, filter_config, verbose=verbose)
    if ddr1_df.empty:
        return
    stat_ds_list = []
    for ddr1_col in ddr1_agg_columns:
        stat_ds = make_stats_for_single_bin_from_subdf(ddr1_df, ddr1_col, ddr1_lat_bin, ddr1_lon_bin, lat_col=ddr1_lat_bin_col, lon_col=ddr1_lon_bin_col)
        stat_ds_list.append(stat_ds)
    if ddr2_agg_columns is not None:
        ddr2_df = loader.load("DDR2", profiles=ddr1_df["Profile_identifier"])
        ddr2_df = loader.merge_ddrs(ddr2_df, ddr1_df)
        ddr2_stat_ds_list = []
        for ddr2_col in ddr2_agg_columns:
            level_stat_ds_list = []
            for plevel, plevel_df in ddr2_df.groupby("level"):
                stat_ds = make_stats_for_single_bin_from_subdf(
                    plevel_df, 
                    ddr2_col, 
                    ddr2_lat_bin, 
                    ddr2_lon_bin, 
                    lat_col=ddr2_lat_bin_col, 
                    lon_col=ddr2_lon_bin_col,
                )
                stat_ds = stat_ds.expand_dims(level=[plevel]).assign_coords({"Pres": ("level", [plevel_df["Pres"].unique().squeeze()])})
                level_stat_ds_list.append(stat_ds)
            merged_level_stat_ds = xr.concat(
                level_stat_ds_list,
                dim="level", 
                join="outer", 
                compat="no_conflicts",
            )
            ddr2_stat_ds_list.append(merged_level_stat_ds)
        merged_ddr2_stat_ds = xr.merge(ddr2_stat_ds_list, join="outer", compat="no_conflicts")
        stat_ds_list.append(merged_ddr2_stat_ds)
    merged_stat_ds = xr.merge(stat_ds_list)
    merged_stat_ds = merged_stat_ds.expand_dims(MY=[my], Ls=[ls_bin.midpoints[ls_index]])
    return merged_stat_ds

def main(
    loader: L2Loader|None = None,
    my_list: List = MY_DEFAULT,
    bin_config: Dict=BIN_CONFIG_DEFAULT,
    filter_config: Dict=FILTER_CONFIG_DEFAULT,
    ddr1_agg_columns: List=DDR1_AGG_DEFAULT,
    ddr1_lat_bin_col: str=DDR1_LAT_BIN_COL,
    ddr1_lon_bin_col: str=DDR1_LON_BIN_COL,
    ddr2_agg_columns: List=DDR2_AGG_DEFAULT,
    ddr2_lat_bin_col: str=DDR2_LAT_BIN_COL,
    ddr2_lon_bin_col: str=DDR2_LON_BIN_COL,
    n_jobs=DEFAULT_N_JOBS,
    verbose=False
):
    if loader is None:
        loader = L2Loader()
    # Don't load all data at once - break into chunks, aggregate, then piece together
    with parallel_config(backend="loky", n_jobs=n_jobs, verbose=10):
        all_my_ds = []
        for my in my_list:
            merged_stat_ds = Parallel()(delayed(
                load_and_aggregate_single_ls_chunk)(
                    loader, 
                    my, 
                    bin_config["Ls"], 
                    ls_index, 
                    filter_config, 
                    ddr1_agg_columns,  
                    bin_config[ddr1_lat_bin_col], 
                    bin_config[ddr1_lon_bin_col], 
                    ddr1_lat_bin_col, 
                    ddr1_lon_bin_col,
                    ddr2_agg_columns=ddr2_agg_columns,
                    ddr2_lat_bin=bin_config[ddr2_lat_bin_col],
                    ddr2_lon_bin=bin_config[ddr2_lon_bin_col],
                    ddr2_lat_bin_col=ddr2_lat_bin_col,
                    ddr2_lon_bin_col=ddr2_lon_bin_col,
                    verbose=verbose
                ) for ls_index, ls_midpoint in enumerate(bin_config["Ls"].midpoints)
            )
            if len(merged_stat_ds) == 0:
                continue
            single_my_ds = xr.concat([ds for ds in merged_stat_ds if ds is not None], dim="Ls", join="outer", compat="no_conflicts")
            print(single_my_ds)
            all_my_ds.append(single_my_ds)
    print("Finished processing.")
    #total_bytes = sum(ds.nbytes for ds in all_my_ds)
    #print(f"Total size: {total_bytes / 1e9:.2f} GB")
    print("Concating all MY Datasets...")
    all_my_ds = xr.concat(all_my_ds, dim="MY", join="outer", compat="no_conflicts")
    print(all_my_ds)
    return all_my_ds



@click.command()
#@click.option("--config-path", help="Path to config file defining structure")
@click.option("--output-path")
def main_cli(output_path):
    print(output_path)
    #config = load_yaml(config_path)
    results = main()
    print(output_path)
    makedirs(output_path)
    results.to_netcdf(output_path)


if __name__=="__main__":
    main_cli()