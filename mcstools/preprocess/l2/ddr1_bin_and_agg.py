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
    "Surf_lon": Bin(-180, 180, 5)
}
FILTER_CONFIG_DEFAULT = {
    "LTST": (9/24, 21/24),
    "Obs_qual": [0, 1, 7, 10, 11, 17],
    "Gqual": [0, 6, 12],
    "1": [0]
}
DDR1_AGG_DEFAULT = ["Dust_column", "T_surf"]
DEFAULT_NJOBS = 32

def load_ls_chunk(loader, my, ls_bin_start, ls_bin_end):
    return loader.load_ls_range(
            MarsTime.from_solar_longitude(my, ls_bin_start),
            MarsTime.from_solar_longitude(my, ls_bin_end),
            verbose=False
        )

def make_ddr1_stats_for_single_bin(ddr1_df, ddr1_column, lat_bin: Bin, lon_bin: Bin, lat_col="Surf_lat", lon_col="Surf_lon"):
    valid_columns = ["Dust_column", "T_surf"]
    valid_lats = ["Surf_lat", "Profile_lat", "Solar_lon"]
    valid_lons = ["Surf_lon", "Profile_lon", "Solar_lat"]
    if ddr1_column not in valid_columns:
        raise ValueError(f"Either {ddr1_column} not valid DDR1 column or not yet implemented for aggregating")
    for lc, valid_lc in zip([lat_col, lon_col], [valid_lats, valid_lons]):
        if lc not in valid_lc:
            raise ValueError(f"{lc} not a valid lat/lon col: {valid_lc}")
    not_null_df = ddr1_df.dropna(subset=ddr1_column)
    null_df = ddr1_df[ddr1_df[ddr1_column].isnull()]
    if not not_null_df.empty: 
        stat_dict = {
            stat: binned_statistic_2d(
                not_null_df[lat_col], 
                not_null_df[lon_col],
                not_null_df[ddr1_column], 
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
                null_df[ddr1_column], 
                bins=[lat_bin.bins, lon_bin.bins],
                statistic="count"
        ).statistic
    ds = xr.Dataset(
        data_vars={
            f"{ddr1_column}_{stat}": ([lat_col, lon_col], stat_dict[stat]) for stat in stat_dict.keys()
        },
        coords={
            lat_col: lat_bin.midpoints,
            lon_col: lon_bin.midpoints,
        }
    )
    return ds

def load_and_aggregate_single(loader, my, ls_bin, ls_index, filter_config, ddr1_agg_columns, lat_bin, lon_bin, lat_col, lon_col, verbose=False):
    print(ls_bin.midpoints[ls_index])
    ddr1_df = load_ls_chunk(loader, my, ls_bin.bins[ls_index], ls_bin.bins[ls_index+1])
    ddr1_df = filter_ddr1_df_from_config(ddr1_df, filter_config, verbose=verbose)
    if ddr1_df.empty:
        return
    stat_ds_list = []
    for ddr1_col in ddr1_agg_columns:
        stat_ds = make_ddr1_stats_for_single_bin(ddr1_df, ddr1_col, lat_bin, lon_bin, lat_col=lat_col, lon_col=lon_col)
        stat_ds_list.append(stat_ds)
    merged_stat_ds = xr.merge(stat_ds_list)
    merged_stat_ds = merged_stat_ds.expand_dims(MY=[my], Ls=[ls_bin.midpoints[ls_index]])
    return merged_stat_ds

def main(
    loader: L2Loader|None = None,
    my_list: List = MY_DEFAULT,
    bin_config: Dict=BIN_CONFIG_DEFAULT,
    filter_config: Dict=FILTER_CONFIG_DEFAULT,
    ddr1_agg_columns: List=DDR1_AGG_DEFAULT,
    njobs=DEFAULT_NJOBS,
    verbose=False
):
    if loader is None:
        loader = L2Loader()
    # Don't load all data at once - break into chunks, aggregate, then piece together
    with parallel_config(backend="loky", njobs=njobs, verbose=verbose):
        all_ds = []
        for my in my_list:
            print(my)
            merged_stat_ds = Parallel()(delayed(
                load_and_aggregate_single)(
                    loader, 
                    my, 
                    bin_config["Ls"], 
                    ls_index, 
                    filter_config, 
                    ddr1_agg_columns,  
                    bin_config["Surf_lat"], 
                    bin_config["Surf_lon"], 
                    "Surf_lat", 
                    "Surf_lon",
                    verbose=verbose
                ) for ls_index, ls_midpoint in enumerate(bin_config["Ls"].midpoints)
            )
            all_ds.extend(merged_stat_ds)
        all_ds = [ds for ds in all_ds if ds is not None]
        all_ds = xr.merge(all_ds, join="outer", compat="no_conflicts")
        print(all_ds)
    return all_ds



@click.command()
#@click.option("--config-path", help="Path to config file defining structure")
@click.option("--output-path")
def main_cli(output_path):
    #config = load_yaml(config_path)
    results = main()
    makedirs(output_path)
    results.to_netcdf(output_path)


if __name__=="__main__":
    main_cli()