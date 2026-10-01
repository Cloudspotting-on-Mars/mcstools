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

MY_DEFAULT = [29]

BIN_CONFIG_DEFAULT = {
    "Ls": BinGrid(0, 140 ,5, "Ls"),
    "Scene_lat": BinGrid(-90, 90, 15, "Scene_lat"),
    "Scene_lon": BinGrid(-180, 180, 15, "Scene_lon"),
}

DEFAULT_N_JOBS = 72
DEFAULT_PIPELINE = L1BStandardInTrack()
AGG_COLUMNS = L1BFile.radcols

def p10(x): return np.quantile(x, 0.1)
def p90(x): return np.quantile(x, 0.9)

AGG_STATS = ["min", "max", "mean", "median", "std", "count", p10, p90]

def load_and_aggregate_single_ls_bin(
    loader: L1BLoader,
    view_pipeline: Union[L1BStandardInTrack],
    my: int,
    ls_bin: Bin,
    lat_bins: BinGrid,
    lon_bins: BinGrid
):
    logger.info(f"Processing MY{my} Ls={ls_bin.midpoint} on PID {os.getpid()}")
    l1b_df = loader.load_ls_range(MarsTime.from_solar_longitude(my, ls_bin.start),
                                             MarsTime.from_solar_longitude(my, ls_bin.stop),
                                             add_cols=["LTST"])
    print(l1b_df)
    if l1b_df.empty:
        return None
    l1b_df = view_pipeline.preprocess(l1b_df)
    day_cond =l1b_df["LTST"].between(9, 21)
    day_df = l1b_df[day_cond]
    night_df = l1b_df[~day_cond]
    print(l1b_df)
    if l1b_df.empty:
        return None
    day_ds_list = []
    for day_ind, subdf in zip([1, 0], [day_df, night_df]):
        rad_ds_list = []
        for rdr_col in AGG_COLUMNS:
            stat_ds = compute_bin_stats_2d(subdf, rdr_col, lat_bins, lon_bins, stats=AGG_STATS, include_nan_count=False)
            rad_ds_list.append(stat_ds)
        rad_ds = xr.merge(rad_ds_list, join="outer", compat="no_conflicts")
        rad_ds = rad_ds.expand_dims(Day=[day_ind])
        day_ds_list.append(rad_ds)
    concat_stat_ds = xr.concat(day_ds_list, dim="Day", join="outer", compat="no_conflicts")
    concat_stat_ds = concat_stat_ds.expand_dims(MY=[my], Ls=[ls_bin.midpoint])
    return concat_stat_ds

def main(
    loader: L1BLoader|None = None,
    view_pipeline: Union[L1BStandardInTrack]| None =  DEFAULT_PIPELINE,
    my_list = MY_DEFAULT,
    bin_config = BIN_CONFIG_DEFAULT,
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
                    bin_config["Scene_lon"]
            ) for ls_i in range(len(bin_config["Ls"]))
        )
        single_my_ds = xr.concat([ds for ds in my_stat_ds_list if ds is not None], dim="Ls", join="outer", compat="no_conflicts")
        all_my_ds.append(single_my_ds)
    return xr.concat(all_my_ds, dim="MY", join="outer", compat="no_conflicts")


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