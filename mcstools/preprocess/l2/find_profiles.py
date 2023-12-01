import click
import numpy as np
import pandas as pd
import xarray as xr

from mcstools.loader import L2Loader
from mcstools.mcsfile import L2File

"""
Search DDR1 records over some time range and
find profiles within various bounds.
"""

config = {
    "dt": ("2007-01-01", "2007-01-01 08:00:00"),
    "Profile_lat": (-20, 20),
    #"Gqual": [0, 5, 6],
    "Obs_qual": [0, 1, 10]
}

bin_config = {
    "L_s": (0, 360, 5),
    "Profile_lat": (-90, 90, 5),
    "Profile_lon": (-180, 180, 15),
    "LTST": (0, 1, 6)
}

def filter_ddr1_df_from_config(ddr1_df: pd.DataFrame, filter_config: dict) -> pd.DataFrame: 
    """
    Filter DDR1 data from a config dictionary, where config gives:
    {"column": values}. Assumes values is list for flag columns
    (and selects any profiles with flag given in values) and tuple
    for range.

    Parameters
    ----------
    ddr1_df: Loaded DDR1 profile data
    filter_config: config info for filtering data

    Returns
    -------
    ddr1_df: reduced DDR1 profile data
    """
    # Go through each entry in config dict
    for field, vals in filter_config.items():
        # Select rows within range for tuple
        if type(vals) in [tuple]:
            print(f"Filtering {field} to within {vals}.")
            ddr1_df = ddr1_df[ddr1_df[field].between(*vals)]
        # Select rows with corresponding flags for list
        if type(vals) in [list]:
            print(f"Selecting rows with {field} in {vals}.")
            ddr1_df = ddr1_df[ddr1_df[field].isin(vals)]
    return ddr1_df

def bin_ddr1_profiles(ddr1_df: pd.DataFrame, bin_config: dict) -> pd.DataFrame:
    """
    Bin DDR1 profiles by bins given in bin_config.
    Adds new "col_mid" column with midpoint of corresponding bin
    for that row/column.

    Assumes bin_config is in format:
    {"column": (bin_start, bin_end, bin_size)}.

    Parameters
    ----------
    ddr1_df: Loaded DDR1 profile data
    bin_config: bin parameters

    Returns
    -------
    ddr1_df: DDR1 data with addtional bin columns
    """
    for bin_col, bin_params in bin_config.items():
        print(f"Binning {bin_col}")
        # Setup bins based on config entry
        bins = np.arange(bin_params[0], bin_params[1]+bin_params[2], bin_params[2])
        mid_points = (bins[1:]+bins[:-1])/2  # Find bin midpoints
        # create new column of midpoint of associated bin
        ddr1_df[f"{bin_col}_mid"] = pd.cut(ddr1_df[bin_col], bins=bins, labels=mid_points)
    return ddr1_df

def convert_binned_df_to_xarray(binned_df: pd.DataFrame) -> xr.DataArray:
    binned_grouped = binned.groupby(
        [f"{x}_mid" for x in bin_config.keys()],
        as_index=True
    )["Profile_identifier"].agg(list)
    print(binned_grouped)
    binned_xr = binned_grouped.to_xarray()
    print(binned_xr)

@click.command()
@click.option("--data-dir", default=None, type=str, help="Path to mcs data directory (defaults to using PDS)")
def main(data_dir):#, start_date, end_date, filters):
    if data_dir:
        l2loader = L2Loader(mcs_data_path = data_dir)
    else:
        l2loader = L2Loader(pds=True)
    if "dt" in config.keys() and "Ls" not in config.keys():
        ddr1 = l2loader.load_date_range(*config["dt"], ddr="DDR1", add_cols=["dt"])
        print(ddr1)
    reduced_ddr1 = filter_ddr1_df_from_config(ddr1, config)
    print(reduced_ddr1)
    binned = bin_ddr1_profiles(reduced_ddr1, bin_config)
    print(binned[list(bin_config.keys())+[f"{x}_mid" for x in bin_config.keys()]])
    

if __name__=="__main__":
    main()