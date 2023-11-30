import click
import numpy as np
import pandas as pd

from mcstools.loader import L2Loader
from mcstools.mcsfile import L2File

"""
Search DDR1 records over some time range and
find profiles within various bounds.
"""

config = {
    "dt": ("2007-01-01", "2007-01-02"),
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

def parse_config(ddr1_df, config_dict):
    for field, vals in config_dict.items():
        if field in ["dt", "Ls"]:
            continue
        else:
            if type(vals) in [tuple]:
                print(f"Filtering {field} to within {vals}.")
                ddr1_df = ddr1_df[ddr1_df[field].between(*vals)]
            if type(vals) in [list]:
                print(f"Selecting rows with {field} in {vals}.")
                ddr1_df = ddr1_df[ddr1_df[field].isin(vals)]
    return ddr1_df

def bin_ddr1_profiles(ddr1_df, bin_config):
    # goal is an xarray with coords MY, L_s_mid, lat_mid, lon_mid, ltst_mid(?)
    # and values are the profile identifiers
    for bin_col, bin_params in bin_config.items():
        print(f"Binning {bin_col}")
        bins = np.arange(bin_params[0], bin_params[1]+bin_params[2], bin_params[2])
        mid_points = (bins[1:]+bins[:-1])/2
        ddr1_df[f"{bin_col}_mid"] = pd.cut(ddr1_df[bin_col], bins=bins, labels=mid_points)
    return ddr1_df

@click.command()
@click.option("--data-dir", default=None, type=str, help="Path to mcs data directory (defaults to using PDS)")
#@click.option("--start-date", type=click.DateTime(), required=False)
#@click.option("--end-date", type=click.DateTime(), required=False)
#@click.option("-f", "--filters", multiple=True, help="Any number of DDR1 column names followed by either minimum/maximum values or flag values to accept")
def main(data_dir):#, start_date, end_date, filters):
    if data_dir:
        l2loader = L2Loader(mcs_data_path = data_dir)
    else:
        l2loader = L2Loader(pds=True)
    if "dt" in config.keys() and "Ls" not in config.keys():
        ddr1 = l2loader.load_date_range(*config["dt"], ddr="DDR1", add_cols=["dt"])
        print(ddr1)
    reduced_ddr1 = parse_config(ddr1, config)
    print(reduced_ddr1)
    binned = bin_ddr1_profiles(reduced_ddr1, bin_config)
    print(binned[list(bin_config.keys())+[f"{x}_mid" for x in bin_config.keys()]])
    binned_grouped = binned.groupby([f"{x}_mid" for x in bin_config.keys()])["Profile_identifier"].apply(list)
    print(binned_grouped)
    binned_xr = binned.set_index(list(bin_config.keys()))["Profile_identifier"].to_xarray()
    print(binned_xr)

if __name__=="__main__":
    main()