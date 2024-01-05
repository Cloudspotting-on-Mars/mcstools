import click
import datetime as dt
import numpy as np
import pandas as pd
import xarray as xr
from mars_time import MarsTime, datetime_to_marstime, marstime_to_datetime

from mcstools.loader import L2Loader
from mcstools.util.io import load_yaml

"""
Search DDR1 records over some time range and
find profiles within various bounds.
"""

filter_config_example = {
    "dt": ("2007-01-01", "2007-01-01 08:00:00"),
    "Profile_lat": (-20, 20),
    "Obs_qual": [0, 1, 10],
}

bin_config_example = {
    "L_s": (0, 360, 5),
    "Profile_lat": (-90, 90, 5),
    "Profile_lon": (-180, 180, 15),
    "LTST": (0, 1, 6 / 24.0),
}


def make_bins(bin_setup: tuple) -> np.array:
    return np.arange(bin_setup[0], bin_setup[1] + bin_setup[2], bin_setup[2])


def find_bin_edges_from_point(loc, bin_setup):
    bins = make_bins(bin_setup)
    start_bin = bins[np.digitize(loc, bins) - 1]
    return (start_bin, start_bin + bin_setup[2])


def generate_filter_config_from_location_and_bins(
    loc_config: dict, bin_config: dict, filter_config: dict = None
) -> dict:
    """
    Given a binning config and an exact location, return filter config to setup finding
    profiles within bin associated with that location.

    loc_config: config, but with only single values
    bin_config: bin parameters
    filter_config: existing filter config to add to
    """
    if not filter_config:
        filter_config = {}
    if ("L_s" in bin_config.keys() and "dt" in bin_config.keys()) or (
        "L_s" in loc_config.keys() and "dt" in loc_config.keys()
    ):
        return ValueError("Can only specify L_s or dt in config")
    elif "L_s" in bin_config.keys():
        bins_ls = np.arange(
            bin_config["L_s"][0],
            bin_config["L_s"][1] + bin_config["L_s"][2],
            bin_config["L_s"][2],
        )
        if "dt" in loc_config:
            mt = datetime_to_marstime(loc_config["dt"])
        else:
            mt = MarsTime.from_solar_longitude(loc_config["MY"], loc_config["L_s"])
        ls_bin_start = bins_ls[int(mt.solar_longitude // bin_config["L_s"][2])]
        start_mt = MarsTime.from_solar_longitude(mt.year, ls_bin_start)
        end_mt = MarsTime.from_solar_longitude(
            mt.year, ls_bin_start + bin_config["L_s"][2]
        )
        filter_config["dt"] = (
            marstime_to_datetime(start_mt),
            marstime_to_datetime(end_mt),
        )
    for key, value in loc_config.items():
        if key in ["L_s", "dt"]:
            continue
        else:
            filter_config[key] = find_bin_edges_from_point(value, bin_config[key])
    return filter_config


def filter_ddr1_df_from_config(
    ddr1_df: pd.DataFrame, filter_config: dict
) -> pd.DataFrame:
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
        if ddr1_df.empty:
            print("No profiles left after filtering")
            break
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
        bins = make_bins(bin_params)
        mid_points = (bins[1:] + bins[:-1]) / 2  # Find bin midpoints
        # create new column of midpoint of associated bin
        ddr1_df[f"{bin_col}_mid"] = pd.cut(
            ddr1_df[bin_col], bins=bins, labels=mid_points
        )
    return ddr1_df


def convert_binned_df_to_xarray(binned_df: pd.DataFrame, bin_config: dict) -> xr.DataArray:
    binned_grouped = binned_df.groupby(
        [f"{x}_mid" for x in bin_config.keys()], as_index=True
    )["Profile_identifier"].agg(list)
    binned_xr = binned_grouped.to_xarray()
    return binned_xr

class ConfigParser():
    def __init__(self) -> None:
        pass

    def parse_yaml(self, yaml_dict: dict):
        for config_type, config_data in yaml_dict.items():
            for key, val in config_data.items():
                if type(val) is dict:
                    if "Step" in val.keys():
                        yaml_dict[config_type][key] = (val["Start"], val["Stop"], val["Step"])
                    else:
                        yaml_dict[config_type][key] = (val["Start"], val["Stop"])
            if "dt" in config_data.keys():
                if type(config_data["dt"]) is str:
                    yaml_dict[config_type]["dt"] = dt.datetime.fromisoformat(config_data["dt"])
                elif type(config_data["dt"]) in [tuple, list]:
                    yaml_dict[config_type]["dt"] = (
                        dt.datetime.fromisoformat(config_data["dt"][x]) for x in config_data["dt"]
                    )
            if "MY" and "L_s" in config_data.keys():
                if type(config_data["L_s"]) in [tuple, list]:
                    yaml_dict[config_type]["Marstime"] = tuple(
                        MarsTime.from_solar_longitude(
                            config_data["MY"], x
                        ) for x in config_data["L_s"]
                    )
        return yaml_dict

    def load_config(self, path):
        config = load_yaml(path)
        config = self.parse_yaml(config)
        return config
