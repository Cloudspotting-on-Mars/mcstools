import datetime as dt

import numpy as np
import pandas as pd
import xarray as xr

from mars_time import MarsTime, datetime_to_marstime, marstime_to_datetime

from mcstools.util.io import load_yaml
from mcstools.util.log import logger
from mcstools.util.time import add_day_column

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




#def find_bin_edges_from_point(loc, bin_setup):
#    bins = make_bins(bin_setup)
#    start_bin = bins[np.digitize(loc, bins) - 1]
#    return (start_bin, start_bin + bin_setup[2])


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




def convert_binned_df_to_xarray(
    binned_df: pd.DataFrame, bin_config: dict
) -> xr.DataArray:
    binned_grouped = binned_df.groupby(
        [f"{x}_mid" for x in bin_config.keys()], as_index=True
    )["Profile_identifier"].agg(list)
    binned_xr = binned_grouped.to_xarray()
    return binned_xr

class FilterConfig():
    """
    Class for filtering DDR1 data.
    """
    def __init__(self, filter_dict: dict):
        self.filter_dict = self._verify_filter(filter_dict)
        self._parse_filter_dict(self.filter_dict)
        self.add_cols = ["dt"]
        if "Day" in self.filter_dict.keys():
            self.add_cols.append("Day")
    
    def _verify_filter(self, filter_dict: dict):
        # Valid keys should be any DDR1 column + addable columns
        if "dt" in filter_dict.keys() and "MarsTime" in filter_dict.keys():
            raise NotImplementedError()
            #logger.warning("Both datetime and MarsTime filters provided, will filter by both windows")
        return filter_dict
    
    def _parse_filter_dict(self, filter_dict):
        # For time filtering, expect start/stop
        if "dt" in filter_dict.keys():
            for ss in ["Start", "Stop"]:
                filter_dict["dt"][ss] = self.check_dt_type_and_convert(filter_dict["dt"][ss])
        if "MarsTime" in filter_dict.keys():
            for ss in ["Start", "Stop"]:
                if "Ls" in filter_dict["MarsTime"][ss].keys():
                    filter_dict["MarsTime"][ss] = MarsTime.from_solar_longitude(
                        filter_dict["MarsTime"][ss]["MY"],
                        filter_dict["MarsTime"][ss]["Ls"]
                    )
                elif "Sol" in filter_dict["MarsTime"][ss].keys():
                    raise NotImplementedError()
                
    def filter_data(self, data: pd.DataFrame):
        """
        Filter DDR1 data from a config dictionary, where config gives:
        {"column": values}. Assumes values is list for flag columns
        (and selects any profiles with flag given in values) and tuple
        for range.

        Parameters
        ----------
        data: Loaded DDR1 profile data
        filter_config: config info for filtering data

        Returns
        -------
        data: reduced DDR1 profile data
        """
        # Go through each entry in config dict
        for field, vals in self.filter_dict.items():
            # Time filtering taken care of via load
            if field in ["MarsTime", "dt"]:
                continue
            # Select rows with corresponding flags for list
            if type(vals) in [list]:
                logger.info(f"Selecting rows with {field} in {vals}.")
                data = data[data[field].isin(vals)]
            # Select rows within range for tuple
            elif isinstance(vals, dict):
                logger.info(f"Filtering {field} to within {vals['Start']}-{vals['Stop']}.")
                data = data[data[field].between(*vals)]
            else:
                raise KeyError(f"Can't parse field: {field} for filtering.")
            if data.empty:
                logger.warning("No profiles left after filtering")
                break
        return data
            
                
    def __repr__(self):
        return self.filter_dict.__repr__()
        

class BinConfig():
    # Valid keys should be any DDR1 column + addable columns
    # Lon should be cyclic
    float_keys = ["L_s", "Profile_lat", "Profile_lon", "Alt"]
    cyclic_keys = ["LTST"]
    flag_keys = ["Obs_qual", "Day"]

    def __init__(self, bin_dict: dict):
        self.bin_dict = self._verify_bin(bin_dict)

    def _verify_bin(self, bin_dict: dict):
        for key in bin_dict.keys():
            if key in self.float_keys:
                for sss in ["Start", "Stop", "Step"]:
                    if sss not in bin_dict[key].keys():
                        raise ValueError(f"{bin_dict[key]} does not have valid bin parameters")
        return bin_dict
    
    def _add_missing_columns(self, data):
        if "Day" in self.bin_dict.keys() and "Day" not in data.columns:
            return add_day_column(data)
        else:
            return data
        
    def make_bins(self, bin_setup: dict) -> np.array:
        return np.arange(
            bin_setup["Start"],
            bin_setup["Stop"] + bin_setup["Step"],
            bin_setup["Step"]
        )

    def create_bin_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Bin DDR1 profiles by bins given in bin_config.
        Adds new "col_mid" column with midpoint of corresponding bin
        for that row/column.

        Assumes bin_config is in format:
        {"column": (bin_start, bin_end, bin_size)}.

        Parameters
        ----------
        data: Loaded DDR1 profile data
        bin_config: bin parameters

        Returns
        -------
        data: DDR1 data with addtional bin columns
        """
        data = self._add_missing_columns(data)
        self.binned_columns = []
        for bin_col, bin_params in self.bin_dict.items():
            if bin_col in self.float_keys:
                print(f"Binning {bin_col}")
                # Setup bins based on config entry
                bins = self.make_bins(bin_params)
                mid_points = (bins[1:] + bins[:-1]) / 2  # Find bin midpoints
                # create new column of midpoint of associated bin
                new_col = f"{bin_col}_mid" 
                data[new_col] = pd.cut(
                    data[bin_col], bins=bins, labels=mid_points
                )
                self.binned_columns.append(new_col)
            elif bin_col in self.flag_keys:
                self.binned_columns.append(bin_col)
            elif bin_col in self.cyclic_keys:
                return NotImplementedError()
            else:
                raise KeyError(f"Bin field {bin_col} not recognized.")
        return data
    
    def __repr__(self):
        return self.bin_dict.__repr__()

class ConfigParser:
    def __init__(
            self,
            filter_config: FilterConfig = None,
            bin_config: BinConfig = None,
            ddr2_fields: list = None,
        ) -> None:
        self.filter_config = filter_config
        self.bin_config = bin_config
        self.ddr2_fields = ddr2_fields

    def check_dt_type_and_convert(self, timeval):
        if isinstance(timeval, dt.datetime):
            return timeval
        elif isinstance(timeval, str):
            return dt.datetime.from_isoformat(timeval)
        else:
            raise TypeError(f"Can't convert {timeval} (type {type(timeval)}) to datetime.")

    @classmethod
    def from_yaml(cls, path):
        config = load_yaml(path)
        if "filter" in config.keys():
            filter_config = FilterConfig(config["filter"])
        else:
            filter_config = None
        if "bin" in config.keys():
            bin_config = BinConfig(config["bin"])
        else:
            bin_config = None
        if "ddr2_fields" in config.keys():
            ddr2_fields = config["ddr2_fields"]
        else:
            ddr2_fields = None
        #config = self.parse_yaml(config)
        return cls(
            filter_config = filter_config,
            bin_config=bin_config,
            ddr2_fields=ddr2_fields
        )


    def __repr__(self) -> str:
        config_dict = {
            "Filter": self.filter_config,
            "Bin": self.bin_config,
            "DDR2_fields": self.ddr2_fields
        }
        return config_dict.__repr__()