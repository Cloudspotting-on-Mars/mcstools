import datetime as dt
from typing import Union

import dask.dataframe as dd
import pandas as pd
from dask import delayed
from mars_time import MarsTime, marstime_to_datetime

from mcstools.data_path_handler import FilenameBuilder
from mcstools.reader import L1BReader, L2Reader
from mcstools.util.time import check_and_convert_start_end_times


class L1BLoader:
    """
    Class to load L1B data (multiple files) in different ways.
    Requires path handler to generate filenames in different.
    """

    def __init__(self, pds=False, mcs_data_path=None):
        self.pds = pds
        self.filename_builder = FilenameBuilder(
            "L1B", pds=self.pds, mcs_data_path=mcs_data_path
        )
        self.reader = L1BReader(pds=pds)

    def load(self, files, dask=False, add_cols: list = None):
        if type(files) != list:
            return self.reader.read(files, add_cols=add_cols)
        elif len(files) == 0:
            df = pd.DataFrame(columns=self.columns)
        else:
            if not dask:
                pieces = []
                for f in sorted(files):
                    try:
                        fdf = self.reader.read(f, add_cols=add_cols)
                    except LookupError as error:
                        print(error)
                    except FileNotFoundError as error:
                        print(error+"\nIgnoring.")
                        continue
                    pieces.append(fdf)
                df = pd.concat(pieces)
            else:
                dfs = [delayed(self.reader.read)(f) for f in sorted(files)]
                df = dd.from_delayed(dfs)
        return df

    def load_date_range(self, start_time, end_time, add_cols=["dt"]):
        times = check_and_convert_start_end_times(start_time, end_time)
        print(f"Loading L1B data from {times[0]} - {times[1]}")
        files = self.filename_builder.make_filenames_from_daterange(*times)
        data = self.load(files, add_cols=add_cols)
        data = data[(data["dt"] >= start_time) & (data["dt"] < end_time)]
        return data

    def load_from_filestr(self, filestr, **kwargs):
        file = self.filename_builder.make_filename_from_filestr(filestr)
        return self.load(file, **kwargs)

    def load_files_around_date(self, date, n=1, **kwargs):
        files, _ = self.find_files_around_date(date, n)
        return self.load(files, *kwargs)

    def load_files_around_file(self, f, n=1, **kwargs):
        files, _ = self.find_files_around_file(f, n)
        return self.load(files, *kwargs)


class L2Loader:
    """
    Class to load L1B data (multiple files) in different ways.
    Requires path handler to generate filenames in different.
    """

    def __init__(self, pds: bool = False, mcs_data_path: str = None) -> None:
        self.filename_builder = FilenameBuilder(
            "L2", pds=pds, mcs_data_path=mcs_data_path
        )  # fore creating paths/urls
        self.reader = L2Reader(pds=pds)  # file reader

    def load(
        self,
        ddr: str,
        files: Union[str, list] = None,
        profiles: Union[list, pd.Series] = None,
        add_cols: list = None,
        dask: bool = False,
    ) -> pd.DataFrame:
        """
        Load data record (DDR) of L2 files

        files: path/url to file(s)
        profiles: specific profiles to reduce to
        ddr: data record [DDR1, DDR2, DDR3]
        add_cols: additional columns to generate and add ["dt"]
        dask: option to load via dask delay
        """
        # Setup files to load if only profiles given
        if files is None:
            if type(profiles) == pd.Series:
                profiles = sorted(profiles.to_list())
            # Get files from profile names
            filestrs = sorted(
                list(set([x[0] for x in [x.split("_") for x in profiles]]))
            )
            files = [
                self.filename_builder.make_filename_from_filestr(f) for f in filestrs
            ]
        # Only one file, just read
        if type(files) != list:
            df = self.reader.read(files, ddr, add_cols)
            # Specific profiles, reduce data set
            if profiles:
                df = df[df["Profile_identifier"].isin(profiles)]
        # No files, make empty DF
        elif len(files) == 0:
            df = pd.DataFrame(columns=self.reader.columns + add_cols)
        # Load multiple files
        else:
            df = self._load_by_file(
                files, ddr, profiles=profiles, add_cols=add_cols, dask=dask
            )
        return df

    def _load_by_file(self, files, ddr, profiles=None, add_cols=None, dask=False):
        """
        Read and concatenate L2 files
        """
        if not dask:
            # Read in fiels one by one
            pieces = []  # initialize list of DFs
            for f in sorted(files):
                try:
                    fdf = self.reader.read(f, ddr, add_cols)
                except LookupError:
                    continue
                if profiles:
                    fdf = fdf[
                        fdf["Profile_identifier"].isin(profiles)
                    ]  # Reduce to subset
                pieces.append(fdf)
            df = pd.concat(pieces, ignore_index=True)  # combine
        else:
            dfs = [delayed(self.reader.read)(f, ddr, add_cols) for f in sorted(files)]
            df = dd.from_delayed(dfs)
        return df

    def load_date_range(self, start_time, end_time, ddr="DDR1", add_cols: list = None):
        times = check_and_convert_start_end_times(start_time, end_time)
        if ddr == "DDR1":
            if not add_cols:
                required_cols = ["dt"]
                remove_cols = ["dt"]
            elif "dt" not in add_cols:
                required_cols = ["dt"]
                remove_cols = ["dt"]
            else:
                required_cols = add_cols
                remove_cols = []
        else:
            required_cols = add_cols
            remove_cols = []
        print(f"Loading L2 {ddr} data from {times[0]} - {times[1]}")
        files = self.filename_builder.make_filenames_from_daterange(*times)
        data = self.load(
            ddr, files=files, add_cols=required_cols
        )  # , profiles=profiles)
        if ddr == "DDR1":
            data = data[(data["dt"] >= times[0]) & (data["dt"] < times[1])]
        data = data.drop(columns=remove_cols)
        return data

    def load_ls_range(
        self,
        start: MarsTime,
        end: MarsTime,
        ddr="DDR1",
        add_cols: list = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Load L2 data within Ls range of a given Mars Year

        Parameters
        ----------
        start/end: beginning/end of MY/Ls range

        Returns
        -------
        _: loaded L2 data
        """
        print(f"Determining approximate start/end dates for " f"range: {start} - {end}")
        date_start = marstime_to_datetime(start) - dt.timedelta(days=2)
        date_end = marstime_to_datetime(end) + dt.timedelta(days=2)
        data = self.load_date_range(
            date_start, date_end, ddr, add_cols=add_cols, **kwargs
        )
        # This reduction will need to be more complicated for multi-MY searches
        if ddr == "DDR1":
            data = data[
                (data["L_s"] >= start.solar_longitude)
                & (data["L_s"] < end.solar_longitude)
            ]
        return data

    def merge_ddrs(self, ddr2_df, ddr1_df):
        return pd.merge(
            ddr2_df,
            ddr1_df,
            on="Profile_identifier",
            how="outer",
            suffixes=("", "_DDR1"),
        )
