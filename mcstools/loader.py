import datetime as dt

import dask.dataframe as dd
import pandas as pd
from dask import delayed

import mcstools.util.mars_time as mt
from mcstools.data_path_handler import FilenameBuilder
from mcstools.reader import L1BReader, L2Reader


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
                    except LookupError:
                        continue
                    pieces.append(fdf)
                df = pd.concat(pieces)
            else:
                dfs = [delayed(self.reader.read)(f) for f in sorted(files)]
                df = dd.from_delayed(dfs)
        return df

    def load_date_range(self, start_time, end_time, add_cols=["dt"]):
        times = [start_time, end_time]
        for i, t in enumerate(times):
            if type(t) not in [dt.datetime, str]:
                raise TypeError(
                    f"Unrecognized type ({type(t)}) for start/end time, "
                    "must be datetime or isoformat str"
                )
            elif type(t) != dt.datetime:
                times[i] = dt.datetime.fromisoformat(t)
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

    def __init__(self, pds=False, mcs_data_path=None):
        self.filename_builder = FilenameBuilder(
            "L2", pds=pds, mcs_data_path=mcs_data_path
        )
        self.reader = L2Reader(pds=pds)

    def load(self, files, ddr, add_cols: list = None, profiles=None, dask=False):
        if type(files) != list:
            df = self.reader.read(files, ddr, add_cols)
        elif len(files) == 0:
            df = pd.DataFrame(columns=self.reader.columns)
        else:
            if not dask:
                pieces = []
                for f in sorted(files):
                    try:
                        fdf = self.reader.read(f, ddr, add_cols)
                    except LookupError:
                        continue
                    pieces.append(fdf)
                df = pd.concat(pieces)
            else:
                dfs = [
                    delayed(self.reader.read)(f, ddr, add_cols) for f in sorted(files)
                ]
                df = dd.from_delayed(dfs)
        if profiles:
            df = df[df["Prof#"].isin(profiles)]
        return df

    def load_from_filebase_profiles(self, filebase, profiles, ddr):
        return self.load(
            self.filename_builder.make_filename_from_filestr(filebase),
            ddr,
            profiles=profiles,
        )

    def load_ddr2_profiles_from_ddr1_df(self, ddr1_df):
        # ddr2s = []
    #...: for name, group in target.groupby(["filename"]):
    #...:     ddr2s.append(l.load_from_filebase_profiles(name, group["Prof#"].to_list(), "DDR2"))

    def load_date_range(
        self, start_time, end_time, ddr="DDR1", add_cols: list = None
    ):  # , profiles=[]):
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
        print(f"Loading L2 {ddr} data from {start_time} - {end_time}")
        files = self.filename_builder.make_filenames_from_daterange(
            start_time, end_time
        )
        data = self.load(files, ddr, add_cols=required_cols)  # , profiles=profiles)
        if ddr == "DDR1":
            data = data[(data["dt"] >= start_time) & (data["dt"] < end_time)]
        data = data.drop(columns=remove_cols)
        return data

    def load_ls_range(
        self,
        my: int,
        start_ls: float,
        end_ls: float,
        ddr="DDR1",
        add_cols: list = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Load L2 data within Ls range of a given Mars Year

        Parameters
        ----------
        my: Mars Year
        start_ls/end_ls: beginning/end of Ls range

        Returns
        -------
        _: loaded L2 data
        """
        print(
            f"Determining approximate start/end dates for MY{my}, "
            f"Ls range: {start_ls} - {end_ls}"
        )
        # Error on too wide, then reduce
        date_start = mt.MY_Ls_to_UTC(my, start_ls) - dt.timedelta(days=2)
        date_end = mt.MY_Ls_to_UTC(my, end_ls) + dt.timedelta(days=2)
        data = self.load_date_range(
            date_start, date_end, ddr, add_cols=add_cols, **kwargs
        )
        # This reduction will need to be more complicated for multi-MY searches
        if ddr == "DDR1":
            data = data[(data["L_s"] >= start_ls) & (data["L_s"] < end_ls)]
        return data
