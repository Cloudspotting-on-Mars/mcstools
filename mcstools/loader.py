import datetime as dt
from typing import Union

import numpy as np
import pandas as pd
from mars_time import MarsTime, marstime_to_datetime

from mcstools.data_path_handler import FilenameBuilder
from mcstools.preprocess.l2.filter_and_bin import filter_ddr1_df_from_config
from mcstools.reader import L1BReader, L2Reader
from mcstools.util.log import logger
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

    def load(self, files, add_cols: list = None, **kwargs):
        if not isinstance(files, (list, np.ndarray, pd.Series)):
            return self.reader.read(files, add_cols=add_cols, **kwargs)
        elif len(files) == 0:
            df = pd.DataFrame(columns=self.columns)
        else:
            pieces = []
            for f in sorted(files):
                try:
                    fdf = self.reader.read(f, add_cols=add_cols, **kwargs)
                except LookupError as error:
                    logger.error(error)
                except FileNotFoundError as error:
                    logger.error(error, "\nIgnoring.")
                    continue
                pieces.append(fdf)
            df = pd.concat(pieces)
        return df

    def load_date_range(self, start_time, end_time, add_cols=["dt"], **kwargs):
        if add_cols is None:
            add_cols = ["dt"]
        elif "dt" not in add_cols:
            add_cols.append("dt")
        times = check_and_convert_start_end_times(start_time, end_time)
        logger.info(f"Loading L1B data from {times[0]} - {times[1]}")
        files = self.filename_builder.make_filenames_from_daterange(*times)
        data = self.load(files, add_cols=add_cols, **kwargs)
        data = data[(data["dt"] >= times[0]) & (data["dt"] < times[1])]
        return data

    def load_ls_range(
        self,
        start: MarsTime,
        end: MarsTime,
        add_cols: list = None,
        verbose=False,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Load L2 data within Ls range of a given Mars Year

        Parameters
        ----------
        start/end: beginning/end of MY/Ls range
        files: path/url to file(s)
        profiles: specific profiles to reduce to
        ddr: data record [DDR1, DDR2, DDR3]
        add_cols: additional columns to generate and add ["dt"]
        verbose: output log details

        Returns
        -------
        _: loaded L2 data
        """
        if verbose:
            logger.info(
                f"Determining approximate start/end dates for "
                f"range: {start} - {end}"
            )
        # Overshoot on both sides, then fix after data is loaded
        date_start = marstime_to_datetime(start) - dt.timedelta(days=2)
        date_end = marstime_to_datetime(end) + dt.timedelta(days=2)
        data = self.load_date_range(date_start, date_end, add_cols=add_cols, **kwargs)
        # This reduction will need to be more complicated for multi-MY searches
        data = data[
            (data["L_sub_s"] >= start.solar_longitude)
            & (data["L_sub_s"] < end.solar_longitude)
        ]
        return data

    def load_from_filestr(self, filestr, **kwargs):
        file = self.filename_builder.make_filename_from_filestr(filestr)
        return self.load(file, **kwargs)

    def load_from_datetimes(self, datetimes, **kwargs):
        """
        Given a list of datetimes (does not need to be same precision as in MCS data),
        load all files with those datetimes [does not reduce to only those datetimes].
        """
        if isinstance(datetimes, pd.Series):
            pass
        elif isinstance(datetimes, list):
            datetimes = pd.Series(datetimes)
        else:
            raise NotImplementedError(
                f"Loading from {type(datetimes)} not implemented."
            )
        filestrs = datetimes.apply(self.filename_builder.handler.convert_dt_to_filestr)
        files = filestrs.apply(
            self.filename_builder.make_filename_from_filestr
        ).unique()
        return self.load(files)

    def load_files_around_file(self, f: str, n: int = 1, **kwargs) -> pd.DataFrame:
        """
        Load file and n files before and after it (if they exist)

        Sometimes it's beneficial to load the files before/after to get
        information about the boundaries.

        Parameters
        ----------
        f: filestr (YYMMDDHHMMSS format)
        n: Number of files before and after to load
            0 will load only given f

        Returns
        -------
        _: L1B data
        """
        if n > 0:
            start_time = self.filename_builder.handler.convert_filestr_to_dt(
                f
            ) - dt.timedelta(hours=4 * n)
            end_time = self.filename_builder.handler.convert_filestr_to_dt(
                f
            ) + dt.timedelta(hours=4 * n)
            filestrs = self.filename_builder._build_filestrs_from_daterange(
                start_time, end_time
            )
        else:
            filestrs = [f]
        files = [self.filename_builder.make_filename_from_filestr(f) for f in filestrs]
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
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Load data record (DDR) of L2 files

        files: path/url to file(s)
        profiles: specific profiles to reduce to
        ddr: data record [DDR1, DDR2, DDR3]
        add_cols: additional columns to generate and add ["dt"]
        verbose: output log details
        """
        # Setup files to load if only profiles given
        if files is None:
            if isinstance(profiles, pd.Series):
                profiles = sorted(profiles.to_list())
            # Get files from profile names
            filestrs = sorted(
                list(set([x[0] for x in [x.split("_") for x in profiles]]))
            )
            files = [
                self.filename_builder.make_filename_from_filestr(f) for f in filestrs
            ]
        # Only one file, just read
        if not isinstance(files, (list, np.ndarray, pd.Series)):
            df = self.reader.read(files, ddr, add_cols)
            # Specific profiles, reduce data set
            if profiles is not None:
                df = df[df["Profile_identifier"].isin(profiles)]
        # No files, make empty DF
        elif len(files) == 0:
            empty_df_cols = self.reader.data_records[ddr]["columns"]
            if add_cols is not None:
                empty_df_cols.append(add_cols)
            df = pd.DataFrame(columns=empty_df_cols)
        # Load multiple files
        else:
            df = self._load_by_file(
                files,
                ddr,
                profiles=profiles,
                add_cols=add_cols,
                verbose=verbose,
            )
        return df

    def _load_by_file(
        self,
        files: list,
        ddr: str,
        profiles: list = None,
        add_cols: list = None,
        verbose: bool = False,
    ):
        """
        Read and concatenate L2 files

        files: path/url to file(s)
        profiles: specific profiles to reduce to
        ddr: data record [DDR1, DDR2, DDR3]
        add_cols: additional columns to generate and add ["dt"]
        verbose: output log details
        """
        if verbose:
            logger.info(f"Loading L2 {ddr} data ({len(files)} files).")
        # Read in fiels one by one
        pieces = []  # initialize list of DFs
        for f in sorted(files):
            try:
                fdf = self.reader.read(f, ddr, add_cols)
            except (FileNotFoundError, LookupError) as e:
                if verbose:
                    logger.warning(e)
                continue
            if profiles is not None:
                fdf = fdf[fdf["Profile_identifier"].isin(profiles)]  # Reduce to subset
            pieces.append(fdf)
        if len(pieces) == 0:
            if add_cols is not None:
                empty_df_cols = self.reader.data_records[ddr]["columns"] + add_cols
            df = pd.DataFrame(columns=empty_df_cols)
        else:
            df = pd.concat(pieces, ignore_index=True)  # combine
        return df

    def load_date_range(
        self,
        start_time,
        end_time,
        ddr="DDR1",
        add_cols: list = None,
        verbose=False,
        **kwargs,
    ):
        """
        Load L2 data between two times

        files: path/url to file(s)
        profiles: specific profiles to reduce to
        ddr: data record [DDR1, DDR2, DDR3]
        add_cols: additional columns to generate and add ["dt"]
        verbose: output log details
        """
        times = check_and_convert_start_end_times(start_time, end_time)
        if ddr == "DDR1":
            if add_cols is None:
                required_cols = ["dt"]
                remove_cols = ["dt"]
            elif "dt" not in add_cols:
                required_cols = add_cols + ["dt"]
                remove_cols = ["dt"]
            else:
                required_cols = add_cols
                remove_cols = []
        else:
            required_cols = add_cols
            remove_cols = []
        if verbose:
            logger.info(f"Loading L2 {ddr} data from {times[0]} - {times[1]}")
        files = self.filename_builder.make_filenames_from_daterange(*times)
        data = self.load(
            ddr, files=files, add_cols=required_cols, verbose=verbose, **kwargs
        )
        if ddr == "DDR1":
            data = data[(data["dt"] >= times[0]) & (data["dt"] < times[1])]
        data = data.drop(columns=remove_cols)
        return data

    def load_from_datetimes(
        self, ddr: str, datetimes: list, verbose: bool = False, **kwargs
    ):
        """
        Given a list of datetimes (does not need to be same precision as in MCS data),
        load all files with those datetimes [does not reduce to only those datetimes].

        files: path/url to file(s)
        profiles: specific profiles to reduce to
        ddr: data record [DDR1, DDR2, DDR3]
        add_cols: additional columns to generate and add ["dt"]
        verbose: output log details
        """
        if isinstance(datetimes, pd.Series):
            pass
        elif isinstance(datetimes, list):
            datetimes = pd.Series(datetimes)
        else:
            raise NotImplementedError(
                f"Loading from {type(datetimes)} not implemented."
            )
        filestrs = datetimes.apply(self.filename_builder.handler.convert_dt_to_filestr)
        files = filestrs.apply(
            self.filename_builder.make_filename_from_filestr
        ).unique()
        return self.load(ddr, files, verbose=verbose, **kwargs)

    def load_ls_range(
        self,
        start: MarsTime,
        end: MarsTime,
        ddr="DDR1",
        add_cols: list = None,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Load L2 data within Ls range of a given Mars Year

        Parameters
        ----------
        start/end: beginning/end of MY/Ls range
        files: path/url to file(s)
        profiles: specific profiles to reduce to
        ddr: data record [DDR1, DDR2, DDR3]
        add_cols: additional columns to generate and add ["dt"]
        verbose: output log details

        Returns
        -------
        _: loaded L2 data
        """
        if verbose:
            logger.info(
                f"Determining approximate start/end dates for "
                f"range: {start} - {end}"
            )
        # Overshoot on both sides, then fix after data is loaded
        date_start = marstime_to_datetime(start) - dt.timedelta(days=2)
        date_end = marstime_to_datetime(end) + dt.timedelta(days=2)
        data = self.load_date_range(
            date_start, date_end, ddr, add_cols=add_cols, verbose=verbose, **kwargs
        )
        # This reduction will need to be more complicated for multi-MY searches
        if ddr == "DDR1":
            data = data[
                (data["L_s"] >= start.solar_longitude)
                & (data["L_s"] < end.solar_longitude)
            ]
        return data

    def load_from_config_dict(self, config_dict, ddr="DDR1", verbose=False):
        if "dt" in config_dict.keys() and "MY" not in config_dict.keys():
            data = self.load_date_range(
                *config_dict["dt"], ddr="DDR1", add_cols=["dt"], verbose=verbose
            )
        else:
            data_pieces = []
            for myls_range in config_dict["Marstime"]:
                data_pieces.append(
                    self.load_ls_range(
                        myls_range[0],
                        myls_range[1],
                        ddr="DDR1",
                        add_cols=["MY"],
                        verbose=verbose,
                    )
                )
            data = pd.concat(data_pieces, ignore_index=True)
            del config_dict["Marstime"]
        data = filter_ddr1_df_from_config(data, config_dict)
        if ddr == "DDR2":
            ddr2 = self.load("DDR2", profiles=data["Profile_identifier"])
            data = self.merge_ddrs(ddr2, data, verbose=verbose)
        return data

    def merge_ddrs(self, ddr2_df, ddr1_df, verbose=False):
        if verbose:
            logger.info(
                f"Merging DDR1 (shape: {ddr1_df.shape}) profile metadata to "
                f"DDR2 profiles: (shape: {ddr2_df.shape})"
            )
        merged = pd.merge(
            ddr2_df,
            ddr1_df,
            on="Profile_identifier",
            how="outer",
            suffixes=("", "_DDR1"),
        )
        if verbose:
            logger.info(f"Merged: {merged.shape}:\n{merged.head()}")
        return merged
