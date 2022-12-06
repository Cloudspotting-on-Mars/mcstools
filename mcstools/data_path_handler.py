import datetime as dt
import os

import pandas as pd
from util.mars_time import MarsDate
from util.time import GDS_DATE_FMT, round_to_x_hour

# TODO: check_file_exists shouldn't be part of path handler, should be part of loader
# TODO: make_n_before_after(f, before, after)


class FilenameBuilder:
    def __init__(self, level: str, pds=False, mcs_data_path=None) -> None:
        if (not pds and mcs_data_path is None) or (pds and mcs_data_path is not None):
            print(pds, mcs_data_path)
            raise ValueError(
                "Must provide one and only one of 'pds' or 'mcs_data_path'"
            )
        elif pds:
            self.handler = PDSFileFormatter(level)
        elif mcs_data_path:
            self.handler = DirectoryFileFormatter(level, mcs_data_path)

    def make_filename_from_filestr(self, filestr) -> str:
        filename = self.handler.build_filename_from_filestr(filestr)
        return filename

    def make_filenames_from_daterange(
        self, start: dt.datetime, end: dt.datetime
    ) -> list:
        """
        Build paths and check if each file exists
        """
        filestrs = self._build_filestrs_from_daterange(start, end)
        filenames = [self.handler.build_filename_from_filestr(f) for f in filestrs]
        return filenames

    def make_filenames_from_marsdaterange(self, start: MarsDate, end: MarsDate):
        """
        Build paths given a start/end MY-Ls range and check if each file exist.
        """
        start_dt = start.to_UTC()
        end_dt = end.to_UTC()
        return self.make_filenames_from_daterange(start_dt, end_dt)

    def _build_filestrs_from_daterange(
        self, start: dt.datetime, end: dt.datetime
    ) -> list:
        """
        Build paths to all files spanning range.
        Rounds start date down to nearest 4-hour and
        end date up to nearest 4-hour
        """
        start = round_to_x_hour(
            start, hours=4, force_down=True
        )  # convert times to 4-hour format
        end = round_to_x_hour(end, hours=4, force_up=True)
        datetimes = pd.date_range(
            start, end, freq="4H", inclusive="left"
        )  # generate file datetimes (4-hour fmt)
        filestrs = [self.handler.convert_dt_to_filestr(d) for d in datetimes]
        return filestrs


class FileFormatterBase:
    level_record_map = {"L1B": "RDR", "L2": "DDR"}

    def format_dt_as_filestr(self, datetime: dt.datetime) -> str:
        """
        Convert datetime to 12-digit filebase structure.
        Will not match real filename if not rounded to 4-hour
        """
        return datetime.strftime(GDS_DATE_FMT)

    def convert_dt_to_filestr(self, datetime: dt.datetime) -> str:
        """
        Round datetime to valid 4-hour time file
        """
        filedt = round_to_x_hour(
            datetime, hours=4, force_down=True
        )  # convert times to 4-hour format
        filestr = self.format_dt_as_filestr(filedt)
        return filestr

    def convert_filestr_to_dt(self, filestr: str) -> dt.datetime:
        "Convert 12-digit filebase structure to datetime"
        filedt = dt.datetime.strptime(filestr, GDS_DATE_FMT)
        return filedt


class PDSFileFormatter(FileFormatterBase):

    url_base = "https://atmos.nmsu.edu/PDS/data/"

    def __init__(self, level: str) -> None:
        self._check_valid_level(level)
        self.level = level
        self.data_record = self.level_record_map[level]

    def _check_valid_level(self, level: str) -> None:
        if level not in self.level_record_map.keys():
            raise ValueError(
                f"Level {level} not recognized. "
                f"Expected one of {self.level_record_map.keys()}"
            )

    def build_filename_from_filestr(self, filestr: str):
        filedt = self.convert_filestr_to_dt(filestr)
        mromstr = self.build_mromstr(filedt)
        yearstr = filedt.strftime("%Y")
        monthstr = filedt.strftime("%m")
        daystr = filedt.strftime("%d")
        hourstr = filedt.strftime("%H")
        return os.path.join(
            self.url_base,
            mromstr,
            "DATA",
            yearstr,
            f"{yearstr}{monthstr}",
            f"{yearstr}{monthstr}{daystr}",
            f"{yearstr}{monthstr}{daystr}{hourstr}_{self.data_record}.TAB",
        )

    def build_mromstr(self, date: dt.datetime):
        if self.level in ["L2", "L22D", "L2_2D", "DDR"]:
            mrom_0 = "2"
        elif self.level == ["L1B", "RDR"]:
            mrom_0 = "1"
        elif self.level == ["L0", "EDR"]:
            mrom_0 = "0"
        # last 3 digits
        mrom_1 = 0  # start at 000
        # 001 is September 2006, +1 from there
        if date.month >= 9:
            mrom_1 += (date.year - 2006) * 12 + date.month - 8
        else:
            mrom_1 += (date.year - 2007) * 12 + 4 + date.month
        mrom_1 = str(mrom_1).zfill(3)  # convert integer to 3 digit string
        mrom = "MROM_" + mrom_0 + mrom_1  # combine with data record
        return mrom


class DirectoryFileFormatter(FileFormatterBase):

    level_dir_map = {"L1B": "level_1b", "L2": "level_2_2d"}

    def __init__(self, level: str, mcs_data_path: str):
        self.level = level
        self.mcs_directory = mcs_data_path
        self.level_directory = self.build_level_directory(self.level_dir_map[level])

    def build_level_directory(self, level_directory):
        """
        Build path to L1B or L2 base directory
        /path/to/mcs_data/level_1b/
        """
        return os.path.join(self.mcs_directory, level_directory)

    def build_date_directory(self, date):
        """
        Build path to L1B year-month directory
        /path/to/mcs_data/level_1b/YYMM/
        """
        day_fmt = date.strftime("%Y%m")[2:]  # Years as two digits
        return os.path.join(self.level_directory, day_fmt)

    def build_date_path(self, date):
        """
        Build path to file based on timestamp.
        (file that contains that timestamp)
        /path/to/mcs_data/level_1b/YYMM/YYMMDDHHMMSS.L1B
        """
        filestr = self.date_to_filestr(date)
        path = os.path.join(
            self.build_date_directory(date),
            f"{filestr}.{self.level}",
        )
        return path

    def build_filename_from_filestr(self, filestr: str):
        path = os.path.join(
            self.level_directory, filestr[0:4], f"{filestr}.{self.level}"
        )
        return path

    def convert_path_to_filedt(self, path: str) -> str:
        """
        Convert full path of file to datetime
        """
        return self.convert_filestr_to_filedt(
            os.path.splitext(os.path.basename(path))[0]
        )

    def find_file_from_date(self, date):
        """
        Build path and check if file exists
        """
        expected_path = self.__build_date_path__(date)
        paths, missing = self.__check_for_files__([expected_path])
        if paths:
            path = paths[0]
        else:
            path = []
        if missing:
            missing = missing[0]
        else:
            missing = []
        return path, missing

    def __check_for_files__(self, expected_paths: list):
        dont_exist = [f for f in expected_paths if not os.path.exists(f)]
        problem_files = ["150826040000.L1B"]
        if dont_exist:
            print(f"The following files were not found:\n{dont_exist}")
        if any(
            [
                x
                for x in expected_paths
                if os.path.basename(x) in problem_files and x not in dont_exist
            ]
        ):
            ignore = [x for x in expected_paths if os.path.basename(x) in problem_files]
            print(f"Ignoring files: {ignore}")
            expected_paths = [x for x in expected_paths if x not in ignore]
        paths = [f for f in expected_paths if os.path.exists(f)]
        return paths, dont_exist


class DataPathHandler:
    """
    Base class for MCS data file path handler.
    Contains methods to parse filenames, build paths based on dates, etc.
    """

    # Level specific variables
    level_dir_name = None  # directory name
    level_suffix = None  # file suffix

    def __init__(self, mcs_data_path):
        self.mcs_directory = mcs_data_path
        self.level_directory = self.__build_level_directory__(self.level_dir_name)

    def find_n_preceding_files_from_date(self, date, n):
        """
        Build paths to files before a given date
        """
        start = date - dt.timedelta(hours=4 * n)  # get datetime for prev file
        end = date - dt.timedelta(hours=4)
        return self.find_files_from_daterange(start, end)

    def find_n_following_files_from_date(self, date, n):
        """
        Build paths to files after a given date
        """
        start = date + dt.timedelta(hours=4)
        end = date + dt.timedelta(hours=4 * n)
        return self.find_files_from_daterange(start, end)

    def find_files_around_date(self, date, n):
        files_before = self.find_n_preceding_files_from_date(date, n)
        files_after = self.find_n_following_files_from_date(date, n)
        file = self.find_file_from_date(date)
        files = (
            [x for x in files_before[0] + [file[0]] + files_after[0] if x],
            [x for x in files_before[1] + [file[1]] + files_after[1] if x],
        )
        return (sorted(files[0]), sorted(files[1]))

    def find_files_around_file(self, f, n):
        return self.find_files_around_date(self.__path_to_filedt__(f), n)
