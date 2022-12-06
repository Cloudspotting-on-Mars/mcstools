import datetime as dt
import os

import pandas as pd
from util.mars_time import MarsDate
from util.time import round_to_x_hour


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
        self.level_directory = self.build_level_directory(self.level_dir_name)

    def check_for_files(self, expected_paths: list):
        dont_exist = [f for f in expected_paths if not os.path.exists(f)]
        problem_files = []
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

    def filedt_to_filestr(self, fdt):
        """
        Convert datetime to 12-digit filebase structure.
        Will not match real filename if not rounded to 4-hour
        """
        return fdt.strftime("%y%m%d%H%M%S")

    def date_to_filestr(self, date):
        """
        Round datetime to 4-hour time file
        """
        file_dt = round_to_x_hour(
            date, hours=4, force_down=True
        )  # convert times to 4-hour format
        file_str = self.filedt_to_filestr(file_dt)
        return file_str

    def build_date_path(self, date):
        """
        Build path to file based on timestamp.
        (file that contains that timestamp)
        /path/to/mcs_data/level_1b/YYMM/YYMMDDHHMMSS.L1B
        """
        file_str = self.date_to_filestr(date)
        path = os.path.join(
            self.build_date_directory(date),
            file_str + self.level_suffix,
        )
        return path

    def build_paths_from_daterange(self, start: dt.datetime, end: dt.datetime) -> list:
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
        # Build paths for each 4-hour date
        files = [self.build_date_path(d) for d in datetimes]
        return files

    def filestr_to_filedt(self, fs):
        "Convert 12-digit filebase structure to datetime"
        file_dt = dt.datetime.strptime(fs, "%y%m%d%H%M%S")
        return file_dt

    def path_to_filedt(self, path):
        """
        Convert full path of file to datetime
        """
        return self.filestr_to_filedt(os.path.splitext(os.path.basename(path))[0])

    def find_file_from_date(self, date):
        """
        Build path and check if file exists
        """
        expected_path = self.build_date_path(date)
        paths, missing = self.check_for_files([expected_path])
        if paths:
            path = paths[0]
        else:
            path = []
        if missing:
            missing = missing[0]
        else:
            missing = []
        return path, missing

    def find_files_from_daterange(self, start: dt.datetime, end: dt.datetime):
        """
        Build paths and check if each file exists
        """
        expected_paths = sorted(self.build_paths_from_daterange(start, end))
        paths, missing = self.check_for_files(expected_paths)
        return paths, missing

    def find_files_from_marsdaterange(self, start: MarsDate, end: MarsDate):
        """
        Build paths given a start/end MY-Ls range and check if each file exist.
        """
        start_dt = start.to_UTC()
        end_dt = end.to_UTC()
        return self.find_files_from_daterange(start_dt, end_dt)

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
        return self.find_files_around_date(self.path_to_filedt(f), n)


class L1BDataPathHandler(DataPathHandler):
    level_dir_name = "level_1b"
    level_suffix = ".L1B"
    problem_files = ["150826040000.L1B"]


class L22dDataPathHandler(DataPathHandler):
    level_dir_name = "level_2_2d"
    level_suffix = ".L2"
