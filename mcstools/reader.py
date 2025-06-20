import datetime as dt
import os

import numpy as np
import pandas as pd
import requests

from mcstools.mcsfile import L1BFile, L2File
from mcstools.util.log import logger
from mcstools.util.time import (
    GDS_DATE_FMT,
    PDS_DATE_FMT,
    add_datetime_column,
    add_marsyear_column,
)


class Reader:
    """
    Base class for MCS file reader.
    """

    def __init__(self):
        pass


class L1BReader(Reader, L1BFile):
    """
    Class to read data from a *single* L1B file.
    """

    def __init__(self, pds=False):
        super().__init__()
        self.pds = pds
        self.output_columns = self.columns + ["Solar_dist", "L_sub_s"]

    def read(self, filename, usecols=None, add_cols: str = None, **kwargs):
        """
        Create DF of data in L1B file. L1B have csv structure,
        so can easily read in with pandas

        Parameters
        ----------
        filename (str): name of file to load data from

        Returns
        -------
        df (DF): Data as pandas DataFrame
        """
        if not usecols:
            usecols = self.columns  # read all columns if not specified
        try:
            df = pd.read_csv(
                filename,
                names=self.columns,
                header=0,
                comment=self.comment_line_character,
                na_values=self.nan_values,
                usecols=usecols,
                dtype=self.dtypes,
                **kwargs,
            )
        except pd.errors.ParserError as e:
            logger.error(
                f"Unable to load {filename}, "
                "number of columns does not match expected.\n"
                f"Pandas parser error: {e}"
            )
            df = pd.DataFrame(columns=usecols)
        header_vals = self.grab_header_values(filename, url=self.pds)
        for newcol in header_vals.keys():
            df[newcol] = header_vals[newcol]
        if add_cols:
            if "dt" in add_cols:
                df = add_datetime_column(df)
            if "MY" in add_cols:
                df = add_marsyear_column(df)
        return df

    def grab_header_values(self, filename: str, url=False) -> dict:
        """
        Open file and grab certain values from header.
        Currently just "Solar_dist" and "L_sub_s".

        Parameters
        ----------
        filename: name of file to open

        Returns
        -------
        vals: dictionary of header values
        """
        nlines = 40
        vals = {"Solar_dist": None, "L_sub_s": None}
        if not url:
            with open(filename, "r") as f:
                lines = [f.readline() for x in range(nlines)]
        else:
            url_text = requests.get(filename).text
            lines = url_text.splitlines()[0:40]
        for line in lines:
            if line[0] != "#":
                break
            if "Solar_dist" in line:
                # Solar distance in km
                vals["Solar_dist"] = float(line.strip().split("=")[-1].split("(km)")[0])
            elif "L_sub_s" in line:
                # Ls in degrees
                vals["L_sub_s"] = float(line.strip().split("=")[-1])
            if vals["Solar_dist"] and vals["L_sub_s"]:
                break
        return vals


class L2Reader(Reader):
    def __init__(self, pds=False):
        super().__init__()
        self.l22dfile = L2File(pds=pds)
        self.data_records = self.l22dfile.data_records

    def read_lines_from_file(self, filepath):
        """
        Read every line in MCS file

        Parameters
        ----------
        file_path (str): path to MCS file
        """
        self.path = filepath
        self.filename = os.path.splitext(os.path.basename(self.path))[0]
        with open(self.path) as f:
            lines = f.readlines()
        self.file_length = len(lines)
        return lines

    def read_lines_from_url(self, url):
        self.path = url
        pds_datestr = os.path.splitext(os.path.basename(url))[0].split("_")[0]
        self.filename = dt.datetime.strptime(pds_datestr, PDS_DATE_FMT).strftime(
            GDS_DATE_FMT
        )
        url_req = requests.get(url)
        if url_req.status_code == 200:
            url_text = url_req.text
            lines = url_text.splitlines()
            self.file_length = len(lines)
        elif url_req.status_code == 404:
            lines = []
            self.file_length = 0
        else:
            print(f"Not setup to handle request status code {url_req} from {url}")
        return lines

    def get_comments_from_lines(self, lines):
        comments = [x.rstrip() for x in lines if x[0] == "#"]
        self.comments = comments
        return comments

    def get_column_names_from_lines(self, lines):
        """
        Read in column names given near the top of the file.
        Used as a check to names in base class

        Parameters
        ----------
        line (str): single line of .L2 file

        Returns
        -------
        names (list): list of name items in line
        """
        # ddrs = len(self.data_records.keys())
        file_columns = {}
        for i, ddr in enumerate(self.data_records.keys()):
            ddr_line = lines[len(self.comments) + i]
            file_columns[ddr] = [x.strip() for x in ddr_line.rstrip().split(",")]
            check_result = self.check_column_names(file_columns[ddr], ddr)
            if not check_result:
                print(f"Problem loading {self.path}")
        return file_columns

    def check_column_names(self, column_names, DDRN):
        """
        Check column names against base class

        Parameters
        ----------
        column_names (list): list of column names from input file
        DDRN (str): DDR-format number for these columns [DDR1 - DDR4]

        Returns
        -------
        """
        exp_cols = self.data_records[DDRN]["columns"]
        if column_names != exp_cols:
            print(
                f"{DDRN} column names given in {self.filename} do not match expected."
            )
            print(f"Filename: {self.filename}")
            print(f"Expected {exp_cols} names for {DDRN} row,")
            print(f"Got: {column_names}")
            return False
        else:
            return True

    def get_data_record(self, lines, record):
        """
        Get data from all records from file lines.

        Parameters
        ----------
        lines (list): list of lines from file

        Returns
        -------
        data (list): nested list of data for each profile in given data record
        """
        len_com = len(self.comments)  # number of comment lines to skip
        len_colnames = len(
            self.data_records.keys()
        )  # number of column-name lines to skip
        # number of rows for first profile up to this data record
        skip_data_rows = np.sum(
            [self.data_records[x]["lines"] for x in self.data_records.keys()][
                0 : int(record[-1]) - 1
            ]
        )
        start_row = (
            len_com + len_colnames + int(skip_data_rows)
        )  # row of file to start recording data
        # number of rows per profile
        rows_per_prof = np.sum(
            [self.data_records[x]["lines"] for x in self.data_records.keys()]
        )
        chunk_size = self.data_records[record][
            "lines"
        ]  # number of lines per profile for this record
        # Get lines for this record over whole file
        rows = [
            lines[i : i + chunk_size]
            for i in range(start_row, self.file_length + 1)
            if (i - start_row) % rows_per_prof == 0
        ]
        rows_items = [
            x.rstrip().split(",") for sublist in rows for x in sublist
        ]  # convert string to item list
        rows_noblank = [
            [x.replace('"', "").strip() for x in sublist] for sublist in rows_items
        ]  # remove blank spaces
        data = rows_noblank
        return data

    def get_data_all(self, lines):
        """
        Get data from all records from file lines.

        Parameters
        ----------
        lines (list): list of lines from file

        Returns
        -------
        data (dict): data in nested lists format for each record
        """
        data = {}  # initialize data dict
        # Gather number of lines for parts of file
        len_com = len(self.comments)  # number of comment lines to skip
        len_colnames = len(
            self.data_records.keys()
        )  # number of column-name lines to skip
        start_row = len_com + len_colnames  # first row of data
        # number of rows per profile
        rows_per_prof = np.sum(
            [self.data_records[x]["lines"] for x in self.data_records.keys()]
        )
        # Get data from each record
        for i, ddr in enumerate(self.data_records.keys()):
            chunk_size = self.data_records[ddr][
                "lines"
            ]  # number of lines per profile for this record
            # Get lines for this record over whole file
            rows = [
                lines[i : i + chunk_size]
                for i in range(start_row, self.file_length + 1)
                if (i - start_row) % rows_per_prof == 0
            ]
            rows_items = [
                x.rstrip().split(",") for sublist in rows for x in sublist
            ]  # convert string to item list
            rows_noblank = [
                [x.replace('"', "").strip() for x in sublist] for sublist in rows_items
            ]  # remove blank spaces
            data[ddr] = rows_noblank  # add to data dict
            start_row = start_row + (
                chunk_size
            )  # change start row for next data record
        return data

    def make_df(self, data, record, columns):
        """
        Convert data in nested list [[prof:[row]]] to DF
        with column names for that DDR

        Parameters
        ----------
        data (list): nested list of data from self.read_file
        names (list): column names for that DDR

        Returns
        -------
        (DF): Data as pandas DataFrame
        """
        col_index = [self.data_records[record]["columns"].index(x) for x in columns]
        inp_data = [[sublist[i] for i in col_index] for sublist in data]
        # getting FutureWarning with float here,should make dict of dtypes in mcsfile.py
        df = pd.DataFrame(data=inp_data, columns=columns)
        df = df.astype(self.data_records[record]["dtypes"])
        df = df.map(
            lambda x: x if x not in self.l22dfile.nan_values else np.nan
        )  # replace(self.l22dfile.nan_values, np.nan).infer_objects(copy=False)
        return df

    def add_profile_filename_number(self, df, record):
        """
        Add two columns to DataFrame for a DDR:
        Prof# (profile number of file) and
        filename.

        Parameters
        ----------
        df (DF): DDR data converted in DF format (standard index)
        DDRprofn (int): DDR number [1,2,3,4]

        Returns
        -------
        df (DF): DDR data with 2 new columns
        """
        DDRprofn = self.data_records[record]["lines"]
        profnum = df.index / DDRprofn
        df["Profile_identifier"] = [f"{self.filename}_{x}" for x in profnum.astype(int)]
        if record == "DDR2":
            df["level"] = df.index % DDRprofn
        return df

    def get_path(self, datetime):
        basepath = self.get_basepath(datetime)
        path = os.path.join(self.path_l22d, f"{basepath}.L2")
        return path

    def read(self, filename, ddr, add_cols: list = None):
        if "https" in filename:
            lines = self.read_lines_from_url(filename)
        else:
            lines = self.read_lines_from_file(filename)
        self.get_comments_from_lines(lines)
        self.get_column_names_from_lines(lines)
        data = self.get_data_record(lines, ddr)
        df = self.make_df(data, ddr, self.data_records[ddr]["columns"])
        df = self.add_profile_filename_number(df, ddr)
        if add_cols:
            if "dt" in add_cols:
                df = add_datetime_column(df)
            if "MY" in add_cols:
                df = add_marsyear_column(df)
        return df
