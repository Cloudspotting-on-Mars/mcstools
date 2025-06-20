import datetime as dt
from typing import Union

import numpy as np
import pandas as pd
from scipy import interpolate
from scipy.stats import circmean

from mcstools.detector_positions import DetectorPositions
from mcstools.mcsfile import L1BFile
from mcstools.util.geom import (
    scattering_angle,
    spherical_coords_mcenter,
    spherical_to_cartesian,
)
from mcstools.util.time import convert_date_utcs, ltst


class DataPipeline:
    """
    Class to hold methods for filtering and transforming MCS data in common ways.
    """

    def __init__(self):
        pass

    def add_datetime_column(
        self, df: pd.DataFrame, dt_name: str = "dt"
    ) -> pd.DataFrame:
        """
        Convert Date and UTC columns to single datetime column.

        Parameters
        ----------
        df: MCS data
        dt_name: column name for new datetime column

        Returns
        -------
        df: data with additional datetime column
        """
        if len(df.index) == 0:
            return pd.DataFrame(columns=list(df.columns) + ["dt"])
        df[dt_name] = df.apply(
            lambda row: convert_date_utcs(row["Date"], row["UTC"]), axis=1
        )
        return df

    def select_range(self, df: pd.DataFrame, col: str, min, max) -> pd.DataFrame:
        """
        Select all rows (observations) with values within input range

        Parameters
        ----------
        df: MCS data
        col: name of column to filter
        min, max: minimum and maximum values to select within

        Returns
        -------
        df: subset of input data
        """
        return df[(df[col] > min) & (df[col] <= max)]

    def select_time_range(
        self, df: pd.DataFrame, start_dt: dt.datetime, end_dt: dt.datetime, dt_col="dt"
    ) -> pd.DataFrame:
        """
        Select data within range of datetimes.

        Parameters
        ----------
        df: MCS data
        start_dt/end_dt: start and end of date range
        dt_col: name of datetime column

        Returns
        -------
        df: subset of input data
        """
        if len(df.index) == 0:
            return df
        return self.select_range(df, dt_col, start_dt, end_dt)

    def select_flag_values(
        self, df: pd.DataFrame, col: str, values: list
    ) -> pd.DataFrame:
        """
        Select subset of data where rows must have certain values of a single column

        Parameters
        ----------
        df: MCS data
        col: name of column to filter by
        values: column values to allow

        Returns
        -------
        _: subset of input data
        """
        return df[df[col].isin(values)]

    def pass_empty_df(self, df: pd.DataFrame, add_cols=None) -> pd.DataFrame:
        if add_cols:
            newcolumns = list(df.columns) + add_cols
        else:
            newcolumns = df.columns
        df = pd.DataFrame(columns=newcolumns)
        return df


class L1BAggregator:
    """
    Class for functions to calculate aggregated values for
    various L1B and derived columns.
    Handles various cyclical columns.
    """

    l1bfile = L1BFile()
    cyclical_range_map = {
        "L_sub_s": (0, 360),
        "LTST": (0, 24),
        "Solar_lon": (-180, 180),
        "SC_lon": (-180, 180),
        "Scene_lon": (-180, 180),
        "Vert_lon": (-180, 180),
        "Last_az_cmd": (0, 360),
        "Limb_ang": (-180, 180),
    }
    cyclical_columns = list(cyclical_range_map.keys())
    temp_flag_columns = [x for x in l1bfile.columns if "temp" in x]
    index_columns = [x for x in l1bfile.columns if "index" in x]
    standard_columns = (
        l1bfile.radcols
        + temp_flag_columns
        + [
            "dt",
            "SCLK",
            "Solar_dist",
            "Solar_lat",
            "SC_rad",
            "SC_lat",
            "Scene_rad",
            "Scene_lat",
            "Scene_alt",
            "Solar_zen",
            "Vert_lat",
            "Limb_ang",
            "Radiance",
            "Last_el_cmd",
            "Scattering_angle",
        ]
        + [x for x in l1bfile.columns if "5V" in x]
    )
    flag_columns = [
        "1",
        "Gqual",
        "Safing",
        "Safed",
        "Freezing",
        "Frozen",
        "Rolling",
        "Dumping",
        "Moving",
        "Temp_Fault",
        "Rqual",
        "Mode",
    ]
    pass_columns = flag_columns + index_columns
    allowed_columns = (
        cyclical_columns + standard_columns
    )  # + flag_columns + index_columns

    def mean(self, vals: pd.Series, column_name: str) -> pd.Series:
        """
        Calculate mean for allowed columns including cyclical columns
        """
        if column_name in self.cyclical_columns:
            cyc_range = self.cyclical_range_map[column_name]
            return circmean(vals, low=cyc_range[0], high=cyc_range[1])
        elif column_name in self.standard_columns:
            return pd.Series.mean(vals)
        else:
            raise ValueError(f"Column {column_name} not recognized for mean")

    def apply_mean(self, column: pd.Series) -> pd.Series:
        return self.mean(column, column.name)


class L1BDataPipeline(DataPipeline):
    """
    Class for filtering and transforming MCS L1B data.
    """

    l1bfile = L1BFile()  # initialize MCS L1B data reader
    detpos = DetectorPositions()  # initialize MCS detector positions
    l1bcols = l1bfile.columns  # all columns of L1b data file
    radcols = l1bfile.radcols
    l1bagg = L1BAggregator()
    az_range_map = {
        "in": (170, 190),
        "left": (80, 100),
        "right": (260, 280),
        "aft": (0, 3),
    }

    def __init__(self):
        super().__init__()

    def select_limb_views(
        self, df: pd.DataFrame, min_alt: float = 20, max_alt: float = 70
    ) -> pd.DataFrame:
        """Select limb views by filtering by Scene_alt"""
        return self.select_range(df, "Scene_alt", min_alt, max_alt)

    def add_first_limb_cols(
        self, df: pd.DataFrame, min_sec_between: float = 5
    ) -> pd.DataFrame:
        """
        Labels first three observations in limb sequence. [1=first 3, 0=not first 3]
        Requires having filtered for limb views.

        Parameters
        ----------
        df: MCS data
        min_sec_between: minimum number of seconds to distinguish
        sequences of limb observations.

        Returns
        -------
        df: data with additional column
        """
        if len(df.index) == 0:
            return self.pass_empty_df(df, ["first_limb", "first_three_limb"])
        # Make new column where True if this row starts new sequence
        df = df.assign(
            first_limb=(df["dt"] - df["dt"].shift()).apply(
                lambda x: True if x > dt.timedelta(seconds=min_sec_between) else False
            )
        )
        # Make new column for limb views 1-3 of sequence
        df = df.assign(
            first_three_limb=df["first_limb"]
            | 1 * df["first_limb"].shift(1)
            | 1 * df["first_limb"].shift(2)
        )
        return df

    def remove_first_three_limb(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes rows where observations is one of first 3 limb views.
        Advantageous to remove because of thermal drift.
        """
        if len(df.index) == 0:
            return self.pass_empty_df(df)
        return df[~df["first_three_limb"]]  # keep rows where this column is False

    def add_limb_view_label(self, df: pd.DataFrame) -> pd.DataFrame:
        if len(df.index) == 0:
            return self.pass_empty_df(df, ["limb_view_label"])
        lv_counter = 0
        lv_col = []
        for i, row in df.iterrows():
            if row["first_limb"]:
                lv_counter = 1
            else:
                lv_counter += 1
            lv_col.append(lv_counter)
        df["limb_view_label"] = lv_col
        return df

    def group_consecutive_rows_as_sequence(
        self, df: pd.DataFrame, consecutive: int = 5, offset_from_first=3
    ) -> pd.DataFrame:
        if len(df.index) == 0:
            return self.pass_empty_df(df, ["sequence_label"])
        seq_counter = 0  # initialize seq counter
        seq_number_col = []  # intialize list of labeled sequences
        for i, row in df.iterrows():
            if row["limb_view_label"] <= offset_from_first:
                seq_number_col.append(np.nan)
            else:
                seq_number_col.append(seq_counter)
                if (row["limb_view_label"] - offset_from_first) % consecutive == 0:
                    seq_counter += 1

        df["sequence_label"] = seq_number_col
        return df

    def add_sequence_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a column to group sequences together by a common index.
        Iterates over a dataframe to label each 'first_limb` row with
        a new label and apply that label to the next observations until
        another first limb.
        """
        if len(df.index) == 0:
            return self.pass_empty_df(df, ["sequence_label"])
        seq_counter = 0  # initialize seq counter
        seq_number_col = []  # intialize list of labeled sequences
        for i, row in df.iterrows():
            if row["first_limb"]:
                seq_counter += 1  # new sequence label
            seq_number_col.append(seq_counter)  # add label for each row
        df["sequence_label"] = seq_number_col  # make new column
        return df

    def select_limb_angle_range(self, df, min_ang=-9, max_ang=9) -> pd.DataFrame:
        """Filter by limb angle"""
        return self.select_range(df, "Limb_ang", min_ang, max_ang)

    def average_limb_sequences(
        self, df, cols: list = None, pass_flag_values=False
    ) -> pd.DataFrame:
        """Calculate average of all limb views in a sequence for set of columns"""
        if not cols:
            cols = [x for x in df.columns if x in self.l1bagg.allowed_columns]
        if not pass_flag_values:
            cols = [x for x in cols if x not in self.l1bagg.pass_columns]
        if len(df.index) == 0:
            return pd.DataFrame(columns=cols + ["sequence_label"])
        grouped = df.groupby("sequence_label")
        grouped_mean = grouped[cols].aggregate(
            self.l1bagg.apply_mean
        )  # mean(numeric_only=False)
        return grouped_mean

    def select_Gqual(self, df: pd.DataFrame, flag_values=[0, 5, 6]) -> pd.DataFrame:
        """Filter by Gqual column"""
        return self.select_flag_values(df, "Gqual", flag_values)

    def select_Rolling(self, df: pd.DataFrame, flag_values=[0]) -> pd.DataFrame:
        """Filter by Rolling column"""
        return self.select_flag_values(df, "Rolling", flag_values)

    def select_Moving(self, df: pd.DataFrame, flag_values=[0]) -> pd.DataFrame:
        """Filter by Moving column"""
        return self.select_flag_values(df, "Moving", flag_values)

    def add_LTST_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add LTST from longitude and solar longitude"""
        if len(df.index) == 0:
            return self.pass_empty_df(df, ["LTST"])
        df = df.assign(
            LTST=df.apply(lambda row: ltst(row["Scene_lon"], row["Solar_lon"]), axis=1)
        )
        return df

    def select_azimuth(self, df: pd.DataFrame, min_az, max_az) -> pd.DataFrame:
        """Filter by azimuth"""
        return self.select_range(df, "Last_az_cmd", min_az, max_az)

    def map_direction(self, last_az_cmd):
        if (
            last_az_cmd >= self.az_range_map["in"][0]
            and last_az_cmd < self.az_range_map["in"][1]
        ):
            return "in"
        elif (
            last_az_cmd >= self.az_range_map["left"][0]
            and last_az_cmd < self.az_range_map["left"][1]
        ):
            return "left"
        elif (
            last_az_cmd >= self.az_range_map["right"][0]
            and last_az_cmd < self.az_range_map["right"][1]
        ):
            return "right"
        elif (
            last_az_cmd >= self.az_range_map["aft"][0]
            and last_az_cmd < self.az_range_map["aft"][1]
        ):
            return "aft"
        else:
            return np.nan

    def add_direction_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add column with label for in-track, left cross-track, and right cross-track
        based on azimuth angle.
        """
        df = df.assign(direction=df["Last_az_cmd"].apply(self.map_direction))
        return df

    def select_direction(
        self,
        df: pd.DataFrame,
        directions: Union[list, str] = ["in"],
        colname: str = "direction",
    ) -> pd.DataFrame:
        """
        Filter data by azimuth angle to select either in-track or left/right cross-track

        Parameters
        ----------
        df: input L1B data (requires direction column -- add_direction_colum)
        direction: ["in", "left", "right"] (in-track or crosstrack)

        Returns
        -------
        : L1B data for given azimuth angle
        """
        if colname not in df.columns:
            raise KeyError(
                f"{colname} not in columns."
                f"First add {colname} with add_direction_column method."
            )
        if isinstance(directions, str):
            directions = [directions]
        return self.select_flag_values(df, colname, directions)

    def melt_channel_detector_radiance(
        self,
        df: pd.DataFrame,
        id_vars: list = None,
        value_name: str = "Radiance",
        channel_name: str = "Channel",
        detector_name: str = "Detector",
    ) -> pd.DataFrame:
        """
        Convert "Rad_[CH]_[DE]" columns to "Channel" and "Detector" columns
        and list all radiance values in "Radiance" column. Keep other id_Vars columns

        Parameters
        ----------
        df: MCS data
        id_vars: columns to keep through to new melted data without changing
        value_name: Name to give new single Radiance column
        channel_name: Name to give new Channel column
        detector_name: Name to give new Detector column
        """
        if not id_vars:
            id_vars = [
                x
                for x in df.columns
                if x not in [channel_name, detector_name] + self.radcols
            ]
        # Move RAD_CH_DE column names to variable column and make new radiance column
        melted = df.melt(id_vars=id_vars, value_name=value_name)
        melted[channel_name] = melted["variable"].apply(
            lambda x: x[4:6]
        )  # Make channel column
        melted[detector_name] = melted["variable"].apply(
            lambda x: int(x[-2:])
        )  # Make detector column
        melted = melted.drop(
            columns="variable"
        )  # Remove columns of "Rad_[CH]_[DE]" values ("Radiance" columns remain)
        return melted

    def add_altitude_column(
        self,
        df: pd.DataFrame,
        channel_col: str = "Channel",
        detector_col: str = "Detector",
        alt_name: str = "Altitude",
    ) -> pd.DataFrame:
        """
        Use detector positions (vertical field of view) to add a
        column of altitudes relative to ellipsoid.
        to a dataframe. Requires ["SC_rad", "Scene_rad",
        "Scene_alt", channel columns, detector column]

        Parameters
        ----------
        df: input dataframe (must include channel and detector columns, see melted)
        channel_col: name of column with channel info
        detector_col: name of column with detector values
        alt_name: name to give new column of altitudes

        Returns
        -------
        df: Output data with additional new altitude column
        """
        if len(df.index) == 0:
            return self.pass_empty_df(df, [alt_name])
        df[alt_name] = df.apply(
            lambda row: self.detpos.convert_fov_to_altitude(
                row["SC_rad"],
                row["Scene_rad"],
                row["Scene_alt"],
                self.detpos.elevation.loc[row[detector_col], row[channel_col]],
            ),
            axis=1,
        )
        return df

    def interpolate_radiances(self, altitudes, radiances, newaltitudes):
        """
        Interpolate radiances onto new array of altitudes.

        Parameters
        ----------
        altitudes: original altitudes
        radiances: original radiances
        newaltitudes: altitudes to interpolate to

        Returns
        -------
        new_rads: interpolated radiances
        """
        f_interp = interpolate.interp1d(
            altitudes, radiances, kind="linear"
        )  # create interpolater
        interp_index = np.where(
            (newaltitudes > altitudes.min()) & (newaltitudes < altitudes.max())
        )  # values to use for interpolation
        new_rads = np.zeros(newaltitudes.shape)  # initialzie new radiance array
        new_rads[:] = np.nan  # set to all nans
        new_rads[interp_index] = f_interp(
            newaltitudes[interp_index]
        )  # interpolate for altitudes within range
        return new_rads

    def convert_to_interpolated_radiances(
        self,
        df: pd.DataFrame,
        group_on: str = "sequence_label",
        altitudes: np.array = np.arange(0, 110, 5),
        rad_col: str = "Radiance",
        alt_col: str = "Altitude",
        channel_col: str = "Channel",
    ) -> pd.DataFrame:
        """
        Convert a given dataframe of radiances
            (with altitudes, channels, sequence labels, and radiacnes)
        to dataframe of interpolated radiances (new altitudes)

        df: input dataframe
            (must include ["sequence_label", radiance, altitude, and channel] columns)
        altitudes: new array of altitudes to interpolate onto
        rad_col: name of radiance column
        alt_col: name of altitude column
        channel_col: name of channel column
        """
        if len(df.index) == 0:
            return self.pass_empty_df(df)
        grouped = df.groupby([group_on, channel_col])  # group by individual profiles
        pieces = []  # initialize new list of dataframes
        for name, group in grouped:
            gsorted = group.sort_values(alt_col)  # sort each profile by altitude
            rads = self.interpolate_radiances(
                gsorted[alt_col], gsorted[rad_col], altitudes
            )  # interpolate to new alts
            idf = pd.DataFrame(
                {rad_col: rads, alt_col: altitudes}
            )  # new DF for each profile
            idf[group_on] = name[0]  # include original labels
            if group_on == "sequence_label":
                # all the same dt for given seq label
                # (could do this for list of columns)
                idf["dt"] = gsorted["dt"].iloc[0]
            idf[channel_col] = name[1]  # and channels
            pieces.append(idf)  # add to new DF
        df_interp = pd.concat(pieces)  # Single DF of all profiles
        return df_interp

    def l1b_row_scattering_angle(
        self, row: pd.Series, scene_dist_col="Scene_rad"
    ) -> float:
        """
        Use L1B geometry columns to define vectors to sun,
        scene, and spacecraft and compute the angle between the
        solar incidence angle to the scene point and the view angle
        to the scene point, which gives the scattering angle.
        (0=forward scattering, 180=back scattering)

        Parameters
        ----------
        row: single row of L1B data

        Returns
        -------
        sa: scattering angle
        """
        # Compute x,y,z of sun
        solar_vec = spherical_to_cartesian(
            spherical_coords_mcenter(
                row["Solar_dist"],
                np.deg2rad(90 - row["Solar_lat"]),
                np.deg2rad(row["Solar_lon"]),
            )
        )
        # Compute x,y,z of scene point
        scene_vec = spherical_to_cartesian(
            spherical_coords_mcenter(
                row[scene_dist_col],
                np.deg2rad(90 - row["Scene_lat"]),
                np.deg2rad(row["Scene_lon"]),
            )
        )
        # Compute x,y,z of spacecraft
        sc_vec = spherical_to_cartesian(
            spherical_coords_mcenter(
                row["SC_rad"], np.deg2rad(90 - row["SC_lat"]), np.deg2rad(row["SC_lon"])
            )
        )
        # Compute scattering angle of scene point
        sa = scattering_angle((solar_vec - scene_vec), (sc_vec - scene_vec))
        return sa

    def add_scattering_angle_column(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df["Scattering_angle"] = df.apply(
            lambda row: self.l1b_row_scattering_angle(row, **kwargs), axis=1
        )
        return df
