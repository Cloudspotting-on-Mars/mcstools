import datetime as dt

import numpy as np
import pandas as pd

from mcstools.util.geom import haversine_dist


class OnPlanetFinder:
    """
    Class to aid in finiding appropriate on-planet views
    close to limb views.

    Methods assume data has been processed into two dataframes:
    limb views and on-planet views.

    Args:
        delta_seconds: number of seconds before/after limb view
            to look for on-planet views.
    """

    def __init__(self, delta_seconds: float = 60 * 20):
        self.delta_seconds = delta_seconds

    def find_nearby_op(self, limb_row: pd.Series, op_df: pd.DataFrame) -> pd.DataFrame:
        """
        Find on-planet views within +/- DELTA_SECONDS of
        limb view in LIMB_ROW.

        Args:
            limb_row: single row of limb-view dataframe
            op_df: on-planet views to search

        Returns:
            op_subdf: on-planet views within search range of limb view
        """
        timedelta = dt.timedelta(seconds=self.delta_seconds)  # setup time delta
        timestamp = limb_row["dt"]  # get time of limb observation
        # Find all on-planet views within time range
        op_subdf = op_df[
            op_df["dt"].between(timestamp - timedelta, timestamp + timedelta)
        ].copy()
        return op_subdf

    def add_haversine_distance_column_to_op_df(
        self, op_df: pd.DataFrame, limb_lat: float, limb_lon: float
    ) -> pd.DataFrame:
        """
        Add a column of the haversine distances between each on-planet view
        and a given limb view location.

        Args:
            op_df: on-planet views to search
            limb_lat: limb-view scene latitude
            limb_lon: limb-view scene longitude

        Returns:
            op_df: on-planet view DF with additional "hdist" column.
        """
        # if no observations, just add empty column
        if op_df.empty:
            op_df["hdist"] = None
        else:
            op_df["hdist"] = op_df.apply(
                lambda row: np.rad2deg(
                    haversine_dist(
                        limb_lat, limb_lon, row["Scene_lat"], row["Scene_lon"]
                    )
                ),
                axis=1,
            )
        return op_df

    def find_closest_op(self, limb_row: pd.Series, op_df: pd.DataFrame) -> pd.Series:
        """
        Find the closest on-planet view (by great circle distance)
        within DELTA_SECONDS of a single limb observation.

        Args:
            limb_row: single row of limb-view dataframe
            op_df: on-planet views to search
        """
        op_subdf = self.find_nearby_op(limb_row, op_df)
        op_subdf = self.add_haversine_distance_column_to_op_df(
            op_subdf, limb_row["Scene_lat"], limb_row["Scene_lon"]
        )
        if op_subdf.empty:
            return pd.Series({x: np.nan for x in op_subdf.columns})
        else:
            return op_subdf.loc[op_subdf["hdist"].idxmin()]

    def add_closest_op_distance_and_dt_to_limb_df(
        self,
        limb_df: pd.DataFrame,
        op_df: pd.DataFrame,
        distance_column_name: str = "closest_op_hdist",
        time_column_name: str = "closest_op_time",
        lat_column_name: str = "closest_op_lat",
        lon_column_name: str = "closest_op_lon",
    ) -> pd.DataFrame:
        """
        Add columns to limb-view data of haversine distance and time of closest
        on-planet view to that observation.

        Args:
            limb_df: limb views to find closest on-planet views for.
            op_df: on-planet views to search
            distance_column_name: name to give haversine distance column
            time_column_name: name to give column of on-planet view timestamps
            lat[/lon]_column_name: name to give column of on-planet view lat/lon
        Returns:
            limb_df: limb views with additional columns
        """
        limb_df[
            [distance_column_name, time_column_name, lat_column_name, lon_column_name]
        ] = limb_df.apply(
            lambda row: self.find_closest_op(row, op_df)[
                ["hdist", "dt", "Scene_lat", "Scene_lon"]
            ],
            axis=1,
        )
        return limb_df
