import pandas as pd
import pytz

from mcstools.preprocess.data_pipeline import L1BDataPipeline
from mcstools.util.log import logger

# TODO: @classmethod() from_config() option to setup processer


class L1BOnPlanetInTrack:
    def __init__(
        self,
        scene_alt_range=(-0.01, 0.01),
        elevation_angle_range=(114, 180),
        limb_angle_range=(-9, 9),
        gqual=[0, 5, 6],
        rolling=[0],
        moving=[0],
        include_aft=False,
    ):
        self.scene_alt_range = scene_alt_range
        self.elevation_angle_range = elevation_angle_range
        self.limb_angle_range = limb_angle_range
        self.gqual = gqual
        self.rolling = rolling
        self.moving = moving
        self.directions = ["in"]
        if include_aft:
            self.directions.append("aft")

    def preprocess(self, df):
        """
        From loaded L1b data, preprocess to get in-track on-planet views
        based on elevation angle, scene altitude, limb angles,
        azimuth angles, and quality flags.

        *Could have option to average together
        """
        pipe = L1BDataPipeline()
        df = pipe.add_datetime_column(df)  # add datetimes
        df = pipe.select_range(
            df, "Scene_alt", *self.scene_alt_range
        )  # apply scene_alt constraint
        df = pipe.select_range(df, "Last_el_cmd", *self.elevation_angle_range)
        df = pipe.select_limb_angle_range(
            df, *self.limb_angle_range
        )  # apply limb angle constraint
        df = pipe.select_Gqual(df, flag_values=self.gqual)
        if len(self.rolling) > 0:
            df = pipe.select_Rolling(df, flag_values=self.rolling)
        if len(self.moving) > 0:
            df = pipe.select_Moving(df, flag_values=self.moving)
        df = pipe.add_direction_column(df)
        df = pipe.select_direction(df, self.directions)
        df = pipe.add_LTST_column(df)
        return df


class L1BStandardInTrack:
    def __init__(
        self,
        limb_scene_alt_range=(20, 70),
        first_limb_col_sec_between=5,
        limb_angle_range=(-9, 9),
        gqual=[0, 5, 6],
        rolling=[0],
        moving=[0],
        directions=None,
        include_aft=False,
    ):
        self.limb_scene_alt_range = limb_scene_alt_range
        self.first_limb_col_sec_between = first_limb_col_sec_between
        self.limb_angle_range = limb_angle_range
        self.gqual = gqual
        self.rolling = rolling
        self.moving = moving
        if directions is None:
            self.directions = ["in"]
        else:
            self.directions = directions
        if include_aft and "aft" not in self.directions:
            self.directions.append("aft")

    def preprocess(self, df, average_sequences=True):
        """
        From loaded L1b data, preprocess to get standard limb in-track values
        based on range of Scene alts, limb angles, azimuth angles, and quality flags.
        Removes first three measurements of a limb sequence (thermal drift) and
        averages the others.
        """
        pipe = L1BDataPipeline()
        df = pipe.add_datetime_column(df)
        df = pipe.select_limb_views(
            df,
            min_alt=self.limb_scene_alt_range[0],
            max_alt=self.limb_scene_alt_range[1],
        )
        df = pipe.add_first_limb_cols(
            df, min_sec_between=self.first_limb_col_sec_between
        )
        df = pipe.add_sequence_column(df)
        df = pipe.remove_first_three_limb(df)
        df = pipe.select_limb_angle_range(
            df, min_ang=self.limb_angle_range[0], max_ang=self.limb_angle_range[1]
        )
        df = pipe.select_Gqual(df, flag_values=self.gqual)
        if len(self.rolling) > 0:
            df = pipe.select_Rolling(df, flag_values=self.rolling)
        if len(self.moving) > 0:
            df = pipe.select_Moving(df, flag_values=self.moving)
        df = pipe.add_direction_column(df)
        df = pipe.select_direction(df, self.directions)
        df = pipe.add_LTST_column(df)
        if average_sequences:
            # Average
            df = pipe.average_limb_sequences(
                df,
                cols=None,  # ["dt", "SC_rad", "Scene_alt", "Scene_rad"] + pipe.radcols
            )
            df = df.reset_index()
        df = df.drop(columns="sequence_label")
        return df

    def melt_to_xarray(self, df, include_cols=["Radiance", "Scene_lat", "Scene_lon"]):
        """
        Convert Dataframe of L1b radiances to xarray with coordinates:
        ["dt", "Detector", "Channel"].
        """
        if "LTST" in include_cols and "LTST" not in df.columns:
            logger.warning("LTST not in data columns, try adding first.")
        pipe = L1BDataPipeline()
        df_melted = pipe.melt_channel_detector_radiance(df.reset_index())
        ds = df_melted.set_index(["dt", "Detector", "Channel"])[
            include_cols
        ].to_xarray()
        # Xarray treats datetimes as naive/UTC
        tz = df["dt"].iloc[0].tz
        ds["dt"] = pd.DatetimeIndex(ds["dt"])
        if tz != pytz.utc:
            # Warn if input was not UTC
            print(
                "Moved non-UTC timezone into xarray dataset, "
                f"not sure if tzinfo {tz} carried correctly"
            )
        return ds


class L1BGravityWaveLimbViews(L1BStandardInTrack):
    def preprocess(self, df, average_sequences=True):
        pipe = L1BDataPipeline()
        df = pipe.add_datetime_column(df)
        df = pipe.select_limb_views(
            df,
            min_alt=self.limb_scene_alt_range[0],
            max_alt=self.limb_scene_alt_range[1],
        )
        df = pipe.add_first_limb_cols(
            df, min_sec_between=self.first_limb_col_sec_between
        )
        df = pipe.add_limb_view_label(df)
        df = pipe.group_consecutive_rows_as_sequence(df)
        df = df.dropna(subset=["sequence_label"])
        df = pipe.select_limb_angle_range(
            df, min_ang=self.limb_angle_range[0], max_ang=self.limb_angle_range[1]
        )
        df = pipe.select_Gqual(df, flag_values=self.gqual)
        if len(self.rolling) > 0:
            df = pipe.select_Rolling(df, flag_values=self.rolling)
        if len(self.moving) > 0:
            df = pipe.select_Moving(df, flag_values=self.moving)
        df = pipe.add_direction_column(df)
        df = pipe.select_direction(df, "in")
        df = pipe.add_LTST_column(df)
        drop_seqs = []
        for seq_label, seq_group in df.groupby("sequence_label"):
            if seq_group.shape[0] < 5:
                drop_seqs.append(seq_label)
        df = df[~df["sequence_label"].isin(drop_seqs)]
        if average_sequences:
            # Average
            df = pipe.average_limb_sequences(df, cols=None)
            df = df.reset_index()
        df = df.drop(columns="sequence_label")
        return df
