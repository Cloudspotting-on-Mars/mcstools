from preprocess.data_pipeline import L1DataPipeline

# TODO: @classmethod() from_config() option to setup processer


class L1BStandardInTrack:
    def __init__(
        self,
        limb_scene_alt_range=(20, 70),
        first_limb_col_sec_between=5,
        limb_angle_range=(-9, 9),
        gqual=[0, 5, 6],
        rolling=[0],
        moving=[0],
    ):
        self.limb_scene_alt_range = limb_scene_alt_range
        self.first_limb_col_sec_between = first_limb_col_sec_between
        self.limb_angle_range = limb_angle_range
        self.gqual = gqual
        self.rolling = rolling
        self.moving = moving

    def preprocess(self, df):
        """
        From loaded L1b data, preprocess to get standard limb in-track values
        based on range of Scene alts, limb angles, azimuth angles, and quality flags.
        Removes first three measurements of a limb sequence (thermal drift) and
        averages the others.
        """
        pipe = L1DataPipeline()
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
        df = pipe.select_Rolling(df, flag_values=self.rolling)
        df = pipe.select_Moving(df, flag_values=self.moving)
        df = pipe.add_direction_column(df)
        df = pipe.select_direction(df, "in")
        # Average
        df_ave = pipe.average_limb_sequences(
            df, cols=None  # ["dt", "SC_rad", "Scene_alt", "Scene_rad"] + pipe.radcols
        )
        df_ave = df_ave.reset_index().drop(columns="sequence_label")
        return df_ave

    def melt_to_xarray(self, df):
        """
        Convert Dataframe of L1b radiances to xarray with coordinates:
        ["dt", "Detector", "Channel"].
        """
        pipe = L1DataPipeline()
        df = pipe.melt_channel_detector_radiance(df.reset_index())
        df = df.set_index(["dt", "Detector", "Channel"])[["Radiance"]].to_xarray()
        return df
