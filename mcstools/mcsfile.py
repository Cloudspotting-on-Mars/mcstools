import numpy as np


class MCSFile:
    """
    Base class for MCS file metadata
    """

    ndetectors = 21  # number of detectors
    detector_range = np.arange(1, ndetectors + 1, 1)
    # Detector numbers in increasing altitude
    detectors = {"A": np.flipud(detector_range), "B": detector_range}
    # Channel names
    channels = [f"A{x}" for x in range(1, 7)] + [f"B{x}" for x in range(1, 4)]

    def __init__(self):
        pass


class L1BLikeFile(MCSFile):
    """
    Class for MCS L1B-like file metadata and methods to access
    metadata (data columns, header comments, channels, detectors).
    """

    file_suffix = None
    # column names L1B file
    ndetectors = 21  # number of detectors
    detector_range = np.arange(1, ndetectors + 1, 1)
    # Detector numbers in increasing altitude
    detectors = {"A": np.flipud(detector_range), "B": detector_range}
    # Channel names
    channels = [f"A{x}" for x in range(1, 7)] + [f"B{x}" for x in range(1, 4)]
    comment_line_character = "#"
    nan_values = [-9999, "", "-9999"]  # NAN values in data

    def make_rad_col_name(self, channel: str, detector: int) -> str:
        return f"Rad_{channel}_{str(int(detector)).zfill(2)}"

    def make_rad_col_names(self, channel: str) -> list:
        """
        Return column names of each detector for given radiance channel

        Parameters
        ----------
        channel (str): Channel name (ex, "A6")

        Returns
        -------
        (list): list of all channel-detector columns (ex, "Rad_A6_11")
        """
        return [self.make_rad_col_name(channel, d) for d in self.detector_range]


class L1BFile(L1BLikeFile):
    """
    Class for MCS L1B metadata and methods to access
    metadata (data columns, header comments, channels, detectors).
    """

    file_suffix = "L1B"

    # column names L1B file
    columns = [
        "1",
        "Date",
        "UTC",
        "SCLK",
        "PKT_count",
        "Last_az_cmd",
        "Last_el_cmd",
        "Gqual",
        "Solar_lat",
        "Solar_lon",
        "Solar_zen",
        "SC_lat",
        "SC_lon",
        "SC_rad",
        "Scene_lat",
        "Scene_lon",
        "Scene_rad",
        "Scene_alt",
        "Vert_lat",
        "Vert_lon",
        "Limb_ang",
        "Safing",
        "Safed",
        "Freezing",
        "Frozen",
        "Rolling",
        "Dumping",
        "Moving",
        "Temp_Fault",
        "Mode",
        "OST_index",
        "EST_index",
        "ROT_index",
        "EOCT_index",
        "SST_index",
        "FPA_temp",
        "FPB_temp",
        "Baffle_A_temp",
        "Baffle_B_temp",
        "BB_1_temp",
        "OBA_1_temp",
        "Error_Time",
        "Error_ID",
        "Error_Detail",
        "Error_count",
        "Commands_received",
        "Commands_executed",
        "Commands_rejected",
        "Last_command_rec",
        "Cmd",
        "Req_ID",
        "Last_time_command",
        "Last_EQX_prediction",
        "Hybrid_temp",
        "FPA_temp_cyc",
        "FPB_temp_cyc",
        "Baffle_A_temp_cyc",
        "Baffle_B_temp_cyc",
        "OBA_1_temp_cyc",
        "OBA_2_temp",
        "BB_1_temp_cyc",
        "BB_2_temp",
        "Solar_target_temp",
        "Yoke_temp",
        "El_actuator_temp",
        "Az_actuator_temp",
        "-15V",
        "+15V",
        "Solar_base_temp",
        "+5V",
        "Rqual",
        "Rad_A1_01",
        "Rad_A1_02",
        "Rad_A1_03",
        "Rad_A1_04",
        "Rad_A1_05",
        "Rad_A1_06",
        "Rad_A1_07",
        "Rad_A1_08",
        "Rad_A1_09",
        "Rad_A1_10",
        "Rad_A1_11",
        "Rad_A1_12",
        "Rad_A1_13",
        "Rad_A1_14",
        "Rad_A1_15",
        "Rad_A1_16",
        "Rad_A1_17",
        "Rad_A1_18",
        "Rad_A1_19",
        "Rad_A1_20",
        "Rad_A1_21",
        "Rad_A2_01",
        "Rad_A2_02",
        "Rad_A2_03",
        "Rad_A2_04",
        "Rad_A2_05",
        "Rad_A2_06",
        "Rad_A2_07",
        "Rad_A2_08",
        "Rad_A2_09",
        "Rad_A2_10",
        "Rad_A2_11",
        "Rad_A2_12",
        "Rad_A2_13",
        "Rad_A2_14",
        "Rad_A2_15",
        "Rad_A2_16",
        "Rad_A2_17",
        "Rad_A2_18",
        "Rad_A2_19",
        "Rad_A2_20",
        "Rad_A2_21",
        "Rad_A3_01",
        "Rad_A3_02",
        "Rad_A3_03",
        "Rad_A3_04",
        "Rad_A3_05",
        "Rad_A3_06",
        "Rad_A3_07",
        "Rad_A3_08",
        "Rad_A3_09",
        "Rad_A3_10",
        "Rad_A3_11",
        "Rad_A3_12",
        "Rad_A3_13",
        "Rad_A3_14",
        "Rad_A3_15",
        "Rad_A3_16",
        "Rad_A3_17",
        "Rad_A3_18",
        "Rad_A3_19",
        "Rad_A3_20",
        "Rad_A3_21",
        "Rad_A4_01",
        "Rad_A4_02",
        "Rad_A4_03",
        "Rad_A4_04",
        "Rad_A4_05",
        "Rad_A4_06",
        "Rad_A4_07",
        "Rad_A4_08",
        "Rad_A4_09",
        "Rad_A4_10",
        "Rad_A4_11",
        "Rad_A4_12",
        "Rad_A4_13",
        "Rad_A4_14",
        "Rad_A4_15",
        "Rad_A4_16",
        "Rad_A4_17",
        "Rad_A4_18",
        "Rad_A4_19",
        "Rad_A4_20",
        "Rad_A4_21",
        "Rad_A5_01",
        "Rad_A5_02",
        "Rad_A5_03",
        "Rad_A5_04",
        "Rad_A5_05",
        "Rad_A5_06",
        "Rad_A5_07",
        "Rad_A5_08",
        "Rad_A5_09",
        "Rad_A5_10",
        "Rad_A5_11",
        "Rad_A5_12",
        "Rad_A5_13",
        "Rad_A5_14",
        "Rad_A5_15",
        "Rad_A5_16",
        "Rad_A5_17",
        "Rad_A5_18",
        "Rad_A5_19",
        "Rad_A5_20",
        "Rad_A5_21",
        "Rad_A6_01",
        "Rad_A6_02",
        "Rad_A6_03",
        "Rad_A6_04",
        "Rad_A6_05",
        "Rad_A6_06",
        "Rad_A6_07",
        "Rad_A6_08",
        "Rad_A6_09",
        "Rad_A6_10",
        "Rad_A6_11",
        "Rad_A6_12",
        "Rad_A6_13",
        "Rad_A6_14",
        "Rad_A6_15",
        "Rad_A6_16",
        "Rad_A6_17",
        "Rad_A6_18",
        "Rad_A6_19",
        "Rad_A6_20",
        "Rad_A6_21",
        "Rad_B1_01",
        "Rad_B1_02",
        "Rad_B1_03",
        "Rad_B1_04",
        "Rad_B1_05",
        "Rad_B1_06",
        "Rad_B1_07",
        "Rad_B1_08",
        "Rad_B1_09",
        "Rad_B1_10",
        "Rad_B1_11",
        "Rad_B1_12",
        "Rad_B1_13",
        "Rad_B1_14",
        "Rad_B1_15",
        "Rad_B1_16",
        "Rad_B1_17",
        "Rad_B1_18",
        "Rad_B1_19",
        "Rad_B1_20",
        "Rad_B1_21",
        "Rad_B2_01",
        "Rad_B2_02",
        "Rad_B2_03",
        "Rad_B2_04",
        "Rad_B2_05",
        "Rad_B2_06",
        "Rad_B2_07",
        "Rad_B2_08",
        "Rad_B2_09",
        "Rad_B2_10",
        "Rad_B2_11",
        "Rad_B2_12",
        "Rad_B2_13",
        "Rad_B2_14",
        "Rad_B2_15",
        "Rad_B2_16",
        "Rad_B2_17",
        "Rad_B2_18",
        "Rad_B2_19",
        "Rad_B2_20",
        "Rad_B2_21",
        "Rad_B3_01",
        "Rad_B3_02",
        "Rad_B3_03",
        "Rad_B3_04",
        "Rad_B3_05",
        "Rad_B3_06",
        "Rad_B3_07",
        "Rad_B3_08",
        "Rad_B3_09",
        "Rad_B3_10",
        "Rad_B3_11",
        "Rad_B3_12",
        "Rad_B3_13",
        "Rad_B3_14",
        "Rad_B3_15",
        "Rad_B3_16",
        "Rad_B3_17",
        "Rad_B3_18",
        "Rad_B3_19",
        "Rad_B3_20",
        "Rad_B3_21",
    ]
    radcols = [x for x in columns if "Rad_" in x]  # subset of Radiance columns
    dtypes = {x: float for x in radcols}

    def __init__(self):
        super().__init__()


class L2File(MCSFile):
    """
    Class with MCS L2_2d file information and methods
    to read in files and store data.
    """

    file_suffix = "L2_2d"
    nan_values = [-9999, "", "-9999"]  # values to treat as NaNs
    comments = []  # initialize header comments
    data_records = {
        "DDR1": {
            "lines": 1,
            "columns": [
                "1",
                "Date",
                "UTC",
                "SCLK",
                "L_s",
                "Solar_dist",
                "Orb_num",
                "Gqual",
                "Solar_lat",
                "Solar_lon",
                "Solar_zen",
                "LTST",
                "Profile_lat",
                "Profile_lon",
                "Profile_rad",
                "Profile_alt",
                "Limb_ang",
                "Are_rad",
                "Surf_lat",
                "Surf_lon",
                "Surf_rad",
                "T_surf",
                "T_surf_err",
                "T_near_surf",
                "T_near_surf_err",
                "Dust_column",
                "Dust_column_err",
                "H2Ovap_column",
                "H2Ovap_column_err",
                "H2Oice_column",
                "H2Oice_column_err",
                "CO2ice_column",
                "CO2ice_column_err",
                "p_surf",
                "p_surf_err",
                "p_ret_alt",
                "p_ret",
                "p_ret_err",
                "Rqual",
                "P_qual",
                "T_qual",
                "Dust_qual",
                "H2Ovap_qual",
                "H2Oice_qual",
                "CO2ice_qual",
                "surf_qual",
                "Obs_qual",
                "Ref_SCLK_0",
                "Ref_SCLK_1",
                "Ref_SCLK_2",
                "Ref_SCLK_3",
                "Ref_SCLK_4",
                "Ref_SCLK_5",
                "Ref_SCLK_6",
                "Ref_SCLK_7",
                "Ref_SCLK_8",
                "Ref_SCLK_9",
                "Ref_Date_0",
                "Ref_UTC_0",
                "Ref_Date_1",
                "Ref_UTC_1",
                "Ref_Date_2",
                "Ref_UTC_2",
                "Ref_Date_3",
                "Ref_UTC_3",
                "Ref_Date_4",
                "Ref_UTC_4",
                "Ref_Date_5",
                "Ref_UTC_5",
                "Ref_Date_6",
                "Ref_UTC_6",
                "Ref_Date_7",
                "Ref_UTC_7",
                "Ref_Date_8",
                "Ref_UTC_8",
                "Ref_Date_9",
                "Ref_UTC_9",
            ],
        },
        "DDR2": {
            "lines": 105,
            "columns": [
                "1",
                "Pres",
                "T",
                "T_err",
                "Dust",
                "Dust_err",
                "H2Ovap",
                "H2Ovap_err",
                "H2Oice",
                "H2Oice_err",
                "CO2ice",
                "CO2ice_err",
                "Alt",
                "Lat",
                "Lon",
            ],
        },
        "DDR3": {
            "lines": 22,
            "columns": [
                "1",
                "Rad_A1",
                "Rad_A1_calc",
                "Rad_A2",
                "Rad_A2_calc",
                "Rad_A3",
                "Rad_A3_calc",
                "Rad_A4",
                "Rad_A4_calc",
                "Rad_A5",
                "Rad_A5_calc",
                "Rad_A6",
                "Rad_A6_calc",
                "Rad_B1",
                "Rad_B1_calc",
                "Rad_B2",
                "Rad_B2_calc",
                "Rad_B3",
                "Rad_B3_calc",
            ],
        },
        "DDR4": {
            "lines": 102,
            "columns": [
                "1",
                "T_resid",
                "p_resid",
                "Dust_resid",
                "H2Ovap_resid",
                "H2Oice_resid",
                "CO2ice_resid",
            ],
        },
    }
    # set dtypes for making DFs
    all_DDR_names = [
        item
        for sublist in [d["columns"] for d in data_records.values()]
        for item in sublist
    ]
    # DDR1 dtypes
    ddr1_dtypes_int = {
        col: int for col in ["1", "Orb_num"] + [x for x in all_DDR_names if "qual" in x]
    }
    ddr1_float_cols = [
        "L_s",
        "Solar_dist",
        "Solar_lat",
        "Solar_lon",
        "Solar_zen",
        "LTST",
        "Profile_lat",
        "Profile_lon",
        "Profile_rad",
        "Profile_alt",
        "Limb_ang",
        "Surf_lat",
        "Surf_lon",
        "Surf_rad",
        "T_surf",
        "T_surf_err",
        "T_near_surf",
        "T_near_surf_err",
        "Dust_column",
        "Dust_column_err",
        "H2Ovap_column",
        "H2Ovap_column_err",
        "H2Oice_column",
        "H2Oice_column_err",
        "CO2ice_column",
        "CO2ice_column_err",
        "p_surf",
        "p_surf_err",
        "p_ret_alt",
        "p_ret",
        "p_ret_err",
    ]
    ddr1_dtypes_float = {col: float for col in ddr1_float_cols}
    ddr1_dtypes = {**ddr1_dtypes_int, **ddr1_dtypes_float}
    data_records["DDR1"]["dtypes"] = ddr1_dtypes
    # DDR2 dtypes
    ddr2_dtypes = {col: float for col in data_records["DDR2"]["columns"]}
    ddr2_dtypes["1"] = int
    data_records["DDR2"]["dtypes"] = ddr2_dtypes

    def __init__(self, pds=False):
        super().__init__()
        if pds and "DDR3" in self.data_records and "DDR3" in self.data_records:
            del self.data_records["DDR3"]
            del self.data_records["DDR4"]
