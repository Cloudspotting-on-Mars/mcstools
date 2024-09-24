import pandas as pd

from mcstools.detector_positions import DetectorPositions
from mcstools.mcsfile import L1BFile

DETECTOR_POSITIONS = DetectorPositions()


class RadianceProfile:
    def __init__(
        self, channel: str, radiance_detector_profile: pd.Series, altitudes=None
    ):
        self.channel = channel
        self.profile = radiance_detector_profile
        self.altitudes = altitudes

    @classmethod
    def from_l1b_row(self, channel, row, include_altitudes=True):
        l1b_file = L1BFile()
        extract_columns = l1b_file.make_rad_col_names(channel)
        detectors = [int(x.split("_")[-1]) for x in extract_columns]
        radiances = row[extract_columns].values
        profile = pd.Series(
            radiances, index=pd.Index(detectors, name="Detector"), name="Radiance"
        )
        if channel[0] == "B":
            profile = profile.sort_index(ascending=False)
        if not include_altitudes:
            altitudes = None
        else:
            altitudes = DETECTOR_POSITIONS.convert_fov_to_altitude(
                row["SC_rad"],
                row["Scene_rad"],
                row["Scene_alt"],
                DETECTOR_POSITIONS.elevation.loc[profile.index, channel],
            )
        return RadianceProfile(channel, profile, altitudes=altitudes)

    def __str__(self):
        return f"Channel {self.channel}. Profile:\n{self.profile}"
