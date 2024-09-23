import pandas as pd

from mcstools.mcsfile import L1BFile

class RadianceProfile():
    def __init__(self, channel: str, radiance_detector_profile: pd.Series):
        self.channel = channel
        self.profile = radiance_detector_profile

    @classmethod
    def from_l1b_row(self, row, channel):
        l1b_file = L1BFile()
        extract_columns = l1b_file.make_rad_col_names(channel)
        detectors = [int(x.split("_")[-1]) for x in extract_columns]
        radiances = row[extract_columns].values
        return RadianceProfile(channel, pd.Series(radiances, index=detectors))