import os

import pandas as pd
from dotenv import load_dotenv


class SpectralResponse:
    def __init__(self, data_dir: str = None):
        if data_dir is None:
            load_dotenv()
            self.data_dir = os.path.environ["SPECTRAL_RESPONE_DIR"]

        self.data_dir = data_dir
        self.make_filepaths()
        self.spectral_response = self.read_all_spectral_response()

    def make_filepaths(self):
        base_name = "spectral_response_data"
        self.filepaths = {
            channel: os.path.join(self.data_dir, f"{base_name}_{channel}.txt")
            for channel in ["A1-A5", "A6", "B1", "B2", "B3"]
        }

    def read_a1_a5_file(self):
        df = pd.read_fwf(
            self.filepaths["A1-A5"],
            names=[
                "A1_wl",
                "A1_R",
                "A2_wl",
                "A2_R",
                "A3_wl",
                "A3_R",
                "A4_wl",
                "A4_R",
                "A5_wl",
                "A5_R",
            ],
        )
        return df

    def read_a6(self):
        df = pd.read_fwf(
            self.filepaths["A6"],
            names=[
                "A6_wl",
                "A6_R",
            ],
        )
        return df

    def read_b(self, bchannel):
        df = pd.read_fwf(self.filepaths[bchannel])
        return df

    def read_all_spectral_response(self):
        achannels = self.read_a1_a5_file()
        responses = {
            f"{c}": achannels[[col for col in achannels.columns if c in col]]
            .dropna()
            .rename(columns={f"{c}_wl": "wl", f"{c}_R": "R"})
            for c in ["A1", "A2", "A3", "A4", "A5"]
        }
        a6 = self.read_a6()
        responses["A6"] = a6
        for b_channel in ["B1", "B2", "B3"]:
            responses[b_channel] = self.read_b(b_channel)
        return responses
