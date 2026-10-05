import pandas as pd

from mcstools.preprocess.bin import BinGrid
from mcstools.preprocess.l1.bin_and_agg import load_and_aggregate_single_ls_bin, main

RAD_COLUMNS = ["Rad_A1_01", "Rad_A2_02"]


class FakeL1BLoader:
    """Duck-typed stand-in for L1BLoader backed by a fixed, in-memory L1B DataFrame."""

    def __init__(self, l1b_df):
        self.l1b_df = l1b_df

    def load_ls_range(self, start, end, add_cols=None, verbose=False):
        return self.l1b_df.copy()


class IdentityViewPipeline:
    """Stand-in for L1BStandardInTrack - the synthetic data is already 'preprocessed'."""

    def preprocess(self, df):
        return df


def make_l1b_df(rows):
    """rows: list of dicts with keys lat, lon, ltst, rad1, rad2."""
    return pd.DataFrame({
        "Scene_lat": [r["lat"] for r in rows],
        "Scene_lon": [r["lon"] for r in rows],
        "LTST": [r["ltst"] for r in rows],
        "Rad_A1_01": [r["rad1"] for r in rows],
        "Rad_A2_02": [r["rad2"] for r in rows],
    })


DAY_ROWS = [
    {"lat": 5, "lon": 5, "ltst": 12, "rad1": 10, "rad2": 100},
    {"lat": 5, "lon": 5, "ltst": 13, "rad1": 20, "rad2": 200},
]
NIGHT_ROWS = [
    {"lat": -5, "lon": -5, "ltst": 0, "rad1": 30, "rad2": 300},
    {"lat": -5, "lon": -5, "ltst": 23, "rad1": 40, "rad2": 400},
]

LAT_BINS_KWARGS = dict(agg_columns=RAD_COLUMNS, stats=["mean", "count"])


def run(rows, my=30, ls_bin=None, **kwargs):
    loader = FakeL1BLoader(make_l1b_df(rows))
    pipeline = IdentityViewPipeline()
    ls_bin = ls_bin or BinGrid(0, 5, 5, "Ls")
    kwargs = {**LAT_BINS_KWARGS, **kwargs}
    return load_and_aggregate_single_ls_bin(
        loader, pipeline, my, ls_bin[0],
        BinGrid(-90, 90, 10, "Scene_lat"), BinGrid(-180, 180, 10, "Scene_lon"),
        **kwargs,
    )


def test_bins_radiance_by_day_and_night():
    ds = run(DAY_ROWS + NIGHT_ROWS)

    assert set(ds["Day"].values) == {0, 1}
    assert ds["Rad_A1_01_mean"].sel(Day=1, Scene_lat=5.0, Scene_lon=5.0).item() == 15
    assert ds["Rad_A1_01_count"].sel(Day=1, Scene_lat=5.0, Scene_lon=5.0).item() == 2
    assert ds["Rad_A2_02_mean"].sel(Day=0, Scene_lat=-5.0, Scene_lon=-5.0).item() == 350


def test_day_only_data_has_single_day_value():
    ds = run(DAY_ROWS)
    assert list(ds["Day"].values) == [1]


def test_returns_none_when_load_is_empty():
    ds = run([])
    assert ds is None


def test_main_includes_every_mars_year():
    loader = FakeL1BLoader(make_l1b_df(DAY_ROWS + NIGHT_ROWS))
    pipeline = IdentityViewPipeline()
    bin_config = {
        "Ls": BinGrid(0, 5, 5, "Ls"),
        "Scene_lat": BinGrid(-90, 90, 10, "Scene_lat"),
        "Scene_lon": BinGrid(-180, 180, 10, "Scene_lon"),
    }

    ds = main(
        loader=loader,
        view_pipeline=pipeline,
        my_list=[29, 30],
        bin_config=bin_config,
        agg_columns=RAD_COLUMNS,
        stats=["mean", "count"],
        n_jobs=1,
    )

    assert set(ds["MY"].values) == {29, 30}
