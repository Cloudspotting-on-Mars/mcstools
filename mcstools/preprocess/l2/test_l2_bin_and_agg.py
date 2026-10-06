import pandas as pd

from mcstools.preprocess.bin import BinGrid
from mcstools.preprocess.l2.bin_and_agg import (
    BIN_CONFIG_DEFAULT,
    FILTER_CONFIG_DEFAULT,
    load_and_aggregate_single_ls_chunk,
    main,
)


class FakeL2Loader:
    """Duck-typed stand-in for L2Loader backed by fixed, in-memory DDR1/DDR2 frames."""

    def __init__(self, ddr1_df, ddr2_df):
        self.ddr1_df = ddr1_df
        self.ddr2_df = ddr2_df

    def load_ls_range(self, start, end, ddr="DDR1", add_cols=None, verbose=False):
        assert ddr == "DDR1"
        return self.ddr1_df.copy()

    def load(self, ddr, profiles=None, verbose=False):
        assert ddr == "DDR2"
        profiles = list(profiles)
        return self.ddr2_df[self.ddr2_df["Profile_identifier"].isin(profiles)].copy()

    def merge_ddrs(self, ddr2_df, ddr1_df, verbose=False):
        return pd.merge(
            ddr2_df,
            ddr1_df,
            on="Profile_identifier",
            how="outer",
            suffixes=("", "_DDR1"),
        )


def make_ddr1_df(profiles):
    """profiles: list of dicts with keys Profile_identifier, lat, lon, ltst, dust,
    t_surf, dt."""
    return pd.DataFrame(
        {
            "Profile_identifier": [p["Profile_identifier"] for p in profiles],
            "Surf_lat": [p["lat"] for p in profiles],
            "Surf_lon": [p["lon"] for p in profiles],
            "Profile_lat": [p["lat"] for p in profiles],
            "Profile_lon": [p["lon"] for p in profiles],
            "LTST": [p["ltst"] for p in profiles],
            "Dust_column": [p["dust"] for p in profiles],
            "T_surf": [p["t_surf"] for p in profiles],
            "Obs_qual": [0] * len(profiles),
            "Gqual": [0] * len(profiles),
            "1": [0] * len(profiles),
            "dt": pd.to_datetime([p["dt"] for p in profiles], utc=True),
        }
    )


def make_ddr2_df(profiles):
    """profiles: list of dicts with keys Profile_identifier, levels: list of
    (pres, dust, t, alt)."""
    rows = []
    for p in profiles:
        for level, (pres, dust, t, alt) in enumerate(p["levels"]):
            rows.append(
                {
                    "Profile_identifier": p["Profile_identifier"],
                    "level": level,
                    "Pres": pres,
                    "Dust": dust,
                    "T": t,
                    "Alt": alt,
                }
            )
    return pd.DataFrame(rows)


DAY_PROFILES = [
    {
        "Profile_identifier": "D1",
        "lat": 5,
        "lon": 5,
        "ltst": 0.5,
        "dust": 10,
        "t_surf": 200,
        "dt": "2020-01-01 12:00:00",
        "levels": [(100, 1, 10, 5), (200, 2, 20, 15)],
    },
    {
        "Profile_identifier": "D2",
        "lat": 5,
        "lon": 5,
        "ltst": 0.55,
        "dust": 20,
        "t_surf": 220,
        "dt": "2020-01-01 13:00:00",
        "levels": [(100, 3, 30, 25), (200, 4, 40, 35)],
    },
]
NIGHT_PROFILES = [
    {
        "Profile_identifier": "N1",
        "lat": -5,
        "lon": -5,
        "ltst": 0.0,
        "dust": 100,
        "t_surf": 150,
        "dt": "2020-01-01 00:00:00",
        "levels": [(100, 5, 50, 45), (200, 6, 60, 55)],
    },
    {
        "Profile_identifier": "N2",
        "lat": -5,
        "lon": -5,
        "ltst": 0.95,
        "dust": 300,
        "t_surf": 250,
        "dt": "2020-01-01 23:00:00",
        "levels": [(100, 7, 70, 65), (200, 8, 80, 75)],
    },
]


def make_loader(profiles):
    return FakeL2Loader(make_ddr1_df(profiles), make_ddr2_df(profiles))


COMMON_KWARGS = dict(
    filter_config=FILTER_CONFIG_DEFAULT,
    ddr1_agg_columns=["Dust_column", "T_surf"],
    ddr1_lat_bin=BIN_CONFIG_DEFAULT["Surf_lat"],
    ddr1_lon_bin=BIN_CONFIG_DEFAULT["Surf_lon"],
    ddr2_agg_columns=["Dust", "T", "Alt"],
    ddr2_lat_bin=BIN_CONFIG_DEFAULT["Profile_lat"],
    ddr2_lon_bin=BIN_CONFIG_DEFAULT["Profile_lon"],
)


def test_bins_ddr1_and_ddr2_by_day_and_night():
    loader = make_loader(DAY_PROFILES + NIGHT_PROFILES)
    ls_bin = BinGrid(0, 15, 15, "Ls")

    ds = load_and_aggregate_single_ls_chunk(loader, 30, ls_bin, 0, **COMMON_KWARGS)

    assert set(ds["Day"].values) == {0, 1}
    assert list(ds["level"].values) == [0, 1]
    assert list(ds["Pres"].values) == [100, 200]

    assert ds["Dust_column_mean"].sel(Day=1, Surf_lat=7.5, Surf_lon=7.5).item() == 15
    assert ds["Dust_column_count"].sel(Day=1, Surf_lat=7.5, Surf_lon=7.5).item() == 2
    assert ds["Dust_column_mean"].sel(Day=0, Surf_lat=-2.5, Surf_lon=-2.5).item() == 200

    assert (
        ds["Dust_mean"].sel(Day=1, level=0, Profile_lat=7.5, Profile_lon=7.5).item()
        == 2
    )
    assert (
        ds["Dust_mean"].sel(Day=1, level=1, Profile_lat=7.5, Profile_lon=7.5).item()
        == 3
    )
    assert (
        ds["Dust_mean"].sel(Day=0, level=0, Profile_lat=-2.5, Profile_lon=-2.5).item()
        == 6
    )
    assert (
        ds["Dust_mean"].sel(Day=0, level=1, Profile_lat=-2.5, Profile_lon=-2.5).item()
        == 7
    )

    assert int(ds["MY"].item()) == 30
    assert ds["Ls"].item() == ls_bin[0].midpoint


def test_day_only_data_has_single_day_value():
    loader = make_loader(DAY_PROFILES)
    ls_bin = BinGrid(0, 15, 15, "Ls")

    ds = load_and_aggregate_single_ls_chunk(loader, 30, ls_bin, 0, **COMMON_KWARGS)

    assert list(ds["Day"].values) == [1]


def test_excludes_profiles_near_excluded_times():
    loader = make_loader(DAY_PROFILES)
    ls_bin = BinGrid(0, 15, 15, "Ls")
    excluded_times = pd.to_datetime(["2020-01-01 13:00:00"], utc=True)
    kwargs = dict(COMMON_KWARGS)
    kwargs["excluded_times"] = excluded_times
    kwargs["exclude_threshold_s"] = 60

    ds = load_and_aggregate_single_ls_chunk(loader, 30, ls_bin, 0, **kwargs)

    # D2 (dt=13:00:00) is excluded; only D1 (dust=10) remains
    assert ds["Dust_column_mean"].sel(Day=1, Surf_lat=7.5, Surf_lon=7.5).item() == 10
    assert ds["Dust_column_count"].sel(Day=1, Surf_lat=7.5, Surf_lon=7.5).item() == 1


def test_excluded_times_none_behaves_as_before():
    loader = make_loader(DAY_PROFILES)
    ls_bin = BinGrid(0, 15, 15, "Ls")
    kwargs = dict(COMMON_KWARGS)
    kwargs["excluded_times"] = None

    ds = load_and_aggregate_single_ls_chunk(loader, 30, ls_bin, 0, **kwargs)

    assert ds["Dust_column_mean"].sel(Day=1, Surf_lat=7.5, Surf_lon=7.5).item() == 15


def test_returns_none_when_filter_empties_chunk():
    loader = make_loader(DAY_PROFILES)
    ls_bin = BinGrid(0, 15, 15, "Ls")
    kwargs = dict(COMMON_KWARGS)
    kwargs["filter_config"] = {"Obs_qual": [12345]}

    ds = load_and_aggregate_single_ls_chunk(loader, 30, ls_bin, 0, **kwargs)

    assert ds is None


def test_main_concats_over_ls_and_my():
    loader = make_loader(DAY_PROFILES + NIGHT_PROFILES)
    bin_config = dict(BIN_CONFIG_DEFAULT)
    bin_config["Ls"] = BinGrid(0, 30, 15, "Ls")

    ds = main(
        loader=loader,
        my_list=[30],
        bin_config=bin_config,
        filter_config=FILTER_CONFIG_DEFAULT,
        ddr1_agg_columns=["Dust_column", "T_surf"],
        ddr2_agg_columns=["Dust", "T", "Alt"],
        n_jobs=1,
    )

    assert list(ds["MY"].values) == [30]
    assert len(ds["Ls"]) == 2
    assert set(ds["Day"].values) == {0, 1}
