import pandas as pd

from mcstools.preprocess.l2.filter_and_bin import filter_ddr1_df_from_config


def make_df():
    return pd.DataFrame({
        "LTST": [0.1, 0.4, 0.6, 0.9],
        "Obs_qual": [0, 0, 1, 99],
    })


def test_tuple_range_filter():
    df = filter_ddr1_df_from_config(make_df(), {"LTST": (0.3, 0.7)})
    assert list(df["LTST"]) == [0.4, 0.6]


def test_tuple_wraparound_filter():
    # vals[1] < vals[0] means "outside" the range, e.g. nighttime wrapping through midnight.
    df = filter_ddr1_df_from_config(make_df(), {"LTST": (0.7, 0.3)})
    assert list(df["LTST"]) == [0.1, 0.9]


def test_list_isin_filter():
    df = filter_ddr1_df_from_config(make_df(), {"Obs_qual": [0, 1]})
    assert list(df["Obs_qual"]) == [0, 0, 1]


def test_breaks_early_once_empty():
    df = filter_ddr1_df_from_config(make_df(), {"Obs_qual": [12345], "LTST": (0, 1)})
    assert df.empty


def test_combined_filters():
    df = filter_ddr1_df_from_config(make_df(), {"LTST": (0.3, 0.7), "Obs_qual": [0]})
    assert list(df["LTST"]) == [0.4]
