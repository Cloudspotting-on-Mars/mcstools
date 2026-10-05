import pandas as pd
from mars_time import MarsTime

from mcstools import L1BLoader


def test_load_with_no_files_returns_empty_df_with_expected_columns():
    loader = L1BLoader()
    df = loader.load([], add_cols=["dt"])
    assert df.empty
    assert "dt" in df.columns
    # output_columns, not just columns - read() always adds Solar_dist/L_sub_s from the
    # file header, and load_ls_range filters on L_sub_s even when it's not an add_col.
    assert set(loader.reader.output_columns).issubset(df.columns)


def test_load_with_only_missing_files_returns_empty_df_instead_of_raising():
    loader = L1BLoader()
    df = loader.load(["/nonexistent/one.TAB", "/nonexistent/two.TAB"], add_cols=["dt"])
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_load_ls_range_with_no_data_returns_empty_df_instead_of_raising():
    loader = L1BLoader()
    df = loader.load_ls_range(
        MarsTime.from_solar_longitude(1, 0),
        MarsTime.from_solar_longitude(1, 1),
    )
    assert isinstance(df, pd.DataFrame)
    assert df.empty
