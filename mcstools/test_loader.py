import pandas as pd

from mcstools import L1BLoader


def test_load_with_no_files_returns_empty_df_with_expected_columns():
    loader = L1BLoader()
    df = loader.load([], add_cols=["dt"])
    assert df.empty
    assert "dt" in df.columns
    assert set(loader.reader.columns).issubset(df.columns)


def test_load_with_only_missing_files_returns_empty_df_instead_of_raising():
    loader = L1BLoader()
    df = loader.load(["/nonexistent/one.TAB", "/nonexistent/two.TAB"], add_cols=["dt"])
    assert isinstance(df, pd.DataFrame)
    assert df.empty
