import pandas as pd
import pytest

from mcstools.preprocess.exclude import filter_excluded_times, load_excluded_times


def test_load_excluded_times_parses_and_sorts(tmp_path):
    path = tmp_path / "excluded.csv"
    path.write_text("timestamp\n2020-01-02 00:00:00\n2020-01-01 00:00:00\n")

    times = load_excluded_times(path)

    assert list(times) == sorted(times)
    assert times.iloc[0] == pd.Timestamp("2020-01-01", tz="UTC")
    assert times.iloc[1] == pd.Timestamp("2020-01-02", tz="UTC")


def test_load_excluded_times_custom_column(tmp_path):
    path = tmp_path / "excluded.csv"
    path.write_text("bad_time\n2020-01-01 00:00:00\n")

    times = load_excluded_times(path, excluded_dt_col="bad_time")

    assert len(times) == 1


def test_load_excluded_times_requires_column(tmp_path):
    path = tmp_path / "excluded.csv"
    path.write_text("not_timestamp\n2020-01-01 00:00:00\n")

    with pytest.raises(ValueError):
        load_excluded_times(path)


def make_df(dts):
    return pd.DataFrame({"dt": pd.to_datetime(dts, utc=True), "val": range(len(dts))})


def test_filter_excluded_times_drops_rows_within_threshold():
    df = make_df(["2020-01-01 00:00:00", "2020-01-01 00:00:05", "2020-01-01 01:00:00"])
    excluded_times = pd.to_datetime(["2020-01-01 00:00:03"], utc=True)

    result = filter_excluded_times(df, excluded_times, threshold_s=10)

    assert list(result["val"]) == [2]


def test_filter_excluded_times_boundary_at_exactly_threshold():
    df = make_df(["2020-01-01 00:00:10"])
    excluded_times = pd.to_datetime(["2020-01-01 00:00:00"], utc=True)

    kept = filter_excluded_times(df, excluded_times, threshold_s=10)
    assert kept.empty

    kept = filter_excluded_times(df, excluded_times, threshold_s=9)
    assert len(kept) == 1


def test_filter_excluded_times_noop_when_no_excluded_times():
    df = make_df(["2020-01-01 00:00:00"])
    assert filter_excluded_times(df, None, threshold_s=10) is df
    empty_times = pd.to_datetime([], utc=True)
    assert filter_excluded_times(df, empty_times, threshold_s=10) is df


def test_filter_excluded_times_noop_on_empty_df():
    df = make_df([])
    excluded_times = pd.to_datetime(["2020-01-01 00:00:00"], utc=True)
    assert filter_excluded_times(df, excluded_times, threshold_s=10) is df
