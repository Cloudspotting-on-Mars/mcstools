import numpy as np
import pandas as pd
import pytest

from mcstools.preprocess.bin import Bin, BinGrid, compute_bin_stats_2d


def test_bin_midpoint():
    assert Bin(0, 10).midpoint == 5


def test_bin_equality():
    assert Bin(0, 10) == Bin(0, 10)
    assert Bin(0, 10) != Bin(0, 11)


def test_bingrid_edges_and_bins():
    grid = BinGrid(0, 10, 5, "x")
    np.testing.assert_array_equal(grid.edges, [0, 5, 10])
    assert grid.bins == [Bin(0, 5), Bin(5, 10)]
    np.testing.assert_array_equal(grid.midpoints, [2.5, 7.5])


def test_bingrid_len_and_getitem():
    grid = BinGrid(0, 10, 5, "x")
    assert len(grid) == 2
    assert grid[0] == Bin(0, 5)
    assert grid[1] == Bin(5, 10)


@pytest.mark.parametrize(
    "value,expected",
    [
        (0, Bin(0, 5)),
        (4.9, Bin(0, 5)),
        (5, Bin(5, 10)),
        (9.9, Bin(5, 10)),
        (-1, None),
        (10, None),
    ],
)
def test_bingrid_find_bin_from_value(value, expected):
    grid = BinGrid(0, 10, 5, "x")
    assert grid.find_bin_from_value(value) == expected


@pytest.fixture
def grid_pair():
    return BinGrid(0, 10, 5, "lat"), BinGrid(0, 10, 5, "lon")


def test_compute_bin_stats_2d_mean_and_count(grid_pair):
    lat_bins, lon_bins = grid_pair
    df = pd.DataFrame(
        {
            "lat": [1, 2, 6, 7],
            "lon": [1, 2, 6, 7],
            "val": [10, 20, 100, 300],
        }
    )
    ds = compute_bin_stats_2d(
        df, "val", lat_bins, lon_bins, stats=["mean", "count"], include_nan_count=False
    )
    assert ds["val_mean"].sel(lat=2.5, lon=2.5).item() == 15
    assert ds["val_count"].sel(lat=2.5, lon=2.5).item() == 2
    assert ds["val_mean"].sel(lat=7.5, lon=7.5).item() == 200
    assert ds["val_count"].sel(lat=7.5, lon=7.5).item() == 2


def test_compute_bin_stats_2d_custom_callable(grid_pair):
    lat_bins, lon_bins = grid_pair
    df = pd.DataFrame(
        {
            "lat": [1, 2],
            "lon": [1, 2],
            "val": [10, 20],
        }
    )

    def my_range(x):
        return x.max() - x.min()

    ds = compute_bin_stats_2d(
        df, "val", lat_bins, lon_bins, stats=[my_range], include_nan_count=False
    )
    assert ds["val_my_range"].sel(lat=2.5, lon=2.5).item() == 10


def test_compute_bin_stats_2d_nan_count(grid_pair):
    lat_bins, lon_bins = grid_pair
    df = pd.DataFrame(
        {
            "lat": [1, 2, 3],
            "lon": [1, 2, 3],
            "val": [10, np.nan, 30],
        }
    )
    ds = compute_bin_stats_2d(
        df, "val", lat_bins, lon_bins, stats=["mean"], include_nan_count=True
    )
    # the null row must not pollute the mean for its bin
    assert ds["val_mean"].sel(lat=2.5, lon=2.5).item() == 20
    assert ds["val_nan_count"].sel(lat=2.5, lon=2.5).item() == 1
    assert ds["val_nan_count"].sel(lat=7.5, lon=7.5).item() == 0


def test_compute_bin_stats_2d_omits_nan_count_when_no_nulls(grid_pair):
    lat_bins, lon_bins = grid_pair
    df = pd.DataFrame(
        {
            "lat": [1, 2],
            "lon": [1, 2],
            "val": [10, 20],
        }
    )
    ds = compute_bin_stats_2d(
        df, "val", lat_bins, lon_bins, stats=["mean"], include_nan_count=True
    )
    assert "val_nan_count" not in ds
