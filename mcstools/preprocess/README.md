# Binning and aggregation

`mcstools/preprocess/l1/bin_and_agg.py` and `mcstools/preprocess/l2/bin_and_agg.py` load
MCS data in Ls (solar longitude) chunks, filter/preprocess it, split it into day/night,
bin it spatially, and aggregate statistics into a single `xarray.Dataset` indexed by
Mars Year (`MY`), Ls, and `Day` (1=day, 0=night) - ready to write to netCDF.

Both scripts share the same binning foundation (`mcstools/preprocess/bin.py`):
- `BinGrid(start, stop, size, name)` describes an evenly-spaced set of bins over a
  column (`name` is both the DataFrame column binned on and the resulting xarray
  dim/coord name).
- `compute_bin_stats_2d(df, value_col, lat_bins, lon_bins, stats=...)` bins `value_col`
  by two `BinGrid`s and computes the requested statistics (any name/callable accepted by
  `scipy.stats.binned_statistic_2d`, e.g. `"mean"`, `"count"`, or a custom function).

## L1B binning

```bash
python -m mcstools.preprocess.l1.bin_and_agg --output-path out/l1b_binned.nc
```

Pipeline per (Mars Year, Ls chunk): load L1B data for that Ls range → run it through a
view pipeline (default `L1BStandardInTrack`, which selects/averages in-track limb views
and is what actually adds the `LTST` column) → split into day/night by LTST (hours,
`DAY_LTST_RANGE = (9, 21)`) → bin radiance columns (`AGG_COLUMNS`, i.e. all `Rad_*`
columns) spatially by `Scene_lat`/`Scene_lon` → concat over `Day`, then `Ls`, then `MY`.

To customize from Python instead of the CLI, call `main()` directly - e.g. with a
coarser spatial grid and fewer radiance columns:
```python
from mcstools.preprocess.bin import BinGrid
from mcstools.preprocess.l1.bin_and_agg import main

bin_config = {
    "Ls": BinGrid(0, 360, 10, "Ls"),
    "Scene_lat": BinGrid(-90, 90, 10, "Scene_lat"),
    "Scene_lon": BinGrid(-180, 180, 10, "Scene_lon"),
}
ds = main(bin_config=bin_config, agg_columns=["Rad_A1_01", "Rad_A2_01"], n_jobs=8)
```
Other notable `main()` parameters: `my_list` (Mars Years to process), `stats` (list of
stat names/callables per radiance column), `view_pipeline` (any object with a
`.preprocess(df)` method).

## L2 (DDR1/DDR2) binning

```bash
python -m mcstools.preprocess.l2.bin_and_agg --output-path out/l2_binned.nc
```

Pipeline per (Mars Year, Ls chunk): load DDR1 profiles for that Ls range → quality-filter
them (`FILTER_CONFIG_DEFAULT`: `Obs_qual`/`Gqual`/the raw `"1"` record-type flag) → split
into day/night by LTST (a 0-1 fraction-of-sol here, unlike L1B's hours,
`DAY_LTST_RANGE = (9/24, 21/24)`) → for each subset, bin DDR1 columns
(`DDR1_AGG_DEFAULT`) spatially by `Surf_lat`/`Surf_lon`, then load the matching DDR2
profiles, merge DDR1's profile-level metadata onto them, and bin DDR2 columns
(`DDR2_AGG_DEFAULT`) per pressure level by `Profile_lat`/`Profile_lon` → concat over
`Day`, then `Ls`, then `MY`.

(DDR2 has no `Profile_lat`/`Profile_lon` of its own, only per-level `Lat`/`Lon` - merging
in DDR1's single representative profile lat/lon is what keeps every level of a profile
in the same spatial bin instead of drifting across bins as the limb path curves with
altitude.)

From Python:
```python
from mcstools.preprocess.l2.bin_and_agg import main

ds = main(my_list=[29, 30], ddr1_agg_columns=["Dust_column"], ddr2_agg_columns=["Dust"])
```
Pass `ddr2_agg_columns=None` to skip DDR2 entirely and only bin DDR1.

## Excluding known-bad time windows

Both scripts take `--exclude-times-file`/`--exclude-threshold-seconds`/`--exclude-times-column`
to drop data within some window of known-bad timestamps (e.g. instrument anomalies or
calibration events) before it's binned:
```bash
python -m mcstools.preprocess.l1.bin_and_agg \
    --exclude-times-file bad_times.csv --exclude-threshold-seconds 30 \
    --output-path out/l1b_binned.nc
```

`bad_times.csv` needs a timestamp column with one timestamp per row, ISO-8601 and UTC
(sub-second precision not required). By default the column must be named `timestamp`:
```csv
timestamp
2018-04-18 06:12:00
2018-11-02 23:45:10
```
Use `--exclude-times-column` to specify a different column name:
```bash
python -m mcstools.preprocess.l1.bin_and_agg \
    --exclude-times-file bad_events.csv --exclude-times-column event_time \
    --exclude-threshold-seconds 30 --output-path out/l1b_binned.nc
```

`--exclude-threshold-seconds` is required whenever `--exclude-times-file` is given; any
row whose timestamp falls within that +/- window of any listed timestamp is dropped.

- **L1B**: matched against each limb sequence's *averaged* `dt` (i.e. after the view
  pipeline's preprocessing/averaging), not each raw per-detector reading.
- **L2**: matched against each DDR1 profile's `dt`. Since DDR2 levels are only loaded
  for DDR1 profiles that survive filtering, excluding a DDR1 profile automatically
  excludes all of its DDR2 levels too - there's no separate DDR2-level exclusion.

From Python, pass an already-loaded set of timestamps instead of a file path:
```python
from mcstools.preprocess.exclude import load_excluded_times
from mcstools.preprocess.l1.bin_and_agg import main

# Default column name "timestamp"
excluded_times = load_excluded_times("bad_times.csv")
# Or specify a custom column name
excluded_times = load_excluded_times("bad_events.csv", excluded_dt_col="event_time")

ds = main(excluded_times=excluded_times, exclude_threshold_s=30)
```

See `mcstools/preprocess/exclude.py` for the underlying `load_excluded_times`/
`filter_excluded_times` functions.
