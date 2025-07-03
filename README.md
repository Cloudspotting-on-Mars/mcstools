# mcstools
Tools to read and process Mars Climate Sounder data.

#### Setup
Setup a virtual environment with `python3[>3.10] -m venv env`.

Then, either pip install: `pip install mcstools`

Or download or clone the repo:
```bash
$ git clone https://github.com/cloudspotting-on-mars/mcstools
```
and install with `pip install -e .`

#### Download data
See https://pds-atmospheres.nmsu.edu/data_and_services/atmospheres_data/MARS/atmosphere_temp_prof.html

#### Read a single file
To read in an L1B file as a DataFrame:
```python
from mcstools import L1BReader
reader = L1BReader()
reader.read(path_to_file)
```

#### Load Data from PDS
To load data from PDS:
```python
from mcstools import L1BLoader
loader = L1BLoader(pds=True)
loader.load_date_range("2016-01-01", "2016-01-02")
```

#### Find and load subset of L2 profiles
```python
from mcstools import L2Loader
loader = L2Loader(pds=True)
ddr1_df = loader.load_date_range("2018-04-18", "2018-04-19", "DDR1")
ddr1_subset = ddr1[ddr1["Profile_lat"].between(-10, 10)]
ddr2 = loader.load("DDR2", profiles=ddr1_subset["Profile_identifier"])
```

#### Plot L1B radiances
To view the radiances for a single 4-hour L1B file, run
```bash
python mcstools/plotting/l1b_panel.py
```

That should bring up a dashboard in a browser allowing you to choose a 4-hour file at the top
(enter the date in `YYMMDDHH0000` format).
You can switch between channels using the tabs.
The slider on the right allows you to set the colorbar limits (radiance units).
There are also tools to zoom in and out, pan, etc.

#### Preprocess data
To preprocess L1B data and reduce to standard in-track limb views:
```python
from mcstools.preprocess.l1b import L1BStandardInTrack
preprocesser = L1BStandardInTrack()
df = preprocesser.process(df)
```