# mcs-tools
Tools to read and process Mars Climate Sounder files.

#### Setup
Download or clone the repo:
```bash
$ pip install mcstools
```

or

```bash
$ git clone https://github.com/cloudspotting-on-mars/com-analysis-tools
```

Setup a virtual environment with `python3 -m venv env` and install with `pip install -e .`

#### Download data
MCS data is available on the PDS at: https://pds-atmospheres.nmsu.edu/data_and_services/atmospheres_data/MARS/atmosphere_temp_prof.html

#### Read a single file
To read in an L1B file as a DataFrame:
```python
from mcstools import L1BReader
reader = L1BReader()
df = reader.read(path_to_file)
```
or to read from the PDS:

```python
from mcstools import L1BReader
reader = L1BReader(pds=True)
df = reader.read(url)
```

#### Load Data from PDS
To load data from PDS:
```python
from mcstools import L1BLoader
loader = L1BLoader(pds=True)
df = loader.load_date_range("2016-01-01", "2016-01-02")
```

### Preprocess data
To preprocess L1B data and reduce to standard in-track limb views:
```python
from mcstools.preprocess.l1b import L1BStandardInTrack
preprocesser = L1BStandardInTrack()
df = preprocesser.process(df)
```
