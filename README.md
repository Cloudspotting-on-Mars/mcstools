# mcs-tools
Tools to read and process Mars Climate Sounder files.

#### Setup
Download or clone the repo:
```bash
$ git clone https://github.com/cloudspotting-on-mars/com-analysis-tools
```

Setup a virtual environment with `python3 -m venv env` and install with `pip install -e .`

#### Download data
See https://pds-atmospheres.nmsu.edu/data_and_services/atmospheres_data/MARS/atmosphere_temp_prof.html

#### Read a single file
To read in a file as a DataFrame:
```
reader = MCSL1bReader()
reader.read(path_to_file)
```
