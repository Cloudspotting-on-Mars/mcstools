import click
import numpy as np

from mcstools.loader import L2Loader
from mcstools.mcsfile import L2File

# TODO: Use a config filter file instead.

"""
Search DDR1 records over some time range and
find profiles within various bounds.
"""

config = {
    "dt": ("2007-01-01", "2007-01-02"),
    "Profile_lat": (-20, 20)
}

def parse_config(ddr1_df, config_dict):
    for field, vals in config_dict.items():
        if field in ["dt", "Ls"]:
            continue
        else:
            if type(vals) in [tuple]:
                print(f"Filtering {field} to within {vals}")
                ddr1_df = ddr1_df[ddr1_df[field].between(*vals)]
    return ddr1_df


@click.command()
@click.option("--data-dir", default=None, type=str, help="Path to mcs data directory (defaults to using PDS)")
#@click.option("--start-date", type=click.DateTime(), required=False)
#@click.option("--end-date", type=click.DateTime(), required=False)
#@click.option("-f", "--filters", multiple=True, help="Any number of DDR1 column names followed by either minimum/maximum values or flag values to accept")
def main(data_dir):#, start_date, end_date, filters):
    if data_dir:
        l2loader = L2Loader(mcs_data_path = data_dir)
    else:
        l2loader = L2Loader(pds=True)
    if "dt" in config.keys() and "Ls" not in config.keys():
        ddr1 = l2loader.load_date_range(*config["dt"], ddr="DDR1")
        print(ddr1)
    reduced_ddr1 = parse_config(ddr1, config)
    print(reduced_ddr1)
    

if __name__=="__main__":
    main()