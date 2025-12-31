import click
from datetime import datetime
from typing import List, Dict

from mcstools import L1BLoader
from mcstools.preprocess.l1b import L1BDataPipeline
from mcstools.util.io import mcs_data_loader_click_options, makedirs
from mcstools.util.log import logger, setup_logging

# Generalize to data instead of profiles specifically

@click.command()
@mcs_data_loader_click_options
@click.option('--utc-range', nargs=2, type=click.DateTime(), required=False, help='Start and end of time range (ISO format)')
@click.option('--lat-range', nargs=2, type=float, required=False, help='Minimum and maximum latitude')
@click.option('--lt-range', nargs=2, type=float, required=False, help='Minimum and maximum local time (hours)')
@click.option('--lon-range', nargs=2, type=float, required=False, help='Minimum and maximum longitude')
@click.option("--alt-range", nargs=2, type=float, required=False, help="Minimum and maximum altitude")
@click.option("--last-az-cmd-range", nargs=2, type=float, required=False, help="Minimum and maximum last azimuth command")
@click.option("--output-path", type=str, required=False, help="Path to save output csv")
def main(mcs_data_path, pds, utc_range, lat_range, lt_range, lon_range, alt_range, last_az_cmd_range, output_path):
    """
    Main function to find and print L1B profiles matching criteria.
    """
    loader = L1BLoader(mcs_data_path=mcs_data_path, pds=pds)
    if utc_range:
        df = loader.load_date_range(*utc_range, add_cols=["dt"])
        logger.info(f"Loaded L1B data for UTC range {utc_range[0].isoformat()} to {utc_range[1].isoformat()}, shape: {df.shape}")
    else:
        raise NotImplementedError("UTC range must be specified -- Ls range not implemented yet")
    if lat_range:
        df = df[df["Scene_lat"].between(*lat_range)]
        logger.info(f"Restricting L1b data to Scene latitude range {lat_range}, shape: {df.shape}")
    if lon_range:
        df = df[df["Scene_lon"].between(*lon_range)]
        logger.info(f"Restricting L1b data to Scene longitude range {lon_range}, shape: {df.shape}")
    if lt_range:
        pipeline = L1BDataPipeline()
        df =pipeline.add_LTST_column(df)
        df = df[df["LTST"].between(*lt_range)]
        logger.info(f"Restricting L1b data to Local Time range {lt_range}, shape: {df.shape}")
    if alt_range:
        df = df[df["Scene_alt"].between(*alt_range)]
        logger.info(f"Restricting L1b data to Scene altitude range {alt_range}, shape: {df.shape}")
    if last_az_cmd_range:
        df = df[df["Last_az_cmd"].between(*last_az_cmd_range)]
        logger.info(f"Restricting L1b data to Last Azimuth Command range {last_az_cmd_range}, shape: {df.shape}")
    logger.info(f"Final DF {df.shape}:\n{df.head()}")
    if output_path:
        makedirs(output_path)
        logger.info(f"Saving output to {output_path}")
        df.to_csv(output_path, index=True, date_format="%Y-%m-%d %H:%M:%S.%f%z", index_label="id")
    return df


if __name__ == "__main__":
    setup_logging()
    main()