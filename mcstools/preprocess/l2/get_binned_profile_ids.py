import click

from mcstools.loader import L2Loader
from mcstools.preprocess.l2.filter_and_bin import ConfigParser
from mcstools.util.log import logger, setup_logging
from mcstools.util.io import mcs_data_loader_click_options, makedirs

@click.command()
@click.argument("config-file")
@mcs_data_loader_click_options
@click.option("--output-path", default=None, type=str, help="Path to save profile identifiers")
def main(config_file, pds, mcs_data_path, output_path):
    loader = L2Loader(pds=pds, mcs_data_path=mcs_data_path)
    filters = ConfigParser.from_yaml(config_file)
    print(filters)
    ddr1 = loader.load_from_filter_config(filters.filter_config, verbose=True)
    print(ddr1.columns)
    ddr1_adj = filters.bin_config.create_bin_columns(ddr1)
    
    ddr1_binned_profiles = ddr1_adj.groupby(
        filters.bin_config.binned_columns,
        as_index=True
    )["Profile_identifier"].aggregate(lambda x: list(x.unique()) if len(x)>0 else [])
    if output_path:
        make_dirs(output_path)
        logger.info(f"Saving to {output_path}:\n{ddr1_binned_profiles}")
        ddr1_binned_profiles.to_csv(output_path, index=True)

if __name__ == "__main__":
    setup_logging()
    main()