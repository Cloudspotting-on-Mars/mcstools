import click

from mcstools.loader import L2Loader
from mcstools.preprocess.l2.filter_and_bin import ConfigParser
from mcstools.util.log import logger, setup_logging
from mcstools.util.io import mcs_data_loader_click_options, makedirs

@click.command()
@click.argument("config-file")
@mcs_data_loader_click_options
@click.option("--ddr1-profiles-only")
@click.option("--output-path", default=None, type=str, help="Path to save binned data to")
def main(config_file, pds, mcs_data_path, ddr1_profiles_only, output_path):
    loader = L2Loader(pds=pds, mcs_data_path=mcs_data_path)
    filters = ConfigParser.from_yaml(config_file)
    print(filters)
    ddr1 = loader.load_from_filter_config(filters.filter_config, verbose=True)
    print(ddr1.columns)
    if ddr1_profiles_only:
        ddr1_adj = filters.bin_config.create_bin_columns(ddr1)
        ddr1_binned_profiles = ddr1_adj.groupby(
            filters.bin_config.binned_columns,
            as_index=True
        )["Profile_identifier"].aggregate(lambda x: list(x.unique()) if len(x)>0 else [])
        if output_path:
            makedirs(output_path)
            logger.info(f"Saving to {output_path}:\n{ddr1_binned_profiles}")
            ddr1_binned_profiles.to_csv(output_path, index=True)
    else:
        ddr2 = loader.load(ddr="DDR2", profiles=ddr1["Profile_identifier"].unique(), verbose=True)
        ddr2.dropna(
            subset=[x for x in filters.ddr2_fields if x not in ["Pres", "level"]],
            inplace=True
        )
        merged =loader.merge_ddrs(ddr2, ddr1, verbose=True)
        merged = filters.bin_config.create_bin_columns(merged)
        merged = merged.set_index(
            filters.bin_config.binned_columns
        )[filters.ddr2_fields]
        if output_path:
            makedirs(output_path)
            logger.info(f"Saving to {output_path}:\n{merged}")
            merged.to_csv(output_path, index=True)

if __name__ == "__main__":
    setup_logging()
    main()