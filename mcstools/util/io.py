import os

import click
import yaml


def mcs_data_loader_click_options(f):
    "Common options for setting up MCS Loader"
    f = click.option(
        "--pds",
        is_flag=True,
        default=False,
        help="Load L2 data from PDS [if False will load from MCS_DATA_PATH]",
    )(f)
    f = click.option(
        "--mcs-data-path",
        type=str,
        default=None,
        help="Path to MCS data path "
        "[if PDS=False and no path provided, will setup via .env]",
    )(f)
    return f


def makedirs(output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)


def load_yaml(path):
    with open(path, "r") as file:
        print(f"Loading config from {path}")
        return yaml.safe_load(file)
