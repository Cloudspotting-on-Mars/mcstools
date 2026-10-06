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


def exclude_times_click_options(f):
    "Common options for excluding known-bad time windows from binning"
    f = click.option(
        "--exclude-times-file",
        type=str,
        default=None,
        help="Path to a CSV of timestamps to exclude from binning (see "
        "mcstools.preprocess.exclude.load_excluded_times)",
    )(f)
    f = click.option(
        "--exclude-threshold-seconds",
        type=float,
        default=None,
        help="+/- window (seconds) around each excluded timestamp to drop "
        "[required if --exclude-times-file is given]",
    )(f)
    return f


def resolve_excluded_times(exclude_times_file, exclude_threshold_seconds):
    """
    Validate and load the options added by `exclude_times_click_options` into an
    excluded_times Series (or None if no file was given). Raises a click.UsageError
    if a file is given without a threshold.
    """
    from mcstools.preprocess.exclude import load_excluded_times

    if exclude_times_file is None:
        return None
    if exclude_threshold_seconds is None:
        raise click.UsageError(
            "--exclude-threshold-seconds is required when --exclude-times-file is given"
        )
    return load_excluded_times(exclude_times_file)


def makedirs(output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)


def load_yaml(path):
    with open(path, "r") as file:
        print(f"Loading config from {path}")
        return yaml.safe_load(file)
