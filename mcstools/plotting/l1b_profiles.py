"""
L1B Profile Plotting Module

This module provides functionality to plot MCS L1B radiance profiles.
It creates multi-panel plots showing radiance profiles for different channels
and supports plotting multiple profiles from file lists.
"""

import logging

import click
import matplotlib.pyplot as plt
import pandas as pd

from mcstools import L1BLoader
from mcstools.globals import ALL_CHANNELS
from mcstools.radiance_profile import RadianceProfile
from mcstools.util.io import makedirs, mcs_data_loader_click_options
from mcstools.util.log import logger, setup_logging

# Suppress matplotlib font warnings to reduce log noise
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)


class ProfilePlot:
    """
    Class for plotting MCS L1B radiance profiles in a multi-panel layout.

    This class sets up a grid of subplots (one per channel), manages axis configuration,
    and provides methods to add radiance profiles to the correct subplot.

    Attributes:
        channels (list): List of channel names to plot.
        fig (matplotlib.figure.Figure): The matplotlib Figure object.
        ax (numpy.ndarray or matplotlib.axes.Axes): Array of subplot axes.
        subplots (dict): Mapping from channel names to subplot axes.
        _nrows (int): Number of subplot grid rows (computed from channels).
        _ncolumns (int): Number of subplot grid columns (computed from channels).
        _vertical_axis (str): Type of vertical axis ("Detector" or "Altitude").
    """

    def __init__(self, channels=None, vertical_axis="Detector"):
        """
        Initialize the ProfilePlot with specified channels and axis configuration.

        Args:
            channels (list, optional): List of channel names to plot.
                                     Defaults to ALL_CHANNELS if None.
            vertical_axis (str): Type of vertical axis, either "Detector" or "Altitude".
                               Defaults to "Detector".
        """
        # Use all channels if none specified
        self.channels = channels if channels is not None else ALL_CHANNELS

        # Calculate grid dimensions (3 columns max) as hidden attributes
        self._nrows = (len(self.channels) - 1) // 3 + 1
        self._ncolumns = 3 if len(self.channels) % 3 == 0 else len(self.channels) % 3

        # Create figure and subplots
        self.fig, self.ax = plt.subplots(
            self.nrows, self.ncolumns, figsize=(5 * self.ncolumns, 4 * self.nrows)
        )

        # Create mapping from channels to subplot axes
        self.subplots = {
            ch: self.ax[i // 3, i % 3] if self.nrows > 1 else self.ax[i % 3]
            for i, ch in enumerate(self.channels)
        }

        self._vertical_axis = vertical_axis

        # Configure each subplot
        for ch, ax in self.subplots.items():
            ax.set_title(f"Channel {ch}")
            ax.set_xlabel("Radiance")

            # Set y-axis label based on vertical axis type
            if vertical_axis == "Detector":
                ax.set_ylabel("Detector")
            elif vertical_axis == "Altitude":
                ax.set_ylabel("Altitude (km)")

            ax.grid(True)

            # Set y-axis limits based on channel type
            if vertical_axis == "Detector":
                # A channels: detector rows 0-21, plot inverted (high to low)
                # B channels: detector rows 0-21, plot normal (low to high)
                if ch[0] == "A":
                    ax.set_ylim(21.5, 0.5)  # Inverted for A channels
                else:
                    ax.set_ylim(0.5, 21.5)  # Normal for B channels
            else:
                ax.set_ylim(0, 90)  # Altitude range in km

    @property
    def nrows(self):
        return self._nrows

    @property
    def ncolumns(self):
        return self._ncolumns

    @property
    def vertical_axis(self):
        """Type of vertical axis ("Detector" or "Altitude")."""
        return self._vertical_axis

    @vertical_axis.setter
    def vertical_axis(self, value):
        self._vertical_axis = value

    def add_profile(self, profile: RadianceProfile):
        """
        Add a radiance profile to the appropriate subplot.

        Args:
            profile (RadianceProfile): The radiance profile to plot
        """
        # Determine y-axis values based on vertical axis type
        if self.vertical_axis == "Detector":
            yaxis = profile.profile.index
        elif self.vertical_axis == "Altitude":
            yaxis = profile.altitudes
        else:
            yaxis = None

        # Plot the profile on the appropriate subplot
        self.subplots[profile.channel].plot(
            profile.profile,
            yaxis,
            # Optional: add labels for multiple profiles
            # label=f"{profile.filestr}_{profile.profile_num}"
        )


@click.command()
@mcs_data_loader_click_options
@click.option(
    "--input-file",
    type=click.Path(exists=True),
    help="Path to a file containing profile times",
)
@click.option(
    "--input-column",
    type=str,
    default="dt",
    show_default=True,
    help="Column name in input file containing profile times (default: dt)",
)
@click.option(
    "--channels", multiple=True, default=None, help="Channels to plot [all by default]"
)
@click.option(
    "--vertical-axis",
    type=click.Choice(["Detector", "Altitude"]),
    default="Detector",
    help="Type of vertical axis to use",
)
@click.option("--output-path", type=click.Path(), help="Path to save the output plot")
def main(
    mcs_data_path, pds, input_file, input_column, channels, vertical_axis, output_path
):
    """
    Main function to plot L1B profiles from MCS data.

    This function reads profile IDs from a file, loads the corresponding L1B data,
    and creates plots showing radiance profiles for the specified channels.

    Args:
        mcs_data_path (str): Path to MCS data directory
        pds (bool): Whether to use PDS data format
        profiles (str): Path to file containing profile IDs
            (format: filename_profilenum)
        channels (tuple): Tuple of channel names to plot

    Raises:
        ValueError: If no profiles file is specified
    """
    # Convert channels tuple to list, use all channels if none specified
    channels = list(channels) if channels else ALL_CHANNELS

    # Initialize the L1B data loader
    loader = L1BLoader(mcs_data_path=mcs_data_path, pds=pds)

    # Create the profile plot object
    pp = ProfilePlot(channels=channels, vertical_axis=vertical_axis)

    # Read profile IDs from the specified file
    input_df = pd.read_csv(
        input_file, parse_dates=[input_column], date_format="%Y-%m-%d %H:%M:%S.%f%z"
    )
    profile_dts = input_df[input_column].tolist()

    # Load L1B data for all required files (batch loading for efficiency)
    data = loader.load_from_datetimes(profile_dts, add_cols=["dt"])
    data = data[data["dt"].isin(profile_dts)]

    include_altitudes = True if vertical_axis == "Altitude" else False

    # Process each profile and add to plot
    # Currently limited to first 5 profiles for performance
    for profile_dt in profile_dts:
        logger.info(f"Processing profile {profile_dt}")

        # Create and add profiles for each specified channel
        for channel in channels:
            profile = RadianceProfile.from_l1b_row(
                channel,
                data[data["dt"] == profile_dt].squeeze(),
                include_altitudes=include_altitudes,
            )
            pp.add_profile(profile)

    # Save the plot
    pp.fig.tight_layout()
    if output_path:
        makedirs(output_path)
        pp.fig.savefig(output_path)
        logger.info(f"Saved plot to {output_path}")


if __name__ == "__main__":
    # Set up logging configuration for the script
    setup_logging()

    # Run the main CLI function
    main()
