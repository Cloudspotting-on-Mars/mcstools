"""
L1B Profile Plotting Module

This module provides functionality to plot MCS L1B radiance profiles.
It creates multi-panel plots showing radiance profiles for different channels
and supports plotting multiple profiles from file lists.
"""

import logging
import click
import matplotlib
matplotlib.use('Qt5Agg')  # Interactive backend for plotting
import matplotlib.pyplot as plt

from mcstools.globals import ALL_CHANNELS
from mcstools import L1BLoader
from mcstools.radiance_profile import RadianceProfile
from mcstools.util.io import mcs_data_loader_click_options
from mcstools.util.log import logger, setup_logging

# Suppress matplotlib font warnings to reduce log noise
logging.getLogger('matplotlib.font_manager').setLevel(logging.ERROR)


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
        self.fig, self.ax = plt.subplots(self.nrows, self.ncolumns, 
                                        figsize=(5*self.ncolumns, 4*self.nrows))
        
        # Create mapping from channels to subplot axes
        self.subplots = {ch: self.ax[i//3, i%3] if self.nrows > 1 else self.ax[i%3] 
                        for i, ch in enumerate(self.channels)}
        
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
            profile.profile, yaxis,
            # Optional: add labels for multiple profiles
            #label=f"{profile.filestr}_{profile.profile_num}"
        )
        

@click.command()
@mcs_data_loader_click_options
@click.option("--profiles", type=click.Path(exists=True), 
              help="Path to a file containing profile IDs to plot")
@click.option("--channels", multiple=True, default=None, 
              help="Channels to plot [all by default]")
@click.option("--vertical-axis", type=click.Choice(["Detector", "Altitude"]), default="Detector",
              help="Type of vertical axis to use")
def main(mcs_data_path, pds, profiles, channels, vertical_axis):
    """
    Main function to plot L1B profiles from MCS data.
    
    This function reads profile IDs from a file, loads the corresponding L1B data,
    and creates plots showing radiance profiles for the specified channels.
    
    Args:
        mcs_data_path (str): Path to MCS data directory
        pds (bool): Whether to use PDS data format
        profiles (str): Path to file containing profile IDs (format: filename_profilenum)
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
    if profiles:
        with open(profiles, "r") as f:
            profile_ids = [line.strip() for line in f]
            logger.info(f"Profile IDs to plot: {profile_ids}")
            # Extract unique filenames from profile IDs
            # Profile IDs are expected in format: "filename_profilenumber"
            filestrs = list(set([p_id.split("_")[0] for p_id in profile_ids]))
            logger.info(f"Files to load: {filestrs}")
    else:
        raise ValueError("No profiles specified for plotting")
    
    # Load L1B data for all required files (batch loading for efficiency)
    data = {filestr: loader.load_from_filestr(filestr, add_cols=["dt"]) 
            for filestr in filestrs}
    
    include_altitudes = True if vertical_axis == "Altitude" else False
    
    # Process each profile and add to plot
    # Currently limited to first 5 profiles for performance
    for filename, profile_num in [p_id.split("_") for p_id in profile_ids]:
        profile_num = int(profile_num)
        logger.info(f"Processing: {filename}, profile {profile_num}")
        
        # Create and add profiles for each specified channel
        for channel in channels:
            profile = RadianceProfile.from_l1b_row(channel, data[filename].loc[profile_num], include_altitudes=include_altitudes)
            pp.add_profile(profile)
    
    # Display the plot
    pp.fig.tight_layout()
    plt.show()



if __name__ == "__main__":
    # Set up logging configuration for the script
    setup_logging()
    
    # Run the main CLI function
    main()