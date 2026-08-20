import click
import cmcrameri.cm as cm
import holoviews as hv
import hvplot.xarray  # noqa
import panel as pn

from mcstools.loader import L1BLoader
from mcstools.preprocess.l1b import L1BStandardInTrack
from mcstools.reader import L1BReader
from mcstools.util.io import mcs_data_loader_click_options
from mcstools.util.log import setup_logging

# Force internal Bokeh/noUiSlider track elements to fill container height
VERTICAL_SLIDER_STYLE = """
.bk-input-group {
    height: 520px !important;
}
.noUi-target {
    height: 460px !important;
}
.noUi-base {
    height: 100% !important;
}
"""


def plot(data):
    """
    Plot radiances as a function of UTC and detector.
    """
    p = hv.QuadMesh(
        data,
        kdims=["dt", "Detector"],
        vdims=["Radiance", "Scene_lat", "Scene_lon", "LTST", "L_sub_s"],
    )
    return p


def create_channel_tab(d, channel_name, detector_bounds):
    """Creates a plot and linked horizontal range slider for a single channel."""
    min_val = float(d["Radiance"].min().values)
    max_val = float(d["Radiance"].max().values)

    # 1. Native horizontal RangeSlider (bug-free in Bokeh 3)
    slider = pn.widgets.RangeSlider(
        start=min_val,
        end=max_val,
        value=(min_val, max_val),
        width=1100,
        name="Colorbar Range (Radiance)",
    )

    # 2. Re-render clim reactively when slider updates
    @pn.depends(slider.param.value)
    def reactive_plot(clim_val):
        return plot(d).opts(
            ylim=detector_bounds,
            shared_axes=False,
            cmap=cm.oslo,
            colorbar=True,
            clim=clim_val,
            width=1100,
            height=500,
            tools=["hover"],
            line_alpha=0,
        )

    # 3. Stack plot and slider in a Column
    return pn.Column(reactive_plot, slider, width=1150)


def all_plots(df_ave):
    reader = L1BReader()
    cdata = [df_ave.sel(Channel=c) for c in reader.channels]

    tabs_items = []
    for d, c in zip(cdata, reader.channels):
        bounds = (reader.detectors[c[0]][0], reader.detectors[c[0]][-1])
        tab_row = create_channel_tab(d, c, bounds)
        tabs_items.append((c, tab_row))

    return pn.Tabs(*tabs_items, height=600)


@click.command()
@mcs_data_loader_click_options
@click.option("--filestr", default="071214040000")
@click.option(
    "--direction",
    type=click.Choice(["in", "aft", "right", "left"]),
    default="in",
    help="Viewing direction to plot",
    show_default=True,
)
@click.option(
    "--port",
    default=5006,
)
def main(pds, mcs_data_path, filestr, direction, port) -> None:
    """
    Plot single 4-hour file radiance file
    """
    FILESTR = pn.widgets.TextInput(value=filestr)

    @pn.depends(FILESTR)
    def panel_main(f):
        loader = L1BLoader(mcs_data_path=mcs_data_path, pds=pds)
        path = loader.filename_builder.make_filename_from_filestr(f)
        df = loader.load([path])
        processer = L1BStandardInTrack(directions=direction)
        df = processer.preprocess(df)
        df_xr = processer.melt_to_xarray(
            df, include_cols=["Radiance", "Scene_lat", "Scene_lon", "LTST", "L_sub_s"]
        )

        tabs = all_plots(df_xr)
        return pn.Column(FILESTR, tabs, sizing_mode="stretch_width")

    pn.serve(pn.panel(panel_main), show=False, port=port)


if __name__ == "__main__":
    setup_logging()
    main()
