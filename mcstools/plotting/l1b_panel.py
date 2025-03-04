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


def all_plots(df_ave):
    """
    Plot 2d radiances for each channel.
    """
    reader = L1BReader()
    cdata = [
        df_ave.sel(Channel=c) for c in reader.channels
    ]  # get radiances for each channel
    plots = [
        plot(d).opts(
            ylim=(reader.detectors[c[0]][0], reader.detectors[c[0]][-1]),
            shared_axes=False,
            cmap=cm.oslo,
            colorbar=True,
            clim=(0, float(d["Radiance"].max().values)),
            width=1200,
            height=600,
            tools=["hover"],
            line_alpha=0,
        )
        for d, c in zip(cdata, reader.channels)
    ]  # make all plots
    cbar_sliders = [
        pn.widgets.RangeSlider(
            start=rad["Radiance"].min().item(),
            end=rad["Radiance"].max().item(),
            orientation="vertical",
            direction="rtl",
        )
        for rad in cdata
    ]  # create sliders for colorbar
    js_codes = (
        [
            """
        color_mapper.low = cb_obj.value[0];
        color_mapper.high = cb_obj.value[1];
        """
        ]
        * len(cbar_sliders)
    )  # required to link slider to widget
    _ = [
        w.jslink(p, code={"value": jsc})
        for w, p, jsc in zip(cbar_sliders, plots, js_codes)
    ]  # link each slider to corresponding plot
    rows = [
        pn.Row(p, c, sizing_mode="stretch_both") for p, c in zip(plots, cbar_sliders)
    ]  # create plot/slider combo for ecah channel
    return pn.Tabs(
        *zip(reader.channels, rows), sizing_mode="stretch_both"
    )  # make tab for each channel dash


@click.command()
@mcs_data_loader_click_options
@click.option("--filestr", default="071214040000")
@click.option(
    "--direction",
    type=click.Choice(["in", "aft", "right", "left"]),
    default="in",
    help="Viewing direction to plot",
)
def main(pds, mcs_data_path, filestr, direction) -> None:
    """
    Plot single 4-hour file radiance file
    """
    FILESTR = pn.widgets.TextInput(value=filestr)  # defines file to load

    @pn.depends(FILESTR)
    def panel_main(f):
        loader = L1BLoader(mcs_data_path=mcs_data_path, pds=pds)  # initialize loader
        path = loader.filename_builder.make_filename_from_filestr(
            f
        )  # make single file path/url
        df = loader.load([path])  # load single 4-hour file
        processer = L1BStandardInTrack(directions=direction)  # initialize processer
        df = processer.preprocess(df)  # reduce to in-track sequence-averaged data
        df_xr = processer.melt_to_xarray(
            df, include_cols=["Radiance", "Scene_lat", "Scene_lon", "LTST", "L_sub_s"]
        )  # melt radiance columns to individual detectors
        tabs = all_plots(df_xr)  # generate all plots
        view = pn.Column(
            FILESTR, tabs
        )  # create main dashboard page (File widget + tabs for each channel)
        return view

    pn.serve(pn.panel(panel_main))


if __name__ == "__main__":
    setup_logging()
    main()
