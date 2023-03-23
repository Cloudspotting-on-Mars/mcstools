
from mcstools.preprocess.l1b import L1BStandardInTrack
from mcstools.reader import L1BReader
import cmcrameri.cm as cm
import hvplot.xarray  # noqa
import panel as pn

from mcstools import L1BLoader

FILESTR = pn.widgets.TextInput(value="230318000000")


mcs_data_path = "../mcs_arches/data/"


def plot(data):
    p = data.hvplot.quadmesh(
        "dt",
        "Detector",
    )
    return p


def all_plots(df_ave):
    reader = L1BReader()
    cdata = [df_ave["Radiance"].sel(Channel=c) for c in reader.channels]
    plots = [
        plot(d).opts(
            ylim=(reader.detectors[c[0]][0], reader.detectors[c[0]][-1]),
            shared_axes=False,
            cmap=cm.oslo,
            clim=(0, float(d.max().values)),
        )
        for d, c in zip(cdata, reader.channels)
    ]
    return pn.Tabs(*zip(reader.channels, plots))


@pn.depends(FILESTR)
def panel_main(f):
    loader = L1BLoader(mcs_data_path=mcs_data_path)
    path = loader.filename_builder.make_filename_from_filestr(f)
    df = loader.load([path])
    processer = L1BStandardInTrack()
    df = processer.preprocess(df)
    df_xr = processer.melt_to_xarray(df)
    tabs = all_plots(df_xr)
    view = pn.Column(FILESTR, tabs)
    return view

pn.serve(pn.panel(panel_main))