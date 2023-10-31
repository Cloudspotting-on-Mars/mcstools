import holoviews as hv
import hvplot.pandas  # noqa
import datetime as dt
import pandas as pd
import panel as pn

from mcstools.loader import L2Loader

L2_LOADER = L2Loader(pds=True)

start_time = "2012-11-25 05:53:05"
end_time = "2012-11-25 15:13:22"


ddr1 = L2_LOADER.load_date_range(start_time, end_time, ddr="DDR1", add_cols=["dt"])

orb_input = 29683

def plot_lon_lat(ddr1_df):
    return ddr1_df.hvplot.scatter(x="Profile_lon", y="Profile_lat", by="Orb_num", legend="right")

profiles = ddr1[
    (ddr1["Orb_num"]==orb_input) 
#    |
#    (
#        (
#            ddr1["dt"] > (
#                ddr1[ddr1["Orb_num"]==orb_input]["dt"].min() - dt.timedelta(minutes=30)
#            )
#        ) &
#        (
#            ddr1["dt"] < (
#                ddr1[ddr1["Orb_num"]==orb_input]["dt"].max() + dt.timedelta(minutes=30)
#            )
#        )
#)
]["Profile_identifier"]

ddr2 = L2_LOADER.load(ddr="DDR2", profiles=profiles)

merged = L2_LOADER.merge_ddrs(ddr2, ddr1)

day = merged[((merged["LTST"]*24<18) & (merged["LTST"]*24>=6)) & (merged["Orb_num"].isin([orb_input-1, orb_input]))]
#night = merged[((merged["LTST"]*24<6) | (merged["LTST"]*24>=18)) & (merged["Orb_num"].isin([orb_input, orb_input+1]))]

def plot_lat_alt_scatter(half_orbit, quantity):
    half_orbit = half_orbit.dropna(subset=[quantity, "Lat", "Alt"]).sort_values(quantity)
    return half_orbit.hvplot.scatter(x="Lat", y="Alt", c=quantity, cmap="inferno").opts(width=1400, height=600, size=10)

app = pn.Column(
    plot_lon_lat(ddr1), 
    plot_lon_lat(ddr1[ddr1["Profile_identifier"].isin(profiles)]),
    plot_lat_alt_scatter(day, "T"),
    plot_lat_alt_scatter(day, "Dust")
    #plot_lat_alt_scatter(night)
)
app.servable()