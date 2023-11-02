import holoviews as hv
import hvplot.pandas  # noqa
import datetime as dt
import pandas as pd
import panel as pn

from mcstools.loader import L2Loader

# TODO: add hover that displays UTC

def plot_lon_lat(ddr1_df):
    return ddr1_df.hvplot.scatter(x="Profile_lon", y="Profile_lat", by="Orb_num", legend="right")

def select_profiles(ddr1_df, orbit_num):
    profiles = ddr1_df[
        ((ddr1_df["Orb_num"]==orbit_num) & (ddr1_df["day"]==1)) | 
        ((ddr1_df["Orb_num"]==orbit_num) & (ddr1_df["day"]==0) & (ddr1_df["dt"]>ddr1_df[(ddr1_df["day"]==1) & (ddr1_df["Orb_num"]==orbit_num)]["dt"].max())) | 
        ((ddr1_df["Orb_num"]==orbit_num + 1) &  (ddr1_df["day"]==0) & (ddr1_df["dt"] < ddr1_df[ddr1_df["Orb_num"]==orbit_num]["dt"].max() + dt.timedelta(hours=0.5)))
    ]["Profile_identifier"]
    return profiles

def get_ddr1_subset(ddr1_df, profiles):
    return ddr1_df[ddr1_df["Profile_identifier"].isin(profiles)]

def plot_lat_alt_scatter(half_orbit, quantity):
    half_orbit = half_orbit.dropna(subset=[quantity, "Lat", "Alt"]).sort_values(quantity)
    return half_orbit.hvplot.scatter(x="Lat", y="Alt", c=quantity, cmap="inferno").opts(width=1400, height=600, size=10)

def load_ddr1(date, loader):
    ddr1 = loader.load_date_range(date, date+dt.timedelta(hours=8), ddr="DDR1", add_cols=["dt"])
    ddr1["LTST"] = ddr1["LTST"] * 24
    ddr1["day"] = ddr1["LTST"].apply(lambda x: 1 if x>=6 and x<18 else 0)
    return ddr1

def main():
    date = pn.widgets.DatetimeInput(
        name="Date Input",
        value=dt.datetime(2008, 11, 20)
    )
    @pn.depends(date)
    def panel_main(f):
        loader = L2Loader(pds=True)
        ddr1 = load_ddr1(f, loader)
        
        overview_lat_lon = pn.Column(date, plot_lon_lat(ddr1))
        
        orbit_list = list(ddr1["Orb_num"].unique())
        orbit = pn.widgets.Select(name="Orbit", options=orbit_list)
        
        @pn.depends(orbit)
        def panel_single_orbit(orb):
            profiles = select_profiles(ddr1, orb)
            ddr1_subset = get_ddr1_subset(ddr1, profiles)
            ddr2 = loader.load(ddr="DDR2", profiles=profiles)
            merged = loader.merge_ddrs( ddr2, ddr1)
            day = merged[merged["day"]==1]
            night = merged[merged["day"]==0]
            return pn.Tabs(
                (
                    "Day", 
                    pn.Column(
                        plot_lat_alt_scatter(day, "T"),
                        plot_lat_alt_scatter(day, "Dust")
                    ),
                ), (
                    "Night",
                    pn.Column(
                        plot_lat_alt_scatter(night, "T"),
                        plot_lat_alt_scatter(night, "Dust")
                    )
                )
            )


        page = pn.Column(
            pn.Row(
                overview_lat_lon,
                pn.Column(orbit)
            ),
            panel_single_orbit
        )
        return page

    pn.serve(pn.panel(panel_main))

if __name__=="__main__":
    main()