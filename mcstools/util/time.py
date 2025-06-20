import datetime as dt

import pandas as pd
import pytz
from mars_time import datetime_to_marstime

GDS_DATE_FMT = "%y%m%d%H%M%S"  # Format used in GDS filenames
PDS_DATE_FMT = "%Y%m%d%H"  # Format used in PDS filenames


def round_to_x_hour(date, hours=4, force_down=False, force_up=False):
    """
    Round datetime to nearest x-hour (For MCS 4-hour files)
    """
    # Find closest interval
    up = (date.hour % hours) > hours // 2  # above midpoint?
    dt_start_of_xhour = date.replace(
        hour=(date.hour // hours) * hours, minute=0, second=0, microsecond=0
    )
    # determine if round up needed
    if force_down and force_up:
        raise ValueError("Can't force rounding up and down")
    elif force_down:
        up = False  # force always round down to get filename from datetime
    elif force_up and date != dt_start_of_xhour:
        up = True  # force always round up
    # Round
    if up:
        # round up
        dt_start_of_xhour = dt_start_of_xhour + dt.timedelta(hours=hours)
    return dt_start_of_xhour


def convert_date_utcs(date: str, utc: str, with_utc_tzinfo=True):
    """
    Convert MCS "Date" and "UTC" column values into datetime

    Parameters
    ----------
    date: "Date" column value
    utc: "UTC" column value

    Returns
    -------
    _: signle datetime value
    """
    fmt = "%d-%b-%Y %H:%M:%S.%f"
    if not isinstance(date, str) or not isinstance(utc, str):
        date_str = pd.NaT
    else:
        date_str = date.strip().replace('"', "") + " " + utc.strip().replace('"', "")
    return pd.to_datetime(date_str, format=fmt, errors="coerce", utc=with_utc_tzinfo)


def check_and_convert_tzinfo(time):
    if time.tzinfo is None:
        time = time.replace(tzinfo=dt.timezone.utc)
    elif time.tzinfo in [dt.timezone.utc, pytz.utc]:
        pass
    else:
        raise ValueError(
            f"time {time} is tz-aware, but not UTC."
            "Converting non-UTC to UTC not implemented."
        )
    return time


def check_and_convert_start_end_times(start_time, end_time):
    times = [start_time, end_time]
    for i, t in enumerate(times):
        if type(t) not in [dt.datetime, pd._libs.tslibs.timestamps.Timestamp, str]:
            raise TypeError(
                f"Unrecognized type ({type(t)}) for start/end time, "
                "must be datetime or isoformat str"
            )
        elif isinstance(t, str):
            times[i] = dt.datetime.fromisoformat(t)
        times[i] = check_and_convert_tzinfo(times[i])
    return times


def add_datetime_column(df: pd.DataFrame, dt_name: str = "dt") -> pd.DataFrame:
    """
    Convert Date and UTC columns to single datetime column.

    Parameters
    ----------
    df: MCS data
    dt_name: column name for new datetime column

    Returns
    -------
    df: data with additional datetime column
    """
    if len(df.index) == 0:
        return pd.DataFrame(columns=list(df.columns) + ["dt"])
    df[dt_name] = df.apply(
        lambda row: convert_date_utcs(row["Date"], row["UTC"]), axis=1
    )
    return df


def add_marsyear_column(
    df: pd.DataFrame, marsyear_column_name: str = "MY", dt_name: str = "dt"
) -> pd.DataFrame:
    """
    Add column with Clancy Mars Year integer from datetime column.
    """
    df[marsyear_column_name] = df[dt_name].apply(lambda x: datetime_to_marstime(x).year)
    return df


def ltst(lon, subsolar_lon):
    # [0, 24)
    delta = lon - subsolar_lon
    if delta < -180:
        delta += 360
    if delta >= 180:
        delta -= 360
    return (delta + 180) * 24 / 360


def sols_elapsed(time_a, time_b):
    """
    Number of sols elapsed from time_a to time_b
    """
    days_per_sol = 88775.244 / 24 / 3600
    return (time_a - time_b).total_seconds() / (3600 * 24) / days_per_sol
