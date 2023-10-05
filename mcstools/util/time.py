import datetime as dt

import pandas as pd

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
    elif force_up:
        up = True  # force always round up
    # Round
    if up:
        # round up
        dt_start_of_xhour = dt_start_of_xhour + dt.timedelta(hours=hours)
    return dt_start_of_xhour


def convert_date_utcs(date: str, utc: str):
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
    if type(date) != str or type(utc) != str:
        date_str = pd.NaT
    else:
        date_str = date.strip().replace('"', "") + " " + utc.strip().replace('"', "")
    return pd.to_datetime(date_str, format=fmt, errors="coerce")


def check_and_convert_start_end_times(start_time, end_time):
    times = [start_time, end_time]
    for i, t in enumerate(times):
        if type(t) not in [dt.datetime, str]:
            raise TypeError(
                f"Unrecognized type ({type(t)}) for start/end time, "
                "must be datetime or isoformat str"
            )
        elif type(t) != dt.datetime:
            times[i] = dt.datetime.fromisoformat(t)
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
