import datetime as dt
import pandas as pd

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