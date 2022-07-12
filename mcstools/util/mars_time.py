import click
import datetime as dt
import marstime as mt
from typing import Callable

"""
Functions that build on ``marstime`` and ``marstiming`` package
(https://pypi.org/project/marstime/)
to convert datetimes to Mars Year and solar longitude and the reverse.
"""

def dt_to_j2000offset(date: dt.datetime):
    """
    Convert datetime to J2000 offset for use in ``marstime`` functions.
    
    Parameters
    ----------
    date: date to convert
    
    Returns
    -------
    j2000_offset: J2000 offset value
    """
    ref = dt.datetime(2000,1,1,12,0,0)
    delta = date - ref
    j2000_offset = delta.days + delta.seconds/86400
    return j2000_offset

def getJD(date: dt.datetime):
    """
    Get Julian date in seconds given datetime
    From ``marstiming`` package
    """
    ref_1970 = dt.datetime(1970, 1, 1)
    offset = 2440587.5 #JD on 1/1/1970 00:00:00
    diff = date - ref_1970
    return diff.total_seconds()/86400. + offset

def getUTC(jd: float):
    '''
    Get UTC given Julian Date in seconds
    From ``marstiming`` package
    '''

    offset = 2440587.5 #JD on 1/1/1970 00:00:00 

    d1970 = dt.datetime(1970, 1, 1)
    return d1970 + dt.timedelta(seconds=((jd-offset)*86400.))


def mt_fnc_convert(date: dt.datetime, mt_fnc: Callable):
    """
    Convert datetime to output of a ``marstime`` function
    """
    return mt_fnc(dt_to_j2000offset(date))

def dt_to_Ls(date: dt.datetime):
    """
    Convert date to Mars solar longitude
    """
    return mt_fnc_convert(date, mt.Mars_Ls)

def dt_to_MY(date: dt.datetime):
    """
    Convert date to Clancy Mars Year
    """
    return mt_fnc_convert(date, mt.Clancy_Year)

def dt_to_MY_Ls(date: dt.datetime):
    """
    Convert date to Mars Year and Ls
    """
    return dt_to_MY(date), dt_to_Ls(date)

def MY_Ls_to_UTC(MY: float, Ls: float, Ls_thresh: float=0.001) -> dt.datetime:
    """
    Determine UTC from Clancy Mars Year and solar longitude.
    Based on ``marstiming`` package
    
    MY: Mars Year
    Ls: Mars solar longitude
    Ls_thresh: threshold for error in Ls
    
    date: UTC date
    """
    DPY = 686.9713
    # Initial guess
    refTime = dt.datetime(1955,4,11,10,56,0) #Mars year 1
    refDate = getJD(refTime) # Julian date MY 1
    date = getUTC(refDate+(MY-1 + Ls/360.)*DPY) #initial guess date
    converge = 0
    counter = 0
    while converge == 0:
        date, converge = check_and_update(date, MY, Ls, Ls_thresh)
        counter +=1
        if counter >= 1000:
            raise ValueError("Could not find Ls, too many attempts")
    return date

def check_and_update(date, MY, Ls, Ls_thresh):
    new_MY, new_Ls = dt_to_MY_Ls(date) # regenerate MY, Ls from guess
    my_diff, ls_diff = MY_Ls_diff(new_MY, new_Ls, MY, Ls)
    converge = 1 if ls_diff < Ls_thresh or abs(360-ls_diff) < Ls_thresh else 0
    if not converge:
        update_days = diff_to_days(ls_diff)
        date = date + dt.timedelta(days=update_days)
    return date, converge

def diff_to_days(ls_diff):
    if abs(ls_diff) < abs(ls_diff - 360):
        days= ls_diff * 2
    else:
        days = (ls_diff - 360) * 2
    return days


def MY_Ls_diff(input_my, input_Ls, goal_my, goal_Ls):
    return goal_my - input_my, goal_Ls - input_Ls


def ltst(lon, subsolar_lon):
    # [0, 24)
    delta = lon - subsolar_lon
    if delta < -180:
        delta += 360
    if delta >= 180:
        delta -= 360
    return (delta+180)*24/360

class MarsDate:
    def __init__(self, my: int, ls: float):
        self._my = my
        self._ls = ls

    # Read-only field accessors
    @property
    def my(self):
        """Clancy Mars Year"""
        return self._my

    @property
    def ls(self):
        """L_sub_s [0, 360)"""
        return self._ls

    @classmethod
    def from_dt(cls, dt: dt.datetime):
        """Create mars date from datetime object"""
        my, ls = mt.dt_to_MY_Ls(dt)
        return cls(int(my), ls)
    
    @classmethod
    def from_str(cls, mylsstr: str):
        """Create mars date from MY{X}Ls{y} formatted string"""
        my, ls = mylsstr.split("MY")[1].split("Ls")[0::1]
        return cls(int(my), float(ls))

    def to_UTC(self, Ls_thresh: float=0.001):
        return mt.MY_Ls_to_UTC(self.my, self.ls, Ls_thresh=Ls_thresh)
    
    def to_str(self):
        return "MY"+str(self.my)+"Ls"+str(int(round(self.ls))).zfill(3)

    def __str__(self) -> str:
        return self.to_str()

    

class ClickMarsDate(click.ParamType):
    """
    Converts Mars/Ls string of type MY{XX}Ls{YYY} to
    MarsDate type
    """
    name = "marsdate"

    def convert(self, value, param, ctx):
        if isinstance(value, MarsDate):
            return value
        try:
            return MarsDate.from_str(value)
        except ValueError:
            self.fail(f"{value!r} is not a valid Mars Date string", param, ctx)
