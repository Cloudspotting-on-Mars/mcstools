import pytest

from mcstools.util.mars_time import MarsDate


@pytest.fixture()
def marsdate():
    return MarsDate(33, 180)