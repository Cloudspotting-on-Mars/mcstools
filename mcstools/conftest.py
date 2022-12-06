import numpy as np
import pytest
from mcsfile import MCSL1BFile
from reader import MCSL1BReader


@pytest.fixture()
def l1b_file():
    return MCSL1BFile()


@pytest.fixture()
def l1b_reader():
    return MCSL1BReader()


@pytest.fixture()
def sphere_coords():
    return np.array([8, np.pi / 6, np.pi / 4])


@pytest.fixture()
def cart_coords():
    return np.array([2 * np.sqrt(2), 2 * np.sqrt(2), 4 * np.sqrt(3)])
