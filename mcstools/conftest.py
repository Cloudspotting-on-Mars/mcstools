import numpy as np
import pytest
from data_path_handler import FilenameBuilder
from mcsfile import MCSL1BFile, MCSL22DFile
from reader import MCSL1BReader, MCSL22DReader


@pytest.fixture()
def l1b_file():
    return MCSL1BFile()


@pytest.fixture()
def l2_file():
    return MCSL22DFile()


@pytest.fixture()
def l1b_reader():
    return MCSL1BReader()


@pytest.fixture()
def l2_pds_reader():
    return MCSL22DReader(pds=True)


@pytest.fixture()
def l2_dir_reader():
    return MCSL22DReader


@pytest.fixture()
def l1b_pds_filename_builder():
    return FilenameBuilder("L1B", pds=True)


@pytest.fixture()
def l1b_dir_filename_builder():
    return FilenameBuilder("L1B", mcs_data_path="testdir")


@pytest.fixture()
def l2_pds_filename_builder():
    return FilenameBuilder("L2", pds=True)


@pytest.fixture()
def l2_dir_filename_builder():
    return FilenameBuilder("L2", mcs_data_path="testdir")


@pytest.fixture()
def sphere_coords():
    return np.array([8, np.pi / 6, np.pi / 4])


@pytest.fixture()
def cart_coords():
    return np.array([2 * np.sqrt(2), 2 * np.sqrt(2), 4 * np.sqrt(3)])
