import os

import numpy as np
import pytest

from mcstools.data_path_handler import FilenameBuilder, PDSFileFormatter
from mcstools.mcsfile import L1BFile, L2File
from mcstools.reader import L1BReader, L2Reader

# Set any environment variables
os.environ["MCS_DATA_DIR_BASE"] = "testdir"
os.environ["MCS_LEVEL_1B_SUBDIR"] = "level_1b"
os.environ["MCS_LEVEL_2_SUBDIR"] = "level_2_2d"


@pytest.fixture()
def l1b_file():
    return L1BFile()


@pytest.fixture()
def l2_file():
    return L2File()


@pytest.fixture()
def l1b_reader():
    return L1BReader()


@pytest.fixture()
def l2_pds_reader():
    return L2Reader(pds=True)


@pytest.fixture()
def l2_dir_reader():
    return L2Reader


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
def l1b_pds_fileformatter():
    return PDSFileFormatter("L1B")


@pytest.fixture()
def l2_pds_fileformatter():
    return PDSFileFormatter("L2")


@pytest.fixture()
def sphere_coords():
    return np.array([8, np.pi / 6, np.pi / 4])


@pytest.fixture()
def cart_coords():
    return np.array([2 * np.sqrt(2), 2 * np.sqrt(2), 4 * np.sqrt(3)])


@pytest.fixture()
def l1b_data_sample(l1b_reader):
    data = l1b_reader.read("test/top.L1B")
    return data
