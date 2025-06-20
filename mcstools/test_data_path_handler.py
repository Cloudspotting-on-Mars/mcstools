import datetime as dt

import mcstools.data_path_handler as dph


def test_dt_filestr_conversions():
    ffb = dph.FileFormatterBase()
    date = dt.datetime(2022, 4, 1, 8, 2, 4)
    assert ffb.format_dt_as_filestr(date) == "220401080204"
    assert ffb.convert_dt_to_filestr(date) == "220401080000"
    assert ffb.convert_filestr_to_dt("220401080000") == dt.datetime(
        2022, 4, 1, 8, tzinfo=dt.timezone.utc
    )


def test_build_mrom_str(l1b_pds_fileformatter, l2_pds_fileformatter):
    date_0 = dt.datetime(2006, 9, 1)
    date_1 = dt.datetime(2021, 1, 1)
    assert l1b_pds_fileformatter.build_mromstr(date_0) == "MROM_1001"
    assert l2_pds_fileformatter.build_mromstr(date_0) == "MROM_2001"
    assert l1b_pds_fileformatter.build_mromstr(date_1) == "MROM_1173"
    assert l2_pds_fileformatter.build_mromstr(date_1) == "MROM_2173"


def test_l2_pds_filename(l2_pds_filename_builder):
    fname = l2_pds_filename_builder.make_filename_from_filestr("120801160000")
    assert "https://atmos" in fname
    assert "201208" in fname
    assert "DDR.TAB"


def test_l1b_pds_filename(l1b_pds_filename_builder):
    fname = l1b_pds_filename_builder.make_filename_from_filestr("120801160000")
    assert "https://atmos" in fname
    assert "201208" in fname
    assert "RDR.TAB" in fname


def test_pds_build_filename_from_filestr(l1b_pds_fileformatter, l2_pds_fileformatter):
    fname_l1b = l1b_pds_fileformatter.build_filename_from_filestr("210101000000")
    fname_l2 = l2_pds_fileformatter.build_filename_from_filestr("210101000000")
    assert fname_l1b == (
        "https://atmos.nmsu.edu/PDS/data/MROM_1173/"
        "DATA/2021/202101/20210101/2021010100_RDR.TAB"
    )
    assert fname_l2 == (
        "https://atmos.nmsu.edu/PDS/data/MROM_2173/"
        "DATA/2021/202101/20210101/2021010100_DDR.TAB"
    )


def test_setup_dir_paths():
    input_path = "path/to/mcs/data/"
    l1b_given_path = dph.DirectoryFileFormatter("L1B", mcs_data_path=input_path)
    assert l1b_given_path.level_directory == "path/to/mcs/data/level_1b"
    l1b_from_env = dph.DirectoryFileFormatter("L1B", mcs_data_path=None)
    assert l1b_from_env.level_directory == "testdir/level_1b"
    l2_given_path = dph.DirectoryFileFormatter("L2", mcs_data_path=input_path)
    assert l2_given_path.level_directory == "path/to/mcs/data/level_2_2d"
    l2_from_env = dph.DirectoryFileFormatter("L2", mcs_data_path=None)
    assert l2_from_env.level_directory == "testdir/level_2_2d"


def test_l2_dir_filename(l2_dir_filename_builder):
    fname = l2_dir_filename_builder.make_filename_from_filestr("120801160000")
    assert "testdir/level_2_2d" in fname
    assert "1208/120801160000.L2" in fname


def test_l2_dir_filenames_daterange(l2_dir_filename_builder):
    fnames = l2_dir_filename_builder.make_filenames_from_daterange(
        dt.datetime(2016, 1, 1), dt.datetime(2016, 1, 2)
    )
    assert fnames == [
        "testdir/level_2_2d/1601/160101000000.L2",
        "testdir/level_2_2d/1601/160101040000.L2",
        "testdir/level_2_2d/1601/160101080000.L2",
        "testdir/level_2_2d/1601/160101120000.L2",
        "testdir/level_2_2d/1601/160101160000.L2",
        "testdir/level_2_2d/1601/160101200000.L2",
    ]


def test_l1b_dir_filename(l1b_dir_filename_builder):
    fname = l1b_dir_filename_builder.make_filename_from_filestr("120801160000")
    assert "testdir/level_1b" in fname
    assert "1208/120801160000.L1B" in fname
