import datetime as dt

def test_l1b_pds_filename(l1b_pds_filename_builder):
    fname = l1b_pds_filename_builder.make_filename_from_filestr("120801160000")
    assert "https://atmos" in fname
    assert "201208" in fname
    assert "RDR.TAB" in fname


def test_l1b_dir_filename(l1b_dir_filename_builder):
    fname = l1b_dir_filename_builder.make_filename_from_filestr("120801160000")
    assert "testdir/level_1b" in fname
    assert "1208/120801160000.L1B" in fname


def test_l2_pds_filename(l2_pds_filename_builder):
    fname = l2_pds_filename_builder.make_filename_from_filestr("120801160000")
    assert "https://atmos" in fname
    assert "201208" in fname
    assert "DDR.TAB"


def test_l2_dir_filename(l2_dir_filename_builder):
    fname = l2_dir_filename_builder.make_filename_from_filestr("120801160000")
    assert "testdir/level_2_2d" in fname
    assert "1208/120801160000.L2" in fname

def test_build_mrom_str(l1b_pds_fileformatter, l2_pds_fileformatter):
    date_0 = dt.datetime(2006, 9, 1)
    date_1 = dt.datetime(2021, 1, 1)
    assert l1b_pds_fileformatter.build_mromstr(date_0) == "MROM_1001"
    assert l2_pds_fileformatter.build_mromstr(date_0) == "MROM_2001"
    assert l1b_pds_fileformatter.build_mromstr(date_1) == "MROM_1173"
    assert l2_pds_fileformatter.build_mromstr(date_1) == "MROM_2173"

def test_pds_build_filename_from_filestr(l1b_pds_fileformatter, l2_pds_fileformatter):
    fname_l1b = l1b_pds_fileformatter.build_filename_from_filestr("210101000000")
    fname_l2 = l2_pds_fileformatter.build_filename_from_filestr("210101000000")
    assert fname_l1b == 'https://atmos.nmsu.edu/PDS/data/MROM_1173/DATA/2021/202101/20210101/2021010100_RDR.TAB'
    assert fname_l2 == 'https://atmos.nmsu.edu/PDS/data/MROM_2173/DATA/2021/202101/20210101/2021010100_DDR.TAB'



