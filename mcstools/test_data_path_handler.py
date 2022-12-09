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