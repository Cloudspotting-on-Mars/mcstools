def test_read_l1b(l1b_reader):
    data = l1b_reader.read("test/top.L1B")
    assert data.shape == (5, 262)
    assert data.columns.to_list() == l1b_reader.output_columns

def test_read_l2_ddr1(l2_pds_reader):
    data = l2_pds_reader.read("test/top.L2", "DDR1")
    assert data.shape == (1, 79)

def test_read_l2_ddr2(l2_pds_reader):
    data = l2_pds_reader.read("test/top.L2", "DDR2")
    assert data.shape == (105, 18)