def test_read_l1b(l1b_reader, l1b_file):
    data = l1b_reader.read("test/top.L1B")
    assert data.shape == (5, 262)
    assert data.columns.to_list() == l1b_reader.output_columns
