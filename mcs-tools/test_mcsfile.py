def test_rad_names(l1b_file):
    assert l1b_file.make_rad_col_name("A2", 5) == "Rad_A2_05"
    assert "Rad_A2_05" in l1b_file.make_rad_col_names("A2")
