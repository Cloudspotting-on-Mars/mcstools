from mcstools.radiance_profile import RadianceProfile

def test_from_l1b(l1b_data_sample):
    rp = RadianceProfile.from_l1b_row(l1b_data_sample.iloc[0], "A1")
    assert rp.profile.loc[1] == -2.27589e-02
    assert rp.profile.loc[21] == 7.33347e-02
