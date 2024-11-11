from mcstools.radiance_profile import RadianceProfile

# TODO: Add test for altitudes


def test_from_l1b(l1b_data_sample):
    rp = RadianceProfile.from_l1b_row(
        "A1", l1b_data_sample.iloc[0], include_altitudes=False, include_utc=False
    )
    assert rp.profile.loc[1] == -2.27589e-02
    assert rp.profile.loc[21] == 7.33347e-02
