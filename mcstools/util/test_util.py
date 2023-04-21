import numpy as np
import pytest

from mcstools.util.geom import (
    mcs_view_angle_to_sc_body_frame,
    scattering_angle,
    spherical_to_cartesian,
)


def test_s2c(sphere_coords, cart_coords):
    """
    Test spherical-->cartesian function
    """
    convert = spherical_to_cartesian(sphere_coords)
    assert convert == pytest.approx(cart_coords)


def test_mcs_sc_transform():
    sc_frame_0 = mcs_view_angle_to_sc_body_frame(60.4, 261.6)
    sc_frame_1 = mcs_view_angle_to_sc_body_frame(110.1, 270)
    sc_frame_2 = mcs_view_angle_to_sc_body_frame(111, 180)
    assert pytest.approx(sc_frame_0, 0.005) == np.array([0.127, 0.860, -0.494])
    assert pytest.approx(sc_frame_1, 0.005) == np.array([0, 0.939, 0.344])
    assert pytest.approx(sc_frame_2, 0.005) == np.array([0.933, 0, 0.360])


def test_scattering_angle(cart_coords):
    """
    Test scattering angle. 0=forward, 180=back
    """
    assert pytest.approx(scattering_angle(cart_coords, -cart_coords)) == 0.0
    assert pytest.approx(scattering_angle(cart_coords, cart_coords)) == 180.0
