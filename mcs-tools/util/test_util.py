import pytest

from util.geom import scattering_angle, spherical_to_cartesian


def test_s2c(sphere_coords, cart_coords):
    """
    Test spherical-->cartesian function
    """
    convert = spherical_to_cartesian(sphere_coords)
    assert convert == pytest.approx(cart_coords)


def test_scattering_angle(cart_coords):
    """
    Test scattering angle. 0=forward, 180=back
    """
    assert pytest.approx(scattering_angle(cart_coords, -cart_coords)) == 0.0
    assert pytest.approx(scattering_angle(cart_coords, cart_coords)) == 180.0
