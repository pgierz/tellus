"""
Tests for Simulation objects
"""

import pytest


@pytest.fixture
def basic_sim():
    from tellus.simulation import Simulation

    return Simulation("/my/path/to/simulation")


def test_import_Simulation_from_submodule():
    from tellus.simulation import Simulation


def test_import_Simulation_from_top_level():
    from tellus import Simulation


def test_init_Simulation(basic_sim):
    assert basic_sim


def test_Simulation_basic_properties(basic_sim):
    assert basic_sim
    assert hasattr(basic_sim, "attrs")
    assert hasattr(basic_sim, "data")
    assert hasattr(basic_sim, "namelists")  # Maybe not?
    assert hasattr(basic_sim, "locations")
