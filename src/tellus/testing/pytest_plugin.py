"""Pytest plugin for tellus.

This module makes test fixtures available to users of the tellus package.
"""

from .fixtures import sample_simulation_awi_locations_with_laptop

# This makes the fixture available to users when they install the package
__all__ = ["sample_simulation_awi_locations_with_laptop"]
