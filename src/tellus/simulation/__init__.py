"""
Tellus Simulation Module

This module provides simulation functionality for the Tellus system,
including location management, simulation state, and data handling.
"""

# Import core simulation classes
from .simulation import (
    Simulation, 
    SimulationExistsError, 
    CacheConfig, 
    CacheManager, 
    CompressedArchive,
    ArchiveRegistry,
    PathMapper,
    PathMapping,
    TagSystem
)

# Make these available at the package level
__all__ = [
    'Simulation',
    'SimulationExistsError',
    'CacheConfig',
    'CacheManager',
    'CompressedArchive',
    'ArchiveRegistry',
    'PathMapper',
    'PathMapping',
    'TagSystem',
]
