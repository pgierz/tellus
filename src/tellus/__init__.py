"""
Tellus - A tool for managing Earth System Model simulations and their data.
"""

from .application.container import get_service_container
from .application.services.location_service import LocationApplicationService
from .application.services.simulation_service import \
    SimulationApplicationService
from .domain.entities.location import LocationEntity, LocationKind
# Domain entities
from .domain.entities.simulation import SimulationEntity
from .infrastructure.adapters.scoutfs_filesystem import ScoutFSFileSystem

# Convenience aliases for cleaner user-facing API
Simulation = SimulationEntity
Location = LocationEntity
SimulationService = SimulationApplicationService
LocationService = LocationApplicationService

__all__ = [
    # Domain entities (full names)
    "SimulationEntity",
    "LocationEntity",
    "LocationKind",
    # Application services (full names)
    "SimulationApplicationService",
    "LocationApplicationService",
    "get_service_container",
    # Infrastructure adapters
    "ScoutFSFileSystem",
    # Convenience aliases
    "Simulation",
    "Location",
    "SimulationService",
    "LocationService",
]
