"""
Tellus - A tool for managing Earth System Model simulations and their data.
"""

# New clean architecture exports
from .domain.entities.simulation import SimulationEntity
from .domain.entities.location import LocationEntity, LocationKind
from .application.services.simulation_service import SimulationApplicationService
from .application.services.location_service import LocationApplicationService
from .application.services.archive_service import ArchiveApplicationService
from .application.container import get_service_container
from .infrastructure.adapters.scoutfs_filesystem import ScoutFSFileSystem

__all__ = [
    "SimulationEntity",
    "LocationEntity", 
    "LocationKind",
    "SimulationApplicationService",
    "LocationApplicationService",
    "ArchiveApplicationService",
    "get_service_container",
    "ScoutFSFileSystem",
]
