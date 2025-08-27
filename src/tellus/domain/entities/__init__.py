"""Domain entities - Pure business objects without infrastructure dependencies."""

from .location import LocationEntity, LocationKind, PathTemplate
from .simulation import SimulationEntity

__all__ = ['LocationEntity', 'LocationKind', 'PathTemplate', 'SimulationEntity']