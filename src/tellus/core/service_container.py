"""
Service container for dependency injection into CLI commands.

This module provides centralized service management for the new architecture,
allowing CLI commands to access application services while maintaining
compatibility with the legacy system.
"""

from typing import Optional
import logging
from pathlib import Path

from ..application.service_factory import ApplicationServiceFactory
from ..infrastructure.repositories.json_simulation_repository import JsonSimulationRepository
from ..infrastructure.repositories.json_location_repository import JsonLocationRepository
from ..application.dtos import CacheConfigurationDto

logger = logging.getLogger(__name__)


class ServiceContainer:
    """Dependency injection container for CLI services."""
    
    def __init__(self, config_path: Optional[Path] = None):
        self._config_path = config_path or Path.cwd()
        self._service_factory: Optional[ApplicationServiceFactory] = None
        
    @property
    def service_factory(self) -> ApplicationServiceFactory:
        """Get or create the application service factory."""
        if self._service_factory is None:
            # Initialize repositories with JSON backends
            simulation_repo = JsonSimulationRepository(
                file_path=self._config_path / "simulations.json"
            )
            location_repo = JsonLocationRepository(
                file_path=self._config_path / "locations.json"
            )
            
            # Configure cache settings
            cache_config = CacheConfigurationDto(
                cache_directory=str(Path.home() / ".cache" / "tellus"),
                archive_size_limit=50 * 1024**3,  # 50 GB
                file_size_limit=10 * 1024**3,  # 10 GB
                cleanup_policy="lru",
                unified_cache=False
            )
            
            self._service_factory = ApplicationServiceFactory(
                simulation_repository=simulation_repo,
                location_repository=location_repo,
                cache_config=cache_config
            )
            
            logger.info("Service factory initialized with repositories")
            
        return self._service_factory
    
    def reset(self):
        """Reset the service container (useful for testing)."""
        self._service_factory = None
        logger.debug("Service container reset")


# Global service container instance
_service_container: Optional[ServiceContainer] = None


def get_service_container() -> ServiceContainer:
    """Get the global service container instance."""
    global _service_container
    if _service_container is None:
        _service_container = ServiceContainer()
    return _service_container


def set_service_container(container: ServiceContainer):
    """Set the global service container (useful for testing)."""
    global _service_container
    _service_container = container