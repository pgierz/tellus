"""
Application service container for dependency injection.

This module provides centralized service management and configuration for the
Tellus application, wiring together repositories, services, and configuration
for use by interface layers (CLI, TUI, REST API, etc.).
"""

from typing import Optional
import logging
from pathlib import Path

from ..application.service_factory import ApplicationServiceFactory
from ..infrastructure.repositories.json_simulation_repository import JsonSimulationRepository
from ..infrastructure.repositories.json_location_repository import JsonLocationRepository
from ..infrastructure.repositories.json_archive_repository import JsonArchiveRepository
from ..infrastructure.repositories.json_progress_tracking_repository import JsonProgressTrackingRepository
from ..application.services.progress_tracking_service import ProgressTrackingService
from ..application.dtos import CacheConfigurationDto

logger = logging.getLogger(__name__)


class ServiceContainer:
    """Dependency injection container for Tellus application services."""
    
    def __init__(self, config_path: Optional[Path] = None):
        self._config_path = config_path or Path.cwd()
        self._service_factory: Optional[ApplicationServiceFactory] = None
        self._progress_tracking_service: Optional[ProgressTrackingService] = None
        
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
            archive_repo = JsonArchiveRepository(
                file_path=self._config_path / "archives.json"
            )
            progress_tracking_repo = JsonProgressTrackingRepository(
                storage_path=str(Path.home() / ".tellus" / "progress_tracking.json")
            )
            
            # Configure cache settings
            cache_config = CacheConfigurationDto(
                cache_directory=str(Path.home() / ".cache" / "tellus"),
                archive_size_limit=50 * 1024**3,  # 50 GB
                file_size_limit=10 * 1024**3,  # 10 GB
                cleanup_policy="lru",
                unified_cache=False
            )
            
            # Initialize progress tracking service
            self._progress_tracking_service = ProgressTrackingService(
                repository=progress_tracking_repo,
                max_workers=4,
                notification_queue_size=1000
            )
            
            self._service_factory = ApplicationServiceFactory(
                simulation_repository=simulation_repo,
                location_repository=location_repo,
                archive_repository=archive_repo,
                progress_tracking_service=self._progress_tracking_service,
                cache_config=cache_config
            )
            
            logger.info("Service factory initialized with repositories")
            
        return self._service_factory
    
    @property
    def progress_tracking_service(self) -> ProgressTrackingService:
        """Get the progress tracking service."""
        # Ensure service factory is initialized first
        _ = self.service_factory
        return self._progress_tracking_service
    
    def reset(self):
        """Reset the service container (useful for testing)."""
        self._service_factory = None
        self._progress_tracking_service = None
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