"""
Application service container for dependency injection.

This module provides centralized service management and configuration for the
Tellus application, wiring together repositories, services, and configuration
for use by interface layers (CLI, TUI, REST API, etc.).
"""

import logging
from pathlib import Path
from typing import Optional

from ..application.dtos import CacheConfigurationDto
from ..application.service_factory import ApplicationServiceFactory
from ..application.services.progress_tracking_service import \
    ProgressTrackingService
from ..infrastructure.repositories.json_location_repository import \
    JsonLocationRepository
from ..infrastructure.repositories.json_progress_tracking_repository import \
    JsonProgressTrackingRepository
from ..infrastructure.repositories.json_simulation_repository import \
    JsonSimulationRepository
from ..infrastructure.repositories.json_simulation_file_repository import \
    JsonSimulationFileRepository
from ..infrastructure.repositories.json_network_topology_repository import \
    JsonNetworkTopologyRepository
from ..infrastructure.adapters.network_benchmarking_adapter import \
    CachedNetworkBenchmarkingAdapter
from ..application.services.network_topology_service import \
    NetworkTopologyApplicationService

logger = logging.getLogger(__name__)


class ServiceContainer:
    """Dependency injection container for Tellus application services."""
    
    def __init__(self, config_path: Optional[Path] = None, project_path: Optional[Path] = None):
        """
        Initialize the service container with hybrid data persistence paths.
        
        Args:
            config_path: Configuration path parameter
            project_path: Path to the current project directory (defaults to current working directory)
        """
        # For backward compatibility, config_path overrides project_path if provided
        if config_path is not None:
            self._project_path = config_path
        else:
            self._project_path = project_path or Path.cwd()
            
        # Global data directory in user home
        self._global_data_path = Path.home() / ".tellus"
        
        # Project-specific data directory
        self._project_data_path = self._project_path / ".tellus"
        
        self._service_factory: Optional[ApplicationServiceFactory] = None
        self._progress_tracking_service: Optional[ProgressTrackingService] = None
        self._network_topology_service: Optional[NetworkTopologyApplicationService] = None
        
        # Ensure directories exist
        self._global_data_path.mkdir(parents=True, exist_ok=True)
        self._project_data_path.mkdir(parents=True, exist_ok=True)
    
    @property
    def global_data_path(self) -> Path:
        """Get the global data directory path (~/.tellus/)."""
        return self._global_data_path
    
    @property
    def project_data_path(self) -> Path:
        """Get the project-specific data directory path (<project>/.tellus/).""" 
        return self._project_data_path
    
    @property
    def project_path(self) -> Path:
        """Get the current project directory path."""
        return self._project_path
        
    @property
    def service_factory(self) -> ApplicationServiceFactory:
        """Get or create the application service factory."""
        if self._service_factory is None:
            # Initialize repositories with hybrid data persistence
            # Global data (shared across projects)
            location_repo = JsonLocationRepository(
                file_path=self._global_data_path / "locations.json"
            )
            progress_tracking_repo = JsonProgressTrackingRepository(
                storage_path=str(self._global_data_path / "progress_tracking.json")
            )
            
            # Project-specific data
            simulation_repo = JsonSimulationRepository(
                file_path=self._project_data_path / "simulations.json"
            )
            simulation_file_repo = JsonSimulationFileRepository(
                storage_dir=str(self._project_data_path)
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
                simulation_file_repository=simulation_file_repo,
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
    
    def get_network_topology_service(self) -> NetworkTopologyApplicationService:
        """Get or create the network topology service."""
        if self._network_topology_service is None:
            # Initialize network topology repository (global data)
            topology_repo = JsonNetworkTopologyRepository(
                storage_file=self._global_data_path / "network_topologies.json"
            )
            
            # Initialize benchmarking adapter with caching
            benchmarking_adapter = CachedNetworkBenchmarkingAdapter(
                cache_ttl_hours=24.0,
                temp_dir=Path.home() / ".cache" / "tellus" / "network_bench",
                enable_file_transfer_tests=True,
                test_file_size_mb=10
            )
            
            # Get location repository from service factory
            location_repo = self.service_factory.get_location_repository()
            
            # Create network topology service
            self._network_topology_service = NetworkTopologyApplicationService(
                location_repo=location_repo,
                topology_repo=topology_repo,
                benchmarking_adapter=benchmarking_adapter
            )
            
            logger.info("Network topology service initialized")
        
        return self._network_topology_service
    
    def network_topology_service(self) -> NetworkTopologyApplicationService:
        """Get the network topology application service."""
        return self.get_network_topology_service()
    
    def get_location_repository(self):
        """Get the location repository."""
        return self.service_factory.get_location_repository()
    
    def reset(self):
        """Reset the service container (useful for testing)."""
        self._service_factory = None
        self._progress_tracking_service = None
        self._network_topology_service = None
        logger.debug("Service container reset")


# Global service container instance
_service_container: Optional[ServiceContainer] = None


def _detect_project_directory() -> Path:
    """
    Detect the current project directory by looking for tellus project markers.
    
    Returns:
        Path: The detected project directory, or current working directory if no markers found
    """
    current = Path.cwd()
    
    # Look for tellus project markers in current directory and parents
    for path in [current] + list(current.parents):
        # Check for existing .tellus directory
        if (path / ".tellus").exists():
            return path
        
        # Check for other tellus project indicators (pyproject.toml with tellus, etc.)
        pyproject_file = path / "pyproject.toml"
        if pyproject_file.exists():
            try:
                # Try to read pyproject.toml to detect tellus project
                with open(pyproject_file, 'r') as f:
                    content = f.read()
                    # Simple string search (avoiding toml dependency)
                    if 'name = "tellus"' in content or "name = 'tellus'" in content:
                        return path
            except Exception:
                pass
                
    # Default to current working directory
    return current


def get_service_container() -> ServiceContainer:
    """Get the global service container instance with project directory detection."""
    global _service_container
    if _service_container is None:
        project_path = _detect_project_directory()
        _service_container = ServiceContainer(project_path=project_path)
    return _service_container


def set_service_container(container: ServiceContainer):
    """Set the global service container (useful for testing)."""
    global _service_container
    _service_container = container