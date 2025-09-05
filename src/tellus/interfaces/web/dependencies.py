"""
FastAPI dependency injection for Tellus services.

This module provides dependency injection functions that connect the FastAPI
routes to the application services through the service container.
"""

from fastapi import Request, Depends

from ...application.container import ServiceContainer
from ...application.services.simulation_service import SimulationApplicationService
from ...application.services.location_service import LocationApplicationService
from ...application.services.unified_file_service import UnifiedFileService


def get_service_container(request: Request) -> ServiceContainer:
    """
    Get the service container from the FastAPI app state.
    
    Args:
        request: FastAPI request object
        
    Returns:
        Service container instance
        
    Raises:
        RuntimeError: If service container is not available
    """
    container = getattr(request.app.state, 'container', None)
    if not container:
        raise RuntimeError("Service container not initialized")
    return container


def get_simulation_service(
    container: ServiceContainer = Depends(get_service_container)
) -> SimulationApplicationService:
    """
    Get the simulation service from the container.
    
    Args:
        container: Service container instance
        
    Returns:
        Simulation service instance
    """
    return container.service_factory.simulation_service


def get_location_service(
    container: ServiceContainer = Depends(get_service_container)
) -> LocationApplicationService:
    """
    Get the location service from the container.
    
    Args:
        container: Service container instance
        
    Returns:
        Location service instance
    """
    return container.service_factory.location_service


def get_unified_file_service(
    container: ServiceContainer = Depends(get_service_container)
) -> UnifiedFileService:
    """
    Get the unified file service from the container.
    
    Args:
        container: Service container instance
        
    Returns:
        Unified file service instance
    """
    return container.service_factory.unified_file_service