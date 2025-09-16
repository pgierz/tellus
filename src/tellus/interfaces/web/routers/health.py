"""
Health check endpoints for the Tellus API.

Provides system health and status information for monitoring and diagnostics.
"""

import time
from datetime import datetime
from typing import Dict, Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from ..version import get_version_info

router = APIRouter()


class HealthResponse(BaseModel):
    """Response model for health check endpoints."""
    status: str
    timestamp: str
    uptime_seconds: float
    version: str
    environment: str = "development"


class DetailedHealthResponse(HealthResponse):
    """Extended health response with system details."""
    services: Dict[str, str]
    memory_usage: Dict[str, Any]
    system_info: Dict[str, Any]


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Basic health check endpoint.
    
    Returns:
        Basic health status and uptime information
    """
    version_info = get_version_info()
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        uptime_seconds=time.time(),
        version=version_info["tellus_version"]
    )


@router.get("/health/detailed", response_model=DetailedHealthResponse)
async def detailed_health_check(request: Request):
    """
    Detailed health check with system information.
    
    Returns:
        Comprehensive health status including service states
    """
    # Get service container from app state
    container = getattr(request.app.state, 'container', None)
    
    services_status = {}
    if container:
        # Check key services by actually trying to access them
        try:
            simulation_service = container.service_factory.simulation_service
            # Try to call a simple method to verify service is working
            _ = getattr(simulation_service, 'list_simulations', None)
            services_status["simulation_service"] = "available"
        except Exception:
            services_status["simulation_service"] = "unavailable"
            
        try:
            location_service = container.service_factory.location_service
            # Try to call a simple method to verify service is working  
            _ = getattr(location_service, 'list_locations', None)
            services_status["location_service"] = "available"
        except Exception:
            services_status["location_service"] = "unavailable"
    else:
        services_status["container"] = "unavailable"
    
    version_info = get_version_info()
    return DetailedHealthResponse(
        status="healthy" if all(v == "available" for v in services_status.values()) else "degraded",
        timestamp=datetime.now().isoformat(),
        uptime_seconds=time.time(),
        version=version_info["tellus_version"],
        services=services_status,
        memory_usage={
            "note": "Memory usage monitoring not yet implemented"
        },
        system_info={
            "api_framework": "FastAPI",
            "python_version": "3.12+",
            "tellus_version": version_info["tellus_version"],
            "api_version": version_info["api_version"]
        }
    )


@router.get("/", response_model=Dict[str, str])
async def api_root():
    """
    API root endpoint with basic information.
    
    Returns:
        Basic API information and documentation links
    """
    version_info = get_version_info()
    api_path = f"/api/{version_info['api_version']}"
    
    return {
        "name": "Tellus Climate Data API",
        "version": version_info["tellus_version"],
        "api_version": version_info["api_version"],
        "description": "REST API for distributed climate simulation data management",
        "docs_url": f"{api_path}/docs",
        "redoc_url": f"{api_path}/redoc",
        "health_url": f"{api_path}/health"
    }


@router.get("/version", response_model=Dict[str, Any])
async def get_version():
    """
    Get detailed version information.
    
    Returns:
        Complete version information including API version, build details, etc.
    """
    return get_version_info()