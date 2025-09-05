"""
API client for the Tellus web UI.

This module provides a unified interface for communicating with:
- The main Tellus REST API (when implemented)
- The existing tellus_chat API
- Direct calls to Tellus services
"""

import asyncio
import aiohttp
import json
from typing import Dict, List, Any, Optional
from dataclasses import asdict

# Import Tellus DTOs
from ....application.dtos import (
    SimulationDto, CreateSimulationDto, UpdateSimulationDto,
    LocationDto, CreateLocationDto, UpdateLocationDto,
    FilterOptions, PaginationInfo
)

# Import Tellus services for direct calls
from ....application.service_factory import ServiceFactory
from ....application.container import Container


class TellusApiClient:
    """
    API client for the Tellus web UI.
    
    This provides a unified interface that can work with:
    1. Future REST API endpoints
    2. Direct service calls to the existing Tellus backend
    3. The tellus_chat API for AI features
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        chat_api_url: str = "http://localhost:8000",
        use_direct_services: bool = True
    ):
        self.base_url = base_url or "http://localhost:8001"  # Future main API
        self.chat_api_url = chat_api_url
        self.use_direct_services = use_direct_services
        
        # Initialize service container for direct service calls
        if use_direct_services:
            self.container = Container()
            self.service_factory = ServiceFactory(self.container)
    
    # Simulation API methods
    
    async def get_simulations(
        self,
        filters: Optional[FilterOptions] = None,
        pagination: Optional[PaginationInfo] = None
    ) -> Dict[str, Any]:
        """Get list of simulations."""
        
        if self.use_direct_services:
            # Use direct service calls
            try:
                simulation_service = self.service_factory.get_simulation_service()
                
                # Convert filters and pagination to service format
                # For now, return mock data since the service interface may vary
                simulations = [
                    {
                        "simulation_id": "CESM2_hist_001",
                        "uid": "sim_001_uid",
                        "model_id": "CESM2",
                        "attributes": {
                            "experiment": "historical",
                            "variant": "r1i1p1f1",
                            "resolution": "f09_g17",
                            "start_year": 1850,
                            "end_year": 2014
                        },
                        "locations": {
                            "hpc_storage": {"path_prefix": "{model}/{experiment}"},
                            "archive_tape": {"path_prefix": "archives/{model}"}
                        },
                        "status": "completed"
                    },
                    {
                        "simulation_id": "CESM2_ssp585_001",
                        "uid": "sim_002_uid", 
                        "model_id": "CESM2",
                        "attributes": {
                            "experiment": "ssp585",
                            "variant": "r1i1p1f1",
                            "resolution": "f09_g17",
                            "start_year": 2015,
                            "end_year": 2100
                        },
                        "locations": {
                            "hpc_storage": {"path_prefix": "{model}/{experiment}"}
                        },
                        "status": "running"
                    }
                ]
                
                return {
                    "simulations": simulations,
                    "total_count": len(simulations),
                    "pagination": {
                        "page": 1,
                        "page_size": 50,
                        "has_next": False,
                        "has_previous": False
                    }
                }
            except Exception as e:
                # Fallback to mock data
                return self._get_mock_simulations()
        else:
            # Use HTTP API when available
            return await self._http_get("/api/v1/simulations", {
                "filters": asdict(filters) if filters else None,
                "pagination": asdict(pagination) if pagination else None
            })
    
    async def get_simulation(self, simulation_id: str) -> Dict[str, Any]:
        """Get a specific simulation by ID."""
        
        if self.use_direct_services:
            # Use direct service call
            simulations = await self.get_simulations()
            for sim in simulations["simulations"]:
                if sim["simulation_id"] == simulation_id:
                    return sim
            raise ValueError(f"Simulation {simulation_id} not found")
        else:
            return await self._http_get(f"/api/v1/simulations/{simulation_id}")
    
    async def create_simulation(self, simulation_data: CreateSimulationDto) -> Dict[str, Any]:
        """Create a new simulation."""
        
        if self.use_direct_services:
            # Use direct service call
            try:
                simulation_service = self.service_factory.get_simulation_service()
                # TODO: Implement actual service call
                # For now, return mock created simulation
                return {
                    "simulation_id": simulation_data.simulation_id,
                    "uid": f"sim_{hash(simulation_data.simulation_id)}_uid",
                    "model_id": simulation_data.model_id,
                    "attributes": simulation_data.attrs,
                    "locations": {},
                    "status": "created"
                }
            except Exception as e:
                raise Exception(f"Failed to create simulation: {str(e)}")
        else:
            return await self._http_post("/api/v1/simulations", asdict(simulation_data))
    
    # Location API methods
    
    async def get_locations(
        self,
        filters: Optional[FilterOptions] = None,
        pagination: Optional[PaginationInfo] = None
    ) -> Dict[str, Any]:
        """Get list of locations."""
        
        if self.use_direct_services:
            # Use direct service calls
            try:
                location_service = self.service_factory.get_location_service()
                # TODO: Implement actual service call
                # For now, return mock data
                return self._get_mock_locations()
            except Exception as e:
                return self._get_mock_locations()
        else:
            return await self._http_get("/api/v1/locations", {
                "filters": asdict(filters) if filters else None,
                "pagination": asdict(pagination) if pagination else None
            })
    
    async def test_location(self, location_name: str) -> Dict[str, Any]:
        """Test connectivity to a location."""
        
        if self.use_direct_services:
            # Mock test result
            import random
            success = random.choice([True, True, True, False])
            return {
                "location_name": location_name,
                "success": success,
                "latency_ms": random.uniform(50, 500) if success else None,
                "available_space": random.randint(100, 10000) * 1024**3 if success else None,
                "error_message": None if success else "Connection timeout"
            }
        else:
            return await self._http_post(f"/api/v1/locations/{location_name}/test")
    
    # Chat API methods
    
    async def send_chat_message(
        self,
        message: str,
        conversation_id: Optional[str] = None,
        stream: bool = False
    ) -> Dict[str, Any]:
        """Send a message to the AI chat interface."""
        
        payload = {
            "message": message,
            "stream": stream,
            "conversation_id": conversation_id
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.chat_api_url}/chat", json=payload) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    raise Exception(f"Chat API error: {error_text}")
    
    async def get_conversation_history(self, conversation_id: str) -> List[Dict[str, Any]]:
        """Get chat conversation history."""
        
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.chat_api_url}/conversations/{conversation_id}") as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception("Failed to load conversation history")
    
    # HTTP client methods
    
    async def _http_get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make HTTP GET request."""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}{endpoint}", params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception(f"API error: {response.status}")
    
    async def _http_post(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make HTTP POST request."""
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.base_url}{endpoint}", json=data) as response:
                if response.status in (200, 201):
                    return await response.json()
                else:
                    raise Exception(f"API error: {response.status}")
    
    # Mock data methods
    
    def _get_mock_simulations(self) -> Dict[str, Any]:
        """Get mock simulation data."""
        simulations = [
            {
                "simulation_id": "CESM2_hist_001",
                "uid": "sim_001_uid",
                "model_id": "CESM2",
                "attributes": {
                    "experiment": "historical",
                    "variant": "r1i1p1f1",
                    "resolution": "f09_g17",
                    "start_year": 1850,
                    "end_year": 2014
                },
                "locations": {
                    "hpc_storage": {"path_prefix": "{model}/{experiment}"},
                    "archive_tape": {"path_prefix": "archives/{model}"}
                },
                "status": "completed"
            },
            {
                "simulation_id": "CESM2_ssp585_001",
                "uid": "sim_002_uid",
                "model_id": "CESM2", 
                "attributes": {
                    "experiment": "ssp585",
                    "variant": "r1i1p1f1",
                    "resolution": "f09_g17",
                    "start_year": 2015,
                    "end_year": 2100
                },
                "locations": {
                    "hpc_storage": {"path_prefix": "{model}/{experiment}"}
                },
                "status": "running"
            }
        ]
        
        return {
            "simulations": simulations,
            "total_count": len(simulations),
            "pagination": {
                "page": 1,
                "page_size": 50,
                "has_next": False,
                "has_previous": False
            }
        }
    
    def _get_mock_locations(self) -> Dict[str, Any]:
        """Get mock location data."""
        locations = [
            {
                "name": "hpc_storage",
                "protocol": "sftp",
                "path": "/work/simulations",
                "kinds": ["COMPUTE", "DISK"],
                "storage_options": {
                    "host": "hpc.example.com",
                    "username": "user",
                    "port": 22
                },
                "is_remote": True,
                "is_accessible": True,
                "last_verified": "2024-01-15T10:30:00Z"
            },
            {
                "name": "local_cache",
                "protocol": "file",
                "path": "/home/user/.cache/tellus",
                "kinds": ["DISK"],
                "storage_options": {},
                "is_remote": False,
                "is_accessible": True,
                "last_verified": "2024-01-15T10:35:00Z"
            }
        ]
        
        return {
            "locations": locations,
            "total_count": len(locations),
            "pagination": {
                "page": 1,
                "page_size": 50,
                "has_next": False,
                "has_previous": False
            }
        }


# Singleton instance for use throughout the app
api_client = TellusApiClient()