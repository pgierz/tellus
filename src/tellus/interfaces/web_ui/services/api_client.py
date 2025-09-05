"""
API client for interfacing with Tellus backend services.

This module provides a unified client for communicating with both the main
Tellus API and the chat API.
"""

try:
    import httpx
except ImportError:
    print("Warning: httpx not installed. Install with: pip install httpx")
    httpx = None
    
from typing import List, Optional, Dict, Any

try:
    from ...application.dtos import SimulationDto, LocationDto
except ImportError:
    # Use mock classes if DTOs not available
    from dataclasses import dataclass, field
    
    @dataclass
    class SimulationDto:
        simulation_id: str
        uid: str
        attributes: Dict[str, Any] = field(default_factory=dict)
        locations: Dict[str, Dict[str, Any]] = field(default_factory=dict)
        workflows: Dict[str, Any] = field(default_factory=dict)
    
    @dataclass  
    class LocationDto:
        name: str
        kinds: List[str] = field(default_factory=list)
        protocol: str = "file"
        path: Optional[str] = None
        storage_options: Dict[str, Any] = field(default_factory=dict)
        additional_config: Dict[str, Any] = field(default_factory=dict)
        is_remote: bool = False
        is_accessible: Optional[bool] = None
        last_verified: Optional[str] = None


class TellusAPIClient:
    """Client for Tellus API services."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        """Initialize the API client."""
        self.base_url = base_url
        self.chat_url = f"{base_url}/chat"
        self.client = httpx.AsyncClient() if httpx else None
    
    async def get_simulations(self) -> List[SimulationDto]:
        """Get list of simulations from the backend."""
        # For now, this would be a placeholder since the main REST API
        # doesn't exist yet. In the future this would call:
        # response = await self.client.get(f"{self.base_url}/api/v1/simulations")
        # return [SimulationDto(**sim) for sim in response.json()]
        
        return []  # Placeholder
    
    async def get_simulation(self, simulation_id: str) -> Optional[SimulationDto]:
        """Get a specific simulation by ID."""
        # Placeholder for future API call
        return None
    
    async def get_locations(self) -> List[LocationDto]:
        """Get list of locations from the backend."""
        # Placeholder for future API call
        return []
    
    async def get_location(self, location_name: str) -> Optional[LocationDto]:
        """Get a specific location by name."""
        # Placeholder for future API call
        return None
    
    async def chat(self, message: str, conversation_id: Optional[str] = None) -> Dict[str, Any]:
        """Send a chat message to the Tellus Chat API."""
        try:
            payload = {
                "message": message,
                "stream": False
            }
            if conversation_id:
                payload["conversation_id"] = conversation_id
                
            response = await self.client.post(self.chat_url, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    async def get_conversation(self, conversation_id: str) -> List[Dict[str, Any]]:
        """Get conversation history from the chat API."""
        try:
            response = await self.client.get(f"{self.base_url}/conversations/{conversation_id}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return []
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.client.aclose()