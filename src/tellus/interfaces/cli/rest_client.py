"""
REST API client for CLI operations.

This module provides client interfaces that communicate with the Tellus REST API,
allowing the CLI to operate in client-server mode instead of direct service injection.
"""

import os
import socket
import functools
from typing import List, Optional, Dict, Any, Callable
from urllib.parse import urljoin
import httpx
import rich_click as click
from rich.console import Console

from ...application.dtos import (
    SimulationDto, CreateSimulationDto, UpdateSimulationDto,
    SimulationListDto, LocationDto, CreateLocationDto, UpdateLocationDto,
    LocationListDto, LocationTestResult, FilterOptions
)

console = Console()


class RestClientError(Exception):
    """Base exception for REST client errors."""
    pass


def handle_rest_errors(func: Callable) -> Callable:
    """
    Decorator to handle REST API errors and convert them to Click exceptions.
    
    This ensures consistent error handling across all CLI commands when using REST API mode.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RestClientError as e:
            # Check if this is a connection error
            if "Request failed" in str(e):
                console.print(f"[red]✗ Could not connect to Tellus API[/red]")
                api_path = get_api_version_path()
                default_url = f"http://localhost:1968{api_path}"
                console.print(f"[dim]Make sure the API server is running at: {os.getenv('TELLUS_API_URL', default_url)}[/dim]")
                console.print(f"[dim]Start the API with: pixi run api[/dim]")
                raise click.ClickException("REST API connection failed")
            else:
                console.print(f"[red]✗ API Error: {str(e)}[/red]")
                raise click.ClickException(str(e))
        except Exception as e:
            console.print(f"[red]✗ Unexpected error: {str(e)}[/red]")
            raise click.ClickException(str(e))
    return wrapper


def get_api_version_path() -> str:
    """
    Get the current API version path dynamically.
    
    Returns:
        API version path (e.g., "/api/v0a3")
    """
    try:
        # Import version utilities
        import sys
        import os
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        sys.path.insert(0, os.path.join(project_root, 'src'))
        
        from tellus.interfaces.web.version import get_version_info
        version_info = get_version_info()
        return version_info["api_path"]
    except Exception:
        # Fallback for older versions or if version detection fails
        return "/api/v0a3"


def discover_api_server(start_port: int = 1968, end_port: int = 2000) -> Optional[str]:
    """
    Auto-discover running Tellus API server in port range.
    
    Args:
        start_port: Start of port range to check
        end_port: End of port range to check
        
    Returns:
        Full API base URL if server found, None otherwise
    """
    api_path = get_api_version_path()
    
    for port in range(start_port, end_port + 1):
        try:
            with httpx.Client(timeout=1.0) as client:
                # Try versioned health endpoint
                response = client.get(f"http://localhost:{port}{api_path}/health")
                if response.status_code == 200:
                    return f"http://localhost:{port}{api_path}/"
        except:
            continue
    return None


class RestApiClient:
    """Base REST API client with common functionality."""
    
    def __init__(self, base_url: Optional[str] = None, timeout: float = 30.0):
        """
        Initialize the REST API client.
        
        Args:
            base_url: Base URL for the API (defaults to TELLUS_API_URL env var or localhost)
            timeout: Request timeout in seconds
        """
        # Try to get URL from environment, fallback to auto-discovery, then default
        if base_url:
            self.base_url = base_url
        elif os.getenv('TELLUS_API_URL'):
            self.base_url = os.getenv('TELLUS_API_URL')
        else:
            # Auto-discover or use versioned default
            discovered_url = discover_api_server()
            if discovered_url:
                self.base_url = discovered_url
            else:
                # Default to versioned API path
                api_path = get_api_version_path()
                self.base_url = f'http://localhost:1968{api_path}'
        
        # Ensure base URL ends with slash for proper urljoin behavior
        if not self.base_url.endswith('/'):
            self.base_url += '/'
        self.timeout = timeout
        self.client = httpx.Client(timeout=self.timeout)
        
    def _handle_response(self, response: httpx.Response) -> Dict[str, Any]:
        """
        Handle HTTP response and raise appropriate errors.
        
        Args:
            response: HTTP response object
            
        Returns:
            JSON response data
            
        Raises:
            RestClientError: For HTTP errors
        """
        try:
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            error_detail = "Unknown error"
            try:
                error_data = response.json()
                error_detail = error_data.get('detail', str(e))
            except:
                error_detail = str(e)
                
            raise RestClientError(f"API request failed: {error_detail}") from e
        except httpx.RequestError as e:
            raise RestClientError(f"Request failed: {str(e)}") from e
            
    def close(self):
        """Close the HTTP client."""
        self.client.close()
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class RestSimulationService:
    """REST client for simulation operations."""
    
    def __init__(self, client: RestApiClient):
        self.client = client
        
    def list_simulations(
        self, 
        page: int = 1, 
        page_size: int = 50, 
        filters: Optional[FilterOptions] = None
    ) -> SimulationListDto:
        """List simulations via REST API."""
        params = {'page': page, 'page_size': page_size}
        if filters and filters.search_term:
            params['search'] = filters.search_term
            
        response = self.client.client.get(
            urljoin(self.client.base_url, 'simulations/'),
            params=params
        )
        data = self.client._handle_response(response)
        return SimulationListDto(**data)
        
    def create_simulation(self, simulation_data: CreateSimulationDto) -> SimulationDto:
        """Create a new simulation via REST API."""
        response = self.client.client.post(
            urljoin(self.client.base_url, 'simulations/'),
            json=simulation_data.model_dump()
        )
        data = self.client._handle_response(response)
        return SimulationDto(**data)
        
    def get_simulation(self, simulation_id: str) -> SimulationDto:
        """Get simulation details via REST API."""
        response = self.client.client.get(
            urljoin(self.client.base_url, f'simulations/{simulation_id}')
        )
        data = self.client._handle_response(response)
        return SimulationDto(**data)
        
    def update_simulation(
        self, 
        simulation_id: str, 
        update_data: UpdateSimulationDto
    ) -> SimulationDto:
        """Update a simulation via REST API."""
        response = self.client.client.put(
            urljoin(self.client.base_url, f'simulations/{simulation_id}'),
            json=update_data.model_dump(exclude_unset=True)
        )
        data = self.client._handle_response(response)
        return SimulationDto(**data)
        
    def delete_simulation(self, simulation_id: str) -> None:
        """Delete a simulation via REST API."""
        response = self.client.client.delete(
            urljoin(self.client.base_url, f'simulations/{simulation_id}')
        )
        self.client._handle_response(response)


class RestLocationService:
    """REST client for location operations."""
    
    def __init__(self, client: RestApiClient):
        self.client = client
        
    def list_locations(
        self, 
        page: int = 1, 
        page_size: int = 50, 
        filters: Optional[FilterOptions] = None
    ) -> LocationListDto:
        """List locations via REST API."""
        params = {'page': page, 'page_size': page_size}
        if filters and filters.search_term:
            params['search'] = filters.search_term
            
        response = self.client.client.get(
            urljoin(self.client.base_url, 'locations/'),
            params=params
        )
        data = self.client._handle_response(response)
        return LocationListDto(**data)
        
    def create_location(self, location_data: CreateLocationDto) -> LocationDto:
        """Create a new location via REST API."""
        response = self.client.client.post(
            urljoin(self.client.base_url, 'locations/'),
            json=location_data.model_dump()
        )
        data = self.client._handle_response(response)
        return LocationDto(**data)
        
    def get_location(self, location_name: str) -> LocationDto:
        """Get location details via REST API."""
        response = self.client.client.get(
            urljoin(self.client.base_url, f'locations/{location_name}')
        )
        data = self.client._handle_response(response)
        return LocationDto(**data)
        
    def update_location(
        self, 
        location_name: str, 
        update_data: UpdateLocationDto
    ) -> LocationDto:
        """Update a location via REST API."""
        response = self.client.client.put(
            urljoin(self.client.base_url, f'locations/{location_name}'),
            json=update_data.model_dump(exclude_unset=True)
        )
        data = self.client._handle_response(response)
        return LocationDto(**data)
        
    def delete_location(self, location_name: str) -> None:
        """Delete a location via REST API."""
        response = self.client.client.delete(
            urljoin(self.client.base_url, f'locations/{location_name}')
        )
        self.client._handle_response(response)
        
    def test_location_connectivity(self, location_name: str) -> LocationTestResult:
        """Test location connectivity via REST API."""
        response = self.client.client.post(
            urljoin(self.client.base_url, f'locations/{location_name}/test')
        )
        data = self.client._handle_response(response)
        return LocationTestResult(**data)


def get_rest_api_client() -> RestApiClient:
    """
    Get a configured REST API client.
    
    Returns:
        RestApiClient instance configured from environment
    """
    return RestApiClient()


def get_rest_simulation_service() -> RestSimulationService:
    """
    Get REST simulation service.
    
    Returns:
        RestSimulationService instance
    """
    client = get_rest_api_client()
    return RestSimulationService(client)


def get_rest_location_service() -> RestLocationService:
    """
    Get REST location service.
    
    Returns:
        RestLocationService instance
    """
    client = get_rest_api_client()
    return RestLocationService(client)