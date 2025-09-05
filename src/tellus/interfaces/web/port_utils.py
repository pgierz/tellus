"""
Port utilities for the Tellus API server.

Provides functionality to find available ports and manage server startup.
"""

import socket
from typing import Optional


def find_available_port(preferred_port: int = 1968, start_range: int = 1968, end_range: int = 2000) -> int:
    """
    Find an available port, starting with the preferred port.
    
    Args:
        preferred_port: The preferred port to try first (default: 1968)
        start_range: Start of port range to search if preferred is unavailable
        end_range: End of port range to search
        
    Returns:
        Available port number
        
    Raises:
        RuntimeError: If no available port is found in the range
    """
    def is_port_available(port: int) -> bool:
        """Check if a port is available."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(('localhost', port))
                return True
            except OSError:
                return False
    
    # Try the preferred port first
    if is_port_available(preferred_port):
        return preferred_port
    
    # Search for an available port in the range
    for port in range(start_range, end_range + 1):
        if port == preferred_port:
            continue  # Already tried
        if is_port_available(port):
            return port
    
    raise RuntimeError(f"No available ports found in range {start_range}-{end_range}")


def get_api_url(port: Optional[int] = None) -> str:
    """
    Get the API URL, finding an available port if needed.
    
    Args:
        port: Specific port to use, or None to find available port
        
    Returns:
        Complete API URL (e.g., "http://localhost:1968")
    """
    if port is None:
        port = find_available_port()
    return f"http://localhost:{port}"