"""
Location state management for the Tellus web UI.

This module manages the state for storage location data and operations.
"""

# Uncomment when Reflex is available
# import reflex as rx
from typing import List, Dict, Any, Optional

# Import Tellus DTOs
from ....application.dtos import LocationDto, CreateLocationDto


class LocationState:
    """
    State management for storage locations in the web UI.
    
    When Reflex is available, this will inherit from rx.State.
    For now, it serves as a blueprint.
    """
    
    def __init__(self):
        # Location data
        self.locations: List[Dict[str, Any]] = []
        self.selected_location: Optional[Dict[str, Any]] = None
        self.loading: bool = False
        self.error_message: str = ""
        
        # Test results
        self.test_results: Dict[str, Dict[str, Any]] = {}
        self.testing: bool = False
        
        # Form data for creating/editing
        self.form_name: str = ""
        self.form_protocol: str = "file"
        self.form_path: str = ""
        self.form_kinds: List[str] = []
        self.form_storage_options: Dict[str, Any] = {}
    
    def load_locations(self):
        """Load locations from the backend API."""
        self.loading = True
        self.error_message = ""
        
        # TODO: Implement API call
        # For now, return mock data
        mock_locations = [
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
            },
            {
                "name": "archive_tape",
                "protocol": "tape",
                "path": "/archive/climate_data",
                "kinds": ["TAPE"],
                "storage_options": {
                    "tape_system": "HPSS",
                    "queue_name": "archive"
                },
                "is_remote": True,
                "is_accessible": None,
                "last_verified": None
            }
        ]
        
        self.locations = mock_locations
        self.loading = False
    
    def select_location(self, location_name: str):
        """Select a location for detailed view."""
        for loc in self.locations:
            if loc["name"] == location_name:
                self.selected_location = loc
                break
    
    def test_location(self, location_name: str):
        """Test connectivity to a location."""
        self.testing = True
        
        # TODO: Implement API call to test location
        # For now, simulate test results
        import time
        import random
        
        # Simulate testing delay
        time.sleep(1)
        
        success = random.choice([True, True, True, False])  # 75% success rate
        
        self.test_results[location_name] = {
            "success": success,
            "latency_ms": random.uniform(50, 500) if success else None,
            "available_space": random.randint(100, 10000) * 1024**3 if success else None,  # GB to bytes
            "error_message": None if success else "Connection timeout",
            "timestamp": time.time()
        }
        
        # Update location accessibility
        for loc in self.locations:
            if loc["name"] == location_name:
                loc["is_accessible"] = success
                loc["last_verified"] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
                break
        
        self.testing = False
    
    def create_location(self):
        """Create a new location."""
        if not self.form_name:
            self.error_message = "Location name is required"
            return
        
        if not self.form_protocol:
            self.error_message = "Protocol is required"
            return
        
        # TODO: Implement API call to create location
        new_location = {
            "name": self.form_name,
            "protocol": self.form_protocol,
            "path": self.form_path,
            "kinds": list(self.form_kinds),
            "storage_options": dict(self.form_storage_options),
            "is_remote": self.form_protocol != "file",
            "is_accessible": None,
            "last_verified": None
        }
        
        self.locations.append(new_location)
        self.clear_form()
    
    def delete_location(self, location_name: str):
        """Delete a location."""
        # TODO: Implement API call
        self.locations = [
            loc for loc in self.locations 
            if loc["name"] != location_name
        ]
        
        if self.selected_location and self.selected_location["name"] == location_name:
            self.selected_location = None
        
        # Remove test results
        if location_name in self.test_results:
            del self.test_results[location_name]
    
    def clear_form(self):
        """Clear the location form."""
        self.form_name = ""
        self.form_protocol = "file"
        self.form_path = ""
        self.form_kinds = []
        self.form_storage_options = {}
    
    def get_protocol_options(self) -> List[str]:
        """Get available protocol options."""
        return ["file", "sftp", "ssh", "s3", "gcs", "azure", "tape"]
    
    def get_kind_options(self) -> List[str]:
        """Get available location kind options."""
        return ["DISK", "COMPUTE", "TAPE", "FILESERVER"]
    
    def get_accessible_locations(self) -> List[Dict[str, Any]]:
        """Get locations that are currently accessible."""
        return [loc for loc in self.locations if loc.get("is_accessible", False)]
    
    def get_remote_locations(self) -> List[Dict[str, Any]]:
        """Get remote locations."""
        return [loc for loc in self.locations if loc.get("is_remote", False)]
    
    def format_storage_size(self, bytes_size: Optional[int]) -> str:
        """Format storage size in human-readable format."""
        if not bytes_size:
            return "Unknown"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} EB"


# When Reflex is available, this will be a proper state class:
"""
class LocationState(rx.State):
    # All the methods above, but with proper Reflex decorators and async support
    
    @rx.var
    def protocol_options(self) -> List[str]:
        return self.get_protocol_options()
    
    @rx.var
    def kind_options(self) -> List[str]:
        return self.get_kind_options()
    
    @rx.var
    def accessible_locations(self) -> List[Dict[str, Any]]:
        return self.get_accessible_locations()
    
    @rx.var
    def remote_locations(self) -> List[Dict[str, Any]]:
        return self.get_remote_locations()
"""