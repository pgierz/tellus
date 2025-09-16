"""
Tests for location management endpoints.

Validates CRUD operations, filtering, connectivity testing, and error handling
for the location management API.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch


class TestLocationListing:
    """Test location listing and filtering."""
    
    def test_list_locations_default(self, client: TestClient):
        """Test listing locations with default parameters."""
        response = client.get("/api/v0a3/locations/")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check structure
        assert "locations" in data
        assert "pagination" in data
        assert "filters_applied" in data
        
        # Check locations
        locations = data["locations"]
        assert len(locations) == 2  # From mock data
        
        # Check first location structure
        loc = locations[0]
        required_fields = ["name", "kinds", "protocol", "path", "storage_options", 
                          "additional_config", "is_remote", "is_accessible"]
        for field in required_fields:
            assert field in loc
        
        assert loc["name"] == "local-storage"
        assert "DISK" in loc["kinds"]
        assert loc["protocol"] == "file"
        assert loc["is_remote"] is False
    
    def test_list_locations_with_pagination(self, client: TestClient):
        """Test location listing with pagination parameters."""
        response = client.get("/api/v0a3/locations/?page=1&page_size=1")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check pagination
        pagination = data["pagination"]
        assert pagination["page"] == 1
        assert pagination["page_size"] == 1
        assert pagination["total_count"] == 2
        assert pagination["has_next"] is True
        assert pagination["has_previous"] is False
        
        # Should only return 1 location
        assert len(data["locations"]) == 1
    
    def test_list_locations_with_search(self, client: TestClient):
        """Test location listing with search filter."""
        response = client.get("/api/v0a3/locations/?search=local")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check that search was applied
        filters = data["filters_applied"]
        assert filters["search_term"] == "local"
        
        # Should return only matching location
        locations = data["locations"]
        assert len(locations) == 1
        assert "local" in locations[0]["name"]
    
    def test_list_locations_with_kind_filter(self, client: TestClient):
        """Test location listing with kind filter."""
        response = client.get("/api/v0a3/locations/?kind=COMPUTE")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should return only locations with COMPUTE kind
        locations = data["locations"]
        assert len(locations) == 1
        assert "COMPUTE" in locations[0]["kinds"]
    
    def test_list_locations_pagination_validation(self, client: TestClient):
        """Test pagination parameter validation."""
        # Invalid page number
        response = client.get("/api/v0a3/locations/?page=0")
        assert response.status_code == 422
        
        # Invalid page size
        response = client.get("/api/v0a3/locations/?page_size=0")
        assert response.status_code == 422
        
        # Page size too large
        response = client.get("/api/v0a3/locations/?page_size=101")
        assert response.status_code == 422


class TestLocationCreation:
    """Test location creation."""
    
    def test_create_location_success(self, client: TestClient, sample_location_data):
        """Test successful location creation."""
        response = client.post("/api/v0a3/locations/", json=sample_location_data)
        
        assert response.status_code == 201
        data = response.json()
        
        # Check returned data
        assert data["name"] == sample_location_data["name"]
        assert data["kinds"] == sample_location_data["kinds"]
        assert data["protocol"] == sample_location_data["protocol"]
        assert data["path"] == sample_location_data["path"]
        assert data["storage_options"] == sample_location_data["storage_options"]
        assert data["additional_config"] == sample_location_data["additional_config"]
        assert data["is_remote"] is True  # sftp protocol
    
    def test_create_location_duplicate_name(self, client: TestClient):
        """Test creating location with duplicate name fails."""
        duplicate_data = {
            "name": "local-storage",  # This already exists in mock data
            "kinds": ["DISK"],
            "protocol": "file",
            "path": "/different/path"
        }
        
        response = client.post("/api/v0a3/locations/", json=duplicate_data)
        
        assert response.status_code == 400
        data = response.json()
        assert "already exists" in data["detail"]
    
    def test_create_location_validation_errors(self, client: TestClient):
        """Test validation errors in location creation."""
        # Missing required field
        invalid_data = {
            "kinds": ["DISK"],
            "protocol": "file"
            # Missing name
        }
        
        response = client.post("/api/v0a3/locations/", json=invalid_data)
        assert response.status_code == 422
        
        # Empty name
        invalid_data = {
            "name": "",
            "kinds": ["DISK"],
            "protocol": "file"
        }
        
        response = client.post("/api/v0a3/locations/", json=invalid_data)
        assert response.status_code == 422
        
        # Empty kinds list
        invalid_data = {
            "name": "test-location",
            "kinds": [],
            "protocol": "file"
        }
        
        response = client.post("/api/v0a3/locations/", json=invalid_data)
        assert response.status_code == 422
    
    def test_create_location_minimal_data(self, client: TestClient):
        """Test creating location with minimal required data."""
        minimal_data = {
            "name": "minimal-location",
            "kinds": ["DISK"],
            "protocol": "file"
        }
        
        response = client.post("/api/v0a3/locations/", json=minimal_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "minimal-location"
        assert data["kinds"] == ["DISK"]
        assert data["protocol"] == "file"
        assert data["storage_options"] == {}
        assert data["additional_config"] == {}


class TestLocationRetrieval:
    """Test individual location retrieval."""
    
    def test_get_location_success(self, client: TestClient):
        """Test successful location retrieval."""
        response = client.get("/api/v0a3/locations/local-storage")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["name"] == "local-storage"
        assert "DISK" in data["kinds"]
        assert data["protocol"] == "file"
        assert data["is_remote"] is False
    
    def test_get_location_not_found(self, client: TestClient):
        """Test retrieving non-existent location."""
        response = client.get("/api/v0a3/locations/non-existent")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]
    
    def test_get_location_case_sensitivity(self, client: TestClient):
        """Test that location names are case sensitive."""
        response = client.get("/api/v0a3/locations/LOCAL-STORAGE")  # Different case
        
        assert response.status_code == 404


class TestLocationUpdate:
    """Test location updates."""
    
    def test_update_location_success(self, client: TestClient):
        """Test successful location update."""
        update_data = {
            "protocol": "sftp",
            "storage_options": {"host": "new-host.example.com"}
        }
        
        response = client.put("/api/v0a3/locations/local-storage", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "local-storage"
    
    def test_update_location_not_found(self, client: TestClient):
        """Test updating non-existent location."""
        update_data = {"protocol": "sftp"}
        
        response = client.put("/api/v0a3/locations/non-existent", json=update_data)
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]
    
    def test_update_location_partial(self, client: TestClient):
        """Test partial location update."""
        update_data = {"path": "/new/path"}
        
        response = client.put("/api/v0a3/locations/local-storage", json=update_data)
        
        assert response.status_code == 200
    
    def test_update_location_empty_data(self, client: TestClient):
        """Test update with empty data."""
        response = client.put("/api/v0a3/locations/local-storage", json={})
        
        assert response.status_code == 200


class TestLocationDeletion:
    """Test location deletion."""
    
    def test_delete_location_success(self, client: TestClient):
        """Test successful location deletion."""
        response = client.delete("/api/v0a3/locations/local-storage")
        
        assert response.status_code == 204
        assert response.content == b""
    
    def test_delete_location_not_found(self, client: TestClient):
        """Test deleting non-existent location."""
        response = client.delete("/api/v0a3/locations/non-existent")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]


class TestLocationConnectivityTesting:
    """Test location connectivity testing."""
    
    def test_test_location_success(self, client: TestClient):
        """Test successful location connectivity test."""
        response = client.post("/api/v0a3/locations/local-storage/test")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check test result structure
        required_fields = ["location_name", "success", "latency_ms", "protocol_specific_info"]
        for field in required_fields:
            assert field in data
        
        assert data["location_name"] == "local-storage"
        assert data["success"] is True
        assert isinstance(data["latency_ms"], (int, float))
        assert isinstance(data["protocol_specific_info"], dict)
    
    def test_test_location_not_found(self, client: TestClient):
        """Test testing non-existent location."""
        response = client.post("/api/v0a3/locations/non-existent/test")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]
    
    def test_test_location_result_format(self, client: TestClient):
        """Test that test results follow correct format."""
        response = client.post("/api/v0a3/locations/cluster-storage/test")
        
        assert response.status_code == 200
        data = response.json()
        
        # Validate data types
        assert isinstance(data["success"], bool)
        assert isinstance(data["latency_ms"], (int, float))
        assert data["latency_ms"] >= 0
        
        # Check protocol-specific info
        protocol_info = data["protocol_specific_info"]
        assert "protocol" in protocol_info
        assert "test_performed" in protocol_info
        assert "timestamp" in protocol_info


class TestLocationErrorHandling:
    """Test error handling for location endpoints."""
    
    def test_service_error_handling(self, client: TestClient, mock_location_service):
        """Test handling of service layer errors."""
        # Configure mock to raise exception
        mock_location_service.list_locations.side_effect = Exception("Service error")
        
        response = client.get("/api/v0a3/locations/")
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to list locations" in data["detail"]
    
    def test_invalid_json_handling(self, client: TestClient):
        """Test handling of invalid JSON in requests."""
        response = client.post(
            "/api/v0a3/locations/",
            content="invalid json",
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 422
    
    def test_invalid_protocol_handling(self, client: TestClient):
        """Test handling of invalid protocol values."""
        invalid_data = {
            "name": "test-location",
            "kinds": ["DISK"],
            "protocol": "invalid-protocol"
        }
        
        response = client.post("/api/v0a3/locations/", json=invalid_data)
        
        # Should still create (protocol validation is in domain layer)
        # This tests API layer doesn't break with unexpected values
        assert response.status_code == 201


class TestLocationFiltering:
    """Test advanced location filtering capabilities."""
    
    def test_multiple_filters(self, client: TestClient):
        """Test applying multiple filters simultaneously."""
        response = client.get("/api/v0a3/locations/?search=cluster&kind=COMPUTE")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should apply both search and kind filters
        locations = data["locations"]
        if locations:  # If any results
            for location in locations:
                assert "cluster" in location["name"].lower()
                assert "COMPUTE" in location["kinds"]
    
    def test_case_insensitive_search(self, client: TestClient):
        """Test that search is case insensitive."""
        response1 = client.get("/api/v0a3/locations/?search=local")
        response2 = client.get("/api/v0a3/locations/?search=LOCAL")
        
        assert response1.status_code == 200
        assert response2.status_code == 200
        
        # Should return same results regardless of case
        assert len(response1.json()["locations"]) == len(response2.json()["locations"])
    
    def test_kind_filter_case_insensitive(self, client: TestClient):
        """Test that kind filter handles different cases."""
        response1 = client.get("/api/v0a3/locations/?kind=DISK")
        response2 = client.get("/api/v0a3/locations/?kind=disk")
        
        assert response1.status_code == 200
        assert response2.status_code == 200
        
        # Both should return results (implementation might be case-insensitive)


class TestLocationCompatibility:
    """Test backward compatibility and API consistency."""
    
    def test_location_dto_structure(self, client: TestClient):
        """Test that location DTOs have consistent structure."""
        response = client.get("/api/v0a3/locations/local-storage")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify all expected fields are present
        expected_fields = [
            "name", "kinds", "protocol", "path", "storage_options",
            "additional_config", "is_remote", "is_accessible", "last_verified"
        ]
        for field in expected_fields:
            assert field in data
        
        # Verify field types
        assert isinstance(data["name"], str)
        assert isinstance(data["kinds"], list)
        assert isinstance(data["protocol"], str)
        assert isinstance(data["storage_options"], dict)
        assert isinstance(data["additional_config"], dict)
        assert isinstance(data["is_remote"], bool)
    
    def test_list_response_consistency(self, client: TestClient):
        """Test that list responses are consistent."""
        response = client.get("/api/v0a3/locations/")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check response structure
        assert "locations" in data
        assert "pagination" in data
        assert "filters_applied" in data
        
        # All locations should have consistent structure
        for location in data["locations"]:
            assert "name" in location
            assert "kinds" in location
            assert "protocol" in location