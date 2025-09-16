"""
Tests for simulation management endpoints.

Validates CRUD operations, pagination, filtering, and error handling
for the simulation management API.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch


class TestSimulationListing:
    """Test simulation listing and filtering."""
    
    def test_list_simulations_default(self, client: TestClient):
        """Test listing simulations with default parameters."""
        response = client.get("/simulations/")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check structure
        assert "simulations" in data
        assert "pagination" in data
        assert "filters_applied" in data
        
        # Check simulations
        simulations = data["simulations"]
        assert len(simulations) == 2
        
        # Check first simulation structure
        sim = simulations[0]
        required_fields = ["simulation_id", "uid", "attributes", "locations", "namelists", "workflows"]
        for field in required_fields:
            assert field in sim
    
    def test_list_simulations_with_pagination(self, client: TestClient):
        """Test simulation listing with pagination parameters."""
        response = client.get("/simulations/?page=1&page_size=1")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check pagination
        pagination = data["pagination"]
        assert pagination["page"] == 1
        assert pagination["page_size"] == 1
        assert pagination["total_count"] == 2
        assert pagination["has_next"] is True
        assert pagination["has_previous"] is False
        
        # Should only return 1 simulation
        assert len(data["simulations"]) == 1
    
    def test_list_simulations_with_search(self, client: TestClient):
        """Test simulation listing with search filter."""
        response = client.get("/simulations/?search=test-sim-1")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check that search was applied
        filters = data["filters_applied"]
        assert filters["search_term"] == "test-sim-1"
        
        # Should return only matching simulation
        simulations = data["simulations"]
        assert len(simulations) == 1
        assert simulations[0]["simulation_id"] == "test-sim-1"
    
    def test_list_simulations_pagination_validation(self, client: TestClient):
        """Test pagination parameter validation."""
        # Invalid page number
        response = client.get("/simulations/?page=0")
        assert response.status_code == 422
        
        # Invalid page size
        response = client.get("/simulations/?page_size=0")
        assert response.status_code == 422
        
        # Page size too large
        response = client.get("/simulations/?page_size=101")
        assert response.status_code == 422


class TestSimulationCreation:
    """Test simulation creation."""
    
    def test_create_simulation_success(self, client: TestClient, sample_simulation_data):
        """Test successful simulation creation."""
        response = client.post("/simulations/", json=sample_simulation_data)
        
        assert response.status_code == 201
        data = response.json()
        
        # Check returned data
        assert data["simulation_id"] == sample_simulation_data["simulation_id"]
        assert "uid" in data
        assert data["attributes"] == sample_simulation_data["attrs"]
        assert data["namelists"] == sample_simulation_data["namelists"]
        assert data["workflows"] == sample_simulation_data["snakemakes"]
    
    def test_create_simulation_duplicate_id(self, client: TestClient):
        """Test creating simulation with duplicate ID fails."""
        duplicate_data = {
            "simulation_id": "test-sim-1",  # This already exists in mock data
            "model_id": "FESOM2",
            "attrs": {"experiment": "PI"}
        }
        
        response = client.post("/simulations/", json=duplicate_data)
        
        assert response.status_code == 400
        data = response.json()
        assert "already exists" in data["detail"]
    
    def test_create_simulation_validation_errors(self, client: TestClient):
        """Test validation errors in simulation creation."""
        # Missing required field
        invalid_data = {
            "model_id": "FESOM2",
            "attrs": {}
            # Missing simulation_id
        }
        
        response = client.post("/simulations/", json=invalid_data)
        assert response.status_code == 422
        
        # Empty simulation_id
        invalid_data = {
            "simulation_id": "",
            "model_id": "FESOM2"
        }
        
        response = client.post("/simulations/", json=invalid_data)
        assert response.status_code == 422
    
    def test_create_simulation_minimal_data(self, client: TestClient):
        """Test creating simulation with minimal required data."""
        minimal_data = {
            "simulation_id": "minimal-sim"
        }
        
        response = client.post("/simulations/", json=minimal_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["simulation_id"] == "minimal-sim"
        assert data["attributes"] == {}
        assert data["namelists"] == {}
        assert data["workflows"] == {}


class TestSimulationRetrieval:
    """Test individual simulation retrieval."""
    
    def test_get_simulation_success(self, client: TestClient):
        """Test successful simulation retrieval."""
        response = client.get("/simulations/test-sim-1")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["simulation_id"] == "test-sim-1"
        assert data["attributes"]["model"] == "FESOM2"
        assert data["attributes"]["experiment"] == "PI"
    
    def test_get_simulation_not_found(self, client: TestClient):
        """Test retrieving non-existent simulation."""
        response = client.get("/simulations/non-existent")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]
    
    def test_get_simulation_case_sensitivity(self, client: TestClient):
        """Test that simulation IDs are case sensitive."""
        response = client.get("/simulations/TEST-SIM-1")  # Different case
        
        assert response.status_code == 404


class TestSimulationUpdate:
    """Test simulation updates."""
    
    def test_update_simulation_success(self, client: TestClient):
        """Test successful simulation update."""
        update_data = {
            "model_id": "Updated-Model",
            "attrs": {"experiment": "Updated-Experiment"}
        }
        
        response = client.put("/simulations/test-sim-1", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["simulation_id"] == "test-sim-1"
        # Note: Mock returns the original data, in real implementation
        # these would be updated
    
    def test_update_simulation_not_found(self, client: TestClient):
        """Test updating non-existent simulation."""
        update_data = {"model_id": "New-Model"}
        
        response = client.put("/simulations/non-existent", json=update_data)
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]
    
    def test_update_simulation_partial(self, client: TestClient):
        """Test partial simulation update."""
        update_data = {"model_id": "Partial-Update"}
        
        response = client.put("/simulations/test-sim-1", json=update_data)
        
        assert response.status_code == 200
    
    def test_update_simulation_empty_data(self, client: TestClient):
        """Test update with empty data."""
        response = client.put("/simulations/test-sim-1", json={})
        
        assert response.status_code == 200


class TestSimulationDeletion:
    """Test simulation deletion."""
    
    def test_delete_simulation_success(self, client: TestClient):
        """Test successful simulation deletion."""
        response = client.delete("/simulations/test-sim-1")
        
        assert response.status_code == 204
        assert response.content == b""
    
    def test_delete_simulation_not_found(self, client: TestClient):
        """Test deleting non-existent simulation."""
        response = client.delete("/simulations/non-existent")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]


class TestSimulationErrorHandling:
    """Test error handling for simulation endpoints."""
    
    def test_service_error_handling(self, client: TestClient, mock_simulation_service):
        """Test handling of service layer errors."""
        # Configure mock to raise exception
        mock_simulation_service.list_simulations.side_effect = Exception("Service error")
        
        response = client.get("/simulations/")
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to list simulations" in data["detail"]
    
    def test_invalid_json_handling(self, client: TestClient):
        """Test handling of invalid JSON in requests."""
        response = client.post(
            "/simulations/",
            content="invalid json",
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 422
    
    def test_content_type_handling(self, client: TestClient):
        """Test handling of incorrect content types."""
        response = client.post(
            "/simulations/",
            content="simulation_id=test",
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        
        # FastAPI should still handle this, but it might not parse correctly
        assert response.status_code in [400, 422]


class TestSimulationCompatibility:
    """Test backward compatibility and API consistency."""
    
    def test_simulation_dto_properties(self, client: TestClient):
        """Test that DTO properties work correctly."""
        response = client.get("/simulations/test-sim-1")
        
        assert response.status_code == 200
        data = response.json()
        
        # Test computed properties that should be available
        # These are handled by the DTO's property methods
        assert isinstance(data.get("uid"), str)
        assert isinstance(data.get("attributes"), dict)
    
    def test_list_response_structure(self, client: TestClient):
        """Test that list response follows expected structure."""
        response = client.get("/simulations/")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response structure matches DTO
        assert isinstance(data["simulations"], list)
        assert isinstance(data["pagination"], dict)
        assert isinstance(data["filters_applied"], dict)
        
        # Verify pagination structure
        pagination = data["pagination"]
        expected_pagination_fields = ["page", "page_size", "total_count", "has_next", "has_previous"]
        for field in expected_pagination_fields:
            assert field in pagination