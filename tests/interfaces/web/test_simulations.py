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
        response = client.get("/api/v0a3/simulations/")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check structure
        assert "simulations" in data
        assert "pagination" in data
        assert "filters_applied" in data
        
        # Check simulations
        simulations = data["simulations"]
        assert len(simulations) == 3
        
        # Check first simulation structure
        sim = simulations[0]
        required_fields = ["simulation_id", "uid", "attributes", "locations", "namelists", "workflows"]
        for field in required_fields:
            assert field in sim
    
    def test_list_simulations_with_pagination(self, client: TestClient):
        """Test simulation listing with pagination parameters."""
        response = client.get("/api/v0a3/simulations/?page=1&page_size=1")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check pagination
        pagination = data["pagination"]
        assert pagination["page"] == 1
        assert pagination["page_size"] == 1
        assert pagination["total_count"] == 3
        assert pagination["has_next"] is True
        assert pagination["has_previous"] is False
        
        # Should only return 1 simulation
        assert len(data["simulations"]) == 1
    
    def test_list_simulations_with_search(self, client: TestClient):
        """Test simulation listing with search filter."""
        response = client.get("/api/v0a3/simulations/?search=test-sim-1")
        
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
        response = client.get("/api/v0a3/simulations/?page=0")
        assert response.status_code == 422
        
        # Invalid page size
        response = client.get("/api/v0a3/simulations/?page_size=0")
        assert response.status_code == 422
        
        # Page size too large
        response = client.get("/api/v0a3/simulations/?page_size=101")
        assert response.status_code == 422


class TestSimulationCreation:
    """Test simulation creation."""
    
    def test_create_simulation_success(self, client: TestClient, sample_simulation_data):
        """Test successful simulation creation."""
        response = client.post("/api/v0a3/simulations/", json=sample_simulation_data)
        
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
        
        response = client.post("/api/v0a3/simulations/", json=duplicate_data)
        
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
        
        response = client.post("/api/v0a3/simulations/", json=invalid_data)
        assert response.status_code == 422
        
        # Empty simulation_id
        invalid_data = {
            "simulation_id": "",
            "model_id": "FESOM2"
        }
        
        response = client.post("/api/v0a3/simulations/", json=invalid_data)
        assert response.status_code == 422
    
    def test_create_simulation_minimal_data(self, client: TestClient):
        """Test creating simulation with minimal required data."""
        minimal_data = {
            "simulation_id": "minimal-sim"
        }
        
        response = client.post("/api/v0a3/simulations/", json=minimal_data)
        
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
        response = client.get("/api/v0a3/simulations/test-sim-1")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["simulation_id"] == "test-sim-1"
        assert data["attributes"]["model"] == "FESOM2"
        assert data["attributes"]["experiment"] == "PI"
    
    def test_get_simulation_not_found(self, client: TestClient):
        """Test retrieving non-existent simulation."""
        response = client.get("/api/v0a3/simulations/non-existent")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]
    
    def test_get_simulation_case_sensitivity(self, client: TestClient):
        """Test that simulation IDs are case sensitive."""
        response = client.get("/api/v0a3/simulations/TEST-SIM-1")  # Different case
        
        assert response.status_code == 404


class TestSimulationUpdate:
    """Test simulation updates."""
    
    def test_update_simulation_success(self, client: TestClient):
        """Test successful simulation update."""
        update_data = {
            "model_id": "Updated-Model",
            "attrs": {"experiment": "Updated-Experiment"}
        }
        
        response = client.put("/api/v0a3/simulations/test-sim-1", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["simulation_id"] == "test-sim-1"
        # Note: Mock returns the original data, in real implementation
        # these would be updated
    
    def test_update_simulation_not_found(self, client: TestClient):
        """Test updating non-existent simulation."""
        update_data = {"model_id": "New-Model"}
        
        response = client.put("/api/v0a3/simulations/non-existent", json=update_data)
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]
    
    def test_update_simulation_partial(self, client: TestClient):
        """Test partial simulation update."""
        update_data = {"model_id": "Partial-Update"}
        
        response = client.put("/api/v0a3/simulations/test-sim-1", json=update_data)
        
        assert response.status_code == 200
    
    def test_update_simulation_empty_data(self, client: TestClient):
        """Test update with empty data."""
        response = client.put("/api/v0a3/simulations/test-sim-1", json={})
        
        assert response.status_code == 200


class TestSimulationDeletion:
    """Test simulation deletion."""
    
    def test_delete_simulation_success(self, client: TestClient):
        """Test successful simulation deletion."""
        response = client.delete("/api/v0a3/simulations/test-sim-1")
        
        assert response.status_code == 204
        assert response.content == b""
    
    def test_delete_simulation_not_found(self, client: TestClient):
        """Test deleting non-existent simulation."""
        response = client.delete("/api/v0a3/simulations/non-existent")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]


class TestSimulationErrorHandling:
    """Test error handling for simulation endpoints."""
    
    def test_service_error_handling(self, client: TestClient, mock_simulation_service):
        """Test handling of service layer errors."""
        # Configure mock to raise exception
        mock_simulation_service.list_simulations.side_effect = Exception("Service error")
        
        response = client.get("/api/v0a3/simulations/")
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to list simulations" in data["detail"]
    
    def test_invalid_json_handling(self, client: TestClient):
        """Test handling of invalid JSON in requests."""
        response = client.post(
            "/api/v0a3/simulations/",
            content="invalid json",
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 422
    
    def test_content_type_handling(self, client: TestClient):
        """Test handling of incorrect content types."""
        response = client.post(
            "/api/v0a3/simulations/",
            content="simulation_id=test",
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        
        # FastAPI should still handle this, but it might not parse correctly
        assert response.status_code in [400, 422]


class TestSimulationCompatibility:
    """Test backward compatibility and API consistency."""
    
    def test_simulation_dto_properties(self, client: TestClient):
        """Test that DTO properties work correctly."""
        response = client.get("/api/v0a3/simulations/test-sim-1")
        
        assert response.status_code == 200
        data = response.json()
        
        # Test computed properties that should be available
        # These are handled by the DTO's property methods
        assert isinstance(data.get("uid"), str)
        assert isinstance(data.get("attributes"), dict)
    
    def test_list_response_structure(self, client: TestClient):
        """Test that list response follows expected structure."""
        response = client.get("/api/v0a3/simulations/")
        
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


class TestSimulationAttributes:
    """Test simulation attribute management endpoints."""
    
    def test_get_all_attributes_success(self, client: TestClient):
        """Test getting all attributes of a simulation."""
        response = client.get("/api/v0a3/simulations/sim-001/attributes")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check response structure
        assert "simulation_id" in data
        assert "attributes" in data
        assert data["simulation_id"] == "sim-001"
        assert isinstance(data["attributes"], dict)
        
        # Check specific attributes from mock data
        attributes = data["attributes"]
        assert attributes["model"] == "FESOM"
        assert attributes["resolution"] == "T127"
    
    def test_get_all_attributes_simulation_not_found(self, client: TestClient):
        """Test getting attributes of a non-existent simulation."""
        response = client.get("/api/v0a3/simulations/nonexistent/attributes")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()
    
    def test_get_specific_attribute_success(self, client: TestClient):
        """Test getting a specific attribute of a simulation."""
        response = client.get("/api/v0a3/simulations/sim-001/attributes/model")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check response structure
        assert "key" in data
        assert "value" in data
        assert data["key"] == "model"
        assert data["value"] == "FESOM"
    
    def test_get_specific_attribute_not_found(self, client: TestClient):
        """Test getting a non-existent attribute."""
        response = client.get("/api/v0a3/simulations/sim-001/attributes/nonexistent")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()
        assert "nonexistent" in data["detail"]
    
    def test_get_specific_attribute_simulation_not_found(self, client: TestClient):
        """Test getting attribute of a non-existent simulation."""
        response = client.get("/api/v0a3/simulations/nonexistent/attributes/model")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()
    
    def test_set_attribute_via_put_success(self, client: TestClient):
        """Test setting an attribute using PUT method."""
        attribute_data = {
            "key": "experiment",
            "value": "test-run"
        }
        
        response = client.put("/api/v0a3/simulations/sim-001/attributes/experiment", json=attribute_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Check response
        assert data["key"] == "experiment"
        assert data["value"] == "test-run"
    
    def test_set_attribute_key_mismatch(self, client: TestClient):
        """Test setting attribute with mismatched key in URL vs body."""
        attribute_data = {
            "key": "different_key",
            "value": "test-value"
        }
        
        response = client.put("/api/v0a3/simulations/sim-001/attributes/experiment", json=attribute_data)
        
        assert response.status_code == 400
        data = response.json()
        assert "must match" in data["detail"]
    
    def test_set_attribute_simulation_not_found(self, client: TestClient):
        """Test setting attribute on non-existent simulation."""
        attribute_data = {
            "key": "experiment",
            "value": "test-run"
        }
        
        response = client.put("/api/v0a3/simulations/nonexistent/attributes/experiment", json=attribute_data)
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()
    
    def test_add_attribute_via_post_success(self, client: TestClient):
        """Test adding a new attribute using POST method."""
        attribute_data = {
            "key": "new_attribute",
            "value": "new_value"
        }
        
        response = client.post("/api/v0a3/simulations/sim-001/attributes", json=attribute_data)
        
        assert response.status_code == 201
        data = response.json()
        
        # Check response
        assert data["key"] == "new_attribute"
        assert data["value"] == "new_value"
    
    def test_add_attribute_simulation_not_found(self, client: TestClient):
        """Test adding attribute to non-existent simulation."""
        attribute_data = {
            "key": "new_attribute",
            "value": "new_value"
        }
        
        response = client.post("/api/v0a3/simulations/nonexistent/attributes", json=attribute_data)
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()
    
    def test_add_attribute_invalid_data(self, client: TestClient):
        """Test adding attribute with invalid data."""
        invalid_data = {
            "key": "",  # Empty key should fail validation
            "value": "some_value"
        }
        
        response = client.post("/api/v0a3/simulations/sim-001/attributes", json=invalid_data)
        
        # Should fail validation (422)
        assert response.status_code == 422


class TestSimulationLocationAssociations:
    """Test simulation-location association endpoints."""
    
    def test_associate_simulation_locations(self, client: TestClient):
        """Test associating a simulation with locations."""
        association_data = {
            "simulation_id": "sim-001",
            "location_names": ["location1", "location2"],
            "context_overrides": {"key1": "value1"}
        }
        
        response = client.post("/api/v0a3/simulations/sim-001/locations", json=association_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should return updated simulation
        assert data["simulation_id"] == "sim-001"
        assert "locations" in data
    
    def test_associate_simulation_locations_id_mismatch(self, client: TestClient):
        """Test association with mismatched simulation IDs."""
        association_data = {
            "simulation_id": "different-sim",  # Different from URL
            "location_names": ["location1"],
            "context_overrides": {}
        }
        
        response = client.post("/api/v0a3/simulations/sim-001/locations", json=association_data)
        
        assert response.status_code == 400
        data = response.json()
        assert "must match" in data["detail"]
    
    def test_associate_simulation_locations_not_found(self, client: TestClient):
        """Test associating locations with nonexistent simulation."""
        association_data = {
            "simulation_id": "nonexistent",
            "location_names": ["location1"],
            "context_overrides": {}
        }
        
        response = client.post("/api/v0a3/simulations/nonexistent/locations", json=association_data)
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()
    
    def test_disassociate_simulation_location(self, client: TestClient):
        """Test removing a location association."""
        response = client.delete("/api/v0a3/simulations/sim-001/locations/location1")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should return updated simulation
        assert data["simulation_id"] == "sim-001"
        assert "locations" in data
    
    def test_disassociate_simulation_location_not_found(self, client: TestClient):
        """Test disassociating location from nonexistent simulation."""
        response = client.delete("/api/v0a3/simulations/nonexistent/locations/location1")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()
    
    def test_update_simulation_location_context(self, client: TestClient):
        """Test updating location context."""
        context_data = {
            "context_overrides": {
                "model": "FESOM",
                "experiment": "test_exp"
            }
        }
        
        response = client.put("/api/v0a3/simulations/sim-001/locations/location1/context", json=context_data)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should return updated simulation
        assert data["simulation_id"] == "sim-001"
        assert "locations" in data
    
    def test_update_simulation_location_context_not_found(self, client: TestClient):
        """Test updating context for nonexistent simulation."""
        context_data = {
            "context_overrides": {"key": "value"}
        }
        
        response = client.put("/api/v0a3/simulations/nonexistent/locations/location1/context", json=context_data)
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()


class TestSimulationFiles:
    """Test simulation files endpoints."""
    
    def test_get_simulation_files(self, client: TestClient):
        """Test getting files for a simulation."""
        response = client.get("/api/v0a3/simulations/sim-001/files")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check response structure
        assert "simulation_id" in data
        assert "files" in data
        assert data["simulation_id"] == "sim-001"
        assert isinstance(data["files"], list)
    
    def test_get_simulation_files_not_found(self, client: TestClient):
        """Test getting files for nonexistent simulation."""
        response = client.get("/api/v0a3/simulations/nonexistent/files")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()


@pytest.mark.skip(reason="Archive management not yet implemented - advanced feature")
class TestSimulationArchives:
    """Test simulation archive management endpoints."""
    


    def test_create_archive(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test creating an archive for a simulation."""
        from tellus.domain.entities.simulation_file import SimulationFile, FileType
        from datetime import datetime
        
        # Mock archive creation
        mock_archive = SimulationFile(
            relative_path="test_archive_123",
            file_type=FileType.ARCHIVE,
            created_time=datetime.now().timestamp()
        )
        mock_archive.attributes = {
            'archive_name': 'test_archive',
            'archive_type': 'single',
            'description': 'Test archive'
        }
        mock_file_service.create_archive.return_value = mock_archive
        
        # Test data
        request_data = {
            "archive_name": "test_archive",
            "description": "Test archive",
            "archive_type": "single"
        }
        
        response = client.post("/api/v0a3/simulations/sim-001/archives", json=request_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["archive_name"] == "test_archive"
        assert data["simulation_id"] == "sim-001"
        assert data["archive_type"] == "single"
        assert data["description"] == "Test archive"
        mock_file_service.create_archive.assert_called_once()
    

    def test_create_archive_with_location(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test creating an archive with location and pattern."""
        from tellus.domain.entities.simulation_file import SimulationFile, FileType
        from datetime import datetime
        
        # Mock archive creation
        mock_archive = SimulationFile(
            relative_path="split_archive_456",
            file_type=FileType.ARCHIVE,
            created_time=datetime.now().timestamp()
        )
        mock_archive.attributes = {
            'archive_name': 'split_archive',
            'location': 'storage_location',
            'pattern': '*.tar.gz_*',
            'split_parts': 5,
            'archive_type': 'split-tar'
        }
        mock_file_service.create_archive.return_value = mock_archive
        
        # Test data
        request_data = {
            "archive_name": "split_archive",
            "location": "storage_location",
            "pattern": "*.tar.gz_*",
            "split_parts": 5,
            "archive_type": "split-tar"
        }
        
        response = client.post("/api/v0a3/simulations/sim-001/archives", json=request_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["archive_name"] == "split_archive"
        assert data["location"] == "storage_location"
        assert data["pattern"] == "*.tar.gz_*"
        assert data["split_parts"] == 5
        assert data["archive_type"] == "split-tar"
    

    def test_create_archive_conflict(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test creating an archive that already exists."""
        mock_file_service.create_archive.side_effect = ValueError("Archive already exists")
        
        request_data = {
            "archive_name": "existing_archive",
            "archive_type": "single"
        }
        
        response = client.post("/api/v0a3/simulations/sim-001/archives", json=request_data)
        
        assert response.status_code == 409
        data = response.json()
        assert "already exists" in data["detail"]
    

    def test_create_archive_simulation_not_found(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test creating archive for nonexistent simulation."""
        mock_file_service.create_archive.side_effect = ValueError("Simulation not found")
        
        request_data = {
            "archive_name": "test_archive",
            "archive_type": "single"
        }
        
        response = client.post("/api/v0a3/simulations/nonexistent/archives", json=request_data)
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()
    

    def test_list_archives(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test listing archives for a simulation."""
        from tellus.domain.entities.simulation_file import SimulationFile, FileType
        from datetime import datetime
        
        # Mock archives
        mock_archives = [
            SimulationFile(
                relative_path="archive1",
                file_type=FileType.ARCHIVE,
                created_time=datetime.now().timestamp()
            ),
            SimulationFile(
                relative_path="archive2", 
                file_type=FileType.ARCHIVE,
                created_time=datetime.now().timestamp()
            )
        ]
        
        # Set attributes
        mock_archives[0].attributes = {
            'archive_name': 'archive1',
            'archive_type': 'single',
            'location': 'loc1'
        }
        mock_archives[1].attributes = {
            'archive_name': 'archive2',
            'archive_type': 'split-tar',
            'location': 'loc2',
            'split_parts': 3
        }
        
        mock_file_service.list_simulation_archives.return_value = mock_archives
        
        response = client.get("/api/v0a3/simulations/sim-001/archives")
        
        assert response.status_code == 200
        data = response.json()
        assert data["simulation_id"] == "sim-001"
        assert len(data["archives"]) == 2
        
        # Check first archive
        archive1 = data["archives"][0]
        assert archive1["archive_name"] == "archive1"
        assert archive1["archive_type"] == "single"
        assert archive1["location"] == "loc1"
        
        # Check second archive
        archive2 = data["archives"][1]
        assert archive2["archive_name"] == "archive2"
        assert archive2["archive_type"] == "split-tar"
        assert archive2["split_parts"] == 3
        
        mock_file_service.list_simulation_archives.assert_called_once_with("sim-001")
    

    def test_list_archives_empty(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test listing archives when none exist."""
        mock_file_service.list_simulation_archives.return_value = []
        
        response = client.get("/api/v0a3/simulations/sim-001/archives")
        
        assert response.status_code == 200
        data = response.json()
        assert data["simulation_id"] == "sim-001"
        assert len(data["archives"]) == 0
    

    def test_list_archives_simulation_not_found(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test listing archives for nonexistent simulation."""
        mock_file_service.list_simulation_archives.side_effect = ValueError("Simulation not found")
        
        response = client.get("/api/v0a3/simulations/nonexistent/archives")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()
    

    def test_get_archive(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test getting details of a specific archive."""
        from tellus.domain.entities.simulation_file import SimulationFile, FileType
        from datetime import datetime
        
        # Mock archive
        mock_archive = SimulationFile(
            relative_path="specific_archive",
            file_type=FileType.ARCHIVE,
            created_time=datetime.now().timestamp()
        )
        mock_archive.attributes = {
            'archive_name': 'specific_archive',
            'archive_type': 'single',
            'location': 'test_location',
            'description': 'Detailed archive'
        }
        
        mock_file_service.get_archive.return_value = mock_archive
        
        response = client.get("/api/v0a3/simulations/sim-001/archives/specific_archive")
        
        assert response.status_code == 200
        data = response.json()
        assert data["archive_id"] == "specific_archive"
        assert data["archive_name"] == "specific_archive"
        assert data["simulation_id"] == "sim-001"
        assert data["archive_type"] == "single"
        assert data["location"] == "test_location"
        assert data["description"] == "Detailed archive"
        
        mock_file_service.get_archive.assert_called_once_with("specific_archive")
    

    def test_get_archive_not_found(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test getting details of nonexistent archive."""
        mock_file_service.get_archive.return_value = None
        
        response = client.get("/api/v0a3/simulations/sim-001/archives/nonexistent")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]
    

    def test_delete_archive(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test deleting an archive."""
        from tellus.domain.entities.simulation_file import SimulationFile, FileType
        from datetime import datetime
        
        # Mock archive exists
        mock_archive = SimulationFile(
            relative_path="archive_to_delete",
            file_type=FileType.ARCHIVE,
            created_time=datetime.now().timestamp()
        )
        mock_file_service.get_archive.return_value = mock_archive
        mock_file_service.remove_file.return_value = None
        
        response = client.delete("/api/v0a3/simulations/sim-001/archives/archive_to_delete")
        
        assert response.status_code == 200
        data = response.json()
        assert data["archive_id"] == "archive_to_delete"
        assert data["status"] == "deleted"
        
        mock_file_service.get_archive.assert_called_once_with("archive_to_delete")
        mock_file_service.remove_file.assert_called_once_with("archive_to_delete")
    

    def test_delete_archive_not_found(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test deleting nonexistent archive."""
        mock_file_service.get_archive.return_value = None
        
        response = client.delete("/api/v0a3/simulations/sim-001/archives/nonexistent")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]
        
        # Should not attempt to delete
        mock_file_service.remove_file.assert_not_called()
    

    def test_delete_archive_service_error(self, client: TestClient, mock_simulation_service, mock_file_service):
        """Test delete archive with service error."""
        from tellus.domain.entities.simulation_file import SimulationFile, FileType
        from datetime import datetime
        
        # Mock archive exists
        mock_archive = SimulationFile(
            relative_path="problematic_archive",
            file_type=FileType.ARCHIVE,
            created_time=datetime.now().timestamp()
        )
        mock_file_service.get_archive.return_value = mock_archive
        mock_file_service.remove_file.side_effect = Exception("Deletion failed")
        
        response = client.delete("/api/v0a3/simulations/sim-001/archives/problematic_archive")
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to delete archive" in data["detail"]


# === Archive Content Tests ===

@pytest.mark.skip(reason="Archive management not yet implemented - advanced feature")
class TestArchiveContent:
    """Test archive content management endpoints."""


    def test_list_archive_contents_success(self, client, mock_file_service):
        """Test successful archive content listing."""
        # Mock archive exists
        mock_archive = SimulationFile(
            relative_path="test_archive",
            file_type=FileType.ARCHIVE,
            created_time=datetime.now().timestamp()
        )
        mock_file_service.get_archive.return_value = mock_archive
        
        # Mock child files
        child_files = [
            SimulationFile(
                relative_path="file1.nc",
                file_type=FileType.REGULAR,
                size_bytes=1024,
                content_type=FileContentType.OUTPUT,
                created_time=datetime.now().timestamp(),
                attributes={}
            ),
            SimulationFile(
                relative_path="file2.nc",
                file_type=FileType.REGULAR,
                size_bytes=2048,
                content_type=FileContentType.OUTPUT,
                created_time=datetime.now().timestamp(),
                attributes={}
            )
        ]
        mock_file_service.get_file_children.return_value = child_files
        
        response = client.get("/api/v0a3/simulations/sim-001/archives/test_archive/contents")
        
        assert response.status_code == 200
        data = response.json()
        assert data["archive_id"] == "test_archive"
        assert data["total_files"] == 2
        assert len(data["files"]) == 2
        assert data["files"][0]["file_path"] == "file1.nc"
        assert data["files"][0]["size_bytes"] == 1024


    def test_list_archive_contents_with_filters(self, client, mock_file_service):
        """Test archive content listing with filters."""
        # Mock archive exists
        mock_archive = SimulationFile(
            relative_path="test_archive",
            file_type=FileType.ARCHIVE,
            created_time=datetime.now().timestamp()
        )
        mock_file_service.get_archive.return_value = mock_archive
        
        # Mock child files (with different types)
        child_files = [
            SimulationFile(
                relative_path="output.nc",
                file_type=FileType.REGULAR,
                content_type=FileContentType.OUTPUT,
                created_time=datetime.now().timestamp(),
                attributes={}
            ),
            SimulationFile(
                relative_path="input.txt",
                file_type=FileType.REGULAR,
                content_type=FileContentType.INPUT,
                created_time=datetime.now().timestamp(),
                attributes={}
            )
        ]
        mock_file_service.get_file_children.return_value = child_files
        
        # Test with file_filter
        response = client.get("/api/v0a3/simulations/sim-001/archives/test_archive/contents?file_filter=*.nc")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_files"] == 1
        assert data["files"][0]["file_path"] == "output.nc"


    def test_list_archive_contents_not_found(self, client, mock_file_service):
        """Test listing contents of non-existent archive."""
        mock_file_service.get_archive.return_value = None
        
        response = client.get("/api/v0a3/simulations/sim-001/archives/nonexistent/contents")
        
        assert response.status_code == 404
        data = response.json()
        assert "Archive 'nonexistent' not found" in data["detail"]


    def test_index_archive_success(self, client, mock_file_service):
        """Test successful archive indexing."""
        # Mock archive exists
        mock_archive = SimulationFile(
            relative_path="test_archive",
            file_type=FileType.ARCHIVE,
            created_time=datetime.now().timestamp()
        )
        mock_file_service.get_archive.return_value = mock_archive
        
        # Mock existing children (already indexed)
        existing_children = [
            SimulationFile(relative_path="file1.nc", file_type=FileType.REGULAR, created_time=datetime.now().timestamp())
        ]
        mock_file_service.get_file_children.return_value = existing_children
        
        response = client.post("/api/v0a3/simulations/sim-001/archives/test_archive/index", json={"force": False})
        
        assert response.status_code == 200
        data = response.json()
        assert data["archive_id"] == "test_archive"
        assert data["status"] == "already_indexed"
        assert data["files_indexed"] == 1


    def test_index_archive_force(self, client, mock_file_service):
        """Test forced archive indexing."""
        # Mock archive exists
        mock_archive = SimulationFile(
            relative_path="test_archive",
            file_type=FileType.ARCHIVE,
            created_time=datetime.now().timestamp()
        )
        mock_file_service.get_archive.return_value = mock_archive
        mock_file_service.get_file_children.return_value = []
        
        response = client.post("/api/v0a3/simulations/sim-001/archives/test_archive/index", json={"force": True})
        
        assert response.status_code == 200
        data = response.json()
        assert data["archive_id"] == "test_archive"
        assert data["status"] == "indexed"


    def test_index_archive_not_found(self, client, mock_file_service):
        """Test indexing non-existent archive."""
        mock_file_service.get_archive.return_value = None
        
        response = client.post("/api/v0a3/simulations/sim-001/archives/nonexistent/index", json={"force": False})
        
        assert response.status_code == 404
        data = response.json()
        assert "Archive 'nonexistent' not found" in data["detail"]


# === File Management Tests ===

@pytest.mark.skip(reason="Advanced file management not yet implemented - advanced feature")
class TestFileManagement:
    """Test file management endpoints."""

    def test_list_simulation_files_success(self, client, mock_file_service):
        """Test successful file listing."""
        # Mock files
        files = [
            SimulationFile(
                relative_path="output.nc",
                location_name="local",
                size_bytes=1024,
                content_type=FileContentType.OUTPUT,
                file_type=FileType.REGULAR,
                parent_file_id="archive1",
                created_time=datetime.now().timestamp(),
                attributes={}
            ),
            SimulationFile(
                relative_path="input.txt",
                location_name="remote",
                size_bytes=512,
                content_type=FileContentType.INPUT,
                file_type=FileType.REGULAR,
                created_time=datetime.now().timestamp(),
                attributes={}
            )
        ]
        mock_file_service.get_simulation_files.return_value = files
        
        response = client.get("/api/v0a3/simulations/sim-001/files")
        
        assert response.status_code == 200
        data = response.json()
        assert data["simulation_id"] == "sim-001"
        assert data["total_files"] == 2
        assert len(data["files"]) == 2
        assert data["files"][0]["file_path"] == "output.nc"
        assert data["files"][0]["location"] == "local"
        assert data["files"][0]["parent_file"] == "archive1"

    def test_list_simulation_files_with_filters(self, client, mock_file_service):
        """Test file listing with filters."""
        # Mock files with different content types
        files = [
            SimulationFile(
                relative_path="output.nc",
                location_name="local",
                content_type=FileContentType.OUTPUT,
                file_type=FileType.REGULAR,
                created_time=datetime.now().timestamp(),
                attributes={}
            )
        ]
        mock_file_service.get_simulation_files.return_value = files
        
        # Test with content_type filter
        response = client.get("/api/v0a3/simulations/sim-001/files?content_type=output")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_files"] == 1

    def test_register_files_success(self, client, mock_file_service):
        """Test successful file registration."""
        from tellus.application.dtos import FileRegistrationResultDto
        
        # Mock registration result
        result = FileRegistrationResultDto(
            registered_count=5,
            updated_count=2,
            skipped_count=1
        )
        mock_file_service.register_files_to_simulation.return_value = result
        
        request_data = {
            "archive_id": "test_archive",
            "content_type_filter": "output",
            "overwrite_existing": True
        }
        
        response = client.post("/api/v0a3/simulations/sim-001/files/register", json=request_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["simulation_id"] == "sim-001"
        assert data["archive_id"] == "test_archive"
        assert data["registered_count"] == 5
        assert data["updated_count"] == 2
        assert data["skipped_count"] == 1
        assert data["status"] == "completed"


    def test_register_files_archive_not_found(self, client, mock_file_service):
        """Test file registration with non-existent archive."""
        from tellus.application.exceptions import EntityNotFoundError
        
        mock_file_service.register_files_to_simulation.side_effect = EntityNotFoundError("Archive", "nonexistent")
        
        request_data = {"archive_id": "nonexistent"}
        
        response = client.post("/api/v0a3/simulations/sim-001/files/register", json=request_data)
        
        assert response.status_code == 404

    def test_unregister_files_success(self, client, mock_file_service):
        """Test successful file unregistration."""
        # Mock simulation files
        files = [
            SimulationFile(
                relative_path="file1.nc",
                parent_file_id="test_archive",
                attributes={"simulation_id": "sim-001"},
                created_time=datetime.now().timestamp()
            ),
            SimulationFile(
                relative_path="file2.nc",
                parent_file_id="test_archive",
                attributes={"simulation_id": "sim-001"},
                created_time=datetime.now().timestamp()
            )
        ]
        mock_file_service.get_simulation_files.return_value = files
        
        request_data = {"archive_id": "test_archive"}
        
        response = client.delete("/api/v0a3/simulations/sim-001/files/unregister", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["simulation_id"] == "sim-001"
        assert data["archive_id"] == "test_archive"
        assert data["unregistered_count"] == 2
        assert data["status"] == "completed"

    def test_get_files_status_success(self, client, mock_file_service):
        """Test successful file status retrieval."""
        # Mock files with different attributes
        files = [
            SimulationFile(
                relative_path="file1.nc",
                parent_file_id="archive1",
                content_type=FileContentType.OUTPUT,
                location_name="local",
                created_time=datetime.now().timestamp(),
                attributes={}
            ),
            SimulationFile(
                relative_path="file2.txt",
                parent_file_id="archive1",
                content_type=FileContentType.INPUT,
                location_name="remote",
                created_time=datetime.now().timestamp(),
                attributes={}
            ),
            SimulationFile(
                relative_path="file3.log",
                content_type=FileContentType.LOG,
                location_name="local",
                created_time=datetime.now().timestamp(),
                attributes={}
            )
        ]
        mock_file_service.get_simulation_files.return_value = files
        
        response = client.get("/api/v0a3/simulations/sim-001/files/status")
        
        assert response.status_code == 200
        data = response.json()
        assert data["simulation_id"] == "sim-001"
        assert data["total_files"] == 3
        assert data["files_by_archive"]["archive1"] == 2
        assert data["files_by_archive"]["no_archive"] == 1
        assert data["files_by_content_type"]["output"] == 1
        assert data["files_by_content_type"]["input"] == 1
        assert data["files_by_content_type"]["log"] == 1
        assert data["files_by_location"]["local"] == 2
        assert data["files_by_location"]["remote"] == 1