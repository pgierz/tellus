"""
Integration tests for the Tellus FastAPI application.

Tests end-to-end workflows, API documentation, and cross-endpoint interactions.
"""

import pytest
import json
from fastapi.testclient import TestClient


class TestAPIDocumentation:
    """Test API documentation and OpenAPI schema."""
    
    def test_openapi_schema_generation(self, client: TestClient):
        """Test that OpenAPI schema is generated correctly."""
        response = client.get("/api/v0a3/openapi.json")
        
        assert response.status_code == 200
        schema = response.json()
        
        # Check basic OpenAPI structure
        assert "openapi" in schema
        assert "info" in schema
        assert "paths" in schema
        
        # Check API info
        info = schema["info"]
        assert info["title"] == "Tellus Climate Data API"
        assert info["version"] == "0.1.0a3"
        assert "description" in info
    
    def test_docs_endpoint_accessible(self, client: TestClient):
        """Test that Swagger UI docs are accessible."""
        response = client.get("/api/v0a3/docs")
        
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        # Check for Swagger UI elements in HTML
        assert "swagger-ui" in response.text.lower()
    
    def test_redoc_endpoint_accessible(self, client: TestClient):
        """Test that ReDoc documentation is accessible."""
        response = client.get("/api/v0a3/redoc")
        
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        # Check for ReDoc elements in HTML
        assert "redoc" in response.text.lower()
    
    def test_openapi_paths_coverage(self, client: TestClient):
        """Test that all expected endpoints are documented."""
        response = client.get("/api/v0a3/openapi.json")
        schema = response.json()
        paths = schema["paths"]
        
        # Check that key endpoints are documented
        expected_paths = [
            "/api/v0a3/",
            "/api/v0a3/health",
            "/api/v0a3/health/detailed",
            "/api/v0a3/simulations/",
            "/api/v0a3/simulations/{simulation_id}",
            "/api/v0a3/locations/",
            "/api/v0a3/locations/{location_name}",
            "/api/v0a3/locations/{location_name}/test"
        ]
        
        for expected_path in expected_paths:
            assert expected_path in paths
    
    def test_openapi_components_schemas(self, client: TestClient):
        """Test that Pydantic models are properly documented."""
        response = client.get("/api/v0a3/openapi.json")
        schema = response.json()
        
        # Check for schema components (generated from Pydantic models)
        assert "components" in schema
        components = schema["components"]
        assert "schemas" in components
        
        schemas = components["schemas"]
        
        # Check for key DTO schemas
        expected_schemas = [
            "SimulationDto",
            "CreateSimulationDto", 
            "LocationDto",
            "CreateLocationDto",
            "HealthResponse",
            "PaginationInfo"
        ]
        
        for expected_schema in expected_schemas:
            assert expected_schema in schemas


class TestEndToEndWorkflows:
    """Test complete workflows across multiple endpoints."""
    
    @pytest.mark.skip(reason="Requires stateful mocks - mock services return static data")
    def test_simulation_crud_workflow(self, client: TestClient):
        """Test complete CRUD workflow for simulations."""
        simulation_data = {
            "simulation_id": "e2e-test-sim",
            "model_id": "FESOM2",
            "attrs": {"experiment": "E2E-Test"}
        }
        
        # 1. Create simulation
        create_response = client.post("/api/v0a3/simulations/", json=simulation_data)
        assert create_response.status_code == 201
        created_sim = create_response.json()
        assert created_sim["simulation_id"] == "e2e-test-sim"
        
        # 2. Retrieve simulation
        get_response = client.get("/api/v0a3/simulations/e2e-test-sim")
        assert get_response.status_code == 200
        retrieved_sim = get_response.json()
        assert retrieved_sim["simulation_id"] == "e2e-test-sim"
        
        # 3. Update simulation
        update_data = {"model_id": "Updated-Model"}
        update_response = client.put("/api/v0a3/simulations/e2e-test-sim", json=update_data)
        assert update_response.status_code == 200
        
        # 4. List simulations (should include our new one)
        list_response = client.get("/api/v0a3/simulations/")
        assert list_response.status_code == 200
        simulations = list_response.json()["simulations"]
        sim_ids = [s["simulation_id"] for s in simulations]
        assert "e2e-test-sim" in sim_ids
        
        # 5. Delete simulation
        delete_response = client.delete("/api/v0a3/simulations/e2e-test-sim")
        assert delete_response.status_code == 204
        
        # 6. Verify deletion
        get_after_delete = client.get("/api/v0a3/simulations/e2e-test-sim")
        assert get_after_delete.status_code == 404
    
    @pytest.mark.skip(reason="Requires stateful mocks - mock services return static data")
    def test_location_crud_workflow(self, client: TestClient):
        """Test complete CRUD workflow for locations."""
        location_data = {
            "name": "e2e-test-location",
            "kinds": ["DISK"],
            "protocol": "file",
            "path": "/tmp/e2e-test"
        }
        
        # 1. Create location
        create_response = client.post("/api/v0a3/locations/", json=location_data)
        assert create_response.status_code == 201
        created_loc = create_response.json()
        assert created_loc["name"] == "e2e-test-location"
        
        # 2. Test location connectivity
        test_response = client.post("/api/v0a3/locations/e2e-test-location/test")
        assert test_response.status_code == 200
        test_result = test_response.json()
        assert test_result["location_name"] == "e2e-test-location"
        
        # 3. Retrieve location
        get_response = client.get("/api/v0a3/locations/e2e-test-location")
        assert get_response.status_code == 200
        retrieved_loc = get_response.json()
        assert retrieved_loc["name"] == "e2e-test-location"
        
        # 4. Update location
        update_data = {"path": "/tmp/updated-path"}
        update_response = client.put("/api/v0a3/locations/e2e-test-location", json=update_data)
        assert update_response.status_code == 200
        
        # 5. List locations (should include our new one)
        list_response = client.get("/api/v0a3/locations/")
        assert list_response.status_code == 200
        locations = list_response.json()["locations"]
        location_names = [l["name"] for l in locations]
        assert "e2e-test-location" in location_names
        
        # 6. Delete location
        delete_response = client.delete("/api/v0a3/locations/e2e-test-location")
        assert delete_response.status_code == 204
        
        # 7. Verify deletion
        get_after_delete = client.get("/api/v0a3/locations/e2e-test-location")
        assert get_after_delete.status_code == 404
    
    def test_health_monitoring_workflow(self, client: TestClient):
        """Test complete health monitoring workflow."""
        # 1. Check basic health
        health_response = client.get("/api/v0a3/health")
        assert health_response.status_code == 200
        health_data = health_response.json()
        assert health_data["status"] == "healthy"
        
        # 2. Check detailed health
        detailed_response = client.get("/api/v0a3/health/detailed")
        assert detailed_response.status_code == 200
        detailed_data = detailed_response.json()
        
        # 3. Verify service status
        services = detailed_data["services"]
        assert services["simulation_service"] == "available"
        assert services["location_service"] == "available"
        
        # 4. Overall health should be healthy when all services are available
        assert detailed_data["status"] == "healthy"


class TestAPICrossEndpointInteractions:
    """Test interactions between different API endpoints."""
    
    def test_pagination_consistency_across_endpoints(self, client: TestClient):
        """Test that pagination works consistently across endpoints."""
        # Test simulation pagination
        sim_response = client.get("/api/v0a3/simulations/?page=1&page_size=1")
        assert sim_response.status_code == 200
        sim_data = sim_response.json()
        assert sim_data["pagination"]["page"] == 1
        assert sim_data["pagination"]["page_size"] == 1
        
        # Test location pagination
        loc_response = client.get("/api/v0a3/locations/?page=1&page_size=1")
        assert loc_response.status_code == 200
        loc_data = loc_response.json()
        assert loc_data["pagination"]["page"] == 1
        assert loc_data["pagination"]["page_size"] == 1
        
        # Pagination structure should be identical
        assert sim_data["pagination"].keys() == loc_data["pagination"].keys()
    
    def test_search_functionality_across_endpoints(self, client: TestClient):
        """Test that search works consistently across endpoints."""
        # Test simulation search
        sim_response = client.get("/api/v0a3/simulations/?search=test")
        assert sim_response.status_code == 200
        sim_data = sim_response.json()
        assert sim_data["filters_applied"]["search_term"] == "test"
        
        # Test location search
        loc_response = client.get("/api/v0a3/locations/?search=test")
        assert loc_response.status_code == 200
        loc_data = loc_response.json()
        assert loc_data["filters_applied"]["search_term"] == "test"
    
    def test_error_response_consistency(self, client: TestClient):
        """Test that error responses are consistent across endpoints."""
        # Test 404 errors
        sim_404 = client.get("/api/v0a3/simulations/non-existent")
        loc_404 = client.get("/api/v0a3/locations/non-existent")
        
        assert sim_404.status_code == 404
        assert loc_404.status_code == 404
        
        sim_error = sim_404.json()
        loc_error = loc_404.json()
        
        # Both should have "detail" field
        assert "detail" in sim_error
        assert "detail" in loc_error
        assert "not found" in sim_error["detail"]
        assert "not found" in loc_error["detail"]


class TestAPISecurityAndValidation:
    """Test API security features and input validation."""
    
    @pytest.mark.skip(reason="Requires input sanitization middleware - not implemented")
    def test_input_sanitization(self, client: TestClient):
        """Test that inputs are properly sanitized."""
        # Test with potentially malicious input
        malicious_data = {
            "simulation_id": "<script>alert('xss')</script>",
            "model_id": "'; DROP TABLE simulations; --",
            "attrs": {"key": "<img src=x onerror=alert(1)>"}
        }
        
        response = client.post("/api/v0a3/simulations/", json=malicious_data)
        
        # Should either accept and sanitize, or reject
        assert response.status_code in [201, 400, 422]
        
        if response.status_code == 201:
            # If accepted, check that data was sanitized
            data = response.json()
            # Basic check that dangerous scripts aren't directly reflected
            assert "<script>" not in json.dumps(data)
    
    def test_request_size_limits(self, client: TestClient):
        """Test handling of large request payloads."""
        # Create a large payload
        large_attrs = {f"key_{i}": f"value_{i}" * 100 for i in range(100)}
        large_data = {
            "simulation_id": "large-test-sim",
            "attrs": large_attrs
        }
        
        response = client.post("/api/v0a3/simulations/", json=large_data)
        
        # Should handle large payloads gracefully
        assert response.status_code in [201, 400, 413, 422]
    
    def test_content_type_validation(self, client: TestClient):
        """Test that content type is properly validated."""
        # Send XML when JSON is expected
        response = client.post(
            "/api/v0a3/simulations/",
            content="<xml>not json</xml>",
            headers={"Content-Type": "application/xml"}
        )
        
        # Should reject non-JSON content
        assert response.status_code in [400, 415, 422]


class TestAPIPerformance:
    """Test API performance characteristics."""
    
    def test_endpoint_response_times(self, client: TestClient):
        """Test that endpoints respond within reasonable time."""
        import time
        
        endpoints = [
            "/api/v0a3/health",
            "/api/v0a3/simulations/",
            "/api/v0a3/locations/",
            "/api/v0a3/"
        ]
        
        for endpoint in endpoints:
            start_time = time.time()
            response = client.get(endpoint)
            end_time = time.time()
            
            assert response.status_code == 200
            # All endpoints should respond within 1 second
            assert (end_time - start_time) < 1.0
    
    def test_concurrent_requests_handling(self, client: TestClient):
        """Test that API can handle multiple concurrent requests."""
        import threading
        import time
        
        results = []
        
        def make_request():
            response = client.get("/api/v0a3/health")
            results.append(response.status_code)
        
        # Create multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=make_request)
            threads.append(thread)
        
        # Start all threads
        start_time = time.time()
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        end_time = time.time()
        
        # All requests should succeed
        assert len(results) == 5
        assert all(status == 200 for status in results)
        
        # Should complete within reasonable time
        assert (end_time - start_time) < 5.0


class TestAPICompatibility:
    """Test API compatibility and versioning."""
    
    def test_api_version_consistency(self, client: TestClient):
        """Test that API version is consistent across endpoints."""
        # Get version from root endpoint (now versioned)
        root_response = client.get("/api/v0a3/")
        root_version = root_response.json()["version"]
        
        # Get version from health endpoint (now versioned)
        health_response = client.get("/api/v0a3/health")
        health_version = health_response.json()["version"]
        
        # Get version from OpenAPI schema (now versioned)
        openapi_response = client.get("/api/v0a3/openapi.json")
        openapi_version = openapi_response.json()["info"]["version"]
        
        # All should report same version
        assert root_version == health_version == openapi_version == "0.1.0a3"
    
    def test_json_serialization_consistency(self, client: TestClient):
        """Test that JSON serialization is consistent."""
        # Test simulation serialization
        sim_response = client.get("/api/v0a3/simulations/")
        assert sim_response.status_code == 200
        sim_data = sim_response.json()
        
        # Should be valid JSON and deserializable
        sim_json_str = json.dumps(sim_data)
        reparsed_data = json.loads(sim_json_str)
        assert reparsed_data == sim_data
        
        # Test location serialization
        loc_response = client.get("/api/v0a3/locations/")
        assert loc_response.status_code == 200
        loc_data = loc_response.json()
        
        loc_json_str = json.dumps(loc_data)
        reparsed_loc_data = json.loads(loc_json_str)
        assert reparsed_loc_data == loc_data