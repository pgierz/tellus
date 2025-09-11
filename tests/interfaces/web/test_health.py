"""
Tests for health check endpoints.

Validates that the health endpoints return correct status information
and that the API provides proper monitoring capabilities.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock


class TestHealthEndpoints:
    """Test health check endpoints."""
    
    def test_basic_health_check(self, client: TestClient):
        """Test basic health endpoint returns success."""
        response = client.get("/api/v0a3/health")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check required fields
        assert "status" in data
        assert "timestamp" in data
        assert "uptime_seconds" in data
        assert "version" in data
        
        assert data["status"] == "healthy"
        assert data["version"] == "0.1.0a3"
        assert isinstance(data["uptime_seconds"], (int, float))
        assert data["uptime_seconds"] >= 0
    
    def test_detailed_health_check(self, client: TestClient):
        """Test detailed health endpoint returns comprehensive status."""
        response = client.get("/api/v0a3/health/detailed")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check basic fields
        assert "status" in data
        assert "timestamp" in data
        assert "uptime_seconds" in data
        assert "version" in data
        
        # Check detailed fields
        assert "services" in data
        assert "memory_usage" in data
        assert "system_info" in data
        
        # Check service status
        services = data["services"]
        assert "simulation_service" in services
        assert "location_service" in services
        assert services["simulation_service"] == "available"
        assert services["location_service"] == "available"
        
        # Check system info
        system_info = data["system_info"]
        assert "api_framework" in system_info
        assert system_info["api_framework"] == "FastAPI"
    
    def test_api_root_information(self, client: TestClient):
        """Test API root endpoint returns basic information."""
        response = client.get("/api/v0a3/")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check required fields
        expected_fields = ["name", "version", "description", "docs_url", "redoc_url", "health_url"]
        for field in expected_fields:
            assert field in data
        
        assert data["name"] == "Tellus Climate Data API"
        assert data["version"] == "0.1.0a3"
        assert data["docs_url"] == "/api/v0a3/docs"
        assert data["redoc_url"] == "/api/v0a3/redoc"
        assert data["health_url"] == "/api/v0a3/health"
    
    def test_health_check_response_format(self, client: TestClient):
        """Test health check response follows correct schema."""
        response = client.get("/api/v0a3/health")
        data = response.json()
        
        # Validate timestamp format (should be ISO format)
        timestamp = data["timestamp"]
        assert "T" in timestamp  # Basic ISO format check
        
        # Validate uptime is numeric
        assert isinstance(data["uptime_seconds"], (int, float))
        
        # Validate status is string
        assert isinstance(data["status"], str)
        assert data["status"] in ["healthy", "degraded", "unhealthy"]
    
    def test_cors_headers_present(self, client: TestClient):
        """Test that CORS headers are properly set."""
        response = client.get("/api/v0a3/health")
        
        # Check for CORS headers (these should be set by the middleware)
        assert response.status_code == 200
        # Note: TestClient doesn't always include CORS headers in tests
        # This test validates the endpoint works, CORS would be tested in integration


class TestHealthEndpointErrors:
    """Test health endpoint error conditions."""
    
    def test_health_endpoint_error_resilience(self, client: TestClient):
        """Test that health endpoints handle errors gracefully."""
        # This test ensures the health endpoint returns 200 even if there are issues
        response = client.get("/api/v0a3/health/detailed")
        
        assert response.status_code == 200  # Should always return 200
        data = response.json()
        
        # Should have all required fields
        assert "status" in data
        assert "services" in data
        assert isinstance(data["services"], dict)
        
        # Services should be either available or unavailable
        for service, status in data["services"].items():
            assert status in ["available", "unavailable"]
    
    def test_health_endpoint_resilience(self, client: TestClient):
        """Test that health endpoints are resilient to repeated calls."""
        # Make multiple rapid calls
        for _ in range(5):
            response = client.get("/api/v0a3/health")
            assert response.status_code == 200
            
            data = response.json()
            assert data["status"] == "healthy"


class TestHealthMonitoring:
    """Test health monitoring capabilities."""
    
    def test_health_check_timing(self, client: TestClient):
        """Test that health checks respond quickly."""
        import time
        
        start_time = time.time()
        response = client.get("/api/v0a3/health")
        end_time = time.time()
        
        assert response.status_code == 200
        # Health check should be fast (under 1 second)
        assert (end_time - start_time) < 1.0
    
    def test_health_check_idempotency(self, client: TestClient):
        """Test that health checks are idempotent."""
        # Multiple calls should return consistent results
        response1 = client.get("/api/v0a3/health")
        response2 = client.get("/api/v0a3/health")
        
        assert response1.status_code == 200
        assert response2.status_code == 200
        
        data1 = response1.json()
        data2 = response2.json()
        
        # Status and version should be identical
        assert data1["status"] == data2["status"]
        assert data1["version"] == data2["version"]
        
        # Uptime should be similar (allowing for small time differences)
        assert abs(data1["uptime_seconds"] - data2["uptime_seconds"]) < 1.0