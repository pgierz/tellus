"""
Integration tests for legacy bridge functionality.

Tests the bridge adapters to ensure they correctly convert between 
legacy format and new application services.
"""

import pytest
from unittest.mock import Mock, MagicMock
from typing import Dict, Any, List

from tellus.core.legacy_bridge import SimulationBridge, LocationBridge
from tellus.application.service_factory import ApplicationServiceFactory
from tellus.application.dtos import (
    CreateSimulationDto, SimulationDto, UpdateSimulationDto,
    LocationDto
)
from tellus.application.exceptions import EntityNotFoundError


class TestSimulationBridge:
    """Test SimulationBridge integration with application services."""
    
    @pytest.fixture
    def mock_service_factory(self):
        """Create mock service factory."""
        factory = Mock(spec=ApplicationServiceFactory)
        factory.simulation_service = Mock()
        factory.location_service = Mock()
        return factory
    
    @pytest.fixture
    def simulation_bridge(self, mock_service_factory):
        """Create SimulationBridge with mocked dependencies."""
        return SimulationBridge(mock_service_factory)
    
    def test_create_simulation_from_legacy_data(self, simulation_bridge, mock_service_factory):
        """Test creating simulation from legacy format."""
        # Setup
        expected_dto = CreateSimulationDto(
            simulation_id="test_sim",
            model_id="test_model",
            experiment_id="test_exp",
            path="/test/path",
            attrs={"key": "value"}
        )
        
        return_dto = SimulationDto(
            simulation_id="test_sim",
            uid="test_uid",
            model_id="test_model",
            experiment_id="test_exp",
            path="/test/path",
            attrs={"key": "value"}
        )
        
        mock_service_factory.simulation_service.create_simulation.return_value = return_dto
        
        # Execute
        result = simulation_bridge.create_simulation_from_legacy_data(
            simulation_id="test_sim",
            model_id="test_model",
            experiment_id="test_exp",
            base_path="/test/path",  # Note: legacy uses base_path
            attributes={"key": "value"}  # Note: legacy uses attributes
        )
        
        # Verify
        mock_service_factory.simulation_service.create_simulation.assert_called_once()
        call_args = mock_service_factory.simulation_service.create_simulation.call_args[0][0]
        
        assert call_args.simulation_id == "test_sim"
        assert call_args.model_id == "test_model"
        assert call_args.experiment_id == "test_exp"
        assert call_args.path == "/test/path"  # Converted to path
        assert call_args.attrs == {"key": "value"}  # Converted to attrs
        
        assert result == return_dto
    
    def test_get_simulation_legacy_format(self, simulation_bridge, mock_service_factory):
        """Test getting simulation in legacy format."""
        # Setup
        sim_dto = SimulationDto(
            simulation_id="test_sim",
            uid="test_uid",
            model_id="test_model",
            experiment_id="test_exp",
            path="/test/path",
            attrs={"key": "value"},
            associated_locations=["loc1", "loc2"]
        )
        
        # Mock location data
        loc1_dto = LocationDto(
            name="loc1",
            kinds=["DISK"],  # Already strings in DTO
            protocol="file",
            path="/loc1/path",
            additional_config={"config": "value1"}
        )
        loc2_dto = LocationDto(
            name="loc2", 
            kinds=["COMPUTE"],  # Already strings in DTO
            protocol="ssh",
            path="/loc2/path",
            additional_config={"config": "value2"}
        )
        
        mock_service_factory.simulation_service.get_simulation.return_value = sim_dto
        mock_service_factory.location_service.get_location.side_effect = [loc1_dto, loc2_dto]
        
        # Execute
        result = simulation_bridge.get_simulation_legacy_format("test_sim")
        
        # Verify
        assert result is not None
        assert result["simulation_id"] == "test_sim"
        assert result["model_id"] == "test_model"
        assert result["experiment_id"] == "test_exp"
        assert result["path"] == "/test/path"
        assert result["attrs"] == {"key": "value"}
        
        # Check legacy format includes expected fields
        assert "created_at" in result
        assert "updated_at" in result
        assert "description" in result
        assert result["created_at"] is None
        assert result["updated_at"] is None
        assert result["description"] is None
        
        # Check locations conversion
        assert "locations" in result
        locations = result["locations"]
        assert "loc1" in locations
        assert "loc2" in locations
        
        # Verify loc1 structure
        loc1_legacy = locations["loc1"]
        assert loc1_legacy["location"]["name"] == "loc1"
        assert loc1_legacy["location"]["kinds"] == ["DISK"]  # Should remain as strings
        assert loc1_legacy["location"]["protocol"] == "file"
        assert loc1_legacy["location"]["config"] == {"config": "value1"}  # additional_config
        assert loc1_legacy["context"]["path_prefix"] == "/loc1/path"  # path field
    
    def test_get_simulation_not_found(self, simulation_bridge, mock_service_factory):
        """Test getting non-existent simulation returns None."""
        mock_service_factory.simulation_service.get_simulation.side_effect = EntityNotFoundError()
        
        result = simulation_bridge.get_simulation_legacy_format("nonexistent")
        
        assert result is None
    
    def test_update_simulation_attributes(self, simulation_bridge, mock_service_factory):
        """Test updating simulation attributes."""
        # Setup
        updated_dto = SimulationDto(
            simulation_id="test_sim",
            uid="test_uid",
            attrs={"updated": "value"}
        )
        mock_service_factory.simulation_service.update_simulation.return_value = updated_dto
        
        # Execute
        result = simulation_bridge.update_simulation_attributes(
            "test_sim", 
            {"updated": "value"}
        )
        
        # Verify
        assert result is True
        mock_service_factory.simulation_service.update_simulation.assert_called_once()
        
        call_args = mock_service_factory.simulation_service.update_simulation.call_args
        assert call_args[0][0] == "test_sim"  # simulation_id
        update_dto = call_args[0][1]
        assert update_dto.attrs == {"updated": "value"}
    
    def test_update_simulation_not_found(self, simulation_bridge, mock_service_factory):
        """Test updating non-existent simulation returns False."""
        mock_service_factory.simulation_service.update_simulation.side_effect = EntityNotFoundError()
        
        result = simulation_bridge.update_simulation_attributes("nonexistent", {})
        
        assert result is False


class TestLocationBridge:
    """Test LocationBridge integration with application services."""
    
    @pytest.fixture
    def mock_service_factory(self):
        """Create mock service factory."""
        factory = Mock(spec=ApplicationServiceFactory)
        factory.location_service = Mock()
        return factory
    
    @pytest.fixture
    def location_bridge(self, mock_service_factory):
        """Create LocationBridge with mocked dependencies."""
        return LocationBridge(mock_service_factory)
    
    def test_list_locations_legacy_format(self, location_bridge, mock_service_factory):
        """Test listing locations in legacy format."""
        # Setup
        loc1_dto = LocationDto(
            name="loc1",
            kinds=["DISK"],  # Already strings in DTO
            protocol="file",
            path="/loc1/path",
            optional=False,
            additional_config={"config1": "value1"}
        )
        loc2_dto = LocationDto(
            name="loc2",
            kinds=["COMPUTE", "FILESERVER"],  # Multiple kinds as strings
            protocol="ssh", 
            path="/loc2/path",
            optional=True,
            additional_config={"config2": "value2"}
        )
        
        location_list_mock = Mock()
        location_list_mock.locations = [loc1_dto, loc2_dto]
        mock_service_factory.location_service.list_locations.return_value = location_list_mock
        
        # Execute
        result = location_bridge.list_locations_legacy_format()
        
        # Verify
        assert "loc1" in result
        assert "loc2" in result
        
        # Check loc1 legacy format
        loc1_legacy = result["loc1"]
        assert loc1_legacy["name"] == "loc1"
        assert loc1_legacy["kinds"] == ["DISK"]  # Should remain as strings
        assert loc1_legacy["protocol"] == "file"
        assert loc1_legacy["config"] == {"config1": "value1"}  # additional_config
        assert loc1_legacy["path_prefix"] == "/loc1/path"  # path field
        assert loc1_legacy["optional"] is False
        assert loc1_legacy["description"] is None  # Not available in LocationDto
        
        # Check loc2 legacy format
        loc2_legacy = result["loc2"]
        assert loc2_legacy["name"] == "loc2"
        assert loc2_legacy["kinds"] == ["COMPUTE", "FILESERVER"]
        assert loc2_legacy["protocol"] == "ssh"
        assert loc2_legacy["config"] == {"config2": "value2"}
        assert loc2_legacy["path_prefix"] == "/loc2/path"
        assert loc2_legacy["optional"] is True
        assert loc2_legacy["description"] is None
    
    def test_get_location_legacy_format(self, location_bridge, mock_service_factory):
        """Test getting single location in legacy format."""
        # Setup
        loc_dto = LocationDto(
            name="test_loc",
            kinds=["TAPE"],  # Single kind as string
            protocol="s3",
            path="/test/path",
            optional=True,
            additional_config={"key": "value"}
        )
        mock_service_factory.location_service.get_location.return_value = loc_dto
        
        # Execute  
        result = location_bridge.get_location_legacy_format("test_loc")
        
        # Verify
        assert result is not None
        assert result["name"] == "test_loc"
        assert result["kinds"] == ["TAPE"]  # Should remain as strings
        assert result["protocol"] == "s3"
        assert result["config"] == {"key": "value"}  # additional_config
        assert result["path_prefix"] == "/test/path"  # path field
        assert result["optional"] is True
        assert result["description"] is None  # Not available in LocationDto
    
    def test_get_location_not_found(self, location_bridge, mock_service_factory):
        """Test getting non-existent location returns None."""
        mock_service_factory.location_service.get_location.side_effect = EntityNotFoundError()
        
        result = location_bridge.get_location_legacy_format("nonexistent")
        
        assert result is None


if __name__ == "__main__":
    pytest.main([__file__])