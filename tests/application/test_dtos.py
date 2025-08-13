"""
Unit tests for application DTOs.

Tests the validation logic and basic functionality of DTO classes.
"""

import pytest
from typing import Dict, Any

from tellus.application.dtos import (
    CreateSimulationDto, UpdateSimulationDto, SimulationDto,
    CreateLocationDto, UpdateLocationDto, LocationDto
)


class TestSimulationDtos:
    """Test simulation DTO classes."""
    
    def test_create_simulation_dto_valid(self):
        """Test creating valid CreateSimulationDto."""
        dto = CreateSimulationDto(
            simulation_id="test_sim",
            model_id="test_model", 
            experiment_id="test_exp",
            path="/test/path",
            attrs={"key": "value"}
        )
        
        assert dto.simulation_id == "test_sim"
        assert dto.model_id == "test_model"
        assert dto.experiment_id == "test_exp"
        assert dto.path == "/test/path"
        assert dto.attrs == {"key": "value"}
    
    def test_create_simulation_dto_empty_strings_fail(self):
        """Test that empty strings are rejected."""
        with pytest.raises(ValueError, match="simulation_id cannot be empty string"):
            CreateSimulationDto(simulation_id="")
        
        with pytest.raises(ValueError, match="model_id cannot be empty string"):
            CreateSimulationDto(simulation_id="test", model_id="")
        
        with pytest.raises(ValueError, match="experiment_id cannot be empty string"):
            CreateSimulationDto(simulation_id="test", experiment_id="")
    
    def test_create_simulation_dto_whitespace_strings_fail(self):
        """Test that whitespace-only strings are rejected.""" 
        with pytest.raises(ValueError, match="simulation_id cannot be empty string"):
            CreateSimulationDto(simulation_id="   ")
        
        with pytest.raises(ValueError, match="model_id cannot be empty string"):
            CreateSimulationDto(simulation_id="test", model_id="  \t  ")
    
    def test_create_simulation_dto_none_values_allowed(self):
        """Test that None values are allowed for optional fields."""
        dto = CreateSimulationDto(
            simulation_id="test",
            model_id=None,
            experiment_id=None
        )
        
        assert dto.simulation_id == "test"
        assert dto.model_id is None
        assert dto.experiment_id is None
    
    def test_update_simulation_dto_validation(self):
        """Test UpdateSimulationDto validation."""
        # Valid update DTO
        dto = UpdateSimulationDto(
            model_id="updated_model",
            experiment_id="updated_exp",
            attrs={"updated": "value"}
        )
        assert dto.model_id == "updated_model"
        assert dto.experiment_id == "updated_exp"
        
        # Empty strings should fail
        with pytest.raises(ValueError, match="model_id cannot be empty string"):
            UpdateSimulationDto(model_id="")
        
        with pytest.raises(ValueError, match="experiment_id cannot be empty string"):
            UpdateSimulationDto(experiment_id="")
    
    def test_simulation_dto_validation(self):
        """Test SimulationDto validation."""
        # Valid DTO
        dto = SimulationDto(
            simulation_id="test_sim",
            uid="test_uid",
            model_id="test_model",
            experiment_id="test_exp"
        )
        assert dto.simulation_id == "test_sim" 
        assert dto.uid == "test_uid"
        assert dto.model_id == "test_model"
        assert dto.experiment_id == "test_exp"
        
        # Empty strings should fail
        with pytest.raises(ValueError, match="simulation_id cannot be empty string"):
            SimulationDto(simulation_id="", uid="test")
        
        with pytest.raises(ValueError, match="uid cannot be empty string"):
            SimulationDto(simulation_id="test", uid="")
        
        with pytest.raises(ValueError, match="model_id cannot be empty string"):
            SimulationDto(simulation_id="test", uid="test", model_id="")
        
        with pytest.raises(ValueError, match="experiment_id cannot be empty string"):
            SimulationDto(simulation_id="test", uid="test", experiment_id="")


class TestLocationDtos:
    """Test location DTO classes."""
    
    def test_create_location_dto_valid(self):
        """Test creating valid CreateLocationDto."""
        dto = CreateLocationDto(
            name="test_location",
            kinds=["DISK", "COMPUTE"],
            protocol="file",
            path="/test/path",
            storage_options={"option": "value"},
            optional=False,
            additional_config={"config": "value"}
        )
        
        assert dto.name == "test_location"
        assert dto.kinds == ["DISK", "COMPUTE"]
        assert dto.protocol == "file"
        assert dto.path == "/test/path"
        assert dto.storage_options == {"option": "value"}
        assert dto.optional is False
        assert dto.additional_config == {"config": "value"}
    
    def test_location_dto_valid(self):
        """Test creating valid LocationDto."""
        dto = LocationDto(
            name="test_location",
            kinds=["DISK"],
            protocol="file",
            path="/test/path",
            storage_options={},
            optional=True,
            additional_config={"key": "value"},
            is_remote=False
        )
        
        assert dto.name == "test_location"
        assert dto.kinds == ["DISK"]
        assert dto.protocol == "file"
        assert dto.path == "/test/path"
        assert dto.optional is True
        assert dto.additional_config == {"key": "value"}
        assert dto.is_remote is False
    
    def test_update_location_dto_valid(self):
        """Test creating valid UpdateLocationDto."""
        dto = UpdateLocationDto(
            kinds=["FILESERVER"],
            protocol="sftp",
            path="/new/path",
            optional=True
        )
        
        assert dto.kinds == ["FILESERVER"]
        assert dto.protocol == "sftp"
        assert dto.path == "/new/path"
        assert dto.optional is True


class TestDtoDefaults:
    """Test that DTO default values work correctly."""
    
    def test_simulation_dto_defaults(self):
        """Test CreateSimulationDto defaults."""
        dto = CreateSimulationDto(simulation_id="test")
        
        assert dto.model_id is None
        assert dto.experiment_id is None
        assert dto.path is None
        assert dto.attrs == {}
        assert dto.namelists == {}
        assert dto.snakemakes == {}
    
    def test_location_dto_defaults(self):
        """Test CreateLocationDto defaults."""
        dto = CreateLocationDto(
            name="test",
            kinds=["DISK"],
            protocol="file"
        )
        
        assert dto.path is None
        assert dto.storage_options == {}
        assert dto.optional is False
        assert dto.additional_config == {}


if __name__ == "__main__":
    pytest.main([__file__])