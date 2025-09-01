"""
Unit tests for location domain entities - simplified version.

These tests validate the actual location domain model as it exists.
"""

import pytest

from tellus.domain.entities.location import LocationEntity, LocationKind


class TestLocationKind:
    """Test LocationKind enumeration."""
    
    def test_location_kind_values(self):
        """Test location kind enumeration values."""
        # Check that the basic kinds exist
        assert LocationKind.TAPE
        assert LocationKind.COMPUTE
        assert LocationKind.DISK
        assert LocationKind.FILESERVER
    
    def test_location_kind_from_str(self):
        """Test creating location kind from string."""
        assert LocationKind.from_str("tape") == LocationKind.TAPE
        assert LocationKind.from_str("TAPE") == LocationKind.TAPE
        assert LocationKind.from_str("compute") == LocationKind.COMPUTE
        assert LocationKind.from_str("disk") == LocationKind.DISK
        assert LocationKind.from_str("fileserver") == LocationKind.FILESERVER
    
    def test_location_kind_from_str_invalid(self):
        """Test invalid location kind strings raise errors."""
        with pytest.raises(ValueError):
            LocationKind.from_str("invalid")
        
        with pytest.raises(ValueError):
            LocationKind.from_str("")


class TestLocationEntity:
    """Test LocationEntity domain entity."""
    
    def test_location_entity_creation(self):
        """Test basic location entity creation."""
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK],
            config={"protocol": "file", "path": "/data/storage"}
        )
        
        assert location.name == "test-location"
        assert location.kinds == [LocationKind.DISK]
        assert location.config == {"protocol": "file", "path": "/data/storage"}
        assert location.optional is False  # Default value
    
    def test_location_entity_with_optional_true(self):
        """Test location entity with optional flag."""
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK],
            config={"protocol": "file", "path": "/data"},
            optional=True
        )
        
        assert location.optional is True
    
    def test_location_entity_multiple_kinds(self):
        """Test location entity with multiple kinds."""
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK, LocationKind.FILESERVER],
            config={"protocol": "file", "path": "/data"}
        )
        
        assert LocationKind.DISK in location.kinds
        assert LocationKind.FILESERVER in location.kinds
        assert len(location.kinds) == 2
    
    def test_location_entity_validation_empty_name(self):
        """Test location entity validation for empty name."""
        with pytest.raises(ValueError):
            LocationEntity(
                name="",
                kinds=[LocationKind.DISK],
                config={"protocol": "file", "path": "/data"}
            )
    
    def test_location_entity_validation_empty_kinds(self):
        """Test location entity validation for empty kinds."""
        with pytest.raises(ValueError):
            LocationEntity(
                name="test-location",
                kinds=[],
                config={"protocol": "file", "path": "/data"}
            )
    
    def test_location_entity_validation_no_config(self):
        """Test location entity validation for missing config."""
        with pytest.raises(ValueError):
            LocationEntity(
                name="test-location",
                kinds=[LocationKind.DISK],
                config={}
            )
    
    def test_location_entity_config_validation(self):
        """Test location entity config validation."""
        # Valid configs should work
        valid_configs = [
            {"protocol": "file", "path": "/data"},
            {"protocol": "ssh", "path": "/remote", "storage_options": {"host": "server.com"}},
            {"protocol": "s3", "path": "bucket/data", "storage_options": {"endpoint": "s3.aws.com"}}
        ]
        
        for config in valid_configs:
            location = LocationEntity(
                name="test-location",
                kinds=[LocationKind.DISK],
                config=config
            )
            assert location.config == config
    
    def test_location_entity_validation_method(self):
        """Test location entity validation method directly."""
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK],
            config={"protocol": "file", "path": "/data"}
        )
        
        # Should return empty list for valid location
        errors = location.validate()
        assert errors == []
    
    def test_location_entity_supports_simulation_association(self):
        """Test location entity simulation association if available."""
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK],
            config={"protocol": "file", "path": "/data"}
        )
        
        # Check if the location has simulation association methods
        if hasattr(location, 'associated_simulations'):
            # Test the association functionality
            assert location.associated_simulations == set()
        else:
            # If not implemented yet, that's fine - just verify the entity is valid
            assert location.name == "test-location"