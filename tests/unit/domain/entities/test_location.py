"""
Unit tests for location domain entities.

These tests validate the location domain model including location kinds,
storage configuration, and connection management.
"""

from pathlib import Path

import pytest

from tellus.domain.entities.location import LocationEntity, LocationKind


class TestLocationKind:
    """Test LocationKind enumeration."""
    
    def test_location_kind_values(self):
        """Test location kind enumeration values."""
        assert LocationKind.TAPE.value == "tape"
        assert LocationKind.COMPUTE.value == "compute"
        assert LocationKind.DISK.value == "disk"
        assert LocationKind.FILESERVER.value == "fileserver"
    
    def test_location_kind_from_string(self):
        """Test creating location kind from string."""
        assert LocationKind("tape") == LocationKind.TAPE
        assert LocationKind("compute") == LocationKind.COMPUTE
        assert LocationKind("disk") == LocationKind.DISK
        assert LocationKind("fileserver") == LocationKind.FILESERVER


class TestStorageConfiguration:
    """Test StorageConfiguration value object."""
    
    def test_storage_configuration_creation(self):
        """Test basic storage configuration creation."""
        config = StorageConfiguration(
            protocol="file",
            path="/data/storage",
            storage_options={"permissions": "755"}
        )
        
        assert config.protocol == "file"
        assert config.path == "/data/storage"
        assert config.storage_options == {"permissions": "755"}
    
    def test_storage_configuration_validation(self):
        """Test storage configuration validation."""
        # Valid configurations
        StorageConfiguration(protocol="file", path="/data")
        StorageConfiguration(protocol="ssh", path="/remote/data", storage_options={"host": "server.com"})
        StorageConfiguration(protocol="s3", path="bucket/data", storage_options={"endpoint": "s3.aws.com"})
        
        # Invalid configurations
        with pytest.raises(ValueError):
            StorageConfiguration(protocol="", path="/data")  # Empty protocol
        
        with pytest.raises(ValueError):
            StorageConfiguration(protocol="file", path="")  # Empty path
    
    def test_storage_configuration_get_host(self):
        """Test storage configuration host extraction."""
        # SSH configuration with host
        ssh_config = StorageConfiguration(
            protocol="ssh",
            path="/data",
            storage_options={"host": "server.example.com", "port": 22}
        )
        assert ssh_config.get_host() == "server.example.com"
        
        # File configuration without host
        file_config = StorageConfiguration(protocol="file", path="/data")
        assert file_config.get_host() is None
    
    def test_storage_configuration_get_port(self):
        """Test storage configuration port extraction."""
        # Configuration with port
        config_with_port = StorageConfiguration(
            protocol="ssh",
            path="/data",
            storage_options={"host": "server.com", "port": 2222}
        )
        assert config_with_port.get_port() == 2222
        
        # Configuration without port
        config_without_port = StorageConfiguration(protocol="file", path="/data")
        assert config_without_port.get_port() is None
    
    def test_storage_configuration_is_remote(self):
        """Test storage configuration remote detection."""
        # Local file system
        local_config = StorageConfiguration(protocol="file", path="/data")
        assert local_config.is_remote() is False
        
        # SSH (remote)
        ssh_config = StorageConfiguration(
            protocol="ssh",
            path="/data",
            storage_options={"host": "server.com"}
        )
        assert ssh_config.is_remote() is True
        
        # S3 (remote)
        s3_config = StorageConfiguration(protocol="s3", path="bucket/data")
        assert s3_config.is_remote() is True
    
    def test_storage_configuration_serialization(self):
        """Test storage configuration serialization."""
        original_config = StorageConfiguration(
            protocol="ssh",
            path="/remote/data",
            storage_options={"host": "server.com", "port": 22, "username": "user"}
        )
        
        # Serialize to dict
        data = original_config.to_dict()
        assert data["protocol"] == "ssh"
        assert data["path"] == "/remote/data"
        assert data["storage_options"]["host"] == "server.com"
        assert data["storage_options"]["port"] == 22
        assert data["storage_options"]["username"] == "user"
        
        # Deserialize from dict
        restored_config = StorageConfiguration.from_dict(data)
        assert restored_config.protocol == original_config.protocol
        assert restored_config.path == original_config.path
        assert restored_config.storage_options == original_config.storage_options


class TestConnectionStatus:
    """Test ConnectionStatus enumeration."""
    
    def test_connection_status_values(self):
        """Test connection status enumeration values."""
        assert ConnectionStatus.CONNECTED.value == "connected"
        assert ConnectionStatus.DISCONNECTED.value == "disconnected"
        assert ConnectionStatus.ERROR.value == "error"
        assert ConnectionStatus.UNKNOWN.value == "unknown"


class TestLocationEntity:
    """Test LocationEntity domain entity."""
    
    def test_location_entity_creation(self):
        """Test basic location entity creation."""
        config = StorageConfiguration(protocol="file", path="/data/storage")
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK],
            storage_config=config
        )
        
        assert location.name == "test-location"
        assert location.kinds == [LocationKind.DISK]
        assert location.storage_config == config
        assert location.optional is False  # Default value
        assert isinstance(location.created_time, float)
    
    def test_location_entity_with_optional_fields(self):
        """Test location entity with optional fields."""
        config = StorageConfiguration(protocol="file", path="/data")
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK, LocationKind.FILESERVER],
            storage_config=config,
            description="Test storage location",
            optional=True,
            tags={"test", "storage"},
            metadata={"capacity": "1TB", "raid": "5"}
        )
        
        assert location.description == "Test storage location"
        assert location.optional is True
        assert location.tags == {"test", "storage"}
        assert location.metadata == {"capacity": "1TB", "raid": "5"}
    
    def test_location_entity_validation(self):
        """Test location entity validation."""
        config = StorageConfiguration(protocol="file", path="/data")
        
        # Invalid name
        with pytest.raises(ValueError):
            LocationEntity(name="", kinds=[LocationKind.DISK], storage_config=config)
        
        # Empty kinds list
        with pytest.raises(ValueError):
            LocationEntity(name="test", kinds=[], storage_config=config)
        
        # Invalid kind type
        with pytest.raises(ValueError):
            LocationEntity(name="test", kinds=["invalid"], storage_config=config)
    
    def test_location_entity_kind_management(self):
        """Test location entity kind management."""
        config = StorageConfiguration(protocol="file", path="/data")
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK],
            storage_config=config
        )
        
        # Add kind
        location.add_kind(LocationKind.FILESERVER)
        assert LocationKind.FILESERVER in location.kinds
        assert LocationKind.DISK in location.kinds
        
        # Remove kind
        removed = location.remove_kind(LocationKind.DISK)
        assert removed is True
        assert LocationKind.DISK not in location.kinds
        assert LocationKind.FILESERVER in location.kinds
        
        # Try to remove non-existent kind
        removed = location.remove_kind(LocationKind.TAPE)
        assert removed is False
        
        # Try to remove last kind (should fail)
        with pytest.raises(ValueError, match="Cannot remove the last kind"):
            location.remove_kind(LocationKind.FILESERVER)
    
    def test_location_entity_has_kind(self):
        """Test location entity kind checking."""
        config = StorageConfiguration(protocol="file", path="/data")
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK, LocationKind.FILESERVER],
            storage_config=config
        )
        
        assert location.has_kind(LocationKind.DISK) is True
        assert location.has_kind(LocationKind.FILESERVER) is True
        assert location.has_kind(LocationKind.TAPE) is False
        assert location.has_kind(LocationKind.COMPUTE) is False
    
    def test_location_entity_tag_management(self):
        """Test location entity tag management."""
        config = StorageConfiguration(protocol="file", path="/data")
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK],
            storage_config=config
        )
        
        # Add tags
        location.add_tag("storage")
        location.add_tag("primary")
        assert "storage" in location.tags
        assert "primary" in location.tags
        
        # Remove tag
        removed = location.remove_tag("storage")
        assert removed is True
        assert "storage" not in location.tags
        assert "primary" in location.tags
        
        # Remove non-existent tag
        removed = location.remove_tag("nonexistent")
        assert removed is False
    
    def test_location_entity_has_tag(self):
        """Test location entity tag checking."""
        config = StorageConfiguration(protocol="file", path="/data")
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK],
            storage_config=config,
            tags={"storage", "primary"}
        )
        
        assert location.has_tag("storage") is True
        assert location.has_tag("primary") is True
        assert location.has_tag("backup") is False
    
    def test_location_entity_is_accessible(self):
        """Test location entity accessibility checking."""
        config = StorageConfiguration(protocol="file", path="/data")
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK],
            storage_config=config
        )
        
        # Initially unknown
        assert location.is_accessible() is None
        
        # Set connection status
        location.connection_status = ConnectionStatus.CONNECTED
        assert location.is_accessible() is True
        
        location.connection_status = ConnectionStatus.DISCONNECTED
        assert location.is_accessible() is False
        
        location.connection_status = ConnectionStatus.ERROR
        assert location.is_accessible() is False
    
    def test_location_entity_update_connection_status(self):
        """Test location entity connection status updates."""
        config = StorageConfiguration(protocol="file", path="/data")
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK],
            storage_config=config
        )
        
        # Update connection status
        location.update_connection_status(ConnectionStatus.CONNECTED)
        assert location.connection_status == ConnectionStatus.CONNECTED
        assert isinstance(location.last_checked_time, float)
        
        # Update with error
        location.update_connection_status(ConnectionStatus.ERROR, "Connection timeout")
        assert location.connection_status == ConnectionStatus.ERROR
        assert location.last_error_message == "Connection timeout"
    
    def test_location_entity_serialization(self):
        """Test location entity serialization."""
        config = StorageConfiguration(
            protocol="ssh",
            path="/remote/data",
            storage_options={"host": "server.com"}
        )
        original_location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.COMPUTE, LocationKind.DISK],
            storage_config=config,
            description="Test remote location",
            optional=True,
            tags={"remote", "compute"},
            metadata={"cpu_cores": 32, "memory": "64GB"}
        )
        
        # Serialize to dict
        data = original_location.to_dict()
        assert data["name"] == "test-location"
        assert set(data["kinds"]) == {"compute", "disk"}
        assert data["storage_config"]["protocol"] == "ssh"
        assert data["storage_config"]["path"] == "/remote/data"
        assert data["description"] == "Test remote location"
        assert data["optional"] is True
        assert set(data["tags"]) == {"remote", "compute"}
        assert data["metadata"]["cpu_cores"] == 32
        
        # Deserialize from dict
        restored_location = LocationEntity.from_dict(data)
        assert restored_location.name == original_location.name
        assert set(restored_location.kinds) == set(original_location.kinds)
        assert restored_location.storage_config.protocol == original_location.storage_config.protocol
        assert restored_location.storage_config.path == original_location.storage_config.path
        assert restored_location.description == original_location.description
        assert restored_location.optional == original_location.optional
        assert restored_location.tags == original_location.tags
        assert restored_location.metadata == original_location.metadata
    
    def test_location_entity_supports_simulation_association(self):
        """Test location entity simulation association tracking."""
        config = StorageConfiguration(protocol="file", path="/data")
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.DISK],
            storage_config=config
        )
        
        # Initially no associations
        assert location.get_associated_simulations() == set()
        assert location.is_simulation_associated("sim1") is False
        
        # Associate simulations
        location.associate_simulation("sim1", {"path_prefix": "/sim1"})
        location.associate_simulation("sim2", {"path_prefix": "/sim2"})
        
        assert location.get_associated_simulations() == {"sim1", "sim2"}
        assert location.is_simulation_associated("sim1") is True
        assert location.is_simulation_associated("sim2") is True
        assert location.is_simulation_associated("sim3") is False
        
        # Get context
        context = location.get_simulation_context("sim1")
        assert context == {"path_prefix": "/sim1"}
        
        # Disassociate simulation
        result = location.disassociate_simulation("sim1")
        assert result is True
        assert location.get_associated_simulations() == {"sim2"}
        assert location.is_simulation_associated("sim1") is False
        
        # Try to disassociate non-associated simulation
        result = location.disassociate_simulation("sim3")
        assert result is False