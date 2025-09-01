"""
Unit tests for archive domain entities - simplified version.

These tests validate the actual archive domain model as it exists.
"""

import time

import pytest

from tellus.domain.entities.archive import (ArchiveId, ArchiveMetadata,
                                            ArchiveType, Checksum,
                                            FileMetadata)


class TestArchiveId:
    """Test ArchiveId value object."""
    
    def test_archive_id_creation(self):
        """Test basic archive ID creation."""
        archive_id = ArchiveId("test-archive")
        assert archive_id.value == "test-archive"
        assert str(archive_id) == "test-archive"
    
    def test_archive_id_validation(self):
        """Test archive ID validation rules."""
        # Valid IDs
        valid_ids = ["test-archive", "archive_123", "my-archive-2024"]
        for valid_id in valid_ids:
            archive_id = ArchiveId(valid_id)
            assert archive_id.value == valid_id
    
    def test_archive_id_invalid(self):
        """Test invalid archive IDs raise errors."""
        with pytest.raises(ValueError):
            ArchiveId("")  # Empty string
        
        with pytest.raises(ValueError):
            ArchiveId("   ")  # Whitespace only
    
    def test_archive_id_equality(self):
        """Test archive ID equality comparison."""
        id1 = ArchiveId("test-archive")
        id2 = ArchiveId("test-archive")
        id3 = ArchiveId("different-archive")
        
        assert id1 == id2
        assert id1 != id3
        assert hash(id1) == hash(id2)
        assert hash(id1) != hash(id3)


class TestArchiveType:
    """Test ArchiveType enumeration."""
    
    def test_archive_type_values(self):
        """Test archive type enumeration values."""
        assert ArchiveType.COMPRESSED.value == "compressed"
        assert ArchiveType.SPLIT_TARBALL.value == "split_tarball"
        assert ArchiveType.ORGANIZED.value == "organized"


class TestChecksum:
    """Test Checksum value object."""
    
    def test_checksum_creation(self):
        """Test basic checksum creation."""
        checksum = Checksum(value="d41d8cd98f00b204e9800998ecf8427e", algorithm="md5")
        assert checksum.value == "d41d8cd98f00b204e9800998ecf8427e"
        assert checksum.algorithm == "md5"
    
    def test_checksum_validation(self):
        """Test checksum validation."""
        # Valid MD5 checksum
        Checksum(value="d41d8cd98f00b204e9800998ecf8427e", algorithm="md5")
        
        # Valid SHA256 checksum  
        Checksum(value="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", algorithm="sha256")
        
        # Invalid checksums
        with pytest.raises(ValueError):
            Checksum(value="", algorithm="md5")  # Empty value
        
        with pytest.raises(ValueError):
            Checksum(value="abc123", algorithm="")  # Empty algorithm
        
        with pytest.raises(ValueError):
            Checksum(value="abc123", algorithm="md5")  # Wrong length for MD5
    
    def test_checksum_string_representation(self):
        """Test checksum string representation."""
        checksum = Checksum(value="d41d8cd98f00b204e9800998ecf8427e", algorithm="md5")
        assert str(checksum) == "md5:d41d8cd98f00b204e9800998ecf8427e"
    
    def test_checksum_equality(self):
        """Test checksum equality comparison."""
        checksum1 = Checksum(value="d41d8cd98f00b204e9800998ecf8427e", algorithm="md5")
        checksum2 = Checksum(value="d41d8cd98f00b204e9800998ecf8427e", algorithm="md5")
        checksum3 = Checksum(value="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", algorithm="sha256")
        
        assert checksum1 == checksum2
        assert checksum1 != checksum3


class TestFileMetadata:
    """Test FileMetadata entity."""
    
    def test_file_metadata_creation(self):
        """Test basic file metadata creation."""
        checksum = Checksum(value="d41d8cd98f00b204e9800998ecf8427e", algorithm="md5")
        file_metadata = FileMetadata(
            path="data/output.nc",
            size=1024,
            checksum=checksum
        )
        
        assert file_metadata.path == "data/output.nc"
        assert file_metadata.size == 1024
        assert file_metadata.checksum == checksum
        assert isinstance(file_metadata.tags, set)
    
    def test_file_metadata_validation(self):
        """Test file metadata validation."""
        # Invalid paths
        with pytest.raises(ValueError):
            FileMetadata(path="", size=1024)  # Empty path
        
        # Invalid size
        with pytest.raises(ValueError):
            FileMetadata(path="test.txt", size=-100)  # Negative size
    
    def test_file_metadata_tag_management(self):
        """Test file metadata tag management."""
        file_metadata = FileMetadata(path="data/output.nc", size=1024)
        
        # Add tags
        file_metadata.add_tag("climate")
        file_metadata.add_tag("model")
        assert file_metadata.has_tag("climate")
        assert file_metadata.has_tag("model")
        
        # Remove tag
        removed = file_metadata.remove_tag("climate")
        assert removed is True
        assert not file_metadata.has_tag("climate")
        assert file_metadata.has_tag("model")
        
        # Remove non-existent tag
        removed = file_metadata.remove_tag("nonexistent")
        assert removed is False
    
    def test_file_metadata_tag_matching(self):
        """Test file metadata tag matching."""
        file_metadata = FileMetadata(
            path="data/output.nc",
            size=1024,
            tags={"climate", "model", "output"}
        )
        
        # Test any tag matching
        assert file_metadata.matches_any_tag({"climate", "test"}) is True
        assert file_metadata.matches_any_tag({"test", "other"}) is False
        
        # Test all tag matching
        assert file_metadata.matches_all_tags({"climate", "model"}) is True
        assert file_metadata.matches_all_tags({"climate", "test"}) is False


class TestArchiveMetadata:
    """Test ArchiveMetadata entity."""
    
    def test_archive_metadata_creation(self):
        """Test basic archive metadata creation."""
        archive_id = ArchiveId("test-archive")
        metadata = ArchiveMetadata(
            archive_id=archive_id,
            location="test-location",
            archive_type=ArchiveType.COMPRESSED,
            simulation_id="test-sim"
        )
        
        assert metadata.archive_id == archive_id
        assert metadata.location == "test-location"
        assert metadata.archive_type == ArchiveType.COMPRESSED
        assert metadata.simulation_id == "test-sim"
        assert isinstance(metadata.created_time, float)
        assert isinstance(metadata.tags, set)
    
    def test_archive_metadata_with_optional_fields(self):
        """Test archive metadata with optional fields."""
        archive_id = ArchiveId("test-archive")
        checksum = Checksum(value="d41d8cd98f00b204e9800998ecf8427e", algorithm="md5")
        
        metadata = ArchiveMetadata(
            archive_id=archive_id,
            location="test-location",
            archive_type=ArchiveType.COMPRESSED,
            simulation_id="test-sim",
            checksum=checksum,
            size=1024 * 1024,  # 1MB
            description="Test archive description",
            version="1.0",
            tags={"climate", "model", "output"}
        )
        
        assert metadata.checksum == checksum
        assert metadata.size == 1024 * 1024
        assert metadata.description == "Test archive description"
        assert metadata.version == "1.0"
        assert metadata.tags == {"climate", "model", "output"}
    
    def test_archive_metadata_validation(self):
        """Test archive metadata validation."""
        archive_id = ArchiveId("test-archive")
        
        # Invalid archive_id type
        with pytest.raises(ValueError):
            ArchiveMetadata(
                archive_id="not-an-archive-id",  # Should be ArchiveId instance
                location="test-location",
                archive_type=ArchiveType.COMPRESSED
            )
        
        # Invalid location
        with pytest.raises(ValueError):
            ArchiveMetadata(
                archive_id=archive_id,
                location="",  # Empty location
                archive_type=ArchiveType.COMPRESSED
            )
        
        # Invalid archive_type
        with pytest.raises(ValueError):
            ArchiveMetadata(
                archive_id=archive_id,
                location="test-location",
                archive_type="not-an-enum"  # Should be ArchiveType enum
            )