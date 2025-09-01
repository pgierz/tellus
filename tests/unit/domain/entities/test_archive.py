"""
Unit tests for archive domain entities.

These tests validate the archive domain model including metadata, checksums,
file handling, and archive type management.
"""

from datetime import datetime
from pathlib import Path

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
        assert ArchiveType.UNCOMPRESSED.value == "uncompressed"
        assert ArchiveType.INCREMENTAL.value == "incremental"
    
    def test_archive_type_from_string(self):
        """Test creating archive type from string."""
        assert ArchiveType("compressed") == ArchiveType.COMPRESSED
        assert ArchiveType("uncompressed") == ArchiveType.UNCOMPRESSED
        assert ArchiveType("incremental") == ArchiveType.INCREMENTAL


class TestChecksum:
    """Test Checksum value object."""
    
    def test_checksum_creation(self):
        """Test basic checksum creation."""
        checksum = Checksum(value="abc123", algorithm="md5")
        assert checksum.value == "abc123"
        assert checksum.algorithm == "md5"
    
    def test_checksum_validation(self):
        """Test checksum validation."""
        # Valid checksums
        Checksum(value="d41d8cd98f00b204e9800998ecf8427e", algorithm="md5")
        Checksum(value="da39a3ee5e6b4b0d3255bfef95601890afd80709", algorithm="sha1")
        
        # Invalid checksums
        with pytest.raises(ValueError):
            Checksum(value="", algorithm="md5")  # Empty value
        
        with pytest.raises(ValueError):
            Checksum(value="abc123", algorithm="")  # Empty algorithm
    
    def test_checksum_string_representation(self):
        """Test checksum string representation."""
        checksum = Checksum(value="abc123", algorithm="md5")
        assert str(checksum) == "md5:abc123"
    
    def test_checksum_equality(self):
        """Test checksum equality comparison."""
        checksum1 = Checksum(value="abc123", algorithm="md5")
        checksum2 = Checksum(value="abc123", algorithm="md5")
        checksum3 = Checksum(value="def456", algorithm="md5")
        checksum4 = Checksum(value="abc123", algorithm="sha1")
        
        assert checksum1 == checksum2
        assert checksum1 != checksum3
        assert checksum1 != checksum4


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
    
    def test_archive_metadata_with_optional_fields(self):
        """Test archive metadata with optional fields."""
        archive_id = ArchiveId("test-archive")
        metadata = ArchiveMetadata(
            archive_id=archive_id,
            location="test-location",
            archive_type=ArchiveType.COMPRESSED,
            simulation_id="test-sim",
            description="Test archive description",
            size=1024 * 1024,  # 1MB
            file_count=50,
            compression_ratio=0.75,
            tags={"climate", "model", "output"},
            custom_metadata={"experiment": "historical", "model": "ECHAM6"}
        )
        
        assert metadata.description == "Test archive description"
        assert metadata.size == 1024 * 1024
        assert metadata.file_count == 50
        assert metadata.compression_ratio == 0.75
        assert metadata.tags == {"climate", "model", "output"}
        assert metadata.custom_metadata == {"experiment": "historical", "model": "ECHAM6"}
    
    def test_archive_metadata_validation(self):
        """Test archive metadata validation."""
        archive_id = ArchiveId("test-archive")
        
        # Invalid size
        with pytest.raises(ValueError):
            ArchiveMetadata(
                archive_id=archive_id,
                location="test-location",
                archive_type=ArchiveType.COMPRESSED,
                simulation_id="test-sim",
                size=-100  # Negative size
            )
        
        # Invalid file count
        with pytest.raises(ValueError):
            ArchiveMetadata(
                archive_id=archive_id,
                location="test-location",
                archive_type=ArchiveType.COMPRESSED,
                simulation_id="test-sim",
                file_count=-5  # Negative count
            )
        
        # Invalid compression ratio
        with pytest.raises(ValueError):
            ArchiveMetadata(
                archive_id=archive_id,
                location="test-location",
                archive_type=ArchiveType.COMPRESSED,
                simulation_id="test-sim",
                compression_ratio=1.5  # > 1.0
            )
    
    def test_archive_metadata_tag_management(self):
        """Test archive metadata tag management."""
        archive_id = ArchiveId("test-archive")
        metadata = ArchiveMetadata(
            archive_id=archive_id,
            location="test-location",
            archive_type=ArchiveType.COMPRESSED,
            simulation_id="test-sim"
        )
        
        # Add tags
        metadata.add_tag("climate")
        metadata.add_tag("model")
        assert "climate" in metadata.tags
        assert "model" in metadata.tags
        
        # Remove tag
        removed = metadata.remove_tag("climate")
        assert removed is True
        assert "climate" not in metadata.tags
        assert "model" in metadata.tags
        
        # Remove non-existent tag
        removed = metadata.remove_tag("nonexistent")
        assert removed is False
    
    def test_archive_metadata_has_tag(self):
        """Test archive metadata tag checking."""
        archive_id = ArchiveId("test-archive")
        metadata = ArchiveMetadata(
            archive_id=archive_id,
            location="test-location",
            archive_type=ArchiveType.COMPRESSED,
            simulation_id="test-sim",
            tags={"climate", "model"}
        )
        
        assert metadata.has_tag("climate") is True
        assert metadata.has_tag("model") is True
        assert metadata.has_tag("nonexistent") is False
    
    def test_archive_metadata_serialization(self):
        """Test archive metadata to_dict and from_dict."""
        archive_id = ArchiveId("test-archive")
        original_metadata = ArchiveMetadata(
            archive_id=archive_id,
            location="test-location",
            archive_type=ArchiveType.COMPRESSED,
            simulation_id="test-sim",
            description="Test archive",
            tags={"climate", "model"},
            custom_metadata={"experiment": "historical"}
        )
        
        # Serialize to dict
        data = original_metadata.to_dict()
        assert data["archive_id"] == "test-archive"
        assert data["location"] == "test-location"
        assert data["archive_type"] == "compressed"
        assert data["simulation_id"] == "test-sim"
        assert data["description"] == "Test archive"
        assert set(data["tags"]) == {"climate", "model"}
        assert data["custom_metadata"] == {"experiment": "historical"}
        
        # Deserialize from dict
        restored_metadata = ArchiveMetadata.from_dict(data)
        assert restored_metadata.archive_id.value == original_metadata.archive_id.value
        assert restored_metadata.location == original_metadata.location
        assert restored_metadata.archive_type == original_metadata.archive_type
        assert restored_metadata.simulation_id == original_metadata.simulation_id
        assert restored_metadata.description == original_metadata.description
        assert restored_metadata.tags == original_metadata.tags
        assert restored_metadata.custom_metadata == original_metadata.custom_metadata


class TestFileMetadata:
    """Test FileMetadata entity."""
    
    def test_file_metadata_creation(self):
        """Test basic file metadata creation."""
        checksum = Checksum(value="abc123", algorithm="md5")
        file_metadata = FileMetadata(
            path="data/output.nc",
            size=1024,
            checksum=checksum
        )
        
        assert file_metadata.path == "data/output.nc"
        assert file_metadata.size == 1024
        assert file_metadata.checksum == checksum
    
    def test_file_metadata_path_normalization(self):
        """Test archive file path normalization."""
        # Test various path formats
        paths = [
            "data\\output.nc",  # Windows-style
            "./data/output.nc",  # Relative with ./
            "data//output.nc",   # Double slashes
        ]
        
        for path in paths:
            file_metadata = FileMetadata(relative_path=path, size=1024)
            # Should be normalized to POSIX style
            assert "\\" not in file_metadata.relative_path
            assert "//" not in file_metadata.relative_path
    
    def test_file_metadata_validation(self):
        """Test archive file validation."""
        # Invalid paths
        with pytest.raises(ValueError):
            FileMetadata(relative_path="", size=1024)  # Empty path
        
        # Invalid size
        with pytest.raises(ValueError):
            FileMetadata(relative_path="test.txt", size=-100)  # Negative size
    
    def test_file_metadata_methods(self):
        """Test archive file utility methods."""
        file_metadata = FileMetadata(
            relative_path="data/experiment/output.nc",
            size=1024
        )
        
        # Test filename extraction
        assert file_metadata.get_filename() == "output.nc"
        
        # Test extension extraction
        assert file_metadata.get_file_extension() == "nc"
        
        # Test directory extraction
        assert file_metadata.get_directory() == "data/experiment"
    
    def test_file_metadata_is_in_directory(self):
        """Test archive file directory checking."""
        file_metadata = FileMetadata(
            relative_path="data/experiment/output.nc",
            size=1024
        )
        
        assert file_metadata.is_in_directory("data") is True
        assert file_metadata.is_in_directory("data/experiment") is True
        assert file_metadata.is_in_directory("other") is False
    
    def test_file_metadata_matches_pattern(self):
        """Test archive file pattern matching."""
        file_metadata = FileMetadata(
            relative_path="data/experiment/output.nc",
            size=1024
        )
        
        assert file_metadata.matches_pattern("*.nc") is True
        assert file_metadata.matches_pattern("data/**/*.nc") is True
        assert file_metadata.matches_pattern("*.txt") is False
    
    def test_file_metadata_serialization(self):
        """Test archive file to_dict and from_dict."""
        checksum = Checksum(value="abc123", algorithm="md5")
        original_file = FileMetadata(
            relative_path="data/output.nc",
            size=1024,
            checksum=checksum,
            content_type="outdata",
            compression="gzip"
        )
        
        # Serialize to dict
        data = original_file.to_dict()
        assert data["relative_path"] == "data/output.nc"
        assert data["size"] == 1024
        assert data["checksum"] == "md5:abc123"
        assert data["content_type"] == "outdata"
        assert data["compression"] == "gzip"
        
        # Deserialize from dict
        restored_file = FileMetadata.from_dict(data)
        assert restored_file.relative_path == original_file.relative_path
        assert restored_file.size == original_file.size
        assert restored_file.checksum.value == original_file.checksum.value
        assert restored_file.checksum.algorithm == original_file.checksum.algorithm
        assert restored_file.content_type == original_file.content_type
        assert restored_file.compression == original_file.compression