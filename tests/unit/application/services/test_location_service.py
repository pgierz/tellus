"""
Unit tests for LocationApplicationService.

Tests the application service layer for location management,
including CRUD operations, protocol validation, and connectivity testing.
"""

from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import pytest

from tellus.application.dtos import (CreateLocationDto, FilterOptions,
                                     LocationDto, LocationListDto,
                                     LocationTestResult, PaginationInfo,
                                     UpdateLocationDto)
from tellus.application.exceptions import (ConfigurationError,
                                           EntityAlreadyExistsError,
                                           EntityNotFoundError,
                                           ExternalServiceError,
                                           LocationAccessError,
                                           ValidationError)
from tellus.application.services.location_service import \
    LocationApplicationService
from tellus.domain.entities.location import LocationEntity, LocationKind
from tellus.domain.repositories.exceptions import (LocationExistsError,
                                                   LocationNotFoundError,
                                                   RepositoryError)


@pytest.fixture
def mock_location_repo():
    """Mock location repository."""
    return Mock()


@pytest.fixture
def service(mock_location_repo):
    """Create service instance with mocked dependencies."""
    return LocationApplicationService(location_repository=mock_location_repo)


@pytest.fixture
def sample_location_entity():
    """Create a sample location entity for testing."""
    return LocationEntity(
        name="test-location",
        kinds=[LocationKind.DISK],
        config={"protocol": "file", "path": "/test/path"},
        optional=False
    )


@pytest.fixture
def sample_remote_location_entity():
    """Create a sample remote location entity for testing."""
    return LocationEntity(
        name="remote-location",
        kinds=[LocationKind.COMPUTE],
        config={
            "protocol": "ssh",
            "storage_options": {"host": "compute.example.com", "port": 22}
        },
        optional=False
    )


class TestLocationApplicationService:
    """Test suite for LocationApplicationService."""


class TestCreateLocation:
    """Test location creation operations."""
    
    def test_create_location_success(self, service, mock_location_repo):
        """Test successful location creation."""
        # Arrange
        dto = CreateLocationDto(
            name="test-location",
            kinds=["DISK"],
            protocol="file",
            path="/test/path",
            optional=False
        )
        
        mock_location_repo.exists.return_value = False
        mock_location_repo.save.return_value = None
        
        # Act
        result = service.create_location(dto)
        
        # Assert
        assert isinstance(result, LocationDto)
        assert result.name == "test-location"
        assert result.kinds == ["DISK"]
        assert result.protocol == "file"
        assert result.path == "/test/path"
        
        mock_location_repo.exists.assert_called_once_with("test-location")
        mock_location_repo.save.assert_called_once()
    
    def test_create_location_with_storage_options(self, service, mock_location_repo):
        """Test creating a location with storage options."""
        # Arrange
        dto = CreateLocationDto(
            name="ssh-location",
            kinds=["COMPUTE"],
            protocol="ssh",
            storage_options={"host": "server.com", "port": 22},
            optional=True
        )
        
        mock_location_repo.exists.return_value = False
        mock_location_repo.save.return_value = None
        
        # Act
        result = service.create_location(dto)
        
        # Assert
        assert result.name == "ssh-location"
        assert result.protocol == "ssh"
        assert result.storage_options == {"host": "server.com", "port": 22}
        assert result.optional is True
    
    def test_create_location_already_exists(self, service, mock_location_repo):
        """Test creating a location that already exists."""
        # Arrange
        dto = CreateLocationDto(name="existing-location", kinds=["DISK"], protocol="file")
        mock_location_repo.exists.return_value = True
        
        # Act & Assert
        with pytest.raises(EntityAlreadyExistsError) as exc_info:
            service.create_location(dto)
        
        assert "existing-location" in str(exc_info.value)
        mock_location_repo.save.assert_not_called()
    
    def test_create_location_invalid_kind(self, service, mock_location_repo):
        """Test creating a location with invalid kind."""
        # Arrange
        dto = CreateLocationDto(
            name="test-location",
            kinds=["INVALID_KIND"],
            protocol="file"
        )
        mock_location_repo.exists.return_value = False
        
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            service.create_location(dto)
        
        assert "Invalid location kind" in str(exc_info.value)
        mock_location_repo.save.assert_not_called()
    
    def test_create_location_invalid_ssh_config(self, service, mock_location_repo):
        """Test creating SSH location without required storage options."""
        # Arrange
        dto = CreateLocationDto(
            name="ssh-location",
            kinds=["COMPUTE"],
            protocol="ssh"
        )
        mock_location_repo.exists.return_value = False
        
        # Act & Assert
        with pytest.raises(ConfigurationError) as exc_info:
            service.create_location(dto)
        
        assert "storage_options required" in str(exc_info.value)
    
    def test_create_location_repository_error(self, service, mock_location_repo):
        """Test location creation with repository error."""
        # Arrange
        dto = CreateLocationDto(name="test-location", kinds=["DISK"], protocol="file")
        mock_location_repo.exists.return_value = False
        mock_location_repo.save.side_effect = LocationExistsError("test-location")
        
        # Act & Assert
        with pytest.raises(EntityAlreadyExistsError):
            service.create_location(dto)


class TestGetLocation:
    """Test location retrieval operations."""
    
    def test_get_location_success(self, service, mock_location_repo, sample_location_entity):
        """Test successful location retrieval."""
        # Arrange
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Act
        result = service.get_location("test-location")
        
        # Assert
        assert isinstance(result, LocationDto)
        assert result.name == "test-location"
        assert result.kinds == ["DISK"]
        assert result.protocol == "file"
        mock_location_repo.get_by_name.assert_called_once_with("test-location")
    
    def test_get_location_not_found(self, service, mock_location_repo):
        """Test retrieving a non-existent location."""
        # Arrange
        mock_location_repo.get_by_name.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.get_location("nonexistent")
        
        assert "nonexistent" in str(exc_info.value)
    
    def test_get_location_repository_error(self, service, mock_location_repo):
        """Test location retrieval with repository error."""
        # Arrange
        mock_location_repo.get_by_name.side_effect = LocationNotFoundError("test-location")
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError):
            service.get_location("test-location")


class TestUpdateLocation:
    """Test location update operations."""
    
    def test_update_location_success(self, service, mock_location_repo, sample_location_entity):
        """Test successful location update."""
        # Arrange
        dto = UpdateLocationDto(
            kinds=["FILESERVER"],
            path="/updated/path"
        )
        
        mock_location_repo.get_by_name.return_value = sample_location_entity
        mock_location_repo.save.return_value = None
        
        # Act
        result = service.update_location("test-location", dto)
        
        # Assert
        assert isinstance(result, LocationDto)
        assert result.name == "test-location"
        mock_location_repo.get_by_name.assert_called_once_with("test-location")
        mock_location_repo.save.assert_called_once()
    
    def test_update_location_not_found(self, service, mock_location_repo):
        """Test updating a non-existent location."""
        # Arrange
        dto = UpdateLocationDto(protocol="sftp")
        mock_location_repo.get_by_name.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError):
            service.update_location("nonexistent", dto)
        
        mock_location_repo.save.assert_not_called()
    
    def test_update_location_invalid_kind(self, service, mock_location_repo, sample_location_entity):
        """Test updating location with invalid kind."""
        # Arrange
        dto = UpdateLocationDto(kinds=["INVALID_KIND"])
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            service.update_location("test-location", dto)
        
        assert "Invalid location kind" in str(exc_info.value)
    
    def test_update_location_clear_path(self, service, mock_location_repo, sample_location_entity):
        """Test updating location to clear the path."""
        # Arrange
        dto = UpdateLocationDto(path="")  # Empty string to clear
        mock_location_repo.get_by_name.return_value = sample_location_entity
        mock_location_repo.save.return_value = None
        
        # Act
        result = service.update_location("test-location", dto)
        
        # Assert
        # The path should be removed from config when empty string is provided
        assert isinstance(result, LocationDto)


class TestDeleteLocation:
    """Test location deletion operations."""
    
    def test_delete_location_success(self, service, mock_location_repo):
        """Test successful location deletion."""
        # Arrange
        mock_location_repo.exists.return_value = True
        mock_location_repo.delete.return_value = True
        
        # Act
        result = service.delete_location("test-location")
        
        # Assert
        assert result is True
        mock_location_repo.exists.assert_called_once_with("test-location")
        mock_location_repo.delete.assert_called_once_with("test-location")
    
    def test_delete_location_not_found(self, service, mock_location_repo):
        """Test deleting a non-existent location."""
        # Arrange
        mock_location_repo.exists.return_value = False
        
        # Act
        result = service.delete_location("nonexistent")
        
        # Assert
        assert result is False
        mock_location_repo.delete.assert_not_called()
    
    def test_delete_location_repository_failure(self, service, mock_location_repo):
        """Test location deletion that fails at repository level."""
        # Arrange
        mock_location_repo.exists.return_value = True
        mock_location_repo.delete.return_value = False
        
        # Act
        result = service.delete_location("test-location")
        
        # Assert
        assert result is False


class TestListLocations:
    """Test location listing operations."""
    
    def test_list_locations_success(self, service, mock_location_repo, sample_location_entity):
        """Test successful location listing."""
        # Arrange
        locations = [sample_location_entity]
        mock_location_repo.list_all.return_value = locations
        
        # Act
        result = service.list_locations()
        
        # Assert
        assert isinstance(result, LocationListDto)
        assert len(result.locations) == 1
        assert result.locations[0].name == "test-location"
        assert isinstance(result.pagination, PaginationInfo)
        assert result.pagination.total_count == 1
    
    def test_list_locations_with_pagination(self, service, mock_location_repo):
        """Test location listing with pagination."""
        # Arrange
        locations = [
            LocationEntity(
                name=f"location-{i}",
                kinds=[LocationKind.DISK],
                config={"protocol": "file"}
            )
            for i in range(10)
        ]
        mock_location_repo.list_all.return_value = locations
        
        # Act
        result = service.list_locations(page=2, page_size=3)
        
        # Assert
        assert len(result.locations) == 3
        assert result.pagination.page == 2
        assert result.pagination.page_size == 3
        assert result.pagination.total_count == 10
        assert result.pagination.has_next is True
        assert result.pagination.has_previous is True
    
    def test_list_locations_with_filters(self, service, mock_location_repo):
        """Test location listing with search filters."""
        # Arrange
        locations = [
            LocationEntity(
                name="test-location-1",
                kinds=[LocationKind.DISK],
                config={"protocol": "file"}
            ),
            LocationEntity(
                name="prod-location-2",
                kinds=[LocationKind.DISK],
                config={"protocol": "ssh", "storage_options": {"host": "server.com"}}
            ),
            LocationEntity(
                name="test-location-3",
                kinds=[LocationKind.DISK],
                config={"protocol": "file"}
            )
        ]
        mock_location_repo.list_all.return_value = locations
        
        filters = FilterOptions(search_term="test")
        
        # Act
        result = service.list_locations(filters=filters)
        
        # Assert
        assert len(result.locations) == 2  # Only test-location-1 and test-location-3
        assert all("test" in loc.name for loc in result.locations)
    
    def test_list_locations_empty(self, service, mock_location_repo):
        """Test location listing with no locations."""
        # Arrange
        mock_location_repo.list_all.return_value = []
        
        # Act
        result = service.list_locations()
        
        # Assert
        assert len(result.locations) == 0
        assert result.pagination.total_count == 0


class TestFindOperations:
    """Test location finding operations."""
    
    def test_find_by_kind_success(self, service, mock_location_repo, sample_location_entity):
        """Test successful find by kind."""
        # Arrange
        mock_location_repo.find_by_kind.return_value = [sample_location_entity]
        
        # Act
        result = service.find_by_kind("DISK")
        
        # Assert
        assert len(result) == 1
        assert result[0].name == "test-location"
        mock_location_repo.find_by_kind.assert_called_once_with(LocationKind.DISK)
    
    def test_find_by_kind_invalid(self, service, mock_location_repo):
        """Test find by kind with invalid kind."""
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            service.find_by_kind("INVALID_KIND")
        
        assert "Invalid location kind" in str(exc_info.value)
        mock_location_repo.find_by_kind.assert_not_called()
    
    def test_find_by_protocol_success(self, service, mock_location_repo, sample_location_entity):
        """Test successful find by protocol."""
        # Arrange
        mock_location_repo.find_by_protocol.return_value = [sample_location_entity]
        
        # Act
        result = service.find_by_protocol("file")
        
        # Assert
        assert len(result) == 1
        assert result[0].name == "test-location"
        assert result[0].protocol == "file"
        mock_location_repo.find_by_protocol.assert_called_once_with("file")
    
    def test_find_by_protocol_empty(self, service, mock_location_repo):
        """Test find by protocol with no results."""
        # Arrange
        mock_location_repo.find_by_protocol.return_value = []
        
        # Act
        result = service.find_by_protocol("unknown")
        
        # Assert
        assert len(result) == 0


class TestConnectivityTesting:
    """Test location connectivity testing operations."""
    
    def test_test_location_connectivity_success(self, service, mock_location_repo, sample_location_entity):
        """Test successful connectivity test."""
        # Arrange
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Mock the local connectivity test
        with patch.object(service, '_test_protocol_connectivity', return_value={
            "success": True,
            "available_space": 1000000,
            "protocol_info": {"path_exists": True}
        }):
            # Act
            result = service.test_location_connectivity("test-location")
        
        # Assert
        assert isinstance(result, LocationTestResult)
        assert result.location_name == "test-location"
        assert result.success is True
        assert result.available_space == 1000000
        assert result.latency_ms is not None
    
    def test_test_location_connectivity_failure(self, service, mock_location_repo, sample_location_entity):
        """Test connectivity test failure."""
        # Arrange
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Mock the local connectivity test to fail
        with patch.object(service, '_test_protocol_connectivity', return_value={
            "success": False,
            "error": "Path does not exist"
        }):
            # Act
            result = service.test_location_connectivity("test-location")
        
        # Assert
        assert result.success is False
        assert result.error_message == "Path does not exist"
    
    def test_test_location_connectivity_not_found(self, service, mock_location_repo):
        """Test connectivity test for non-existent location."""
        # Arrange
        mock_location_repo.get_by_name.return_value = None
        
        # Act
        result = service.test_location_connectivity("nonexistent")
        
        # Assert - The service catches the exception and returns an error result
        assert result.success is False
        assert "not found" in result.error_message
    
    def test_test_location_connectivity_exception(self, service, mock_location_repo, sample_location_entity):
        """Test connectivity test with unexpected exception."""
        # Arrange
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Mock the test method to raise an exception
        with patch.object(service, '_test_protocol_connectivity', side_effect=Exception("Network error")):
            # Act
            result = service.test_location_connectivity("test-location")
        
        # Assert
        assert result.success is False
        assert "Network error" in result.error_message


class TestPathValidation:
    """Test path validation operations."""
    
    def test_validate_location_path_success(self, service, mock_location_repo, sample_location_entity):
        """Test successful path validation."""
        # Arrange
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Mock the path validation
        with patch.object(service, '_validate_protocol_path', return_value=True):
            # Act
            result = service.validate_location_path("test-location", "/valid/path")
        
        # Assert
        assert result is True
    
    def test_validate_location_path_invalid(self, service, mock_location_repo, sample_location_entity):
        """Test path validation failure."""
        # Arrange
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Mock the path validation to fail
        with patch.object(service, '_validate_protocol_path', return_value=False):
            # Act & Assert
            with pytest.raises(LocationAccessError) as exc_info:
                service.validate_location_path("test-location", "/invalid/path")
            
            assert "not accessible" in str(exc_info.value)
    
    def test_validate_location_path_not_found(self, service, mock_location_repo):
        """Test path validation for non-existent location."""
        # Arrange
        mock_location_repo.get_by_name.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError):
            service.validate_location_path("nonexistent", "/some/path")


class TestPrivateHelperMethods:
    """Test private helper methods."""
    
    def test_entity_to_dto_conversion(self, service, sample_location_entity):
        """Test entity to DTO conversion."""
        # Act
        result = service._entity_to_dto(sample_location_entity)
        
        # Assert
        assert isinstance(result, LocationDto)
        assert result.name == "test-location"
        assert result.kinds == ["DISK"]
        assert result.protocol == "file"
        assert result.path == "/test/path"
        assert result.is_remote is False
    
    def test_entity_to_dto_remote_location(self, service, sample_remote_location_entity):
        """Test entity to DTO conversion for remote location."""
        # Act
        result = service._entity_to_dto(sample_remote_location_entity)
        
        # Assert
        assert result.name == "remote-location"
        assert result.protocol == "ssh"
        assert result.is_remote is True
        assert result.storage_options == {"host": "compute.example.com", "port": 22}
    
    def test_apply_filters_search_term(self, service):
        """Test applying search term filters."""
        # Arrange
        locations = [
            LocationEntity(
                name="test-location-1",
                kinds=[LocationKind.DISK],
                config={"protocol": "file"}
            ),
            LocationEntity(
                name="prod-location-2",
                kinds=[LocationKind.COMPUTE],
                config={"protocol": "ssh", "storage_options": {"host": "server.com"}}
            ),
            LocationEntity(
                name="test-location-3",
                kinds=[LocationKind.DISK],
                config={"protocol": "file"}
            )
        ]
        filters = FilterOptions(search_term="test")
        
        # Act
        result = service._apply_filters(locations, filters)
        
        # Assert
        assert len(result) == 2
        assert all("test" in loc.name for loc in result)
    
    def test_apply_filters_protocol_search(self, service):
        """Test applying filters with protocol search."""
        # Arrange
        locations = [
            LocationEntity(
                name="location-1",
                kinds=[LocationKind.DISK],
                config={"protocol": "file"}
            ),
            LocationEntity(
                name="location-2",
                kinds=[LocationKind.COMPUTE],
                config={"protocol": "ssh", "storage_options": {"host": "server.com"}}
            )
        ]
        filters = FilterOptions(search_term="ssh")
        
        # Act
        result = service._apply_filters(locations, filters)
        
        # Assert
        assert len(result) == 1
        assert result[0].get_protocol() == "ssh"


class TestProtocolValidation:
    """Test protocol-specific validation methods."""
    
    def test_validate_protocol_config_file_success(self, service):
        """Test valid file protocol configuration."""
        # Arrange
        config = {"protocol": "file", "path": "/test/path"}
        
        # Act & Assert - Should not raise any exception
        service._validate_protocol_config("file", config)
    
    def test_validate_protocol_config_ssh_success(self, service):
        """Test valid SSH protocol configuration."""
        # Arrange
        config = {
            "protocol": "ssh",
            "storage_options": {"host": "server.com", "port": 22}
        }
        
        # Act & Assert - Should not raise any exception
        service._validate_protocol_config("ssh", config)
    
    def test_validate_protocol_config_ssh_missing_storage_options(self, service):
        """Test SSH protocol without storage options."""
        # Arrange
        config = {"protocol": "ssh"}
        
        # Act & Assert
        with pytest.raises(ConfigurationError) as exc_info:
            service._validate_protocol_config("ssh", config)
        
        assert "storage_options required" in str(exc_info.value)
    
    def test_validate_protocol_config_ssh_missing_host(self, service):
        """Test SSH protocol without required host."""
        # Arrange
        config = {
            "protocol": "ssh",
            "storage_options": {"port": 22}  # Missing host
        }
        
        # Act & Assert
        with pytest.raises(ConfigurationError) as exc_info:
            service._validate_protocol_config("ssh", config)
        
        assert "Missing required storage_options" in str(exc_info.value)
    
    def test_validate_protocol_config_s3_success(self, service):
        """Test valid S3 protocol configuration."""
        # Arrange
        config = {
            "protocol": "s3",
            "storage_options": {"endpoint_url": "s3.amazonaws.com"}
        }
        
        # Act & Assert - Should not raise any exception
        service._validate_protocol_config("s3", config)
    
    def test_validate_protocol_config_local_invalid_path(self, service):
        """Test local protocol with invalid path type."""
        # Arrange
        config = {"protocol": "file", "path": 123}  # Path should be string
        
        # Act & Assert
        with pytest.raises(ConfigurationError) as exc_info:
            service._validate_protocol_config("file", config)
        
        assert "Path must be a string" in str(exc_info.value)


class TestConnectivityTestMethods:
    """Test specific connectivity test methods."""
    
    def test_test_local_connectivity_success(self, service, sample_location_entity):
        """Test local connectivity with existing path."""
        # Mock Path operations
        with patch('tellus.application.services.location_service.Path') as mock_path:
            mock_path_obj = mock_path.return_value
            mock_path_obj.exists.return_value = True
            mock_path_obj.is_dir.return_value = True
            
            with patch('shutil.disk_usage') as mock_usage:
                mock_usage.return_value = Mock(free=1000000)
                
                # Act
                result = service._test_local_connectivity(sample_location_entity)
        
        # Assert
        assert result["success"] is True
        assert result["available_space"] == 1000000
        assert result["protocol_info"]["path_exists"] is True
    
    def test_test_local_connectivity_path_not_exists(self, service, sample_location_entity):
        """Test local connectivity with non-existent path."""
        # Mock Path operations
        with patch('tellus.application.services.location_service.Path') as mock_path:
            mock_path_obj = mock_path.return_value
            mock_path_obj.exists.return_value = False
            
            # Act
            result = service._test_local_connectivity(sample_location_entity)
        
        # Assert
        assert result["success"] is False
        assert "does not exist" in result["error"]
    
    def test_test_local_connectivity_not_directory(self, service, sample_location_entity):
        """Test local connectivity with path that's not a directory."""
        # Mock Path operations
        with patch('tellus.application.services.location_service.Path') as mock_path:
            mock_path_obj = mock_path.return_value
            mock_path_obj.exists.return_value = True
            mock_path_obj.is_dir.return_value = False
            
            # Act
            result = service._test_local_connectivity(sample_location_entity)
        
        # Assert
        assert result["success"] is False
        assert "not a directory" in result["error"]
    
    def test_test_sftp_connectivity_placeholder(self, service, sample_remote_location_entity):
        """Test SFTP connectivity (placeholder implementation)."""
        # Act
        result = service._test_sftp_connectivity(sample_remote_location_entity, 30)
        
        # Assert
        assert result["success"] is False
        assert "not implemented" in result["error"]
        assert result["protocol_info"]["host"] == "compute.example.com"
    
    def test_test_s3_connectivity_placeholder(self, service):
        """Test S3 connectivity (placeholder implementation)."""
        # Arrange
        s3_location = LocationEntity(
            name="s3-location",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "s3",
                "storage_options": {"endpoint_url": "s3.amazonaws.com"}
            }
        )
        
        # Act
        result = service._test_s3_connectivity(s3_location, 30)
        
        # Assert
        assert result["success"] is False
        assert "not implemented" in result["error"]
        assert result["protocol_info"]["endpoint"] == "s3.amazonaws.com"


class TestPathValidationMethods:
    """Test path validation methods."""
    
    def test_validate_protocol_path_local_success(self, service, sample_location_entity):
        """Test local path validation success."""
        # Mock Path operations
        with patch('tellus.application.services.location_service.Path') as mock_path:
            mock_path_obj = mock_path.return_value
            mock_path_obj.exists.return_value = True
            
            # Act
            result = service._validate_protocol_path(sample_location_entity, "/valid/path")
        
        # Assert
        assert result is True
    
    def test_validate_protocol_path_local_failure(self, service, sample_location_entity):
        """Test local path validation failure."""
        # Mock Path operations
        with patch('tellus.application.services.location_service.Path') as mock_path:
            mock_path_obj = mock_path.return_value
            mock_path_obj.exists.return_value = False
            
            # Act
            result = service._validate_protocol_path(sample_location_entity, "/invalid/path")
        
        # Assert
        assert result is False
    
    def test_validate_protocol_path_remote_basic(self, service, sample_remote_location_entity):
        """Test remote path validation (basic implementation)."""
        # Act
        result = service._validate_protocol_path(sample_remote_location_entity, "/remote/path")
        
        # Assert
        assert result is True  # Basic validation only checks if string is non-empty
    
    def test_validate_protocol_path_remote_empty(self, service, sample_remote_location_entity):
        """Test remote path validation with empty path."""
        # Act
        result = service._validate_protocol_path(sample_remote_location_entity, "")
        
        # Assert
        assert result is False


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_repository_error_propagation(self, service, mock_location_repo):
        """Test that repository errors are properly propagated."""
        # Arrange
        mock_location_repo.list_all.side_effect = RepositoryError("Database connection failed")
        
        # Act & Assert
        with pytest.raises(RepositoryError) as exc_info:
            service.list_locations()
        
        assert "Database connection failed" in str(exc_info.value)
    
    def test_unexpected_error_handling(self, service, mock_location_repo):
        """Test handling of unexpected errors."""
        # Arrange
        dto = CreateLocationDto(name="test-location", kinds=["DISK"], protocol="file")
        mock_location_repo.exists.return_value = False
        mock_location_repo.save.side_effect = RuntimeError("Unexpected error")
        
        # Act & Assert
        with pytest.raises(RuntimeError):
            service.create_location(dto)