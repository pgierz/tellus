"""
Location Application Service - Orchestrates location-related use cases.

This service manages storage location operations including protocol-specific
validation, connectivity testing, and configuration management.
"""

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ...domain.entities.location import LocationEntity, LocationKind
from ...domain.repositories.exceptions import (LocationExistsError,
                                               LocationNotFoundError,
                                               RepositoryError)
from ...domain.repositories.location_repository import ILocationRepository
from ..dtos import (CreateLocationDto, FilterOptions, LocationDto,
                    LocationListDto, LocationTestResult, PaginationInfo,
                    UpdateLocationDto)
from ..exceptions import (ConfigurationError, EntityAlreadyExistsError,
                          EntityNotFoundError, ExternalServiceError,
                          LocationAccessError, ValidationError)

logger = logging.getLogger(__name__)


class LocationApplicationService:
    """
    Application service for location management.
    
    Handles location CRUD operations, protocol-specific validation,
    connectivity testing, and configuration management for Earth System
    Model storage backends.
    """
    
    def __init__(self, location_repository: ILocationRepository):
        """
        Initialize the location service.
        
        Args:
            location_repository: Repository for location persistence
        """
        self._location_repo = location_repository
        self._logger = logger
    
    def _expand_path(self, path: str) -> str:
        """
        Expand user home directory (~) and environment variables in paths.
        
        Args:
            path: Path that may contain ~ or $VAR patterns
            
        Returns:
            Expanded path with ~ and environment variables resolved
            
        Examples:
            ~/data -> /Users/username/data
            $HOME/files -> /Users/username/files  
            ${HOME}/work -> /Users/username/work
            /absolute/path -> /absolute/path (unchanged)
        """
        if not path:
            return path
            
        # First expand user home directory (~)
        expanded = os.path.expanduser(path)
        
        # Then expand environment variables ($VAR, ${VAR})
        expanded = os.path.expandvars(expanded)
        
        return expanded
    
    def create_location(self, dto: CreateLocationDto) -> LocationDto:
        """
        Create a new storage location.
        
        Args:
            dto: Data transfer object with location creation data
            
        Returns:
            Created location DTO
            
        Raises:
            EntityAlreadyExistsError: If location already exists
            ValidationError: If validation fails
            ConfigurationError: If configuration is invalid
        """
        self._logger.info(f"Creating location: {dto.name}")
        
        try:
            # Check if location already exists
            if self._location_repo.exists(dto.name):
                raise EntityAlreadyExistsError("Location", dto.name)
            
            # Convert string kinds to enum values
            kinds = []
            for kind_str in dto.kinds:
                try:
                    kinds.append(LocationKind.from_str(kind_str))
                except ValueError as e:
                    raise ValidationError(f"Invalid location kind: {kind_str}")
            
            # Build configuration dictionary
            config = {
                "protocol": dto.protocol,
                **dto.additional_config
            }
            
            if dto.path:
                config["path"] = self._expand_path(dto.path)
            
            if dto.storage_options:
                config["storage_options"] = dto.storage_options
            
            # Validate protocol-specific configuration
            self._validate_protocol_config(dto.protocol, config)
            
            # Create domain entity
            location = LocationEntity(
                name=dto.name,
                kinds=kinds,
                config=config
            )
            
            # Persist the location
            self._location_repo.save(location)
            
            self._logger.info(f"Successfully created location: {dto.name}")
            return self._entity_to_dto(location)
            
        except LocationExistsError as e:
            raise EntityAlreadyExistsError("Location", e.name)
        except ValueError as e:
            raise ValidationError(f"Invalid location data: {str(e)}")
        except RepositoryError as e:
            self._logger.error(f"Repository error creating location: {str(e)}")
            raise
        except Exception as e:
            self._logger.error(f"Unexpected error creating location: {str(e)}")
            raise
    
    def get_location(self, name: str) -> LocationDto:
        """
        Get a location by its name.
        
        Args:
            name: The name of the location to retrieve
            
        Returns:
            Location DTO
            
        Raises:
            EntityNotFoundError: If location not found
        """
        self._logger.debug(f"Retrieving location: {name}")
        
        try:
            location = self._location_repo.get_by_name(name)
            if location is None:
                raise EntityNotFoundError("Location", name)
            
            return self._entity_to_dto(location)
            
        except LocationNotFoundError as e:
            raise EntityNotFoundError("Location", e.name)
        except RepositoryError as e:
            self._logger.error(f"Repository error retrieving location: {str(e)}")
            raise
    
    def update_location(self, name: str, dto: UpdateLocationDto) -> LocationDto:
        """
        Update an existing location.
        
        Args:
            name: The name of the location to update
            dto: Data transfer object with update data
            
        Returns:
            Updated location DTO
            
        Raises:
            EntityNotFoundError: If location not found
            ValidationError: If validation fails
            ConfigurationError: If configuration is invalid
        """
        self._logger.info(f"Updating location: {name}")
        
        try:
            # Get existing location
            location = self._location_repo.get_by_name(name)
            if location is None:
                raise EntityNotFoundError("Location", name)
            
            # Apply updates
            if dto.kinds is not None:
                # Convert string kinds to enum values
                kinds = []
                for kind_str in dto.kinds:
                    try:
                        kinds.append(LocationKind.from_str(kind_str))
                    except ValueError:
                        raise ValidationError(f"Invalid location kind: {kind_str}")
                location.kinds = kinds
            
            if dto.protocol is not None:
                location.update_config("protocol", dto.protocol)
            
            if dto.path is not None:
                if dto.path:
                    location.update_config("path", self._expand_path(dto.path))
                else:
                    # Remove path if empty string provided
                    location.config.pop("path", None)
            
            if dto.storage_options is not None:
                if dto.storage_options:
                    location.update_config("storage_options", dto.storage_options)
                else:
                    # Remove storage_options if empty dict provided
                    location.config.pop("storage_options", None)
            
            
            if dto.config is not None:
                for key, value in dto.config.items():
                    location.update_config(key, value)
            
            # Validate protocol-specific configuration after updates
            protocol = location.get_protocol()
            self._validate_protocol_config(protocol, location.config)
            
            # Persist the changes
            self._location_repo.save(location)
            
            self._logger.info(f"Successfully updated location: {name}")
            return self._entity_to_dto(location)
            
        except LocationNotFoundError as e:
            raise EntityNotFoundError("Location", e.name)
        except ValueError as e:
            raise ValidationError(f"Invalid update data: {str(e)}")
        except RepositoryError as e:
            self._logger.error(f"Repository error updating location: {str(e)}")
            raise
    
    def delete_location(self, name: str) -> bool:
        """
        Delete a location.
        
        Args:
            name: The name of the location to delete
            
        Returns:
            True if location was deleted, False if it didn't exist
        """
        self._logger.info(f"Deleting location: {name}")
        
        try:
            # Check if location exists
            if not self._location_repo.exists(name):
                self._logger.warning(f"Location not found for deletion: {name}")
                return False
            
            # TODO: Check for active usage (simulations, archives, etc.)
            # This would typically involve checking other repositories
            
            # Delete the location
            success = self._location_repo.delete(name)
            
            if success:
                self._logger.info(f"Successfully deleted location: {name}")
            else:
                self._logger.warning(f"Failed to delete location: {name}")
            
            return success
            
        except RepositoryError as e:
            self._logger.error(f"Repository error deleting location: {str(e)}")
            raise
    
    def list_locations(
        self,
        page: int = 1,
        page_size: int = 50,
        filters: Optional[FilterOptions] = None
    ) -> LocationListDto:
        """
        List locations with pagination and filtering.
        
        Args:
            page: Page number (1-based)
            page_size: Number of locations per page
            filters: Optional filtering criteria
            
        Returns:
            Paginated list of locations
        """
        self._logger.debug(f"Listing locations (page {page}, size {page_size})")
        
        try:
            # Get all locations (repository would typically handle pagination)
            all_locations = self._location_repo.list_all()
            
            # Apply filters if provided
            if filters:
                all_locations = self._apply_filters(all_locations, filters)
            
            # Calculate pagination
            total_count = len(all_locations)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            locations_page = all_locations[start_idx:end_idx]
            
            # Convert to DTOs
            location_dtos = [self._entity_to_dto(loc) for loc in locations_page]
            
            # Create pagination info
            pagination = PaginationInfo(
                page=page,
                page_size=page_size,
                total_count=total_count,
                has_next=end_idx < total_count,
                has_previous=page > 1
            )
            
            return LocationListDto(
                locations=location_dtos,
                pagination=pagination,
                filters_applied=filters or FilterOptions()
            )
            
        except RepositoryError as e:
            self._logger.error(f"Repository error listing locations: {str(e)}")
            raise
    
    def find_by_kind(self, kind: str) -> List[LocationDto]:
        """
        Find all locations that have a specific kind.
        
        Args:
            kind: The location kind to search for
            
        Returns:
            List of locations with the specified kind
            
        Raises:
            ValidationError: If kind is invalid
        """
        self._logger.debug(f"Finding locations by kind: {kind}")
        
        try:
            # Convert string to enum
            location_kind = LocationKind.from_str(kind)
            
            # Find locations
            locations = self._location_repo.find_by_kind(location_kind)
            
            # Convert to DTOs
            return [self._entity_to_dto(loc) for loc in locations]
            
        except ValueError as e:
            raise ValidationError(f"Invalid location kind: {kind}")
        except RepositoryError as e:
            self._logger.error(f"Repository error finding locations by kind: {str(e)}")
            raise
    
    def find_by_protocol(self, protocol: str) -> List[LocationDto]:
        """
        Find all locations that use a specific protocol.
        
        Args:
            protocol: The protocol to search for
            
        Returns:
            List of locations with the specified protocol
        """
        self._logger.debug(f"Finding locations by protocol: {protocol}")
        
        try:
            locations = self._location_repo.find_by_protocol(protocol)
            return [self._entity_to_dto(loc) for loc in locations]
            
        except RepositoryError as e:
            self._logger.error(f"Repository error finding locations by protocol: {str(e)}")
            raise
    
    def test_location_connectivity(self, name: str, timeout_seconds: int = 30) -> LocationTestResult:
        """
        Test connectivity to a storage location.
        
        Args:
            name: The name of the location to test
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            Test result with connectivity information
            
        Raises:
            EntityNotFoundError: If location not found
        """
        self._logger.info(f"Testing connectivity to location: {name}")
        
        start_time = time.time()
        
        try:
            location = self._location_repo.get_by_name(name)
            if location is None:
                raise EntityNotFoundError("Location", name)
            
            # Perform protocol-specific connectivity test
            protocol = location.get_protocol()
            test_result = self._test_protocol_connectivity(location, timeout_seconds)
            
            latency_ms = (time.time() - start_time) * 1000
            
            result = LocationTestResult(
                location_name=name,
                success=test_result["success"],
                error_message=test_result.get("error"),
                latency_ms=latency_ms,
                available_space=test_result.get("available_space"),
                protocol_specific_info=test_result.get("protocol_info", {})
            )
            
            if result.success:
                self._logger.info(f"Location connectivity test passed: {name}")
            else:
                self._logger.warning(f"Location connectivity test failed: {name} - {result.error_message}")
            
            return result
            
        except LocationNotFoundError as e:
            raise EntityNotFoundError("Location", e.name)
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            error_msg = f"Connectivity test failed: {str(e)}"
            self._logger.error(f"Location connectivity test error: {name} - {error_msg}")
            
            return LocationTestResult(
                location_name=name,
                success=False,
                error_message=error_msg,
                latency_ms=latency_ms
            )
    
    def validate_location_path(self, name: str, path: str) -> bool:
        """
        Validate that a path is accessible at the given location.
        
        Args:
            name: The name of the location
            path: The path to validate
            
        Returns:
            True if path is valid and accessible
            
        Raises:
            EntityNotFoundError: If location not found
            LocationAccessError: If path cannot be accessed
        """
        self._logger.debug(f"Validating path at location {name}: {path}")
        
        try:
            location = self._location_repo.get_by_name(name)
            if location is None:
                raise EntityNotFoundError("Location", name)
            
            # Perform protocol-specific path validation
            protocol = location.get_protocol()
            is_valid = self._validate_protocol_path(location, path)
            
            if not is_valid:
                raise LocationAccessError(
                    name, 
                    protocol, 
                    f"Path '{path}' is not accessible"
                )
            
            return True
            
        except LocationNotFoundError as e:
            raise EntityNotFoundError("Location", e.name)
        except RepositoryError as e:
            self._logger.error(f"Repository error validating path: {str(e)}")
            raise
    
    def get_location_filesystem(self, name: str):
        """Get location object for filesystem access (for completion, browsing, etc.)."""
        try:
            location = self._location_repo.get_by_name(name)
            # Return the location entity itself - the SmartPathCompleter expects a Location with .config
            return location
        except LocationNotFoundError as e:
            raise EntityNotFoundError("Location", e.name)
    
    # Private helper methods
    
    def _create_location_filesystem(self, location: LocationEntity):
        """Create filesystem access for a location."""
        protocol = location.get_protocol()
        base_path = location.get_base_path()
        storage_options = location.get_storage_options()
        
        if protocol in ("file", "local"):
            # Local filesystem
            import fsspec

            from ...infrastructure.adapters.sandboxed_filesystem import \
                PathSandboxedFileSystem
            base_fs = fsspec.filesystem('file')
            return PathSandboxedFileSystem(base_fs, base_path)
            
        elif protocol in ('ssh', 'sftp'):
            # SSH filesystem
            import fsspec

            from ...infrastructure.adapters.sandboxed_filesystem import \
                PathSandboxedFileSystem
            
            host = storage_options.get("host", "localhost")
            ssh_config = {
                'host': host,
                'timeout': 30  # Default timeout
            }
            
            # Add optional SSH configuration
            for key in ['username', 'password', 'key_filename', 'port']:
                if key in storage_options:
                    ssh_config[key] = storage_options[key]
            
            base_fs = fsspec.filesystem('ssh', **ssh_config)
            return PathSandboxedFileSystem(base_fs, base_path)
            
        elif protocol == 'scoutfs':
            # ScoutFS filesystem (extends SFTP)
            from ...infrastructure.adapters.sandboxed_filesystem import \
                PathSandboxedFileSystem
            from ...infrastructure.adapters.scoutfs_filesystem import \
                ScoutFSFileSystem
            
            host = storage_options.get("host", "localhost")
            scoutfs_config = {k: v for k, v in storage_options.items() if k != 'host'}
            scoutfs_config['timeout'] = 30  # Default timeout
            
            # Pass warning filters from unified config structure
            warning_filters = location.config.get('warning_filters', {})
            scoutfs_config['warning_filters'] = warning_filters
            
            base_fs = ScoutFSFileSystem(host=host, **scoutfs_config)
            return PathSandboxedFileSystem(base_fs, base_path)
            
        else:
            raise ConfigurationError(protocol, f"Unsupported protocol for filesystem access: {protocol}")
    
    def _create_unsandboxed_filesystem(self, location: LocationEntity):
        """Create unsandboxed filesystem access for tab completion (allows browsing entire remote filesystem)."""
        protocol = location.get_protocol()
        storage_options = location.get_storage_options()
        
        if protocol in ("file", "local"):
            # Local filesystem - no sandboxing needed for tab completion
            import fsspec
            return fsspec.filesystem('file')
            
        elif protocol in ('ssh', 'sftp'):
            # SSH filesystem without sandboxing
            import fsspec
            
            host = storage_options.get("host", "localhost")
            ssh_config = {
                'host': host,
                'timeout': 30  # Default timeout
            }
            
            # Add optional SSH configuration
            for key in ['username', 'password', 'key_filename', 'port']:
                if key in storage_options:
                    ssh_config[key] = storage_options[key]
            
            return fsspec.filesystem('ssh', **ssh_config)
            
        elif protocol == 'scoutfs':
            # ScoutFS filesystem without sandboxing
            from ...infrastructure.adapters.scoutfs_filesystem import \
                ScoutFSFileSystem
            
            host = storage_options.get("host", "localhost")
            scoutfs_config = {k: v for k, v in storage_options.items() if k != 'host'}
            scoutfs_config['timeout'] = 30  # Default timeout
            
            # Pass warning filters from location configuration  
            warning_filters = location.additional_config.get('warning_filters', {})
            scoutfs_config['warning_filters'] = warning_filters
            
            return ScoutFSFileSystem(host=host, **scoutfs_config)
            
        else:
            raise ConfigurationError(protocol, f"Unsupported protocol for filesystem access: {protocol}")
    
    def _entity_to_dto(self, location: LocationEntity) -> LocationDto:
        """Convert domain entity to DTO."""
        return LocationDto(
            name=location.name,
            kinds=[kind.name for kind in location.kinds],
            protocol=location.get_protocol(),
            path=location.get_base_path(),
            storage_options=location.get_storage_options(),
            additional_config={
                k: v for k, v in location.config.items()
                if k not in ("protocol", "path", "storage_options")
            },
            is_remote=location.is_remote()
        )
    
    def _apply_filters(
        self,
        locations: List[LocationEntity],
        filters: FilterOptions
    ) -> List[LocationEntity]:
        """Apply filtering to location list."""
        filtered = locations
        
        if filters.search_term:
            search_term = filters.search_term.lower()
            filtered = [
                loc for loc in filtered
                if (search_term in loc.name.lower() or
                    search_term in loc.get_protocol().lower() or
                    any(search_term in kind.name.lower() for kind in loc.kinds))
            ]
        
        # Additional filtering could be added here
        
        return filtered
    
    def _validate_protocol_config(self, protocol: str, config: Dict[str, Any]) -> None:
        """
        Validate protocol-specific configuration.
        
        Raises:
            ConfigurationError: If configuration is invalid
        """
        if protocol in ("sftp", "ssh"):
            storage_options = config.get("storage_options", {})
            
            if not storage_options:
                raise ConfigurationError(
                    protocol,
                    "storage_options required for SFTP/SSH protocols"
                )
            
            if not isinstance(storage_options, dict):
                raise ConfigurationError(
                    protocol,
                    "storage_options must be a dictionary"
                )
            
            # Check for required SFTP/SSH options
            required_keys = {"host"}  # At minimum, host is required
            missing_keys = required_keys - set(storage_options.keys())
            if missing_keys:
                raise ConfigurationError(
                    protocol,
                    f"Missing required storage_options: {', '.join(missing_keys)}"
                )
        
        elif protocol == "s3":
            storage_options = config.get("storage_options", {})
            if storage_options:
                # Validate S3-specific options
                s3_keys = {"endpoint_url", "key", "secret", "token", "profile"}
                invalid_keys = set(storage_options.keys()) - s3_keys
                if invalid_keys:
                    self._logger.warning(f"Unknown S3 options: {', '.join(invalid_keys)}")
        
        elif protocol in ("file", "local"):
            # For local filesystem, validate path if provided
            path = config.get("path")
            if path and not isinstance(path, str):
                raise ConfigurationError(
                    protocol,
                    "Path must be a string for local filesystem"
                )
    
    def _test_protocol_connectivity(
        self,
        location: LocationEntity,
        timeout_seconds: int
    ) -> Dict[str, Any]:
        """
        Test connectivity for a specific protocol.
        
        Returns:
            Dictionary with test results
        """
        protocol = location.get_protocol()
        
        if protocol in ("file", "local"):
            return self._test_local_connectivity(location)
        elif protocol in ("sftp", "ssh", "scoutfs"):
            return self._test_sftp_connectivity(location, timeout_seconds)
        elif protocol == "s3":
            return self._test_s3_connectivity(location, timeout_seconds)
        else:
            return {
                "success": False,
                "error": f"Connectivity testing not implemented for protocol: {protocol}"
            }
    
    def _test_local_connectivity(self, location: LocationEntity) -> Dict[str, Any]:
        """Test local filesystem connectivity."""
        try:
            base_path = location.get_base_path()
            if base_path:
                path_obj = Path(base_path)
                if not path_obj.exists():
                    return {
                        "success": False,
                        "error": f"Path does not exist: {base_path}"
                    }
                
                if not path_obj.is_dir():
                    return {
                        "success": False,
                        "error": f"Path is not a directory: {base_path}"
                    }
                
                # Try to get available space
                try:
                    import shutil
                    available_space = shutil.disk_usage(base_path).free
                except Exception:
                    available_space = None
                
                return {
                    "success": True,
                    "available_space": available_space,
                    "protocol_info": {
                        "path_exists": True,
                        "is_directory": True
                    }
                }
            else:
                return {"success": True}  # No specific path to test
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Local filesystem test failed: {str(e)}"
            }
    
    def _test_sftp_connectivity(
        self,
        location: LocationEntity,
        timeout_seconds: int
    ) -> Dict[str, Any]:
        """Test SFTP connectivity using filesystem abstraction (works for SSH, SFTP, and ScoutFS)."""
        storage_options = location.get_storage_options()
        host = storage_options.get("host", "unknown")
        port = storage_options.get("port", 22)
        username = storage_options.get("username")
        
        try:
            # Create filesystem using fsspec
            import fsspec

            from ...infrastructure.adapters.sandboxed_filesystem import \
                PathSandboxedFileSystem
            
            protocol = location.get_protocol()
            base_path = location.get_base_path() or "."
            
            if protocol in ('ssh', 'sftp'):
                # SSH filesystem
                ssh_config = {
                    'host': host,
                    'username': username,
                    'port': port,
                    'timeout': timeout_seconds
                }
                # Add authentication config
                for key in ['password', 'key_filename']:
                    if key in storage_options:
                        if key == 'key_filename':
                            ssh_config['client_keys'] = [storage_options[key]]
                        else:
                            ssh_config[key] = storage_options[key]
                
                base_fs = fsspec.filesystem('ssh', **ssh_config)
                sandboxed_fs = PathSandboxedFileSystem(base_fs, base_path)
                
            elif protocol == 'scoutfs':
                # ScoutFS filesystem (extends SFTP)
                from ...infrastructure.adapters.scoutfs_filesystem import \
                    ScoutFSFileSystem
                scoutfs_config = {k: v for k, v in storage_options.items() if k != 'host'}
                scoutfs_config['timeout'] = timeout_seconds
                
                # Pass warning filters from unified config structure
                warning_filters = location.config.get('warning_filters', {})
                scoutfs_config['warning_filters'] = warning_filters
                
                base_fs = ScoutFSFileSystem(host, **scoutfs_config)
                sandboxed_fs = PathSandboxedFileSystem(base_fs, base_path)
            
            else:
                return {
                    "success": False,
                    "error": f"Unsupported protocol for SFTP testing: {protocol}"
                }
            
            # Test basic filesystem operations
            try:
                file_list = sandboxed_fs.ls(".", detail=False)
                can_list = True
                file_count = len(file_list)
            except Exception:
                can_list = False
                file_count = 0
            
            # Skip disk usage checks - they can be very slow on remote systems
            
            return {
                "success": True,
                "available_space": None,  # Skip disk usage for performance
                "protocol_info": {
                    "host": host,
                    "port": port,
                    "username": username,
                    "can_list_directory": can_list,
                    "file_count": file_count,
                    "base_path": base_path,
                    "filesystem_type": type(sandboxed_fs._fs).__name__
                }
            }
            
        except Exception as e:
            error_msg = str(e)
            
            # Provide specific error messages for common issues
            if "Authentication" in error_msg or "permission denied" in error_msg.lower():
                return {
                    "success": False,
                    "error": f"Authentication failed for {username}@{host}",
                    "protocol_info": {
                        "host": host,
                        "port": port,
                        "username": username
                    }
                }
            elif "Connection" in error_msg or "timeout" in error_msg.lower():
                return {
                    "success": False,
                    "error": f"Connection failed to {host}:{port} - {error_msg}",
                    "protocol_info": {
                        "host": host,
                        "port": port
                    }
                }
            else:
                return {
                    "success": False,
                    "error": f"SFTP connectivity test failed: {error_msg}",
                    "protocol_info": {
                        "host": host,
                        "port": port
                    }
                }
    
    def _test_s3_connectivity(
        self,
        location: LocationEntity,
        timeout_seconds: int
    ) -> Dict[str, Any]:
        """Test S3 connectivity."""
        # This would require actual S3 implementation
        # For now, return a placeholder result
        storage_options = location.get_storage_options()
        endpoint = storage_options.get("endpoint_url", "s3.amazonaws.com")
        
        return {
            "success": False,
            "error": f"S3 connectivity testing not implemented (endpoint: {endpoint})",
            "protocol_info": {
                "endpoint": endpoint
            }
        }
    
    def _validate_protocol_path(self, location: LocationEntity, path: str) -> bool:
        """
        Validate a path for a specific protocol.
        
        Returns:
            True if path is valid and accessible
        """
        protocol = location.get_protocol()
        
        if protocol in ("file", "local"):
            # For local filesystem, check if path exists
            try:
                return Path(path).exists()
            except Exception:
                return False
        
        # For remote protocols, this would require actual connectivity
        # For now, just do basic validation
        return isinstance(path, str) and len(path) > 0
    
    def ensure_localhost_location(self) -> str:
        """
        Ensure a localhost location exists, creating a temporary one if needed.
        
        Returns:
            Name of the localhost location that can be used
        """
        # Check if localhost already exists
        try:
            self.get_location("localhost")
            return "localhost"
        except EntityNotFoundError:
            pass
        
        # Check if tellus_localhost exists (common in development)
        try:
            self.get_location("tellus_localhost")
            return "tellus_localhost"
        except EntityNotFoundError:
            pass
        
        # Create a temporary localhost location
        self._logger.info("Creating temporary localhost location")
        
        dto = CreateLocationDto(
            name="localhost",
            kinds=["compute", "disk"],
            protocol="file",
            config={}
        )
        
        try:
            self.create_location(dto)
            return "localhost"
        except Exception as e:
            self._logger.error(f"Failed to create temporary localhost location: {e}")
            raise LocationAccessError(
                "localhost", 
                f"Could not create localhost location for archive operation: {e}"
            )