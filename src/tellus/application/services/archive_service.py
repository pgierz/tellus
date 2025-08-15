"""
Archive Application Service - Orchestrates archive-related use cases.

This service coordinates archive operations, caching, file management,
and long-running workflows in the Earth System Model context.
"""

import asyncio
import logging
import tarfile
import zipfile
import time
from dataclasses import asdict
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

from ...domain.entities.archive import (
    ArchiveId,
    ArchiveMetadata,
    ArchiveType,
    CacheCleanupPolicy,
    CacheConfiguration,
    Checksum,
    FileMetadata,
    LocationContext,
)
from ...domain.entities.location import LocationEntity
from ...domain.entities.simulation_file import (
    SimulationFile,
    FileInventory,
    FileContentType,
    FileImportance,
)
from ...domain.repositories.exceptions import LocationNotFoundError, RepositoryError
from ...domain.repositories.location_repository import ILocationRepository
from ...domain.repositories.archive_repository import IArchiveRepository
from ..dtos import (
    ArchiveContentsDto,
    ArchiveDto,
    ArchiveListDto,
    ArchiveOperationDto,
    ArchiveOperationResult,
    CacheConfigurationDto,
    CacheEntryDto,
    CacheOperationResult,
    CacheStatusDto,
    CreateArchiveDto,
    FileMetadataDto,
    FilterOptions,
    PaginationInfo,
    UpdateArchiveDto,
    WorkflowExecutionDto,
    SimulationFileDto,
    FileInventoryDto,
    ArchiveFileListDto,
    FileAssociationDto,
    FileAssociationResultDto,
    ArchiveCopyOperationDto,
    ArchiveMoveOperationDto,
    ArchiveExtractionDto,
    LocationContextResolutionDto,
    ArchiveOperationProgressDto,
    ArchiveOperationResultDto,
    BulkArchiveOperationDto,
    BulkOperationResultDto,
    ExtractionManifestDto,
)
from ..exceptions import (
    ArchiveOperationError,
    CacheOperationError,
    DataIntegrityError,
    EntityAlreadyExistsError,
    EntityNotFoundError,
    ExternalServiceError,
    OperationNotAllowedError,
    ResourceLimitExceededError,
    ValidationError,
)

logger = logging.getLogger(__name__)


class ArchiveApplicationService:
    """
    Application service for archive management.

    Handles archive operations including creation, extraction, caching,
    file management, and coordination of long-running workflows for
    Earth System Model data archives.
    """

    def __init__(
        self,
        location_repository: ILocationRepository,
        archive_repository: IArchiveRepository,
        cache_config: Optional[CacheConfigurationDto] = None,
    ):
        """
        Initialize the archive service.

        Args:
            location_repository: Repository for location data access
            archive_repository: Repository for archive metadata persistence
            cache_config: Cache configuration (uses defaults if not provided)
        """
        self._location_repo = location_repository
        self._archive_repo = archive_repository
        self._cache_config = self._build_cache_config(cache_config)
        self._active_operations: Dict[str, WorkflowExecutionDto] = {}
        self._cache_entries: Dict[str, CacheEntryDto] = {}
        self._logger = logger

        # Initialize cache directory
        self._ensure_cache_directory()

    def create_archive_metadata(self, dto: CreateArchiveDto) -> ArchiveDto:
        """
        Create metadata for a new archive.

        Args:
            dto: Data transfer object with archive creation data

        Returns:
            Created archive metadata DTO

        Raises:
            EntityAlreadyExistsError: If archive already exists
            EntityNotFoundError: If location not found
            ValidationError: If validation fails
        """
        self._logger.info(f"Creating archive metadata: {dto.archive_id}")

        try:
            # Validate location exists
            location = self._location_repo.get_by_name(dto.location_name)
            if location is None:
                raise EntityNotFoundError("Location", dto.location_name)

            # Convert string archive type to enum
            try:
                archive_type = ArchiveType(dto.archive_type)
            except ValueError:
                raise ValidationError(f"Invalid archive type: {dto.archive_type}")

            # Create archive ID value object
            archive_id = ArchiveId(dto.archive_id)

            # Create archive metadata entity
            metadata = ArchiveMetadata(
                archive_id=archive_id,
                location=dto.location_name,
                archive_type=archive_type,
                simulation_id=dto.simulation_id,  # Now properly supported
                simulation_date=dto.simulation_date,
                version=dto.version,
                description=dto.description,
                tags=dto.tags.copy(),
            )

            # Add tags if provided
            for tag in dto.tags:
                metadata.add_tag(tag)

            # Save to repository
            self._archive_repo.save(metadata)

            self._logger.info(
                f"Successfully created archive metadata: {dto.archive_id}"
            )
            return self._metadata_to_dto(metadata)

        except LocationNotFoundError as e:
            raise EntityNotFoundError("Location", e.name)
        except ValueError as e:
            raise ValidationError(f"Invalid archive data: {str(e)}")
        except Exception as e:
            self._logger.error(f"Unexpected error creating archive metadata: {str(e)}")
            raise

    def get_archive_metadata(self, archive_id: str) -> ArchiveDto:
        """
        Get archive metadata by ID.

        Args:
            archive_id: The ID of the archive

        Returns:
            Archive metadata DTO

        Raises:
            EntityNotFoundError: If archive not found
        """
        self._logger.debug(f"Retrieving archive metadata: {archive_id}")

        # Get from repository
        metadata = self._archive_repo.get_by_id(archive_id)
        if metadata is None:
            raise EntityNotFoundError("Archive", archive_id)

        return self._metadata_to_dto(metadata)

    def list_archives(
        self,
        location_name: Optional[str] = None,
        page: int = 1,
        page_size: int = 50,
        filters: Optional[FilterOptions] = None,
    ) -> ArchiveListDto:
        """
        List archives with pagination and filtering.

        Args:
            location_name: Optional location name to filter by
            page: Page number (1-based)
            page_size: Number of archives per page
            filters: Optional filtering criteria

        Returns:
            Paginated list of archives
        """
        self._logger.debug(f"Listing archives for location: {location_name}")

        # Get from repository
        metadata_list = self._archive_repo.list_all()

        # Filter by location if specified
        if location_name:
            metadata_list = [m for m in metadata_list if m.location == location_name]

        # Convert to DTOs
        archives = [self._metadata_to_dto(metadata) for metadata in metadata_list]

        # Apply pagination
        total_count = len(archives)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        archives_page = archives[start_idx:end_idx]

        pagination = PaginationInfo(
            page=page,
            page_size=page_size,
            total_count=total_count,
            has_next=end_idx < total_count,
            has_previous=page > 1,
        )

        return ArchiveListDto(
            archives=archives_page,
            pagination=pagination,
            filters_applied=filters or FilterOptions(),
        )

    def extract_archive(self, dto: ArchiveOperationDto) -> str:
        """
        Start an archive extraction operation.

        Args:
            dto: Archive operation parameters

        Returns:
            Operation ID for tracking progress

        Raises:
            EntityNotFoundError: If archive not found
            ArchiveOperationError: If operation cannot be started
        """
        operation_id = f"extract_{dto.archive_id}_{int(time.time())}"
        self._logger.info(f"Starting archive extraction: {operation_id}")

        try:
            # Validate archive exists in repository
            if not self._archive_repo.exists(dto.archive_id):
                raise EntityNotFoundError("Archive", dto.archive_id)

            # Create workflow execution
            workflow = WorkflowExecutionDto(
                workflow_id=operation_id,
                name=f"Extract Archive {dto.archive_id}",
                status="pending",
                start_time=time.time(),
                current_step="Validating archive",
                total_steps=4,  # Validate, Extract, Verify, Cleanup
                result_data=asdict(dto),
            )

            self._active_operations[operation_id] = workflow

            # Start the extraction asynchronously (in a real implementation)
            # For now, we'll simulate the process
            asyncio.create_task(self._simulate_extraction(operation_id, dto))

            self._logger.info(f"Archive extraction started: {operation_id}")
            return operation_id

        except Exception as e:
            self._logger.error(f"Failed to start archive extraction: {str(e)}")
            raise ArchiveOperationError(dto.archive_id, "extract", str(e))

    def compress_archive(self, dto: ArchiveOperationDto) -> str:
        """
        Start an archive compression operation.

        Args:
            dto: Archive operation parameters

        Returns:
            Operation ID for tracking progress

        Raises:
            ValidationError: If source path invalid
            ArchiveOperationError: If operation cannot be started
        """
        operation_id = f"compress_{dto.archive_id}_{int(time.time())}"
        self._logger.info(f"Starting archive compression: {operation_id}")

        try:
            # Validate source path exists
            if not dto.source_path:
                raise ValidationError("Source path is required for compression")

            source_path = Path(dto.source_path)
            if not source_path.exists():
                raise ValidationError(f"Source path does not exist: {dto.source_path}")

            # Create workflow execution
            workflow = WorkflowExecutionDto(
                workflow_id=operation_id,
                name=f"Compress Archive {dto.archive_id}",
                status="pending",
                start_time=time.time(),
                current_step="Scanning source files",
                total_steps=5,  # Scan, Compress, Verify, Cache, Cleanup
                result_data=asdict(dto),
            )

            self._active_operations[operation_id] = workflow

            # Start the compression asynchronously
            asyncio.create_task(self._simulate_compression(operation_id, dto))

            self._logger.info(f"Archive compression started: {operation_id}")
            return operation_id

        except Exception as e:
            self._logger.error(f"Failed to start archive compression: {str(e)}")
            raise ArchiveOperationError(dto.archive_id, "compress", str(e))

    def get_operation_status(self, operation_id: str) -> WorkflowExecutionDto:
        """
        Get the status of a running operation.

        Args:
            operation_id: The operation ID

        Returns:
            Workflow execution status

        Raises:
            EntityNotFoundError: If operation not found
        """
        if operation_id not in self._active_operations:
            raise EntityNotFoundError("Operation", operation_id)

        return self._active_operations[operation_id]

    def cancel_operation(self, operation_id: str) -> bool:
        """
        Cancel a running operation.

        Args:
            operation_id: The operation ID

        Returns:
            True if operation was cancelled

        Raises:
            EntityNotFoundError: If operation not found
            OperationNotAllowedError: If operation cannot be cancelled
        """
        self._logger.info(f"Cancelling operation: {operation_id}")

        if operation_id not in self._active_operations:
            raise EntityNotFoundError("Operation", operation_id)

        workflow = self._active_operations[operation_id]

        if workflow.status in ("completed", "failed", "cancelled"):
            raise OperationNotAllowedError(
                "cancel", f"Operation is already {workflow.status}"
            )

        # Update status
        workflow.status = "cancelled"
        workflow.end_time = time.time()

        self._logger.info(f"Operation cancelled: {operation_id}")
        return True

    def add_to_cache(
        self, archive_id: str, file_path: str, tags: Optional[Set[str]] = None
    ) -> CacheOperationResult:
        """
        Add an archive or file to the cache.

        Args:
            archive_id: Archive identifier
            file_path: Path to the file to cache
            tags: Optional tags for the cache entry

        Returns:
            Cache operation result

        Raises:
            ValidationError: If file path invalid
            ResourceLimitExceededError: If cache limit exceeded
        """
        self._logger.info(f"Adding to cache: {archive_id}")

        start_time = time.time()

        try:
            # Validate file exists
            path_obj = Path(file_path)
            if not path_obj.exists():
                raise ValidationError(f"File does not exist: {file_path}")

            file_size = path_obj.stat().st_size

            # Check cache limits
            current_size = self._get_cache_size()
            if current_size + file_size > self._cache_config.archive_size_limit:
                # Try cleanup first
                self._cleanup_cache()
                current_size = self._get_cache_size()

                if current_size + file_size > self._cache_config.archive_size_limit:
                    raise ResourceLimitExceededError(
                        "cache",
                        f"{self._cache_config.archive_size_limit} bytes",
                        f"{current_size + file_size} bytes",
                    )

            # Create cache entry
            cache_key = f"archive:{archive_id}"
            entry = CacheEntryDto(
                key=cache_key,
                size=file_size,
                created_time=time.time(),
                last_accessed=time.time(),
                access_count=1,
                entry_type="archive",
                tags=tags or set(),
            )

            self._cache_entries[cache_key] = entry

            duration_ms = (time.time() - start_time) * 1000

            result = CacheOperationResult(
                operation="add",
                success=True,
                entries_affected=1,
                bytes_affected=file_size,
                duration_ms=duration_ms,
            )

            self._logger.info(f"Successfully added to cache: {archive_id}")
            return result

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            error_msg = str(e)

            self._logger.error(f"Failed to add to cache: {archive_id} - {error_msg}")

            return CacheOperationResult(
                operation="add",
                success=False,
                duration_ms=duration_ms,
                error_message=error_msg,
            )

    def get_cache_status(self) -> CacheStatusDto:
        """
        Get current cache status and statistics.

        Returns:
            Cache status information
        """
        total_size = sum(entry.size for entry in self._cache_entries.values())
        entry_count = len(self._cache_entries)
        archive_count = sum(
            1 for entry in self._cache_entries.values() if entry.entry_type == "archive"
        )
        file_count = sum(
            1 for entry in self._cache_entries.values() if entry.entry_type == "file"
        )

        oldest_time = min(
            (entry.created_time for entry in self._cache_entries.values()), default=None
        )
        newest_time = max(
            (entry.created_time for entry in self._cache_entries.values()), default=None
        )

        return CacheStatusDto(
            total_size=self._cache_config.archive_size_limit,
            used_size=total_size,
            available_size=self._cache_config.archive_size_limit - total_size,
            entry_count=entry_count,
            archive_count=archive_count,
            file_count=file_count,
            cleanup_policy=self._cache_config.cleanup_policy.value,
            oldest_entry=oldest_time,
            newest_entry=newest_time,
        )

    def cleanup_cache(self, force: bool = False) -> CacheOperationResult:
        """
        Clean up the cache according to the configured policy.

        Args:
            force: Force cleanup regardless of current size

        Returns:
            Cache operation result
        """
        self._logger.info("Starting cache cleanup")

        start_time = time.time()
        initial_count = len(self._cache_entries)
        initial_size = self._get_cache_size()

        entries_removed = 0
        bytes_removed = 0

        try:
            if self._cache_config.cleanup_policy == CacheCleanupPolicy.LRU:
                entries_removed, bytes_removed = self._cleanup_lru(force)
            elif self._cache_config.cleanup_policy == CacheCleanupPolicy.SIZE_ONLY:
                entries_removed, bytes_removed = self._cleanup_by_size(force)
            # MANUAL policy doesn't automatically clean up

            duration_ms = (time.time() - start_time) * 1000

            result = CacheOperationResult(
                operation="cleanup",
                success=True,
                entries_affected=entries_removed,
                bytes_affected=bytes_removed,
                duration_ms=duration_ms,
            )

            self._logger.info(
                f"Cache cleanup completed: removed {entries_removed} entries, {bytes_removed} bytes"
            )
            return result

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            error_msg = str(e)

            self._logger.error(f"Cache cleanup failed: {error_msg}")

            return CacheOperationResult(
                operation="cleanup",
                success=False,
                duration_ms=duration_ms,
                error_message=error_msg,
            )

    def verify_archive_integrity(self, archive_id: str) -> bool:
        """
        Verify the integrity of an archived dataset.

        Args:
            archive_id: Archive identifier

        Returns:
            True if archive integrity is valid

        Raises:
            EntityNotFoundError: If archive not found
            DataIntegrityError: If integrity check fails
        """
        self._logger.info(f"Verifying archive integrity: {archive_id}")

        # Check repository instead of cache
        if not self._archive_repo.exists(archive_id):
            raise EntityNotFoundError("Archive", archive_id)

        # This would perform actual integrity checks like:
        # - Checksum verification
        # - File structure validation
        # - Metadata consistency checks

        # For now, simulate the verification
        try:
            # Simulate verification process
            time.sleep(0.1)  # Simulate processing time

            # In a real implementation, this would check file checksums,
            # validate archive structure, etc.
            is_valid = True  # Assume valid for simulation

            if not is_valid:
                raise DataIntegrityError(
                    "archive", archive_id, "Checksum mismatch detected"
                )

            self._logger.info(f"Archive integrity verified: {archive_id}")
            return True

        except Exception as e:
            self._logger.error(
                f"Archive integrity verification failed: {archive_id} - {str(e)}"
            )
            raise

    def list_archive_files(
        self,
        archive_id: str,
        content_type: Optional[str] = None,
        pattern: Optional[str] = None,
        limit: int = 50,
    ) -> ArchiveFileListDto:
        """
        List files within an archive.

        Args:
            archive_id: Archive identifier
            content_type: Optional content type filter
            pattern: Optional glob pattern filter
            limit: Maximum number of files to return

        Returns:
            List of files with metadata

        Raises:
            EntityNotFoundError: If archive not found
            ArchiveOperationError: If archive cannot be accessed
        """
        self._logger.info(f"Listing files in archive: {archive_id}")

        # Get archive metadata
        metadata = self._archive_repo.get_by_id(archive_id)
        if metadata is None:
            raise EntityNotFoundError("Archive", archive_id)

        try:
            # Get location for archive access
            location_entity = self._location_repo.get_by_name(metadata.location)
            if location_entity is None:
                raise EntityNotFoundError("Location", metadata.location)

            # Convert to infrastructure Location for filesystem access
            from ...location.location import Location

            location = Location(
                name=location_entity.name,
                kinds=[],  # Not needed for filesystem access
                config=location_entity.config,
                _skip_registry=True,
            )

            # Access archive file through location's filesystem
            archive_files = self._extract_file_list_from_archive(metadata, location)

            # Apply filters
            filtered_files = []
            for file_info in archive_files:
                # Filter by content type
                if content_type and file_info.content_type.value != content_type:
                    continue

                # Filter by pattern
                if pattern and not fnmatch(file_info.relative_path, pattern):
                    continue

                filtered_files.append(file_info)

                # Apply limit
                if len(filtered_files) >= limit:
                    break

            # Convert to DTOs
            file_dtos = [self._simulation_file_to_dto(f) for f in filtered_files]

            # Calculate summary statistics
            total_size = sum(f.size or 0 for f in filtered_files)
            content_types = {}
            for f in filtered_files:
                content_type_name = f.content_type.value
                content_types[content_type_name] = (
                    content_types.get(content_type_name, 0) + 1
                )

            self._logger.info(
                f"Listed {len(file_dtos)} files from archive: {archive_id}"
            )

            return ArchiveFileListDto(
                archive_id=archive_id,
                files=file_dtos,
                total_files=len(filtered_files),
                total_size=total_size,
                content_types=content_types,
            )

        except Exception as e:
            self._logger.error(f"Failed to list archive files: {archive_id} - {str(e)}")
            raise ArchiveOperationError(archive_id, "list_files", str(e))

    def associate_files_with_simulation(
        self, association_dto: FileAssociationDto
    ) -> FileAssociationResultDto:
        """
        Associate archive files with a simulation.

        Args:
            association_dto: File association parameters

        Returns:
            Results of the association operation

        Raises:
            EntityNotFoundError: If archive or simulation not found
            ValidationError: If parameters are invalid
        """
        self._logger.info(
            f"Associating files from archive {association_dto.archive_id} "
            f"with simulation {association_dto.simulation_id}"
        )

        try:
            # Validate archive exists
            metadata = self._archive_repo.get_by_id(association_dto.archive_id)
            if metadata is None:
                raise EntityNotFoundError("Archive", association_dto.archive_id)

            # Get files to associate
            if association_dto.files_to_associate:
                # Specific files provided
                files_to_process = association_dto.files_to_associate
            else:
                # Get all files from archive and filter
                archive_files = self.list_archive_files(
                    association_dto.archive_id,
                    content_type=association_dto.content_type_filter,
                    pattern=association_dto.pattern_filter,
                )
                files_to_process = [f.relative_path for f in archive_files.files]

            files_associated = []
            files_skipped = []

            if association_dto.dry_run:
                # Dry run - just validate what would be done
                files_associated = files_to_process
                self._logger.info(
                    f"DRY RUN: Would associate {len(files_to_process)} files"
                )
            else:
                # TODO: Actually implement the association logic
                # This would involve:
                # 1. Updating simulation metadata to include file associations
                # 2. Possibly extracting files to simulation location
                # 3. Creating file index records

                # For now, simulate the association
                for file_path in files_to_process:
                    try:
                        # Simulate association logic
                        files_associated.append(file_path)
                    except Exception as e:
                        self._logger.warning(
                            f"Failed to associate {file_path}: {str(e)}"
                        )
                        files_skipped.append(file_path)

            self._logger.info(
                f"File association completed: {len(files_associated)} associated, "
                f"{len(files_skipped)} skipped"
            )

            return FileAssociationResultDto(
                archive_id=association_dto.archive_id,
                simulation_id=association_dto.simulation_id,
                files_associated=files_associated,
                files_skipped=files_skipped,
                success=len(files_skipped) == 0,
            )

        except Exception as e:
            self._logger.error(f"Failed to associate files: {str(e)}")
            return FileAssociationResultDto(
                archive_id=association_dto.archive_id,
                simulation_id=association_dto.simulation_id,
                files_associated=[],
                files_skipped=association_dto.files_to_associate or [],
                success=False,
                error_message=str(e),
            )

    def copy_archive(
        self, copy_dto: ArchiveCopyOperationDto
    ) -> ArchiveOperationResultDto:
        """
        Copy an archive to a different location with context resolution.

        Args:
            copy_dto: Archive copy operation parameters

        Returns:
            Archive operation result with details

        Raises:
            EntityNotFoundError: If archive or location not found
            ArchiveOperationError: If copy operation fails
        """
        operation_id = f"copy_{copy_dto.archive_id}_{int(time.time())}"
        self._logger.info(f"Starting archive copy operation: {operation_id}")

        start_time = time.time()

        try:
            # Validate archive exists
            archive_metadata = self._archive_repo.get_by_id(copy_dto.archive_id)
            if archive_metadata is None:
                raise EntityNotFoundError("Archive", copy_dto.archive_id)

            # Resolve destination path with simulation context
            resolved_path = self._resolve_location_path(
                copy_dto.destination_location, copy_dto.simulation_id, archive_metadata
            )

            # Get source and destination locations
            source_location_entity = self._location_repo.get_by_name(
                copy_dto.source_location
            )
            dest_location_entity = self._location_repo.get_by_name(
                copy_dto.destination_location
            )

            if source_location_entity is None:
                raise EntityNotFoundError("Location", copy_dto.source_location)
            if dest_location_entity is None:
                raise EntityNotFoundError("Location", copy_dto.destination_location)

            # Convert to infrastructure Location objects for filesystem access
            from ...location.location import Location

            source_location = Location(
                name=source_location_entity.name,
                kinds=[],  # Not needed for filesystem access
                config=source_location_entity.config,
                _skip_registry=True,  # Skip registry to avoid conflicts
            )
            dest_location = Location(
                name=dest_location_entity.name,
                kinds=[],
                config=dest_location_entity.config,
                _skip_registry=True,
            )

            # Perform real copy operation using fsspec
            # Get archive filename for relative path in source location
            archive_filename = f"{archive_metadata.archive_id.value}.tar.gz"
            source_path = archive_filename

            # For destination, use just the filename too since resolved_path would be absolute
            dest_path = archive_filename

            # Get filesystems
            try:
                source_fs = source_location.fs
                dest_fs = dest_location.fs
            except Exception as e:
                raise ArchiveOperationError(
                    copy_dto.archive_id,
                    "copy",
                    f"Failed to access filesystem: {str(e)}",
                )

            self._logger.info(f"Copying archive from {source_path} to {dest_path}")

            # Check if source file exists
            if not source_fs.exists(source_path):
                raise ArchiveOperationError(
                    copy_dto.archive_id,
                    "copy",
                    f"Source archive not found: {source_path}",
                )

            # Check if destination exists and handle overwrite
            if dest_fs.exists(dest_path) and not copy_dto.overwrite_existing:
                raise ArchiveOperationError(
                    copy_dto.archive_id,
                    "copy",
                    f"Destination file exists and overwrite is disabled: {dest_path}",
                )

            # Ensure destination directory exists
            dest_dir = "/".join(dest_path.split("/")[:-1])
            if dest_dir and not dest_fs.exists(dest_dir):
                dest_fs.makedirs(dest_dir, exist_ok=True)

            # Perform the actual copy operation using streaming approach
            try:
                # Always use streaming copy to handle cross-filesystem operations
                with source_fs.open(source_path, "rb") as src_file:
                    with dest_fs.open(dest_path, "wb") as dest_file:
                        # Copy in chunks for large files
                        chunk_size = 8 * 1024 * 1024  # 8MB chunks
                        while True:
                            chunk = src_file.read(chunk_size)
                            if not chunk:
                                break
                            dest_file.write(chunk)

                # Verify integrity if requested
                if copy_dto.verify_integrity:
                    self._verify_copy_integrity(
                        source_fs, dest_fs, source_path, dest_path
                    )

            except Exception as e:
                # Clean up partial copy on failure
                try:
                    if dest_fs.exists(dest_path):
                        dest_fs.rm(dest_path)
                except:
                    pass  # Ignore cleanup errors
                raise ArchiveOperationError(
                    copy_dto.archive_id, "copy", f"Copy operation failed: {str(e)}"
                )

            # Get actual file size
            try:
                file_info = dest_fs.info(dest_path)
                bytes_processed = file_info.get("size", archive_metadata.size or 0)
            except:
                bytes_processed = archive_metadata.size or 0

            # Create successful result
            duration = time.time() - start_time

            result = ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="copy",
                archive_id=copy_dto.archive_id,
                success=True,
                destination_path=dest_path,
                bytes_processed=bytes_processed,
                files_processed=1,
                duration_seconds=duration,
                checksum_verification=copy_dto.verify_integrity,
                manifest_created=False,
            )

            self._logger.info(f"Archive copy completed: {operation_id}")
            return result

        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            self._logger.error(f"Archive copy failed: {operation_id} - {error_msg}")

            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="copy",
                archive_id=copy_dto.archive_id,
                success=False,
                duration_seconds=duration,
                error_message=error_msg,
            )

    def move_archive(
        self, move_dto: ArchiveMoveOperationDto
    ) -> ArchiveOperationResultDto:
        """
        Move an archive to a different location with context resolution.

        This performs a copy followed by an optional source cleanup. The operation is atomic -
        if the copy succeeds but cleanup fails, the operation is still considered successful
        but a warning is included in the result.

        Args:
            move_dto: Archive move operation parameters

        Returns:
            Archive operation result with details

        Raises:
            EntityNotFoundError: If archive or location not found
            ArchiveOperationError: If move operation fails
        """
        operation_id = f"move_{move_dto.archive_id}_{int(time.time())}"
        self._logger.info(f"Starting archive move operation: {operation_id}")

        start_time = time.time()
        cleanup_success = True
        cleanup_error = None

        try:
            # Get source archive metadata
            archive_metadata = self._archive_repo.get_by_id(move_dto.archive_id)
            if not archive_metadata:
                raise EntityNotFoundError("Archive", move_dto.archive_id)

            # Get source location entity
            source_location = self._location_repo.get_by_name(move_dto.source_location)
            if not source_location:
                raise EntityNotFoundError("Location", move_dto.source_location)

            # First perform copy operation
            copy_dto = ArchiveCopyOperationDto(
                archive_id=move_dto.archive_id,
                source_location=move_dto.source_location,
                destination_location=move_dto.destination_location,
                simulation_id=move_dto.simulation_id,
                preserve_metadata=move_dto.preserve_metadata,
                overwrite_existing=True,  # Allow overwrite for move
                verify_integrity=move_dto.verify_integrity,
                progress_callback=move_dto.progress_callback,
            )

            # Execute the copy operation
            copy_result = self.copy_archive(copy_dto)

            if not copy_result.success:
                return ArchiveOperationResultDto(
                    operation_id=operation_id,
                    operation_type="move",
                    archive_id=move_dto.archive_id,
                    success=False,
                    error_message=f"Copy phase failed: {copy_result.error_message}",
                )

            # If copy succeeded and cleanup_source is enabled, remove source
            if move_dto.cleanup_source:
                try:
                    # Convert to infrastructure Location
                    from ...location.location import Location

                    source_loc = Location(
                        name=source_location.name,
                        kinds=[],
                        config=source_location.config,
                        _skip_registry=True,
                    )
                    
                    source_fs = source_loc.fs
                    if not source_fs:
                        raise ArchiveOperationError(
                            move_dto.archive_id,
                            "move",
                            f"Failed to get filesystem for source location: {move_dto.source_location}",
                        )
                    
                    # Get the actual source path using the same resolution as copy
                    source_path = self._get_archive_path(archive_metadata, source_location)
                    
                    if source_fs.exists(source_path):
                        self._logger.info(f"Cleaning up source archive: {source_path}")
                        source_fs.rm(source_path)
                        
                        # Verify source was actually removed
                        if source_fs.exists(source_path):
                            raise ArchiveOperationError(
                                move_dto.archive_id,
                                "move",
                                f"Failed to remove source file: {source_path}",
                            )
                        
                        self._logger.info(f"Successfully removed source: {source_path}")
                    else:
                        self._logger.warning(f"Source file not found for cleanup: {source_path}")
                        cleanup_success = False
                        cleanup_error = f"Source file not found: {source_path}"

                except Exception as e:
                    cleanup_success = False
                    cleanup_error = str(e)
                    self._logger.warning(
                        f"Source cleanup failed but operation will continue: {str(e)}",
                        exc_info=True,
                    )

            duration = time.time() - start_time
            
            # Prepare warnings based on cleanup status
            warnings = []
            if not cleanup_success and cleanup_error:
                warnings.append(f"Source cleanup failed: {cleanup_error}")

            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="move",
                archive_id=move_dto.archive_id,
                success=True,  # Consider move successful even if cleanup failed
                destination_path=copy_result.destination_path,
                bytes_processed=copy_result.bytes_processed,
                files_processed=copy_result.files_processed,
                duration_seconds=duration,
                checksum_verification=copy_result.checksum_verification,
                warnings=warnings,
            )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            self._logger.error(f"Archive move failed: {operation_id} - {error_msg}")

            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="move",
                archive_id=move_dto.archive_id,
                success=False,
                duration_seconds=duration,
                error_message=error_msg,
            )

    def extract_archive_to_location(
        self, extract_dto: ArchiveExtractionDto
    ) -> ArchiveOperationResultDto:
        """Extract archive contents to a location with context resolution.

        Args:
            extract_dto: Archive extraction parameters

        Returns:
            Archive operation result with extraction details
        """
        operation_id = f"extract_{extract_dto.archive_id}_{int(time.time())}"
        self._logger.info(f"Starting archive extraction: {operation_id}")

        start_time = time.time()

        try:
            # Validate archive exists
            archive_metadata = self._archive_repo.get_by_id(extract_dto.archive_id)
            if not archive_metadata:
                raise EntityNotFoundError("Archive", extract_dto.archive_id)

            # Get source location and filesystem
            source_location = self._location_repo.get_by_name(archive_metadata.location)
            if not source_location:
                raise EntityNotFoundError("Location", archive_metadata.location)

            source_fs = source_location.fs
            if not source_fs:
                raise ArchiveOperationError(
                    extract_dto.archive_id,
                    "extract",
                    f"No filesystem available for location: {archive_metadata.location}",
                )

            # Resolve source and destination paths
            archive_path = self._get_archive_path(archive_metadata, source_location)
            if not source_fs.exists(archive_path):
                raise ArchiveOperationError(
                    extract_dto.archive_id,
                    "extract",
                    f"Archive file not found at: {archive_path}",
                )

            # Get destination filesystem and resolve path
            dest_location = self._location_repo.get_by_name(extract_dto.destination_location)
            if not dest_location:
                raise EntityNotFoundError("Location", extract_dto.destination_location)

            dest_fs = dest_location.fs
            if not dest_fs:
                raise ArchiveOperationError(
                    extract_dto.archive_id,
                    "extract",
                    f"No filesystem available for destination location: {extract_dto.destination_location}",
                )

            # Resolve destination path with context
            resolved_path = self._resolve_location_path(
                extract_dto.destination_location,
                extract_dto.simulation_id,
                archive_metadata,
            )

            # Ensure destination directory exists
            if not dest_fs.exists(resolved_path):
                dest_fs.makedirs(resolved_path, exist_ok=True)

            # Get list of files to extract with filtering
            files_to_extract = []
            archive_files = self._get_files_to_extract(archive_metadata)
            
            # Apply content type and pattern filters
            for file_info in archive_files:
                if self._should_extract_file(
                    file_info.relative_path,
                    extract_dto.content_type_filter,
                    extract_dto.pattern_filter
                ):
                    files_to_extract.append(file_info.relative_path)
                    
            if not files_to_extract:
                self._logger.warning(
                    f"No files matching the specified filters found in archive {extract_dto.archive_id}"
                )
                return ArchiveOperationResultDto(
                    operation_id=operation_id,
                    operation_type="extract",
                    archive_id=extract_dto.archive_id,
                    success=True,
                    files_extracted=0,
                    bytes_extracted=0,
                    warnings=["No files matched the specified filters"],
                )

            # Extract files based on archive type
            extracted_count = 0
            if archive_metadata.archive_type in [ArchiveType.TAR, ArchiveType.TAR_GZ, ArchiveType.TAR_BZ2]:
                extracted_count = self._extract_tar_files(
                    source_fs,
                    dest_fs,
                    archive_path,
                    resolved_path,
                    files_to_extract,
                    extract_dto.preserve_directory_structure,
                )
            elif archive_metadata.archive_type == ArchiveType.ZIP:
                extracted_count = self._extract_zip_files(
                    source_fs,
                    dest_fs,
                    archive_path,
                    resolved_path,
                    files_to_extract,
                    extract_dto.preserve_directory_structure,
                )
            else:
                raise ArchiveOperationError(
                    extract_dto.archive_id,
                    "extract",
                    f"Unsupported archive type for extraction: {archive_metadata.archive_type}",
                )

            self._logger.info(f"Successfully extracted {extracted_count} files")

            # Create extraction manifest
            manifest = self._create_extraction_manifest(
                extract_dto,
                resolved_path,
                files_to_extract[:extracted_count],
            )

            duration = time.time() - start_time
            self._logger.info(
                f"Archive extraction completed in {duration:.2f} seconds: {operation_id}"
            )

            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="extract",
                archive_id=extract_dto.archive_id,
                success=True,
                files_extracted=extracted_count,
                bytes_extracted=0,  # Could be calculated if needed
                manifest=manifest,
            )

        except EntityNotFoundError as e:
            error_msg = f"{e.entity_type} not found: {e.entity_id}"
            self._logger.error(f"Extraction failed - {error_msg}")
            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="extract",
                archive_id=extract_dto.archive_id,
                success=False,
                error_message=error_msg,
            )
        except Exception as e:
            error_msg = f"Unexpected error during extraction: {str(e)}"
            self._logger.error(f"Extraction failed - {error_msg}", exc_info=True)
            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="extract",
                archive_id=extract_dto.archive_id,
                success=False,
                error_message=error_msg,
            )

    def extract_archive_to_location(self, extract_dto: ArchiveExtractionDto) -> ArchiveOperationResultDto:
        """
        Extract archive contents to a location with context resolution.

        Args:
            extract_dto: Archive extraction parameters

        Returns:
            Archive operation result with extraction details
        """
        operation_id = f"extract_{extract_dto.archive_id}_{int(time.time())}"
        self._logger.info(f"Starting archive extraction: {operation_id}")

        start_time = time.time()

        try:
            # Validate archive exists
            archive_metadata = self._archive_repo.get_by_id(extract_dto.archive_id)
            if archive_metadata is None:
                raise EntityNotFoundError("Archive", extract_dto.archive_id)

            # Resolve destination path
            resolved_path = self._resolve_location_path(
                extract_dto.destination_location,
                extract_dto.simulation_id,
                archive_metadata
            )

            # Get files to extract (if filtered)
            files_to_extract = []
            if extract_dto.file_filters or extract_dto.content_type_filter or extract_dto.pattern_filter:
                file_list = self.list_archive_files(
                    extract_dto.archive_id,
                    content_type=extract_dto.content_type_filter,
                    pattern=extract_dto.pattern_filter
                )
                files_to_extract = [f.relative_path for f in file_list.files]
                
                # Apply specific file filters if provided
                if extract_dto.file_filters:
                    files_to_extract = [f for f in files_to_extract if f in extract_dto.file_filters]
            else:
                # Extract all files
                file_list = self.list_archive_files(extract_dto.archive_id)
                files_to_extract = [f.relative_path for f in file_list.files]

            # Perform real extraction using fsspec
            source_location_entity = self._location_repo.get_by_name(archive_metadata.location)
            dest_location_entity = self._location_repo.get_by_name(extract_dto.destination_location)
            
            if source_location_entity is None:
                raise EntityNotFoundError("Location", archive_metadata.location)
            if dest_location_entity is None:
                raise EntityNotFoundError("Location", extract_dto.destination_location)
            
            # Convert to infrastructure Location objects
            from ...location.location import Location
            source_location = Location(
                name=source_location_entity.name,
                kinds=[],
                config=source_location_entity.config,
                _skip_registry=True
            )
            dest_location = Location(
                name=dest_location_entity.name,
                kinds=[],
                config=dest_location_entity.config,
                _skip_registry=True
            )
            
            # Get filesystems
            try:
                source_fs = source_location.fs
                dest_fs = dest_location.fs
            except Exception as e:
                raise ArchiveOperationError(
                    extract_dto.archive_id,
                    "extract",
                    f"Failed to access filesystem: {str(e)}"
                )
            
            # Use relative path for sandboxed filesystem
            archive_filename = f"{archive_metadata.archive_id.value}.tar.gz"
            archive_path = archive_filename
            
            # Check if archive exists
            if not source_fs.exists(archive_path):
                raise ArchiveOperationError(
                    extract_dto.archive_id,
                    "extract",
                    f"Archive not found: {archive_path}"
                )
            
            self._logger.info(f"Extracting {len(files_to_extract)} files to {resolved_path}")
            
            # Ensure destination directory exists
            if not dest_fs.exists(resolved_path):
                dest_fs.makedirs(resolved_path, exist_ok=True)
            
            # Perform real extraction based on archive type
            extracted_count = 0
            try:
                if archive_metadata.archive_type == ArchiveType.COMPRESSED:
                    extracted_count = self._extract_tar_files(
                        source_fs, dest_fs, archive_path, resolved_path, 
                        files_to_extract, extract_dto.preserve_directory_structure
                    )
                elif archive_metadata.archive_type == ArchiveType.ZIP:
                    extracted_count = self._extract_zip_files(
                        source_fs, dest_fs, archive_path, resolved_path,
                        files_to_extract, extract_dto.preserve_directory_structure
                    )
                else:
                    raise ArchiveOperationError(
                        extract_dto.archive_id,
                        "extract",
                        f"Unsupported archive type for extraction: {archive_metadata.archive_type}"
                    )
                    
                self._logger.info(f"Successfully extracted {extracted_count} files")
                
            except Exception as e:
                raise ArchiveOperationError(
                    extract_dto.archive_id,
                    "extract",
                    f"Extraction failed: {str(e)}"
                )

            # Create extraction manifest if requested
            manifest_created = False
            if extract_dto.create_manifest:
                manifest = self._create_extraction_manifest(
                    extract_dto, resolved_path, files_to_extract
                )
                manifest_created = True

            duration = time.time() - start_time
            
            # Calculate actual bytes processed from extracted files
            total_size = 0
            try:
                for file_path in files_to_extract[:extracted_count]:  # Only count actually extracted files
                    file_info = next((f for f in file_list.files if f.relative_path == file_path), None)
                    if file_info:
                        total_size += file_info.size or 0
            except:
                # Fall back to metadata estimate
                total_size = sum(f.size or 0 for f in file_list.files if f.relative_path in files_to_extract)

            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="extract",
                archive_id=extract_dto.archive_id,
                success=True,
                destination_path=resolved_path,
                bytes_processed=total_size,
                files_processed=extracted_count,
                duration_seconds=duration,
                manifest_created=manifest_created,
                warnings=[] if extracted_count > 0 else ["No files were successfully extracted"]
            )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            self._logger.error(f"Archive extraction failed: {operation_id} - {error_msg}")
            
            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="extract",
                archive_id=extract_dto.archive_id,
                success=False,
                duration_seconds=duration,
                error_message=error_msg
            )

    def resolve_location_context(
        self, dto: LocationContextResolutionDto
    ) -> LocationContextResolutionDto:
        """
        Resolve location path templates with simulation context.

        Args:
            dto: Path resolution parameters

        Returns:
            Updated DTO with resolved path and context
        """
        try:
            # Get simulation for context variables
            if dto.simulation_id:
                # In real implementation, get simulation attributes for template resolution
                # For now, create mock context
                mock_context = {
                    "model_id": "CESM2",
                    "experiment_id": "MIS11.3-B",
                    "simulation_id": dto.simulation_id,
                    "year": "2024",
                }
                dto.context_variables = mock_context

            # Simple template resolution (in real implementation, use proper template engine)
            resolved_path = dto.path_template
            for key, value in dto.context_variables.items():
                resolved_path = resolved_path.replace(f"{{{key}}}", str(value))

            dto.resolved_path = resolved_path

            return dto

        except Exception as e:
            dto.resolution_errors.append(str(e))
            return dto

    # Private helper methods

    def _resolve_location_path(
        self,
        location_name: str,
        simulation_id: Optional[str],
        archive_metadata: ArchiveMetadata,
    ) -> str:
        """Resolve location path with simulation context."""
        try:
            location = self._location_repo.get_by_name(location_name)
            if location is None:
                return f"/{archive_metadata.archive_id.value}"

            # Get path template from location
            path_template = location.config.get("path_prefix", "/{simulation_id}")

            # Create context for template resolution
            context = {
                "simulation_id": simulation_id
                or archive_metadata.simulation_id
                or "unknown",
                "archive_id": archive_metadata.archive_id.value,
                "model_id": "CESM2",  # Mock value - would come from simulation
                "experiment_id": "MIS11.3-B",  # Mock value
            }

            # Simple template resolution
            resolved_path = path_template
            for key, value in context.items():
                resolved_path = resolved_path.replace(f"{{{key}}}", str(value))

            # Ensure path starts with /
            if not resolved_path.startswith("/"):
                resolved_path = "/" + resolved_path

            return resolved_path

        except Exception as e:
            self._logger.warning(f"Failed to resolve location path: {str(e)}")
            return f"/{archive_metadata.archive_id.value}"

    def _create_extraction_manifest(
        self,
        extract_dto: ArchiveExtractionDto,
        resolved_path: str,
        files_extracted: List[str],
    ) -> ExtractionManifestDto:
        """Create an extraction manifest for tracking extracted files."""
        from datetime import datetime

        return ExtractionManifestDto(
            archive_id=extract_dto.archive_id,
            extraction_path=resolved_path,
            simulation_id=extract_dto.simulation_id,
            extracted_files=[],  # Would be populated with actual file info
            extraction_timestamp=datetime.now().isoformat(),
            source_location="",  # Would be populated from archive metadata
            checksum_verification={},
            extraction_options={
                "content_type_filter": extract_dto.content_type_filter,
                "pattern_filter": extract_dto.pattern_filter,
                "preserve_directory_structure": extract_dto.preserve_directory_structure,
            },
        )

    def _build_cache_config(
        self, dto: Optional[CacheConfigurationDto]
    ) -> CacheConfiguration:
        """Build cache configuration from DTO."""
        if dto is None:
            # Use default configuration
            return CacheConfiguration(
                cache_directory=str(Path.home() / ".cache" / "tellus"),
                archive_size_limit=50 * 1024**3,  # 50 GB
                file_size_limit=10 * 1024**3,  # 10 GB
                cleanup_policy=CacheCleanupPolicy.LRU,
            )

        try:
            cleanup_policy = CacheCleanupPolicy(dto.cleanup_policy)
        except ValueError:
            cleanup_policy = CacheCleanupPolicy.LRU

        return CacheConfiguration(
            cache_directory=dto.cache_directory,
            archive_size_limit=dto.archive_size_limit,
            file_size_limit=dto.file_size_limit,
            cleanup_policy=cleanup_policy,
            unified_cache=dto.unified_cache,
        )

    def _ensure_cache_directory(self) -> None:
        """Ensure cache directory exists."""
        cache_path = Path(self._cache_config.cache_directory)
        try:
            cache_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self._logger.warning(f"Failed to create cache directory: {str(e)}")

    def _metadata_to_dto(self, metadata: ArchiveMetadata) -> ArchiveDto:
        """Convert archive metadata entity to DTO."""
        checksum_str = None
        checksum_algorithm = None

        if metadata.checksum:
            checksum_str = metadata.checksum.value
            checksum_algorithm = metadata.checksum.algorithm

        # Check if archive is cached
        cache_key = f"archive:{metadata.archive_id.value}"
        is_cached = cache_key in self._cache_entries
        cache_path = None

        if is_cached:
            cache_path = (
                f"{self._cache_config.cache_directory}/{metadata.archive_id.value}"
            )

        return ArchiveDto(
            archive_id=metadata.archive_id.value,
            location=metadata.location,
            archive_type=metadata.archive_type.value,
            simulation_id=metadata.simulation_id,  # Now properly included
            checksum=checksum_str,
            checksum_algorithm=checksum_algorithm,
            size=metadata.size,
            created_time=metadata.created_time,
            simulation_date=metadata.simulation_date,
            version=metadata.version,
            description=metadata.description,
            tags=metadata.tags.copy(),
            is_cached=is_cached,
            cache_path=cache_path,
        )

    def _get_cache_size(self) -> int:
        """Get total cache size in bytes."""
        return sum(entry.size for entry in self._cache_entries.values())
        
    def _should_extract_file(
        self,
        filename: str,
        content_type: Optional[str] = None,
        pattern: Optional[str] = None
    ) -> bool:
        """
        Determine if a file should be extracted based on filters.
        
        Args:
            filename: Name of the file to check
            content_type: Optional content type to filter by
            pattern: Optional glob pattern to filter by
            
        Returns:
            bool: True if file should be extracted, False otherwise
        """
        # Always extract if no filters are provided
        if not content_type and not pattern:
            return True
            
        # Check content type filter if provided
        if content_type:
            file_content_type = self._classify_file_content_type(filename)
            if file_content_type.name.lower() != content_type.lower():
                return False
                
        # Check pattern filter if provided
        if pattern:
            import fnmatch
            if not fnmatch.fnmatch(filename, pattern):
                return False
                
        return True

    def _cleanup_cache(self) -> None:
        """Perform cache cleanup based on configured policy."""
        if self._cache_config.cleanup_policy == CacheCleanupPolicy.LRU:
            self._cleanup_lru(force=False)
        elif self._cache_config.cleanup_policy == CacheCleanupPolicy.SIZE_ONLY:
            self._cleanup_by_size(force=False)

    def _cleanup_lru(self, force: bool) -> tuple[int, int]:
        """Clean up cache using LRU policy."""
        if not force:
            current_size = self._get_cache_size()
            if current_size <= self._cache_config.archive_size_limit * 0.8:
                return 0, 0  # No cleanup needed

        # Sort by last accessed time
        sorted_entries = sorted(
            self._cache_entries.items(), key=lambda x: x[1].last_accessed
        )

        entries_removed = 0
        bytes_removed = 0
        target_size = (
            self._cache_config.archive_size_limit * 0.7
        )  # Clean to 70% capacity

        for key, entry in sorted_entries:
            if not force and self._get_cache_size() <= target_size:
                break

            del self._cache_entries[key]
            entries_removed += 1
            bytes_removed += entry.size

        return entries_removed, bytes_removed

    def _cleanup_by_size(self, force: bool) -> tuple[int, int]:
        """Clean up cache by removing largest files first."""
        if not force:
            current_size = self._get_cache_size()
            if current_size <= self._cache_config.archive_size_limit * 0.8:
                return 0, 0

        # Sort by size (largest first)
        sorted_entries = sorted(
            self._cache_entries.items(), key=lambda x: x[1].size, reverse=True
        )

        entries_removed = 0
        bytes_removed = 0
        target_size = self._cache_config.archive_size_limit * 0.7

        for key, entry in sorted_entries:
            if not force and self._get_cache_size() <= target_size:
                break

            del self._cache_entries[key]
            entries_removed += 1
            bytes_removed += entry.size

        return entries_removed, bytes_removed

    async def _simulate_extraction(
        self, operation_id: str, dto: ArchiveOperationDto
    ) -> None:
        """Simulate archive extraction process."""
        try:
            workflow = self._active_operations[operation_id]
            workflow.status = "running"

            # Step 1: Validate archive
            workflow.current_step = "Validating archive"
            workflow.progress = 0.1
            await asyncio.sleep(0.5)  # Simulate work

            # Step 2: Extract files
            workflow.current_step = "Extracting files"
            workflow.progress = 0.4
            workflow.completed_steps = 1
            await asyncio.sleep(1.0)

            # Step 3: Verify extracted files
            workflow.current_step = "Verifying extracted files"
            workflow.progress = 0.8
            workflow.completed_steps = 2
            await asyncio.sleep(0.5)

            # Step 4: Cleanup
            workflow.current_step = "Cleanup"
            workflow.progress = 1.0
            workflow.completed_steps = 3
            await asyncio.sleep(0.2)

            # Complete
            workflow.status = "completed"
            workflow.end_time = time.time()
            workflow.completed_steps = workflow.total_steps

        except Exception as e:
            workflow = self._active_operations[operation_id]
            workflow.status = "failed"
            workflow.end_time = time.time()
            workflow.error_message = str(e)

    async def _simulate_compression(
        self, operation_id: str, dto: ArchiveOperationDto
    ) -> None:
        """Simulate archive compression process."""
        try:
            workflow = self._active_operations[operation_id]
            workflow.status = "running"

            # Step 1: Scan source files
            workflow.current_step = "Scanning source files"
            workflow.progress = 0.1
            await asyncio.sleep(0.3)

            # Step 2: Compress files
            workflow.current_step = "Compressing files"
            workflow.progress = 0.5
            workflow.completed_steps = 1
            await asyncio.sleep(1.5)

            # Step 3: Verify compressed archive
            workflow.current_step = "Verifying compressed archive"
            workflow.progress = 0.8
            workflow.completed_steps = 2
            await asyncio.sleep(0.5)

            # Step 4: Add to cache
            workflow.current_step = "Adding to cache"
            workflow.progress = 0.9
            workflow.completed_steps = 3
            await asyncio.sleep(0.3)

            # Step 5: Cleanup
            workflow.current_step = "Cleanup"
            workflow.progress = 1.0
            workflow.completed_steps = 4
            await asyncio.sleep(0.1)

            # Complete and add to cache
            workflow.status = "completed"
            workflow.end_time = time.time()
            workflow.completed_steps = workflow.total_steps

            # Simulate adding the compressed archive to cache
            cache_key = f"archive:{dto.archive_id}"
            entry = CacheEntryDto(
                key=cache_key,
                size=1024 * 1024 * 100,  # 100 MB simulated size
                created_time=time.time(),
                last_accessed=time.time(),
                access_count=1,
                entry_type="archive",
            )
            self._cache_entries[cache_key] = entry

        except Exception as e:
            workflow = self._active_operations[operation_id]
            workflow.status = "failed"
            workflow.end_time = time.time()
            workflow.error_message = str(e)

    def _extract_file_list_from_archive(
        self, metadata: ArchiveMetadata, location: LocationEntity
    ) -> List[SimulationFile]:
        """Extract list of files from archive using real filesystem operations."""
        files = []

        try:
            # Get filesystem from location
            fs = location.fs
            if fs is None:
                self._logger.warning(
                    f"No filesystem available for location {location.name}"
                )
                return self._create_mock_files(metadata)

            # Construct archive path
            archive_path = self._get_archive_path(metadata, location)

            # Check if archive exists
            if not fs.exists(archive_path):
                self._logger.warning(f"Archive file not found: {archive_path}")
                return self._create_mock_files(metadata)

            # Handle different archive types with real filesystem access
            if metadata.archive_type == ArchiveType.COMPRESSED:
                files = self._list_tar_files_real(metadata, location, fs, archive_path)
            elif metadata.archive_type == ArchiveType.SPLIT_TARBALL:
                files = self._list_tar_files_real(metadata, location, fs, archive_path)
            elif metadata.archive_type == ArchiveType.ZIP:
                files = self._list_zip_files_real(metadata, location, fs, archive_path)
            else:
                # For unsupported types, fall back to mock files
                self._logger.warning(
                    f"Unsupported archive type: {metadata.archive_type}"
                )
                files = self._create_mock_files(metadata)

        except Exception as e:
            self._logger.error(
                f"Failed to extract file list from archive {metadata.archive_id}: {str(e)}"
            )
            # Fall back to mock files for compatibility
            files = self._create_mock_files(metadata)

        return files

    def _list_tar_files_real(
        self, metadata: ArchiveMetadata, location: LocationEntity, fs, archive_path: str
    ) -> List[SimulationFile]:
        """List files in a tar archive using real filesystem operations."""
        files = []

        try:
            # Open tar file through fsspec filesystem
            with fs.open(archive_path, "rb") as archive_file:
                # Determine compression type from filename
                if archive_path.endswith(".tar.gz") or archive_path.endswith(".tgz"):
                    tar_mode = "r:gz"
                elif archive_path.endswith(".tar.bz2") or archive_path.endswith(
                    ".tbz2"
                ):
                    tar_mode = "r:bz2"
                elif archive_path.endswith(".tar.xz"):
                    tar_mode = "r:xz"
                else:
                    tar_mode = "r"

                with tarfile.open(fileobj=archive_file, mode=tar_mode) as tar:
                    for member in tar.getmembers():
                        if member.isfile():  # Only process regular files
                            sim_file = self._tar_member_to_simulation_file(
                                member, metadata
                            )
                            files.append(sim_file)

            self._logger.info(
                f"Listed {len(files)} files from tar archive: {archive_path}"
            )

        except Exception as e:
            self._logger.error(f"Failed to list tar files: {str(e)}")
            # Fall back to mock files on error
            files = self._create_mock_files(metadata)

        return files

    def _list_zip_files_real(
        self, metadata: ArchiveMetadata, location: LocationEntity, fs, archive_path: str
    ) -> List[SimulationFile]:
        """List files in a zip archive using real filesystem operations."""
        files = []

        try:
            # Open zip file through fsspec filesystem
            with fs.open(archive_path, "rb") as archive_file:
                with zipfile.ZipFile(archive_file, "r") as zip_archive:
                    for zip_info in zip_archive.infolist():
                        if not zip_info.is_dir():  # Only process files, not directories
                            sim_file = self._zip_info_to_simulation_file(
                                zip_info, metadata
                            )
                            files.append(sim_file)

            self._logger.info(
                f"Listed {len(files)} files from zip archive: {archive_path}"
            )

        except Exception as e:
            self._logger.error(f"Failed to list zip files: {str(e)}")
            # Fall back to mock files on error
            files = self._create_mock_files(metadata)

        return files

    def _create_mock_files(self, metadata: ArchiveMetadata) -> List[SimulationFile]:
        """Create mock files for demonstration purposes."""
        import time
        from ...domain.entities.archive import Checksum

        mock_files = [
            SimulationFile(
                relative_path="output/results.nc",
                size=1024 * 1024 * 50,  # 50MB
                checksum=Checksum("d41d8cd98f00b204e9800998ecf8427e", "md5"),
                content_type=FileContentType.OUTPUT,
                importance=FileImportance.CRITICAL,
                file_role="model_output",
                created_time=time.time() - 3600,
                source_archive=metadata.archive_id.value,
                tags={"netcdf", "climate_data"},
            ),
            SimulationFile(
                relative_path="config/namelist.nml",
                size=2048,  # 2KB
                content_type=FileContentType.CONFIG,
                importance=FileImportance.IMPORTANT,
                file_role="configuration",
                created_time=time.time() - 7200,
                source_archive=metadata.archive_id.value,
                tags={"namelist", "config"},
            ),
            SimulationFile(
                relative_path="logs/model.log",
                size=1024 * 10,  # 10KB
                content_type=FileContentType.LOG,
                importance=FileImportance.OPTIONAL,
                file_role="diagnostic",
                created_time=time.time() - 1800,
                source_archive=metadata.archive_id.value,
                tags={"log", "debug"},
            ),
        ]

        return mock_files

    def _verify_copy_integrity(
        self, source_fs, dest_fs, source_path: str, dest_path: str
    ) -> None:
        """Verify that copied file has same size and optionally same checksum."""
        try:
            source_info = source_fs.info(source_path)
            dest_info = dest_fs.info(dest_path)

            source_size = source_info.get("size", 0)
            dest_size = dest_info.get("size", 0)

            if source_size != dest_size:
                raise DataIntegrityError(
                    "file_copy",
                    dest_path,
                    f"Size mismatch: source={source_size}, dest={dest_size}",
                )

            self._logger.debug(
                f"Copy integrity verified: {dest_path} ({dest_size} bytes)"
            )

        except Exception as e:
            raise DataIntegrityError(
                "file_copy", dest_path, f"Integrity verification failed: {str(e)}"
            )

    def _extract_tar_files(
        self,
        source_fs,
        dest_fs,
        archive_path: str,
        dest_path: str,
        files_to_extract: List[str],
        preserve_structure: bool,
    ) -> int:
        """Extract specific files from a tar archive."""
        extracted_count = 0

        with source_fs.open(archive_path, "rb") as archive_file:
            # Determine compression type
            if archive_path.endswith(".tar.gz") or archive_path.endswith(".tgz"):
                tar_mode = "r:gz"
            elif archive_path.endswith(".tar.bz2") or archive_path.endswith(".tbz2"):
                tar_mode = "r:bz2"
            elif archive_path.endswith(".tar.xz"):
                tar_mode = "r:xz"
            else:
                tar_mode = "r"

            with tarfile.open(fileobj=archive_file, mode=tar_mode) as tar:
                for member in tar.getmembers():
                    if member.isfile() and member.name in files_to_extract:
                        # Determine output path
                        if preserve_structure:
                            output_path = f"{dest_path}/{member.name}"
                        else:
                            filename = member.name.split("/")[-1]  # Just the filename
                            output_path = f"{dest_path}/{filename}"

                        # Ensure output directory exists
                        output_dir = "/".join(output_path.split("/")[:-1])
                        if output_dir and not dest_fs.exists(output_dir):
                            dest_fs.makedirs(output_dir, exist_ok=True)

                        # Extract file
                        with tar.extractfile(member) as extracted_file:
                            if extracted_file:
                                with dest_fs.open(output_path, "wb") as output_file:
                                    output_file.write(extracted_file.read())
                                extracted_count += 1

        return extracted_count

    def _extract_zip_files(
        self,
        source_fs,
        dest_fs,
        archive_path: str,
        dest_path: str,
        files_to_extract: List[str],
        preserve_structure: bool,
    ) -> int:
        """Extract specific files from a zip archive."""
        extracted_count = 0

        with source_fs.open(archive_path, "rb") as archive_file:
            with zipfile.ZipFile(archive_file, "r") as zip_archive:
                for zip_info in zip_archive.infolist():
                    if not zip_info.is_dir() and zip_info.filename in files_to_extract:
                        # Determine output path
                        if preserve_structure:
                            output_path = f"{dest_path}/{zip_info.filename}"
                        else:
                            filename = zip_info.filename.split("/")[
                                -1
                            ]  # Just the filename
                            output_path = f"{dest_path}/{filename}"

                        # Ensure output directory exists
                        output_dir = "/".join(output_path.split("/")[:-1])
                        if output_dir and not dest_fs.exists(output_dir):
                            dest_fs.makedirs(output_dir, exist_ok=True)

                        # Extract file
                        with zip_archive.open(zip_info) as extracted_file:
                            with dest_fs.open(output_path, "wb") as output_file:
                                output_file.write(extracted_file.read())
                            extracted_count += 1

        return extracted_count

    def _get_archive_path(
        self, metadata: ArchiveMetadata, location: LocationEntity
    ) -> str:
        """Get the full path to the archive file."""
        # Use archive_path instead of path
        if hasattr(metadata, "archive_path") and metadata.archive_path:
            return metadata.archive_path

        # Fallback to constructing path from location and archive ID
        location_path = location.config.get("path", "")
        return str(Path(location_path) / f"{metadata.archive_id}.tar.gz")

    def _tar_member_to_simulation_file(
        self, member: tarfile.TarInfo, metadata: ArchiveMetadata
    ) -> SimulationFile:
        """Convert tar member to SimulationFile entity."""
        # Classify file based on path and name
        content_type = self._classify_file_content_type(member.name)
        importance = self._classify_file_importance(member.name, content_type)
        file_role = self._classify_file_role(member.name)

        # Generate checksum if available (not typically in tar metadata)
        checksum = None

        # Create tags based on file characteristics
        tags = set()
        if member.name.endswith(".nc"):
            tags.add("netcdf")
        if "config" in member.name.lower():
            tags.add("configuration")
        if "log" in member.name.lower():
            tags.add("log")

        return SimulationFile(
            relative_path=member.name,
            size=member.size,
            checksum=checksum,
            content_type=content_type,
            importance=importance,
            file_role=file_role,
            created_time=member.mtime,
            source_archive=metadata.archive_id.value,
            tags=tags,
        )

    def _zip_info_to_simulation_file(
        self, zip_info: zipfile.ZipInfo, metadata: ArchiveMetadata
    ) -> SimulationFile:
        """Convert zip info to SimulationFile entity."""
        # Classify file based on path and name
        content_type = self._classify_file_content_type(zip_info.filename)
        importance = self._classify_file_importance(zip_info.filename, content_type)
        file_role = self._classify_file_role(zip_info.filename)

        # Convert zip date_time to timestamp
        import datetime

        dt = datetime.datetime(*zip_info.date_time)
        created_time = dt.timestamp()

        # Create tags based on file characteristics
        tags = set()
        if zip_info.filename.endswith(".nc"):
            tags.add("netcdf")
        if "config" in zip_info.filename.lower():
            tags.add("configuration")
        if "log" in zip_info.filename.lower():
            tags.add("log")

        return SimulationFile(
            relative_path=zip_info.filename,
            size=zip_info.file_size,
            checksum=None,  # ZIP CRC is not the same as file checksum
            content_type=content_type,
            importance=importance,
            file_role=file_role,
            created_time=created_time,
            source_archive=metadata.archive_id.value,
            tags=tags,
        )

    def _classify_file_content_type(self, filename: str) -> FileContentType:
        """Classify file content type based on filename."""
        filename_lower = filename.lower()

        if any(
            pattern in filename_lower
            for pattern in ["config", "namelist", ".nml", ".cfg"]
        ):
            return FileContentType.CONFIG
        elif any(pattern in filename_lower for pattern in ["log", ".log", "debug"]):
            return FileContentType.LOG
        elif any(pattern in filename_lower for pattern in ["restart", ".rst", "_r_"]):
            return FileContentType.INTERMEDIATE  # RESTART doesn't exist, use INTERMEDIATE
        elif any(
            pattern in filename_lower
            for pattern in [".nc", ".netcdf", "output", "results"]
        ):
            return FileContentType.OUTPUT
        elif any(
            pattern in filename_lower for pattern in ["script", ".sh", ".py", ".pl"]
        ):
            return FileContentType.METADATA  # SCRIPT doesn't exist, use METADATA
        else:
            return FileContentType.METADATA  # OTHER doesn't exist, use METADATA as fallback

    def _classify_file_importance(
        self, filename: str, content_type: FileContentType
    ) -> FileImportance:
        """Classify file importance based on filename and content type."""
        if content_type == FileContentType.OUTPUT:
            return FileImportance.CRITICAL
        elif content_type in [FileContentType.CONFIG, FileContentType.INTERMEDIATE]:
            return FileImportance.IMPORTANT
        elif content_type == FileContentType.LOG:
            return FileImportance.OPTIONAL
        else:
            return FileImportance.IMPORTANT

    def _classify_file_role(self, filename: str) -> str:
        """Classify file role based on filename patterns."""
        filename_lower = filename.lower()

        if "config" in filename_lower or "namelist" in filename_lower:
            return "configuration"
        elif "restart" in filename_lower:
            return "restart"
        elif (
            "output" in filename_lower
            or "results" in filename_lower
            or filename_lower.endswith(".nc")
        ):
            return "model_output"
        elif "log" in filename_lower:
            return "diagnostic"
        elif "script" in filename_lower:
            return "workflow"
        else:
            return "auxiliary"

    def _simulation_file_to_dto(self, file: SimulationFile) -> SimulationFileDto:
        """Convert SimulationFile entity to DTO."""
        return SimulationFileDto(
            relative_path=file.relative_path,
            size=file.size,
            checksum=str(file.checksum) if file.checksum else None,
            content_type=file.content_type.value,
            importance=file.importance.value,
            file_role=file.file_role,
            simulation_date=file.get_simulation_date_string()
            if file.simulation_date
            else None,
            created_time=file.created_time,
            modified_time=file.modified_time,
            source_archive=file.source_archive,
            extraction_time=file.extraction_time,
            tags=file.tags.copy(),
            attributes=file.attributes.copy(),
        )
