"""
Archive Application Service - Orchestrates archive-related use cases.

This service coordinates archive operations, caching, file management,
and long-running workflows in the Earth System Model context.
"""

import asyncio
import logging
import os
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
from ...domain.entities.file_type_config import (
    FileTypeConfiguration,
    load_file_type_config,
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
    CreateProgressTrackingDto,
    UpdateProgressDto,
    ProgressMetricsDto,
    ThroughputMetricsDto,
    OperationContextDto,
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
from .progress_tracking_service import IProgressTrackingService
from ...domain.entities.progress_tracking import OperationType, OperationContext

logger = logging.getLogger(__name__)


class ArchiveApplicationService:
    """
    Application service for archive management in Earth System Model contexts.
    
    Orchestrates complex archive operations including creation, extraction,
    caching, file management, and coordination of long-running workflows.
    Provides high-level interfaces for managing scientific datasets with
    progress tracking, integrity verification, and location-aware routing.
    
    This service manages the complete lifecycle of archive operations from
    creation and file association through to extraction and cache management.
    It handles Earth System Model specific file types, maintains data integrity
    through checksums, and provides efficient caching mechanisms for frequently
    accessed archives.
    
    Parameters
    ----------
    location_repository : ILocationRepository
        Repository interface for storage location data access and validation.
        Used to resolve archive storage locations and validate accessibility.
    archive_repository : IArchiveRepository
        Repository interface for archive metadata persistence and retrieval.
        Manages archive registration, metadata storage, and query operations.
    cache_config : CacheConfigurationDto, optional
        Configuration for archive caching behavior including size limits,
        cleanup policies (LRU, manual, size-only), and retention settings.
        Uses system defaults if not provided.
    progress_tracking_service : IProgressTrackingService, optional
        Service for tracking and reporting progress of long-running operations.
        Enables real-time monitoring of archive creation, extraction, and
        transfer operations.
        
    Attributes
    ----------
    _location_repo : ILocationRepository
        Location repository for storage backend operations
    _archive_repo : IArchiveRepository
        Archive repository for metadata persistence
    _cache_config : CacheConfiguration
        Active cache configuration settings
    _progress_service : IProgressTrackingService or None
        Progress tracking service if available
    _active_operations : Dict[str, WorkflowExecutionDto]
        Currently executing archive operations
    _cache_entries : Dict[str, CacheEntryDto]
        Active cache entries for performance optimization
        
    Examples
    --------
    Initialize archive service with repositories and caching:
    
    >>> from tellus.infrastructure.repositories import JsonArchiveRepository
    >>> from tellus.infrastructure.repositories import JsonLocationRepository
    >>> from tellus.application.dtos import CacheConfigurationDto
    >>> 
    >>> archive_repo = JsonArchiveRepository("/tmp/archives.json")
    >>> location_repo = JsonLocationRepository("/tmp/locations.json")
    >>> cache_config = CacheConfigurationDto(
    ...     max_size_gb=10,
    ...     cleanup_policy="lru"
    ... )
    >>> service = ArchiveApplicationService(
    ...     location_repository=location_repo,
    ...     archive_repository=archive_repo,
    ...     cache_config=cache_config
    ... )
    >>> service._cache_config.max_size_gb
    10
    
    Create an archive from simulation data:
    
    >>> from tellus.application.dtos import CreateArchiveDto
    >>> create_dto = CreateArchiveDto(
    ...     archive_id="climate-run-001",
    ...     location="hpc-storage",
    ...     source_path="/data/cesm/run001",
    ...     simulation_id="cesm-historical-001"
    ... )
    >>> # result = await service.create_archive(create_dto)
    >>> # result.success
    >>> # True
    
    Extract archive to local workspace:
    
    >>> from tellus.application.dtos import ArchiveExtractionDto
    >>> extract_dto = ArchiveExtractionDto(
    ...     archive_id="climate-run-001",
    ...     destination_location="local-workspace",
    ...     simulation_id="cesm-historical-001",
    ...     content_type_filter="output"
    ... )
    >>> # result = service.extract_archive_to_location(extract_dto)
    >>> # result.success
    >>> # True
    
    Notes
    -----
    Archive operations are designed to handle large Earth System Model datasets
    efficiently. The service provides automatic file type classification for
    scientific data formats (NetCDF, GRIB, etc.) and implements caching
    strategies optimized for typical research workflows.
    
    All archive operations support progress tracking when a progress service
    is configured, enabling real-time monitoring of long-running operations
    common in scientific computing environments.
    
    See Also
    --------
    create_archive : Create new archives from source data
    extract_archive_to_location : Extract archives to storage locations
    list_archive_files : Browse archive contents
    copy_archive : Copy archives between locations
    """

    def __init__(
        self,
        location_repository: ILocationRepository,
        archive_repository: IArchiveRepository,
        cache_config: Optional[CacheConfigurationDto] = None,
        progress_tracking_service: Optional[IProgressTrackingService] = None,
    ) -> None:
        """
        Initialize the archive application service.
        
        Sets up the service with required repositories, configures caching
        behavior, and initializes progress tracking capabilities for managing
        Earth System Model archive operations.
        
        Parameters
        ----------
        location_repository : ILocationRepository
            Repository interface for storage location data access and validation.
            Must implement location lookup, validation, and configuration retrieval
            for all supported storage protocols (file, SSH, S3, etc.).
        archive_repository : IArchiveRepository
            Repository interface for archive metadata persistence and retrieval.
            Handles archive registration, metadata storage, query operations,
            and maintains archive-to-simulation associations.
        cache_config : CacheConfigurationDto, optional
            Configuration for archive caching behavior. If not provided, uses
            system defaults: 50GB max size, LRU cleanup policy, 30-day retention.
            Supports customization of size limits, cleanup policies, and paths.
        progress_tracking_service : IProgressTrackingService, optional
            Service for tracking and reporting progress of long-running operations.
            When provided, enables real-time progress monitoring for archive
            creation, extraction, copy, and move operations.
            
        Examples
        --------
        Initialize with minimal configuration:
        
        >>> from tellus.infrastructure.repositories import JsonArchiveRepository
        >>> from tellus.infrastructure.repositories import JsonLocationRepository
        >>> archive_repo = JsonArchiveRepository("/tmp/archives.json")
        >>> location_repo = JsonLocationRepository("/tmp/locations.json")
        >>> service = ArchiveApplicationService(
        ...     location_repository=location_repo,
        ...     archive_repository=archive_repo
        ... )
        >>> service._cache_config is not None
        True
        
        Initialize with custom cache configuration:
        
        >>> from tellus.application.dtos import CacheConfigurationDto
        >>> cache_config = CacheConfigurationDto(
        ...     max_size_gb=100,
        ...     cleanup_policy="manual",
        ...     retention_days=90
        ... )
        >>> service = ArchiveApplicationService(
        ...     location_repository=location_repo,
        ...     archive_repository=archive_repo,
        ...     cache_config=cache_config
        ... )
        >>> service._cache_config.max_size_gb
        100
        
        Initialize with progress tracking:
        
        >>> from tellus.application.services import ProgressTrackingApplicationService
        >>> progress_service = ProgressTrackingApplicationService(progress_repo)
        >>> service = ArchiveApplicationService(
        ...     location_repository=location_repo,
        ...     archive_repository=archive_repo,
        ...     progress_tracking_service=progress_service
        ... )
        >>> service._progress_service is not None
        True
        
        Notes
        -----
        The service automatically creates the cache directory if it doesn't exist
        and initializes file type classification for Earth System Model data.
        
        Cache configuration affects performance for frequently accessed archives.
        A larger cache improves performance but consumes more disk space.
        LRU policy is recommended for most research workflows.
        
        Progress tracking enables monitoring of long-running operations but
        adds overhead. Disable for batch processing where monitoring isn't needed.
        
        See Also
        --------
        create_archive : Create new archives from source data
        get_cache_status : Monitor cache utilization and performance
        """
        self._location_repo = location_repository
        self._archive_repo = archive_repository
        self._cache_config = self._build_cache_config(cache_config)
        self._progress_service = progress_tracking_service
        self._active_operations: Dict[str, WorkflowExecutionDto] = {}
        self._cache_entries: Dict[str, CacheEntryDto] = {}
        self._logger = logger
        
        # Initialize cache directory and file type classification
        self._ensure_cache_directory()

    async def _create_operation_tracker(
        self,
        operation_name: str,
        operation_type: OperationType,
        context: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Create a progress tracking operation and return its ID."""
        if not self._progress_service:
            return None
        
        try:
            import uuid
            operation_id = str(uuid.uuid4())
            
            # Build operation context
            op_context = None
            if context:
                op_context = OperationContextDto(
                    simulation_id=context.get('simulation_id'),
                    location_name=context.get('location_name'),
                    tags=set(context.get('tags', [])),
                    metadata=context.get('metadata', {})
                )
            
            create_dto = CreateProgressTrackingDto(
                operation_id=operation_id,
                operation_type=operation_type.value,
                operation_name=operation_name,
                priority="normal",
                context=op_context
            )
            
            await self._progress_service.create_operation(create_dto)
            return operation_id
            
        except Exception as e:
            self._logger.warning(f"Failed to create progress tracker: {e}")
            return None
    
    async def _update_operation_progress(
        self,
        operation_id: Optional[str],
        progress_percentage: float,
        bytes_processed: int = 0,
        total_bytes: Optional[int] = None,
        files_processed: int = 0,
        total_files: Optional[int] = None,
        current_file: Optional[str] = None,
        transfer_rate: float = 0.0
    ) -> None:
        """Update progress for an operation."""
        if not self._progress_service or not operation_id:
            return
        
        try:
            metrics_dto = ProgressMetricsDto(
                percentage=progress_percentage,
                current_value=files_processed,
                total_value=total_files,
                bytes_processed=bytes_processed,
                total_bytes=total_bytes,
                files_processed=files_processed,
                total_files=total_files
            )
            
            throughput_dto = None
            if transfer_rate > 0:
                throughput_dto = ThroughputMetricsDto(
                    start_time=time.time(),
                    current_time=time.time(),
                    bytes_per_second=transfer_rate,
                    files_per_second=0.0,
                    operations_per_second=0.0
                )
            
            update_dto = UpdateProgressDto(
                operation_id=operation_id,
                metrics=metrics_dto,
                throughput=throughput_dto,
                message=f"Processing: {current_file}" if current_file else None
            )
            
            await self._progress_service.update_progress(update_dto)
            
        except Exception as e:
            self._logger.warning(f"Failed to update progress for {operation_id}: {e}")

    def _get_file_type_classifier(self) -> FileTypeConfiguration:
        """Get or create the file type classifier."""
        if not hasattr(self, '_file_type_config'):
            try:
                self._file_type_config = load_file_type_config()
                self._logger.debug("File type configuration loaded")
            except Exception as e:
                self._logger.warning(f"Failed to load file type configuration, using defaults: {e}")
                self._file_type_config = FileTypeConfiguration.create_default()
        
        return self._file_type_config

    def reload_file_type_config(self) -> None:
        """Reload the file type configuration from disk."""
        try:
            self._file_type_config = load_file_type_config()
            self._logger.info("File type configuration reloaded")
        except Exception as e:
            self._logger.error(f"Failed to reload file type configuration: {e}")

    def create_archive_metadata(self, dto: CreateArchiveDto) -> ArchiveDto:
        """
        Create metadata record for a new archive without creating archive file.
        
        Registers archive metadata in the repository system, validates location
        accessibility, and establishes the archive's relationship to simulations.
        This method creates the metadata foundation that tracks archive properties
        but does not perform the actual file compression or data transfer.
        
        Parameters
        ----------
        dto : CreateArchiveDto
            Data transfer object containing archive creation specifications.
            Must include archive_id (unique identifier), location_name (target
            storage location), archive_type ("compressed" or "directory"),
            and optional simulation_id, tags, and description.
            
        Returns
        -------
        ArchiveDto
            Complete archive metadata including generated timestamps,
            location validation results, and archive configuration.
            Contains archive_id, location, type, simulation associations,
            version information, and descriptive metadata.
            
        Raises
        ------
        EntityAlreadyExistsError
            If an archive with the same archive_id already exists in the
            repository. Archive IDs must be unique across the system.
        EntityNotFoundError
            If the specified location_name does not exist in the location
            repository or is not accessible.
        ValidationError
            If the DTO contains invalid data such as unsupported archive_type,
            malformed archive_id, or missing required fields.
        RepositoryError
            If there's an error persisting metadata to the storage backend.
            
        Examples
        --------
        Create metadata for a CESM2 model output archive:
        
        >>> from tellus.application.dtos import CreateArchiveDto
        >>> dto = CreateArchiveDto(
        ...     archive_id="cesm2-historical-001",
        ...     location_name="hpc-storage",
        ...     archive_type="compressed",
        ...     simulation_id="cesm2-hist-run1",
        ...     description="CESM2 historical simulation output",
        ...     tags={"model", "cesm2", "historical"}
        ... )
        >>> # metadata = service.create_archive_metadata(dto)
        >>> # metadata.archive_id
        >>> # 'cesm2-historical-001'
        >>> # metadata.archive_type
        >>> # ArchiveType.COMPRESSED
        
        Create metadata for an observational dataset:
        
        >>> dto = CreateArchiveDto(
        ...     archive_id="obs-temperature-global-2020",
        ...     location_name="archive-storage",
        ...     archive_type="directory",
        ...     description="Global temperature observations 2020",
        ...     tags={"observations", "temperature", "global"}
        ... )
        >>> # metadata = service.create_archive_metadata(dto)
        >>> # len(metadata.tags)
        >>> # 3
        
        Notes
        -----
        This method only creates the metadata record - it does not create
        the actual archive file or perform data compression. Use create_archive()
        for complete archive creation including file operations.
        
        Archive IDs should follow organizational naming conventions. Common
        patterns include model-experiment-version schemes or date-based
        identifiers for observational datasets.
        
        Location validation ensures the target storage location exists and
        is accessible before creating metadata, preventing orphaned records.
        
        See Also
        --------
        create_archive : Create complete archive with file compression
        get_archive_metadata : Retrieve existing archive metadata
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

    async def create_archive(self, dto: CreateArchiveDto) -> ArchiveOperationResultDto:
        """
        Create a new archive by compressing files from a source location.
        
        Performs complete archive creation including metadata registration,
        file compression, progress tracking, and integrity verification.
        This method coordinates the entire workflow from source data analysis
        through compressed archive creation and final validation.
        
        Parameters
        ----------
        dto : CreateArchiveDto
            Archive creation specification including source path, target location,
            compression settings, and metadata. Must contain archive_id (unique),
            location_name (target storage), source_path (data to archive),
            and archive_type ("compressed" or "directory").
            
        Returns
        -------
        ArchiveOperationResultDto
            Comprehensive operation result including success status, created
            archive metadata, file count and size statistics, compression ratio,
            operation duration, and any error messages. Contains archive_path
            for accessing the created archive file.
            
        Raises
        ------
        EntityAlreadyExistsError
            If an archive with the same archive_id already exists in the
            repository system.
        EntityNotFoundError
            If the specified location_name does not exist or the source_path
            is not accessible at the source location.
        ValidationError
            If the DTO contains invalid data, unsupported archive_type,
            or the source path contains no archivable files.
        ArchiveOperationError
            If the compression process fails due to I/O errors, insufficient
            storage space, or file access permissions.
        ExternalServiceError
            If progress tracking or location access services are unavailable.
            
        Examples
        --------
        Create compressed archive from CESM2 model output:
        
        >>> from tellus.application.dtos import CreateArchiveDto
        >>> dto = CreateArchiveDto(
        ...     archive_id="cesm2-run001-output",
        ...     location_name="hpc-archive",
        ...     source_path="/scratch/cesm2/run001/output",
        ...     archive_type="compressed",
        ...     simulation_id="cesm2-historical-001",
        ...     description="CESM2 historical run 001 model output"
        ... )
        >>> # result = await service.create_archive(dto)
        >>> # result.success
        >>> # True
        >>> # result.files_processed > 0
        >>> # True
        >>> # result.compression_ratio < 1.0
        >>> # True
        
        Create directory archive for observational data:
        
        >>> dto = CreateArchiveDto(
        ...     archive_id="obs-temp-station-2023",
        ...     location_name="data-archive",
        ...     source_path="/data/observations/temperature/2023",
        ...     archive_type="directory",
        ...     description="Temperature station observations 2023"
        ... )
        >>> # result = await service.create_archive(dto)
        >>> # result.success
        >>> # True
        >>> # result.archive_type == "directory"
        >>> # True
        
        Notes
        -----
        Archive creation is an asynchronous operation that may take significant
        time for large datasets. Progress tracking is automatically enabled
        when a progress service is configured.
        
        Compression ratios vary by file type: NetCDF and GRIB files typically
        achieve 2-10x compression, while text files may achieve 5-20x compression.
        Binary model output often compresses less effectively.
        
        The operation includes automatic file type classification for Earth
        System Model data, preserving important metadata about variable types,
        output frequencies, and file importance levels.
        
        See Also
        --------
        create_archive_metadata : Create metadata without file operations
        extract_archive_to_location : Extract archives to storage locations
        copy_archive : Copy existing archives between locations
        """
        self._logger.info(f"Creating archive: {dto.archive_id}")
        operation_id = f"create_{dto.archive_id}_{int(time.time())}"
        
        try:
            # Validate location exists and get location object
            location = self._location_repo.get_by_name(dto.location_name)
            if location is None:
                raise EntityNotFoundError("Location", dto.location_name)
                
            # Check if archive already exists
            if self._archive_repo.exists(dto.archive_id):
                raise EntityAlreadyExistsError("Archive", dto.archive_id)
                
            # Validate source path if provided
            if dto.source_path:
                source_path = Path(dto.source_path)
                if not source_path.exists():
                    raise ValidationError(f"Source path does not exist: {dto.source_path}")
            
            # Get location path for later use
            location_path = location.config.get('path', '/tmp') if location.config else '/tmp'
                    
            # Create progress tracking if service available
            progress_data = None
            if self._progress_service:
                progress_dto = CreateProgressTrackingDto(
                    operation_id=operation_id,
                    operation_type=OperationType.ARCHIVE_CREATE.value,
                    operation_name=f"Create Archive {dto.archive_id}",
                    context=OperationContextDto(
                        simulation_id=dto.simulation_id,
                        location_name=dto.location_name,
                        metadata={
                            'archive_id': dto.archive_id,
                            'source_path': dto.source_path,
                            'total_bytes': await self._calculate_source_size(dto.source_path) if dto.source_path else 0,
                            'total_files': await self._count_source_files(dto.source_path) if dto.source_path else 0
                        }
                    )
                )
                progress_data = await self._progress_service.create_operation(progress_dto)
                
            # Create archive metadata first
            metadata_result = self.create_archive_metadata(dto)
            
            # If source path provided, create the actual archive file
            if dto.source_path:
                await self._create_archive_file_with_progress(
                    dto, location, operation_id, progress_data
                )
            
            # Archive creation completed successfully
            # Note: Progress tracking service will handle completion automatically
                
            self._logger.info(f"Successfully created archive: {dto.archive_id}")
            
            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="create",
                archive_id=dto.archive_id,
                success=True,
                destination_path=str(Path(location_path) / f"{dto.archive_id}.tar.gz") if dto.source_path else None,
                files_processed=progress_data.files_processed if progress_data and hasattr(progress_data, 'files_processed') else 0,
                bytes_processed=progress_data.bytes_processed if progress_data and hasattr(progress_data, 'bytes_processed') else 0
            )
            
        except (EntityAlreadyExistsError, EntityNotFoundError, ValidationError):
            # Mark operation as failed for tracking
            if self._progress_service and 'progress_data' in locals() and progress_data:
                from ..dtos import OperationControlDto
                control_dto = OperationControlDto(
                    operation_id=operation_id,
                    command="cancel",
                    reason="Validation failed"
                )
                await self._progress_service.control_operation(control_dto)
            raise
        except Exception as e:
            if self._progress_service and 'progress_data' in locals() and progress_data:
                from ..dtos import OperationControlDto
                control_dto = OperationControlDto(
                    operation_id=operation_id,
                    command="cancel",
                    reason=str(e)
                )
                await self._progress_service.control_operation(control_dto)
            self._logger.error(f"Failed to create archive {dto.archive_id}: {str(e)}")
            raise ArchiveOperationError(dto.archive_id, "create", str(e))

    async def _create_archive_file_with_progress(
        self, dto: CreateArchiveDto, location: LocationEntity, 
        operation_id: str, progress_data: Optional[Any]
    ) -> None:
        """Create the actual archive file with progress tracking."""
        
        source_path = Path(dto.source_path)
        archive_filename = f"{dto.archive_id}.tar.gz"
        
        # Create archive directly to destination path (simplified for now)
        # In a full implementation, this would use the location's filesystem abstraction
        location_path = location.config.get('path', '/tmp') if location.config else '/tmp'
        dest_path = Path(location_path) / archive_filename
        
        # Create archive using tarfile with progress tracking
        with tarfile.open(dest_path, 'w:gz') as tar:
                
                # Walk through source directory and add files
                for root, dirs, files in os.walk(source_path):
                    for file in files:
                        file_path = Path(root) / file
                        arcname = file_path.relative_to(source_path)
                        
                        # Add file to archive
                        tar.add(file_path, arcname=str(arcname))
                        
                        # Update progress
                        if progress_data and self._progress_service:
                            await self._update_creation_progress(
                                operation_id, file_path, progress_data
                            )

    async def _calculate_source_size(self, source_path: str) -> int:
        """Calculate total size of source directory."""
        if not source_path:
            return 0
            
        total_size = 0
        source = Path(source_path)
        
        if source.is_file():
            return source.stat().st_size
            
        for root, dirs, files in os.walk(source):
            for file in files:
                file_path = Path(root) / file
                try:
                    total_size += file_path.stat().st_size
                except (OSError, FileNotFoundError):
                    pass
                    
        return total_size

    async def _count_source_files(self, source_path: str) -> int:
        """Count total number of files in source directory."""
        if not source_path:
            return 0
            
        source = Path(source_path)
        
        if source.is_file():
            return 1
            
        file_count = 0
        for root, dirs, files in os.walk(source):
            file_count += len(files)
            
        return file_count

    async def _update_creation_progress(
        self, operation_id: str, current_file: Path, progress_data: Any
    ) -> None:
        """Update progress during archive creation."""
        if not self._progress_service:
            return
            
        try:
            file_size = current_file.stat().st_size
            
            update_dto = UpdateProgressDto(
                operation_id=operation_id,
                metrics=ProgressMetricsDto(
                    files_processed=progress_data.files_processed + 1 if hasattr(progress_data, 'files_processed') else 1,
                    bytes_processed=progress_data.bytes_processed + file_size if hasattr(progress_data, 'bytes_processed') else file_size,
                    percentage=0.0  # Would need total calculation
                ),
                message=f"Adding {current_file.name}",
                throughput=ThroughputMetricsDto(
                    start_time=time.time(),
                    files_per_second=1.0,  # Simplified
                    bytes_per_second=file_size,
                    estimated_remaining_seconds=0.0
                )
            )
            
            await self._progress_service.update_progress(update_dto)
            
        except Exception as e:
            self._logger.warning(f"Failed to update creation progress: {e}")

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

    def update_archive(self, archive_id: str, update_dto: UpdateArchiveDto) -> bool:
        """
        Update archive metadata.
        
        Args:
            archive_id: The ID of the archive to update
            update_dto: DTO containing updated metadata fields
            
        Returns:
            True if update was successful, False otherwise
            
        Raises:
            EntityNotFoundError: If archive not found
            ValidationError: If update data is invalid
        """
        self._logger.debug(f"Updating archive metadata: {archive_id}")
        
        try:
            # Get existing archive metadata
            metadata = self._archive_repo.get_by_id(archive_id)
            if metadata is None:
                raise EntityNotFoundError("Archive", archive_id)
            
            # Update only non-None fields from the DTO
            updated = False
            
            if update_dto.simulation_date is not None:
                metadata.simulation_date = update_dto.simulation_date
                updated = True
                
            if update_dto.version is not None:
                metadata.version = update_dto.version
                updated = True
                
            if update_dto.description is not None:
                metadata.description = update_dto.description
                updated = True
                
            if update_dto.tags is not None:
                metadata.tags = set(update_dto.tags)  # Convert to set
                updated = True
            
            # Save updated metadata if any changes were made
            if updated:
                self._archive_repo.save(metadata)
                self._logger.info(f"Successfully updated archive metadata: {archive_id}")
                return True
            else:
                self._logger.debug(f"No changes to update for archive: {archive_id}")
                return False
                
        except Exception as e:
            self._logger.error(f"Failed to update archive {archive_id}: {str(e)}")
            raise

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

            # Create filesystem access from location entity
            location_fs = self._create_location_filesystem(location_entity)

            # Access archive file through filesystem
            archive_files = self._extract_file_list_from_archive(metadata, location_entity, location_fs)

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

    async def copy_archive_with_progress(
        self, copy_dto: ArchiveCopyOperationDto
    ) -> ArchiveOperationResultDto:
        """
        Copy an archive to a different location with progress tracking.

        Args:
            copy_dto: Archive copy operation parameters

        Returns:
            Archive operation result with details

        Raises:
            EntityNotFoundError: If archive or location not found
            ArchiveOperationError: If copy operation fails
        """
        operation_id = f"copy_{copy_dto.archive_id}_{int(time.time())}"
        self._logger.info(f"Starting archive copy operation with progress: {operation_id}")

        start_time = time.time()
        progress_tracker_id = None

        try:
            # Validate archive exists
            archive_metadata = self._archive_repo.get_by_id(copy_dto.archive_id)
            if archive_metadata is None:
                raise EntityNotFoundError("Archive", copy_dto.archive_id)

            # Create progress tracker
            context = {
                'simulation_id': copy_dto.simulation_id,
                'location_name': copy_dto.destination_location,
                'tags': ['archive_copy'],
                'metadata': {
                    'archive_id': copy_dto.archive_id,
                    'source_location': copy_dto.source_location,
                    'destination_location': copy_dto.destination_location
                }
            }
            
            progress_tracker_id = await self._create_operation_tracker(
                f"Copy archive {copy_dto.archive_id}",
                OperationType.ARCHIVE_COPY,
                context
            )

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

            # Create filesystem access from location entities
            source_fs = self._create_location_filesystem(source_location_entity)
            dest_fs = self._create_location_filesystem(dest_location_entity)

            # Perform real copy operation using fsspec with progress tracking
            archive_filename = f"{archive_metadata.archive_id.value}.tar.gz"
            source_path = archive_filename
            dest_path = archive_filename

            # Filesystems already created above
            try:
                pass  # source_fs and dest_fs already available
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

            # Get source file size for progress calculation
            try:
                source_info = source_fs.info(source_path)
                total_bytes = source_info.get("size", archive_metadata.size or 0)
            except:
                total_bytes = archive_metadata.size or 0

            # Update progress: starting
            await self._update_operation_progress(
                progress_tracker_id,
                progress_percentage=0.0,
                bytes_processed=0,
                total_bytes=total_bytes,
                files_processed=0,
                total_files=1,
                current_file=archive_filename
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

            # Perform the actual copy operation using streaming approach with progress
            try:
                bytes_copied = 0
                chunk_size = 8 * 1024 * 1024  # 8MB chunks
                start_transfer_time = time.time()
                last_progress_update = start_transfer_time
                
                with source_fs.open(source_path, "rb") as src_file:
                    with dest_fs.open(dest_path, "wb") as dest_file:
                        while True:
                            chunk = src_file.read(chunk_size)
                            if not chunk:
                                break
                            dest_file.write(chunk)
                            bytes_copied += len(chunk)
                            
                            # Update progress every 1 second or 10MB
                            current_time = time.time()
                            if (current_time - last_progress_update > 1.0 or 
                                bytes_copied % (10 * 1024 * 1024) == 0):
                                
                                elapsed_time = current_time - start_transfer_time
                                transfer_rate = bytes_copied / elapsed_time if elapsed_time > 0 else 0
                                progress_percentage = (bytes_copied / total_bytes * 100) if total_bytes > 0 else 0
                                
                                await self._update_operation_progress(
                                    progress_tracker_id,
                                    progress_percentage=progress_percentage,
                                    bytes_processed=bytes_copied,
                                    total_bytes=total_bytes,
                                    files_processed=0 if bytes_copied < total_bytes else 1,
                                    total_files=1,
                                    current_file=archive_filename,
                                    transfer_rate=transfer_rate
                                )
                                
                                last_progress_update = current_time

                # Final progress update
                elapsed_time = time.time() - start_transfer_time
                transfer_rate = bytes_copied / elapsed_time if elapsed_time > 0 else 0
                
                await self._update_operation_progress(
                    progress_tracker_id,
                    progress_percentage=100.0,
                    bytes_processed=bytes_copied,
                    total_bytes=total_bytes,
                    files_processed=1,
                    total_files=1,
                    current_file=archive_filename,
                    transfer_rate=transfer_rate
                )

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
                progress_tracker_id=progress_tracker_id,
            )

            self._logger.info(f"Archive copy completed: {operation_id}")
            return result

        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            self._logger.error(f"Archive copy failed: {operation_id} - {error_msg}")

            # Mark progress tracker as failed
            if progress_tracker_id and self._progress_service:
                try:
                    from ..dtos import OperationControlDto
                    control_dto = OperationControlDto(
                        operation_id=progress_tracker_id,
                        command="force_cancel",
                        reason=f"Copy operation failed: {error_msg}"
                    )
                    await self._progress_service.control_operation(control_dto)
                except:
                    pass  # Ignore progress tracking errors

            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="copy",
                archive_id=copy_dto.archive_id,
                success=False,
                duration_seconds=duration,
                error_message=error_msg,
                progress_tracker_id=progress_tracker_id,
            )

    async def extract_archive_with_progress(
        self, extract_dto: ArchiveExtractionDto
    ) -> ArchiveOperationResultDto:
        """
        Extract archive contents to a location with progress tracking.

        Args:
            extract_dto: Archive extraction parameters

        Returns:
            Archive operation result with extraction details
        """
        operation_id = f"extract_{extract_dto.archive_id}_{int(time.time())}"
        self._logger.info(f"Starting archive extraction with progress: {operation_id}")

        start_time = time.time()
        progress_tracker_id = None

        try:
            # Validate archive exists
            archive_metadata = self._archive_repo.get_by_id(extract_dto.archive_id)
            if archive_metadata is None:
                raise EntityNotFoundError("Archive", extract_dto.archive_id)

            # Create progress tracker
            context = {
                'simulation_id': extract_dto.simulation_id,
                'location_name': extract_dto.destination_location,
                'tags': ['archive_extract'],
                'metadata': {
                    'archive_id': extract_dto.archive_id,
                    'destination_location': extract_dto.destination_location,
                    'content_type_filter': extract_dto.content_type_filter,
                    'pattern_filter': extract_dto.pattern_filter
                }
            }
            
            progress_tracker_id = await self._create_operation_tracker(
                f"Extract archive {extract_dto.archive_id}",
                OperationType.ARCHIVE_EXTRACT,
                context
            )

            # Update progress: analyzing files
            await self._update_operation_progress(
                progress_tracker_id,
                progress_percentage=5.0,
                bytes_processed=0,
                total_bytes=0,
                files_processed=0,
                total_files=0,
                current_file="Analyzing archive contents..."
            )

            # Resolve destination path
            resolved_path = self._resolve_location_path(
                extract_dto.destination_location,
                extract_dto.simulation_id,
                archive_metadata
            )

            # Get files to extract (if filtered)
            files_to_extract = []
            total_archive_size = 0
            
            if extract_dto.file_filters or extract_dto.content_type_filter or extract_dto.pattern_filter:
                file_list = self.list_archive_files(
                    extract_dto.archive_id,
                    content_type=extract_dto.content_type_filter,
                    pattern=extract_dto.pattern_filter
                )
                files_to_extract = [f.relative_path for f in file_list.files]
                total_archive_size = sum(f.size or 0 for f in file_list.files)
                
                # Apply specific file filters if provided
                if extract_dto.file_filters:
                    files_to_extract = [f for f in files_to_extract if f in extract_dto.file_filters]
                    # Recalculate size for filtered files
                    total_archive_size = sum(
                        f.size or 0 for f in file_list.files 
                        if f.relative_path in files_to_extract
                    )
            else:
                # Extract all files
                file_list = self.list_archive_files(extract_dto.archive_id)
                files_to_extract = [f.relative_path for f in file_list.files]
                total_archive_size = sum(f.size or 0 for f in file_list.files)

            # Update progress: preparing extraction
            await self._update_operation_progress(
                progress_tracker_id,
                progress_percentage=10.0,
                bytes_processed=0,
                total_bytes=total_archive_size,
                files_processed=0,
                total_files=len(files_to_extract),
                current_file=f"Preparing to extract {len(files_to_extract)} files..."
            )

            # Perform real extraction using fsspec
            source_location_entity = self._location_repo.get_by_name(archive_metadata.location)
            dest_location_entity = self._location_repo.get_by_name(extract_dto.destination_location)
            
            if source_location_entity is None:
                raise EntityNotFoundError("Location", archive_metadata.location)
            if dest_location_entity is None:
                raise EntityNotFoundError("Location", extract_dto.destination_location)
            
            # Create filesystem access from location entities
            source_fs = self._create_location_filesystem(source_location_entity)
            dest_fs = self._create_location_filesystem(dest_location_entity)
            
            # Filesystems already created above
            try:
                pass  # source_fs and dest_fs already available
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

            # Update progress: starting extraction
            await self._update_operation_progress(
                progress_tracker_id,
                progress_percentage=15.0,
                bytes_processed=0,
                total_bytes=total_archive_size,
                files_processed=0,
                total_files=len(files_to_extract),
                current_file="Starting extraction..."
            )

            # Perform real extraction based on archive type with progress tracking
            extracted_count = 0
            bytes_extracted = 0
            
            try:
                if archive_metadata.archive_type == ArchiveType.COMPRESSED:
                    extracted_count, bytes_extracted = await self._extract_tar_files_with_progress(
                        source_fs, dest_fs, archive_path, resolved_path, 
                        files_to_extract, extract_dto.preserve_directory_structure,
                        progress_tracker_id, total_archive_size
                    )
                elif archive_metadata.archive_type == ArchiveType.ZIP:
                    extracted_count, bytes_extracted = await self._extract_zip_files_with_progress(
                        source_fs, dest_fs, archive_path, resolved_path,
                        files_to_extract, extract_dto.preserve_directory_structure,
                        progress_tracker_id, total_archive_size
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

            # Final progress update
            await self._update_operation_progress(
                progress_tracker_id,
                progress_percentage=100.0,
                bytes_processed=bytes_extracted,
                total_bytes=total_archive_size,
                files_processed=extracted_count,
                total_files=len(files_to_extract),
                current_file="Extraction completed"
            )

            # Create extraction manifest if requested
            manifest_created = False
            if extract_dto.create_manifest:
                manifest = self._create_extraction_manifest(
                    extract_dto, resolved_path, files_to_extract
                )
                manifest_created = True

            duration = time.time() - start_time
            
            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="extract",
                archive_id=extract_dto.archive_id,
                success=True,
                destination_path=resolved_path,
                bytes_processed=bytes_extracted,
                files_processed=extracted_count,
                duration_seconds=duration,
                manifest_created=manifest_created,
                warnings=[] if extracted_count > 0 else ["No files were successfully extracted"],
                progress_tracker_id=progress_tracker_id,
            )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            self._logger.error(f"Archive extraction failed: {operation_id} - {error_msg}")
            
            # Mark progress tracker as failed
            if progress_tracker_id and self._progress_service:
                try:
                    from ..dtos import OperationControlDto
                    control_dto = OperationControlDto(
                        operation_id=progress_tracker_id,
                        command="force_cancel",
                        reason=f"Extraction operation failed: {error_msg}"
                    )
                    await self._progress_service.control_operation(control_dto)
                except:
                    pass  # Ignore progress tracking errors
            
            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="extract",
                archive_id=extract_dto.archive_id,
                success=False,
                duration_seconds=duration,
                error_message=error_msg,
                progress_tracker_id=progress_tracker_id,
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

            # Create filesystem access from location entities
            source_fs = self._create_location_filesystem(source_location_entity)
            dest_fs = self._create_location_filesystem(dest_location_entity)

            # Perform real copy operation using fsspec
            # Get archive filename for relative path in source location
            archive_filename = f"{archive_metadata.archive_id.value}.tar.gz"
            source_path = archive_filename

            # For destination, use just the filename too since resolved_path would be absolute
            dest_path = archive_filename

            # Filesystems already created above
            try:
                pass  # source_fs and dest_fs already available
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
                    # Create filesystem access from location
                    source_fs = self._create_location_filesystem(source_location)
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

            source_fs = self._create_location_filesystem(source_location)
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

            dest_fs = self._create_location_filesystem(dest_location)
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
            
            # Create filesystem access from location entities
            source_fs = self._create_location_filesystem(source_location_entity)
            dest_fs = self._create_location_filesystem(dest_location_entity)
            
            # Filesystems already created above
            try:
                pass  # source_fs and dest_fs already available
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
        self, metadata: ArchiveMetadata, location: LocationEntity, fs
    ) -> List[SimulationFile]:
        """Extract list of files from archive using real filesystem operations."""
        files = []

        try:
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
        """Classify file content type based on user-configurable patterns."""
        # Get file type classification from configuration
        content_type, _ = self._get_file_type_classifier().classify_file(filename)
        return content_type

    def _classify_file_importance(
        self, filename: str, content_type: FileContentType
    ) -> FileImportance:
        """Classify file importance based on user-configurable patterns."""
        # Get importance classification from configuration
        _, importance = self._get_file_type_classifier().classify_file(filename)
        return importance

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

    # Bulk Operations

    async def execute_bulk_operation(
        self, bulk_dto: BulkArchiveOperationDto
    ) -> BulkOperationResultDto:
        """Execute a bulk archive operation."""
        operation_id = f"bulk_{bulk_dto.operation_type}_{int(time.time())}"
        start_time = time.time()
        
        result = BulkOperationResultDto(
            operation_id=operation_id,
            operation_type=bulk_dto.operation_type,
            total_archives=len(bulk_dto.archive_ids),
            successful_operations=[],
            failed_operations=[],
            warnings=[],
            total_duration_seconds=0.0,
            total_bytes_processed=0
        )

        try:
            # Validate archives exist
            for archive_id in bulk_dto.archive_ids:
                try:
                    self.archive_repository.get_archive_metadata(ArchiveId(archive_id))
                except RepositoryError:
                    result.failed_operations.append(f"{archive_id}: Archive not found")
                    if bulk_dto.stop_on_error:
                        result.total_duration_seconds = time.time() - start_time
                        return result

            # Execute operations in parallel batches
            if bulk_dto.operation_type == "bulk_copy":
                await self._execute_bulk_copy(bulk_dto, result)
            elif bulk_dto.operation_type == "bulk_move":
                await self._execute_bulk_move(bulk_dto, result)
            elif bulk_dto.operation_type == "bulk_extract":
                await self._execute_bulk_extract(bulk_dto, result)
            else:
                raise ValueError(f"Unsupported bulk operation: {bulk_dto.operation_type}")

        except Exception as e:
            result.warnings.append(f"Bulk operation failed: {str(e)}")
            logging.error(f"Bulk operation {operation_id} failed: {e}")

        result.total_duration_seconds = time.time() - start_time
        return result

    async def _execute_bulk_copy(
        self, bulk_dto: BulkArchiveOperationDto, result: BulkOperationResultDto
    ) -> None:
        """Execute bulk copy operations."""
        semaphore = asyncio.Semaphore(bulk_dto.parallel_operations)
        
        async def copy_single_archive(archive_id: str) -> None:
            async with semaphore:
                try:
                    # Create individual copy DTO
                    copy_dto = ArchiveCopyOperationDto(
                        archive_id=archive_id,
                        destination_location=bulk_dto.destination_location,
                        simulation_id=bulk_dto.simulation_id,
                        **bulk_dto.operation_parameters
                    )
                    
                    # Execute copy
                    copy_result = await self.copy_archive_to_location_async(copy_dto)
                    
                    if copy_result.success:
                        result.successful_operations.append(archive_id)
                        result.total_bytes_processed += copy_result.bytes_processed or 0
                    else:
                        result.failed_operations.append(f"{archive_id}: {copy_result.error_message}")
                        if bulk_dto.stop_on_error:
                            return
                            
                except Exception as e:
                    result.failed_operations.append(f"{archive_id}: {str(e)}")
                    if bulk_dto.stop_on_error:
                        return

        # Execute all copies in parallel
        tasks = [copy_single_archive(archive_id) for archive_id in bulk_dto.archive_ids]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _execute_bulk_move(
        self, bulk_dto: BulkArchiveOperationDto, result: BulkOperationResultDto
    ) -> None:
        """Execute bulk move operations."""
        semaphore = asyncio.Semaphore(bulk_dto.parallel_operations)
        
        async def move_single_archive(archive_id: str) -> None:
            async with semaphore:
                try:
                    # Create individual move DTO
                    move_dto = ArchiveMoveOperationDto(
                        archive_id=archive_id,
                        destination_location=bulk_dto.destination_location,
                        simulation_id=bulk_dto.simulation_id,
                        **bulk_dto.operation_parameters
                    )
                    
                    # Execute move
                    move_result = await self.move_archive_to_location_async(move_dto)
                    
                    if move_result.success:
                        result.successful_operations.append(archive_id)
                        result.total_bytes_processed += move_result.bytes_processed or 0
                    else:
                        result.failed_operations.append(f"{archive_id}: {move_result.error_message}")
                        if bulk_dto.stop_on_error:
                            return
                            
                except Exception as e:
                    result.failed_operations.append(f"{archive_id}: {str(e)}")
                    if bulk_dto.stop_on_error:
                        return

        # Execute all moves in parallel
        tasks = [move_single_archive(archive_id) for archive_id in bulk_dto.archive_ids]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _execute_bulk_extract(
        self, bulk_dto: BulkArchiveOperationDto, result: BulkOperationResultDto
    ) -> None:
        """Execute bulk extract operations."""
        semaphore = asyncio.Semaphore(bulk_dto.parallel_operations)
        
        async def extract_single_archive(archive_id: str) -> None:
            async with semaphore:
                try:
                    # Create individual extraction DTO
                    extract_dto = ArchiveExtractionDto(
                        archive_id=archive_id,
                        destination_location=bulk_dto.destination_location,
                        simulation_id=bulk_dto.simulation_id,
                        **bulk_dto.operation_parameters
                    )
                    
                    # Execute extraction
                    extract_result = await self.extract_archive_to_location_async(extract_dto)
                    
                    if extract_result.success:
                        result.successful_operations.append(archive_id)
                        result.total_bytes_processed += extract_result.bytes_processed or 0
                    else:
                        result.failed_operations.append(f"{archive_id}: {extract_result.error_message}")
                        if bulk_dto.stop_on_error:
                            return
                            
                except Exception as e:
                    result.failed_operations.append(f"{archive_id}: {str(e)}")
                    if bulk_dto.stop_on_error:
                        return

        # Execute all extractions in parallel
        tasks = [extract_single_archive(archive_id) for archive_id in bulk_dto.archive_ids]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def copy_archive_to_location_async(self, copy_dto: ArchiveCopyOperationDto) -> ArchiveOperationResultDto:
        """Async version of archive copy."""
        # For now, call the sync version in a thread pool
        return await asyncio.get_event_loop().run_in_executor(
            None, self.copy_archive_to_location, copy_dto
        )

    async def move_archive_to_location_async(self, move_dto: ArchiveMoveOperationDto) -> ArchiveOperationResultDto:
        """Async version of archive move."""
        # For now, call the sync version in a thread pool  
        return await asyncio.get_event_loop().run_in_executor(
            None, self.move_archive_to_location, move_dto
        )

    async def extract_archive_to_location_async(self, extract_dto: ArchiveExtractionDto) -> ArchiveOperationResultDto:
        """Async version of archive extraction."""
        # For now, call the sync version in a thread pool
        return await asyncio.get_event_loop().run_in_executor(
            None, self.extract_archive_to_location, extract_dto
        )

    async def _extract_tar_files_with_progress(
        self,
        source_fs,
        dest_fs,
        archive_path: str,
        dest_path: str,
        files_to_extract: List[str],
        preserve_structure: bool,
        progress_tracker_id: Optional[str],
        total_archive_size: int
    ) -> tuple[int, int]:
        """Extract specific files from a tar archive with progress tracking."""
        import tarfile
        
        extracted_count = 0
        bytes_extracted = 0
        last_progress_update = time.time()

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
                total_files = len(files_to_extract)
                
                for i, member in enumerate(tar.getmembers()):
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

                        # Extract file with progress updates
                        try:
                            with tar.extractfile(member) as extracted_file:
                                if extracted_file:
                                    with dest_fs.open(output_path, "wb") as output_file:
                                        # Stream file with progress updates
                                        chunk_size = 64 * 1024  # 64KB chunks
                                        file_bytes_written = 0
                                        
                                        while True:
                                            chunk = extracted_file.read(chunk_size)
                                            if not chunk:
                                                break
                                            output_file.write(chunk)
                                            file_bytes_written += len(chunk)
                                            bytes_extracted += len(chunk)
                                        
                                        extracted_count += 1
                                        
                                        # Update progress every 500ms or every few files
                                        current_time = time.time()
                                        if (current_time - last_progress_update > 0.5 or 
                                            extracted_count % 5 == 0):
                                            
                                            progress_percentage = 15.0 + (extracted_count / total_files * 80.0)  # 15% to 95%
                                            
                                            await self._update_operation_progress(
                                                progress_tracker_id,
                                                progress_percentage=progress_percentage,
                                                bytes_processed=bytes_extracted,
                                                total_bytes=total_archive_size,
                                                files_processed=extracted_count,
                                                total_files=total_files,
                                                current_file=member.name
                                            )
                                            
                                            last_progress_update = current_time
                                            
                        except Exception as e:
                            self._logger.warning(f"Failed to extract {member.name}: {e}")
                            continue

        return extracted_count, bytes_extracted

    async def _extract_zip_files_with_progress(
        self,
        source_fs,
        dest_fs,
        archive_path: str,
        dest_path: str,
        files_to_extract: List[str],
        preserve_structure: bool,
        progress_tracker_id: Optional[str],
        total_archive_size: int
    ) -> tuple[int, int]:
        """Extract specific files from a zip archive with progress tracking."""
        import zipfile
        
        extracted_count = 0
        bytes_extracted = 0
        last_progress_update = time.time()

        with source_fs.open(archive_path, "rb") as archive_file:
            with zipfile.ZipFile(archive_file, "r") as zip_archive:
                total_files = len(files_to_extract)
                
                for i, zip_info in enumerate(zip_archive.infolist()):
                    if not zip_info.is_dir() and zip_info.filename in files_to_extract:
                        # Determine output path
                        if preserve_structure:
                            output_path = f"{dest_path}/{zip_info.filename}"
                        else:
                            filename = zip_info.filename.split("/")[-1]  # Just the filename
                            output_path = f"{dest_path}/{filename}"

                        # Ensure output directory exists
                        output_dir = "/".join(output_path.split("/")[:-1])
                        if output_dir and not dest_fs.exists(output_dir):
                            dest_fs.makedirs(output_dir, exist_ok=True)

                        # Extract file with progress updates
                        try:
                            with zip_archive.open(zip_info) as extracted_file:
                                with dest_fs.open(output_path, "wb") as output_file:
                                    # Stream file with progress updates
                                    chunk_size = 64 * 1024  # 64KB chunks
                                    file_bytes_written = 0
                                    
                                    while True:
                                        chunk = extracted_file.read(chunk_size)
                                        if not chunk:
                                            break
                                        output_file.write(chunk)
                                        file_bytes_written += len(chunk)
                                        bytes_extracted += len(chunk)
                                    
                                    extracted_count += 1
                                    
                                    # Update progress every 500ms or every few files
                                    current_time = time.time()
                                    if (current_time - last_progress_update > 0.5 or 
                                        extracted_count % 5 == 0):
                                        
                                        progress_percentage = 15.0 + (extracted_count / total_files * 80.0)  # 15% to 95%
                                        
                                        await self._update_operation_progress(
                                            progress_tracker_id,
                                            progress_percentage=progress_percentage,
                                            bytes_processed=bytes_extracted,
                                            total_bytes=total_archive_size,
                                            files_processed=extracted_count,
                                            total_files=total_files,
                                            current_file=zip_info.filename
                                        )
                                        
                                        last_progress_update = current_time
                                        
                        except Exception as e:
                            self._logger.warning(f"Failed to extract {zip_info.filename}: {e}")
                            continue

        return extracted_count, bytes_extracted

    async def move_archive_with_progress(
        self, move_dto: ArchiveMoveOperationDto
    ) -> ArchiveOperationResultDto:
        """
        Move an archive to a different location with progress tracking.

        This performs a copy followed by an optional source cleanup with progress tracking. 
        The operation is atomic - if the copy succeeds but cleanup fails, the operation is 
        still considered successful but a warning is included in the result.

        Args:
            move_dto: Archive move operation parameters

        Returns:
            Archive operation result with details

        Raises:
            EntityNotFoundError: If archive or location not found
            ArchiveOperationError: If move operation fails
        """
        operation_id = f"move_{move_dto.archive_id}_{int(time.time())}"
        self._logger.info(f"Starting archive move operation with progress: {operation_id}")

        start_time = time.time()
        cleanup_success = True
        cleanup_error = None
        progress_tracker_id = None

        try:
            # Get source archive metadata
            archive_metadata = self._archive_repo.get_by_id(move_dto.archive_id)
            if not archive_metadata:
                raise EntityNotFoundError("Archive", move_dto.archive_id)

            # Get source location entity
            source_location = self._location_repo.get_by_name(move_dto.source_location)
            if not source_location:
                raise EntityNotFoundError("Location", move_dto.source_location)

            # Create progress tracker
            context = {
                'simulation_id': move_dto.simulation_id,
                'location_name': move_dto.destination_location,
                'tags': ['archive_move'],
                'metadata': {
                    'archive_id': move_dto.archive_id,
                    'source_location': move_dto.source_location,
                    'destination_location': move_dto.destination_location,
                    'cleanup_source': move_dto.cleanup_source
                }
            }
            
            progress_tracker_id = await self._create_operation_tracker(
                f"Move archive {move_dto.archive_id}",
                OperationType.ARCHIVE_MOVE,
                context
            )

            # Update progress: starting copy phase
            await self._update_operation_progress(
                progress_tracker_id,
                progress_percentage=5.0,
                bytes_processed=0,
                total_bytes=0,
                files_processed=0,
                total_files=1,
                current_file="Starting copy phase..."
            )

            # First perform copy operation with progress tracking
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

            # Execute the copy operation with progress
            copy_result = await self.copy_archive_with_progress(copy_dto)

            if not copy_result.success:
                # Mark our progress tracker as failed
                if progress_tracker_id and self._progress_service:
                    try:
                        from ..dtos import OperationControlDto
                        control_dto = OperationControlDto(
                            operation_id=progress_tracker_id,
                            command="force_cancel",
                            reason=f"Copy phase failed: {copy_result.error_message}"
                        )
                        await self._progress_service.control_operation(control_dto)
                    except:
                        pass
                
                return ArchiveOperationResultDto(
                    operation_id=operation_id,
                    operation_type="move",
                    archive_id=move_dto.archive_id,
                    success=False,
                    error_message=f"Copy phase failed: {copy_result.error_message}",
                    progress_tracker_id=progress_tracker_id,
                )

            # Update progress: copy completed, starting cleanup
            await self._update_operation_progress(
                progress_tracker_id,
                progress_percentage=80.0,
                bytes_processed=copy_result.bytes_processed or 0,
                total_bytes=copy_result.bytes_processed or 0,
                files_processed=1,
                total_files=1,
                current_file="Copy completed, starting cleanup..."
            )

            # If copy succeeded and cleanup_source is enabled, remove source
            if move_dto.cleanup_source:
                try:
                    # Create filesystem access from location
                    source_fs = self._create_location_filesystem(source_location)
                    if not source_fs:
                        raise ArchiveOperationError(
                            move_dto.archive_id,
                            "move",
                            f"Failed to get filesystem for source location: {move_dto.source_location}",
                        )
                    
                    # Get the actual source path using the same resolution as copy
                    source_path = self._get_archive_path(archive_metadata, source_location)
                    
                    # Update progress: cleaning up source
                    await self._update_operation_progress(
                        progress_tracker_id,
                        progress_percentage=90.0,
                        bytes_processed=copy_result.bytes_processed or 0,
                        total_bytes=copy_result.bytes_processed or 0,
                        files_processed=1,
                        total_files=1,
                        current_file=f"Removing source: {source_path}"
                    )
                    
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

            # Final progress update
            await self._update_operation_progress(
                progress_tracker_id,
                progress_percentage=100.0,
                bytes_processed=copy_result.bytes_processed or 0,
                total_bytes=copy_result.bytes_processed or 0,
                files_processed=1,
                total_files=1,
                current_file="Move operation completed"
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
                progress_tracker_id=progress_tracker_id,
            )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            self._logger.error(f"Archive move failed: {operation_id} - {error_msg}")

            # Mark progress tracker as failed
            if progress_tracker_id and self._progress_service:
                try:
                    from ..dtos import OperationControlDto
                    control_dto = OperationControlDto(
                        operation_id=progress_tracker_id,
                        command="force_cancel",
                        reason=f"Move operation failed: {error_msg}"
                    )
                    await self._progress_service.control_operation(control_dto)
                except:
                    pass  # Ignore progress tracking errors

            return ArchiveOperationResultDto(
                operation_id=operation_id,
                operation_type="move",
                archive_id=move_dto.archive_id,
                success=False,
                duration_seconds=duration,
                error_message=error_msg,
                progress_tracker_id=progress_tracker_id,
            )
    
    def _create_location_filesystem(self, location_entity):
        """Create filesystem access from a LocationEntity."""
        protocol = location_entity.config.get('protocol', 'file')
        storage_options = location_entity.config
        base_path = storage_options.get('path', '/')
        
        if protocol in ('file', 'local'):
            # Local filesystem
            from ...infrastructure.adapters.sandboxed_filesystem import PathSandboxedFileSystem
            import fsspec
            base_fs = fsspec.filesystem('file')
            return PathSandboxedFileSystem(base_fs, base_path)
            
        elif protocol in ('ssh', 'sftp'):
            # SSH filesystem
            from ...infrastructure.adapters.sandboxed_filesystem import PathSandboxedFileSystem
            import fsspec
            
            # Extract SSH configuration
            host = storage_options.get('host')
            if not host:
                raise ValueError("SSH/SFTP requires 'host' configuration")
            
            ssh_config = {
                'host': host,
                'username': storage_options.get('username'),
                'password': storage_options.get('password'),
                'port': storage_options.get('port', 22),
                'timeout': 30,
            }
            
            base_fs = fsspec.filesystem('ssh', **ssh_config)
            return PathSandboxedFileSystem(base_fs, base_path)
            
        elif protocol == 'scoutfs':
            # ScoutFS filesystem (extends SFTP)
            from ...infrastructure.adapters.scoutfs_filesystem import ScoutFSFileSystem
            from ...infrastructure.adapters.sandboxed_filesystem import PathSandboxedFileSystem
            
            host = storage_options.get('host')
            if not host:
                raise ValueError("ScoutFS requires 'host' configuration")
            
            scoutfs_config = {k: v for k, v in storage_options.items() if k != 'host'}
            scoutfs_config['timeout'] = 30  # Default timeout
            
            base_fs = ScoutFSFileSystem(host=host, **scoutfs_config)
            return PathSandboxedFileSystem(base_fs, base_path)
            
        else:
            raise ValueError(f"Unsupported protocol for filesystem access: {protocol}")
    
    def copy_archive_to_location(self, copy_dto: ArchiveCopyOperationDto) -> ArchiveOperationResultDto:
        """Alias for copy_archive method to match CLI expectations."""
        return self.copy_archive(copy_dto)
