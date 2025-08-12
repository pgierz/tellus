"""
Archive Application Service - Orchestrates archive-related use cases.

This service coordinates archive operations, caching, file management,
and long-running workflows in the Earth System Model context.
"""

import logging
import time
import asyncio
from typing import List, Optional, Dict, Any, Set, Union
from pathlib import Path
from dataclasses import asdict

from ..exceptions import (
    EntityNotFoundError, EntityAlreadyExistsError, ValidationError,
    ArchiveOperationError, CacheOperationError, ResourceLimitExceededError,
    OperationNotAllowedError, DataIntegrityError, ExternalServiceError
)
from ..dtos import (
    CreateArchiveDto, UpdateArchiveDto, ArchiveDto, ArchiveListDto,
    ArchiveContentsDto, FileMetadataDto, ArchiveOperationDto, ArchiveOperationResult,
    CacheConfigurationDto, CacheStatusDto, CacheEntryDto, CacheOperationResult,
    WorkflowExecutionDto, PaginationInfo, FilterOptions
)
from ...domain.entities.archive import (
    ArchiveId, ArchiveMetadata, ArchiveType, FileMetadata, Checksum,
    CacheConfiguration, CacheCleanupPolicy, LocationContext
)
from ...domain.entities.location import LocationEntity
from ...domain.repositories.location_repository import ILocationRepository
from ...domain.repositories.exceptions import LocationNotFoundError, RepositoryError

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
        cache_config: Optional[CacheConfigurationDto] = None
    ):
        """
        Initialize the archive service.
        
        Args:
            location_repository: Repository for location data access
            cache_config: Cache configuration (uses defaults if not provided)
        """
        self._location_repo = location_repository
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
                simulation_date=dto.simulation_date,
                version=dto.version,
                description=dto.description,
                tags=dto.tags.copy()
            )
            
            # Add tags if provided
            for tag in dto.tags:
                metadata.add_tag(tag)
            
            self._logger.info(f"Successfully created archive metadata: {dto.archive_id}")
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
        
        # This would typically interact with an archive repository
        # For now, we'll simulate the operation
        
        # Check if archive exists in cache
        cache_key = f"archive:{archive_id}"
        if cache_key in self._cache_entries:
            cache_entry = self._cache_entries[cache_key]
            return ArchiveDto(
                archive_id=archive_id,
                location="unknown",  # Would come from repository
                archive_type="compressed",
                size=cache_entry.size,
                created_time=cache_entry.created_time,
                is_cached=True,
                cache_path=f"{self._cache_config.cache_directory}/{archive_id}"
            )
        
        raise EntityNotFoundError("Archive", archive_id)
    
    def list_archives(
        self,
        location_name: Optional[str] = None,
        page: int = 1,
        page_size: int = 50,
        filters: Optional[FilterOptions] = None
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
        
        # This would typically query an archive repository
        # For now, we'll return cached archives
        archives = []
        for cache_key, cache_entry in self._cache_entries.items():
            if cache_key.startswith("archive:") and cache_entry.entry_type == "archive":
                archive_id = cache_key.replace("archive:", "")
                archives.append(ArchiveDto(
                    archive_id=archive_id,
                    location=location_name or "unknown",
                    archive_type="compressed",
                    size=cache_entry.size,
                    created_time=cache_entry.created_time,
                    is_cached=True,
                    cache_path=f"{self._cache_config.cache_directory}/{archive_id}"
                ))
        
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
            has_previous=page > 1
        )
        
        return ArchiveListDto(
            archives=archives_page,
            pagination=pagination,
            filters_applied=filters or FilterOptions()
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
            # Validate archive exists (would check repository)
            # For now, check if it's cached
            cache_key = f"archive:{dto.archive_id}"
            if cache_key not in self._cache_entries:
                raise EntityNotFoundError("Archive", dto.archive_id)
            
            # Create workflow execution
            workflow = WorkflowExecutionDto(
                workflow_id=operation_id,
                name=f"Extract Archive {dto.archive_id}",
                status="pending",
                start_time=time.time(),
                current_step="Validating archive",
                total_steps=4,  # Validate, Extract, Verify, Cleanup
                result_data=asdict(dto)
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
                result_data=asdict(dto)
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
                "cancel",
                f"Operation is already {workflow.status}"
            )
        
        # Update status
        workflow.status = "cancelled"
        workflow.end_time = time.time()
        
        self._logger.info(f"Operation cancelled: {operation_id}")
        return True
    
    def add_to_cache(self, archive_id: str, file_path: str, tags: Optional[Set[str]] = None) -> CacheOperationResult:
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
                        f"{current_size + file_size} bytes"
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
                tags=tags or set()
            )
            
            self._cache_entries[cache_key] = entry
            
            duration_ms = (time.time() - start_time) * 1000
            
            result = CacheOperationResult(
                operation="add",
                success=True,
                entries_affected=1,
                bytes_affected=file_size,
                duration_ms=duration_ms
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
                error_message=error_msg
            )
    
    def get_cache_status(self) -> CacheStatusDto:
        """
        Get current cache status and statistics.
        
        Returns:
            Cache status information
        """
        total_size = sum(entry.size for entry in self._cache_entries.values())
        entry_count = len(self._cache_entries)
        archive_count = sum(1 for entry in self._cache_entries.values() if entry.entry_type == "archive")
        file_count = sum(1 for entry in self._cache_entries.values() if entry.entry_type == "file")
        
        oldest_time = min((entry.created_time for entry in self._cache_entries.values()), default=None)
        newest_time = max((entry.created_time for entry in self._cache_entries.values()), default=None)
        
        return CacheStatusDto(
            total_size=self._cache_config.archive_size_limit,
            used_size=total_size,
            available_size=self._cache_config.archive_size_limit - total_size,
            entry_count=entry_count,
            archive_count=archive_count,
            file_count=file_count,
            cleanup_policy=self._cache_config.cleanup_policy.value,
            oldest_entry=oldest_time,
            newest_entry=newest_time
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
                duration_ms=duration_ms
            )
            
            self._logger.info(f"Cache cleanup completed: removed {entries_removed} entries, {bytes_removed} bytes")
            return result
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            error_msg = str(e)
            
            self._logger.error(f"Cache cleanup failed: {error_msg}")
            
            return CacheOperationResult(
                operation="cleanup",
                success=False,
                duration_ms=duration_ms,
                error_message=error_msg
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
        
        cache_key = f"archive:{archive_id}"
        if cache_key not in self._cache_entries:
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
                    "archive",
                    archive_id,
                    "Checksum mismatch detected"
                )
            
            self._logger.info(f"Archive integrity verified: {archive_id}")
            return True
            
        except Exception as e:
            self._logger.error(f"Archive integrity verification failed: {archive_id} - {str(e)}")
            raise
    
    # Private helper methods
    
    def _build_cache_config(self, dto: Optional[CacheConfigurationDto]) -> CacheConfiguration:
        """Build cache configuration from DTO."""
        if dto is None:
            # Use default configuration
            return CacheConfiguration(
                cache_directory=str(Path.home() / ".cache" / "tellus"),
                archive_size_limit=50 * 1024**3,  # 50 GB
                file_size_limit=10 * 1024**3,     # 10 GB
                cleanup_policy=CacheCleanupPolicy.LRU
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
            unified_cache=dto.unified_cache
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
            cache_path = f"{self._cache_config.cache_directory}/{metadata.archive_id.value}"
        
        return ArchiveDto(
            archive_id=metadata.archive_id.value,
            location=metadata.location,
            archive_type=metadata.archive_type.value,
            checksum=checksum_str,
            checksum_algorithm=checksum_algorithm,
            size=metadata.size,
            created_time=metadata.created_time,
            simulation_date=metadata.simulation_date,
            version=metadata.version,
            description=metadata.description,
            tags=metadata.tags.copy(),
            is_cached=is_cached,
            cache_path=cache_path
        )
    
    def _get_cache_size(self) -> int:
        """Get total cache size in bytes."""
        return sum(entry.size for entry in self._cache_entries.values())
    
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
            self._cache_entries.items(),
            key=lambda x: x[1].last_accessed
        )
        
        entries_removed = 0
        bytes_removed = 0
        target_size = self._cache_config.archive_size_limit * 0.7  # Clean to 70% capacity
        
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
            self._cache_entries.items(),
            key=lambda x: x[1].size,
            reverse=True
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
    
    async def _simulate_extraction(self, operation_id: str, dto: ArchiveOperationDto) -> None:
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
    
    async def _simulate_compression(self, operation_id: str, dto: ArchiveOperationDto) -> None:
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
                entry_type="archive"
            )
            self._cache_entries[cache_key] = entry
            
        except Exception as e:
            workflow = self._active_operations[operation_id]
            workflow.status = "failed"
            workflow.end_time = time.time()
            workflow.error_message = str(e)