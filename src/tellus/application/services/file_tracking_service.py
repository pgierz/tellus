"""Application service for file tracking functionality."""

import hashlib
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from ...domain.entities.file_tracking import (DVCConfiguration, FileChange,
                                              FileChangeType, FileHash,
                                              FileTrackingRepository,
                                              RepositorySnapshot,
                                              RepositoryState,
                                              TrackedFileMetadata,
                                              TrackingStatus)
from ...domain.repositories.file_tracking_repository import (
    IDVCService, IFileSystemService, IFileTrackingRepository)
from ..dtos import (AddFilesDto, CreateFileTrackingRepositoryDto,
                    CreateSnapshotDto, DVCConfigurationDto, DVCStatusDto,
                    FileStatusDto, FileTrackingRepositoryDto,
                    RepositorySnapshotDto, TrackedFileDto)
from ..exceptions import (ApplicationError, EntityAlreadyExistsError,
                          EntityNotFoundError, ValidationError)

logger = logging.getLogger(__name__)


class FileTrackingApplicationService:
    """Application service for file tracking operations."""

    def __init__(
        self,
        file_tracking_repository: IFileTrackingRepository,
        filesystem_service: IFileSystemService,
        dvc_service: Optional[IDVCService] = None
    ):
        """Initialize the file tracking service."""
        self._repo = file_tracking_repository
        self._fs = filesystem_service
        self._dvc = dvc_service
        self._logger = logger

    def initialize_repository(self, dto: CreateFileTrackingRepositoryDto) -> FileTrackingRepositoryDto:
        """Initialize a new file tracking repository."""
        try:
            root_path = Path(dto.root_path).resolve()
            
            # Check if repository already exists
            existing_repo = self._repo.get_repository(str(root_path))
            if existing_repo:
                raise EntityAlreadyExistsError("FileTrackingRepository", str(root_path))
            
            # Create repository
            repository = self._repo.create_repository(str(root_path))
            
            # Configure DVC if requested
            if dto.enable_dvc and self._dvc:
                if self._dvc.is_dvc_available():
                    success = self._dvc.initialize_dvc(root_path)
                    if success and dto.dvc_remote_name and dto.dvc_remote_url:
                        self._dvc.configure_remote(
                            root_path, 
                            dto.dvc_remote_name, 
                            dto.dvc_remote_url
                        )
                    
                    repository.dvc_config = DVCConfiguration(
                        enabled=success,
                        remote_name=dto.dvc_remote_name,
                        remote_url=dto.dvc_remote_url,
                        large_file_threshold=dto.large_file_threshold
                    )
                else:
                    self._logger.warning("DVC requested but not available")
            
            # Save repository
            self._repo.save_repository_state(repository)
            
            return self._repository_to_dto(repository)
            
        except Exception as e:
            self._logger.error(f"Error initializing repository: {e}")
            raise ApplicationError(f"Failed to initialize repository: {e}")

    def get_repository_info(self, root_path: str) -> FileTrackingRepositoryDto:
        """Get repository information."""
        repository = self._get_repository_or_raise(root_path)
        return self._repository_to_dto(repository)

    def add_files(self, root_path: str, dto: AddFilesDto) -> FileStatusDto:
        """Add files to tracking."""
        repository = self._get_repository_or_raise(root_path)
        
        try:
            added_files = []
            skipped_files = []
            
            for file_path_str in dto.file_paths:
                file_path = Path(root_path) / file_path_str
                
                if not self._fs.file_exists(file_path):
                    self._logger.warning(f"File not found: {file_path}")
                    continue
                
                # Check if file should be ignored
                if repository.is_file_ignored(str(file_path.relative_to(repository.working_directory))):
                    skipped_files.append(str(file_path))
                    continue
                
                # Get file metadata
                metadata = self._fs.get_file_metadata(file_path)
                
                # Determine if file should use DVC
                if (dto.use_dvc_for_large_files and 
                    repository.dvc_config.should_use_dvc(metadata.size) and 
                    self._dvc):
                    
                    success = self._dvc.add_to_dvc(file_path)
                    if success:
                        self._logger.info(f"Added to DVC: {file_path}")
                
                # Add to repository state
                rel_path = str(file_path.relative_to(repository.working_directory))
                metadata.status = TrackingStatus.STAGED
                repository.state.tracked_files[rel_path] = metadata
                added_files.append(rel_path)
            
            # Save repository state
            self._repo.save_repository_state(repository)
            
            self._logger.info(f"Added {len(added_files)} files, skipped {len(skipped_files)} files")
            
            return self.get_status(root_path)
            
        except Exception as e:
            self._logger.error(f"Error adding files: {e}")
            raise ApplicationError(f"Failed to add files: {e}")

    def remove_files(self, root_path: str, file_paths: List[str]) -> FileStatusDto:
        """Remove files from tracking."""
        repository = self._get_repository_or_raise(root_path)
        
        try:
            removed_files = []
            
            for file_path_str in file_paths:
                rel_path = str(Path(file_path_str).relative_to(repository.working_directory))
                
                if rel_path in repository.state.tracked_files:
                    del repository.state.tracked_files[rel_path]
                    removed_files.append(rel_path)
                    
                    # Remove from DVC if applicable
                    if self._dvc:
                        file_path = repository.working_directory / rel_path
                        self._dvc.remove_from_dvc(file_path)
            
            # Save repository state
            self._repo.save_repository_state(repository)
            
            self._logger.info(f"Removed {len(removed_files)} files from tracking")
            
            return self.get_status(root_path)
            
        except Exception as e:
            self._logger.error(f"Error removing files: {e}")
            raise ApplicationError(f"Failed to remove files: {e}")

    def get_status(self, root_path: str) -> FileStatusDto:
        """Get repository status."""
        repository = self._get_repository_or_raise(root_path)
        
        try:
            # Scan filesystem for current state
            all_files = self._fs.scan_directory(
                repository.working_directory,
                repository.state.ignore_patterns
            )
            
            tracked_files = []
            modified_files = []
            staged_files = []
            untracked_files = []
            deleted_files = []
            
            # Check tracked files
            for rel_path, metadata in repository.state.tracked_files.items():
                file_path = repository.working_directory / rel_path
                
                if not self._fs.file_exists(file_path):
                    deleted_files.append(rel_path)
                    continue
                
                # Check if modified
                current_metadata = self._fs.get_file_metadata(file_path)
                if current_metadata.content_hash.value != metadata.content_hash.value:
                    metadata.status = TrackingStatus.MODIFIED
                    modified_files.append(rel_path)
                elif metadata.status == TrackingStatus.STAGED:
                    staged_files.append(rel_path)
                
                tracked_files.append(self._metadata_to_dto(metadata))
            
            # Check for untracked files
            for file_path in all_files:
                rel_path = str(file_path.relative_to(repository.working_directory))
                if rel_path not in repository.state.tracked_files:
                    untracked_files.append(rel_path)
            
            return FileStatusDto(
                tracked_files=tracked_files,
                modified_files=modified_files,
                staged_files=staged_files,
                untracked_files=untracked_files,
                deleted_files=deleted_files
            )
            
        except Exception as e:
            self._logger.error(f"Error getting status: {e}")
            raise ApplicationError(f"Failed to get status: {e}")

    def create_snapshot(self, root_path: str, dto: CreateSnapshotDto) -> RepositorySnapshotDto:
        """Create a repository snapshot."""
        repository = self._get_repository_or_raise(root_path)
        
        try:
            # Determine files to include
            if dto.include_files:
                file_paths = dto.include_files
            else:
                # Include all staged files
                file_paths = repository.state.get_staged_files()
            
            # Create file changes
            changes = []
            for file_path in file_paths:
                if file_path in repository.state.tracked_files:
                    metadata = repository.state.tracked_files[file_path]
                    
                    # Determine change type based on status
                    if metadata.status == TrackingStatus.STAGED:
                        change_type = FileChangeType.ADDED
                    elif metadata.status == TrackingStatus.MODIFIED:
                        change_type = FileChangeType.MODIFIED
                    else:
                        continue
                    
                    change = FileChange(
                        file_path=file_path,
                        change_type=change_type,
                        new_hash=metadata.content_hash,
                        timestamp=datetime.now()
                    )
                    changes.append(change)
                    
                    # Update status to tracked
                    metadata.status = TrackingStatus.TRACKED
            
            # Create snapshot
            snapshot = self._repo.create_snapshot(
                repository, 
                dto.message, 
                dto.author, 
                changes
            )
            
            # Save repository state
            self._repo.save_repository_state(repository)
            
            self._logger.info(f"Created snapshot {snapshot.short_id}: {dto.message}")
            
            return self._snapshot_to_dto(snapshot)
            
        except Exception as e:
            self._logger.error(f"Error creating snapshot: {e}")
            raise ApplicationError(f"Failed to create snapshot: {e}")

    def list_snapshots(self, root_path: str) -> List[RepositorySnapshotDto]:
        """List all snapshots for a repository."""
        repository = self._get_repository_or_raise(root_path)
        
        try:
            snapshots = self._repo.get_snapshots(repository)
            return [self._snapshot_to_dto(snapshot) for snapshot in snapshots]
            
        except Exception as e:
            self._logger.error(f"Error listing snapshots: {e}")
            raise ApplicationError(f"Failed to list snapshots: {e}")

    def get_dvc_status(self, root_path: str) -> DVCStatusDto:
        """Get DVC status for repository."""
        if not self._dvc:
            return DVCStatusDto(is_available=False, repository_initialized=False)
        
        try:
            repository = self._get_repository_or_raise(root_path)
            
            is_available = self._dvc.is_dvc_available()
            dvc_status = self._dvc.get_dvc_status(repository.working_directory) if is_available else {}
            
            return DVCStatusDto(
                is_available=is_available,
                repository_initialized=repository.dvc_config.enabled,
                configured_remotes=[repository.dvc_config.remote_name] if repository.dvc_config.remote_name else [],
                tracked_files=list(dvc_status.keys()) if dvc_status else [],
                pending_pushes=[],  # Would need DVC API to determine
                pending_pulls=[]    # Would need DVC API to determine
            )
            
        except Exception as e:
            self._logger.error(f"Error getting DVC status: {e}")
            raise ApplicationError(f"Failed to get DVC status: {e}")

    def _get_repository_or_raise(self, root_path: str) -> FileTrackingRepository:
        """Get repository or raise exception if not found."""
        repository = self._repo.get_repository(root_path)
        if not repository:
            raise EntityNotFoundError("FileTrackingRepository", root_path)
        return repository

    def _repository_to_dto(self, repository: FileTrackingRepository) -> FileTrackingRepositoryDto:
        """Convert repository entity to DTO."""
        status = repository.state
        
        return FileTrackingRepositoryDto(
            root_path=repository.root_path,
            tracked_file_count=len(status.tracked_files),
            modified_file_count=len(status.get_modified_files()),
            staged_file_count=len(status.get_staged_files()),
            untracked_file_count=0,  # Would need filesystem scan
            dvc_enabled=repository.dvc_config.enabled,
            last_snapshot_id=repository.snapshots[-1].id if repository.snapshots else None,
            last_snapshot_time=repository.snapshots[-1].timestamp.isoformat() if repository.snapshots else None
        )

    def _metadata_to_dto(self, metadata: TrackedFileMetadata) -> TrackedFileDto:
        """Convert file metadata to DTO."""
        return TrackedFileDto(
            path=metadata.path,
            size=metadata.size,
            modification_time=metadata.modification_time.isoformat(),
            content_hash=metadata.content_hash.value,
            hash_algorithm=metadata.content_hash.algorithm,
            status=metadata.status.value,
            stage_hash=metadata.stage_hash.value if metadata.stage_hash else None,
            created_time=metadata.created_time.isoformat() if metadata.created_time else None
        )

    def _snapshot_to_dto(self, snapshot: RepositorySnapshot) -> RepositorySnapshotDto:
        """Convert snapshot entity to DTO."""
        return RepositorySnapshotDto(
            id=snapshot.id,
            short_id=snapshot.short_id,
            timestamp=snapshot.timestamp.isoformat(),
            message=snapshot.message,
            author=snapshot.author,
            parent_id=snapshot.parent_id,
            changed_files=[change.file_path for change in snapshot.changes],
            change_types={change.file_path: change.change_type.value for change in snapshot.changes}
        )