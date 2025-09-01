"""JSON-based implementation of file tracking repository."""

import json
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
from ...domain.repositories.file_tracking_repository import \
    IFileTrackingRepository


class JsonFileTrackingRepository(IFileTrackingRepository):
    """JSON-based implementation of file tracking repository."""

    def __init__(self, base_path: Optional[str] = None):
        """Initialize repository with optional base path for storing data."""
        self._base_path = Path(base_path) if base_path else Path.cwd()

    def create_repository(self, root_path: str) -> FileTrackingRepository:
        """Create a new file tracking repository."""
        root = Path(root_path).resolve()
        tellus_dir = root / ".tellus"
        tellus_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        (tellus_dir / "data").mkdir(exist_ok=True)
        (tellus_dir / "snapshots").mkdir(exist_ok=True)
        
        repository = FileTrackingRepository(
            root_path=str(root),
            state=RepositoryState(),
            dvc_config=DVCConfiguration(),
            snapshots=[]
        )
        
        self.save_repository_state(repository)
        return repository

    def get_repository(self, root_path: str) -> Optional[FileTrackingRepository]:
        """Get an existing repository by root path."""
        root = Path(root_path).resolve()
        tellus_dir = root / ".tellus"
        
        if not tellus_dir.exists():
            return None
        
        state = self.load_repository_state(str(root))
        if not state:
            return None
        
        # Load DVC configuration
        dvc_config_file = tellus_dir / "dvc_config.json"
        dvc_config = DVCConfiguration()
        if dvc_config_file.exists():
            try:
                with open(dvc_config_file, 'r') as f:
                    dvc_data = json.load(f)
                    dvc_config = DVCConfiguration(**dvc_data)
            except (json.JSONDecodeError, TypeError):
                pass  # Use default config
        
        # Load snapshots
        snapshots = self._load_snapshots(root)
        
        return FileTrackingRepository(
            root_path=str(root),
            state=state,
            dvc_config=dvc_config,
            snapshots=snapshots
        )

    def save_repository_state(self, repository: FileTrackingRepository) -> None:
        """Save repository state to persistent storage."""
        root = Path(repository.root_path)
        tellus_dir = root / ".tellus"
        tellus_dir.mkdir(exist_ok=True)
        
        # Save tracked files
        tracked_files_file = tellus_dir / "tracked_files.json"
        tracked_data = {}
        
        for path, metadata in repository.state.tracked_files.items():
            tracked_data[path] = {
                "path": metadata.path,
                "size": metadata.size,
                "modification_time": metadata.modification_time.isoformat(),
                "content_hash": {
                    "algorithm": metadata.content_hash.algorithm,
                    "value": metadata.content_hash.value
                },
                "status": metadata.status.value,
                "stage_hash": {
                    "algorithm": metadata.stage_hash.algorithm,
                    "value": metadata.stage_hash.value
                } if metadata.stage_hash else None,
                "created_time": metadata.created_time.isoformat() if metadata.created_time else None
            }
        
        with open(tracked_files_file, 'w') as f:
            json.dump(tracked_data, f, indent=2)
        
        # Save ignore patterns
        ignore_file = tellus_dir / "ignore_patterns.json"
        with open(ignore_file, 'w') as f:
            json.dump(repository.state.ignore_patterns, f, indent=2)
        
        # Save DVC configuration
        dvc_config_file = tellus_dir / "dvc_config.json"
        dvc_data = {
            "enabled": repository.dvc_config.enabled,
            "remote_name": repository.dvc_config.remote_name,
            "remote_url": repository.dvc_config.remote_url,
            "cache_dir": repository.dvc_config.cache_dir,
            "large_file_threshold": repository.dvc_config.large_file_threshold
        }
        with open(dvc_config_file, 'w') as f:
            json.dump(dvc_data, f, indent=2)

    def load_repository_state(self, root_path: str) -> Optional[RepositoryState]:
        """Load repository state from persistent storage."""
        root = Path(root_path).resolve()
        tellus_dir = root / ".tellus"
        
        if not tellus_dir.exists():
            return None
        
        state = RepositoryState()
        
        # Load tracked files
        tracked_files_file = tellus_dir / "tracked_files.json"
        if tracked_files_file.exists():
            try:
                with open(tracked_files_file, 'r') as f:
                    tracked_data = json.load(f)
                
                for path, data in tracked_data.items():
                    content_hash = FileHash(
                        algorithm=data["content_hash"]["algorithm"],
                        value=data["content_hash"]["value"]
                    )
                    
                    stage_hash = None
                    if data.get("stage_hash"):
                        stage_hash = FileHash(
                            algorithm=data["stage_hash"]["algorithm"],
                            value=data["stage_hash"]["value"]
                        )
                    
                    metadata = TrackedFileMetadata(
                        path=data["path"],
                        size=data["size"],
                        modification_time=datetime.fromisoformat(data["modification_time"]),
                        content_hash=content_hash,
                        status=TrackingStatus(data["status"]),
                        stage_hash=stage_hash,
                        created_time=datetime.fromisoformat(data["created_time"]) if data.get("created_time") else None
                    )
                    state.tracked_files[path] = metadata
                    
            except (json.JSONDecodeError, KeyError, ValueError):
                pass  # Return empty state
        
        # Load ignore patterns
        ignore_file = tellus_dir / "ignore_patterns.json"
        if ignore_file.exists():
            try:
                with open(ignore_file, 'r') as f:
                    state.ignore_patterns = json.load(f)
            except (json.JSONDecodeError, TypeError):
                pass  # Use default patterns
        
        return state

    def create_snapshot(
        self, 
        repository: FileTrackingRepository, 
        message: str, 
        author: str,
        changes: List[FileChange]
    ) -> RepositorySnapshot:
        """Create a new repository snapshot."""
        snapshot_id = str(uuid.uuid4())
        
        # Find parent (last snapshot)
        parent_id = repository.snapshots[-1].id if repository.snapshots else None
        
        snapshot = RepositorySnapshot(
            id=snapshot_id,
            timestamp=datetime.now(),
            message=message,
            author=author,
            changes=changes,
            parent_id=parent_id
        )
        
        # Save snapshot to file
        root = Path(repository.root_path)
        snapshot_file = root / ".tellus" / "snapshots" / f"{snapshot_id}.json"
        
        snapshot_data = {
            "id": snapshot.id,
            "timestamp": snapshot.timestamp.isoformat(),
            "message": snapshot.message,
            "author": snapshot.author,
            "parent_id": snapshot.parent_id,
            "changes": [
                {
                    "file_path": change.file_path,
                    "change_type": change.change_type.value,
                    "old_hash": {
                        "algorithm": change.old_hash.algorithm,
                        "value": change.old_hash.value
                    } if change.old_hash else None,
                    "new_hash": {
                        "algorithm": change.new_hash.algorithm,
                        "value": change.new_hash.value
                    } if change.new_hash else None,
                    "old_path": change.old_path,
                    "timestamp": change.timestamp.isoformat()
                }
                for change in changes
            ]
        }
        
        with open(snapshot_file, 'w') as f:
            json.dump(snapshot_data, f, indent=2)
        
        # Add to repository
        repository.snapshots.append(snapshot)
        
        return snapshot

    def get_snapshots(self, repository: FileTrackingRepository) -> List[RepositorySnapshot]:
        """Get all snapshots for a repository."""
        return self._load_snapshots(Path(repository.root_path))

    def get_snapshot(self, repository: FileTrackingRepository, snapshot_id: str) -> Optional[RepositorySnapshot]:
        """Get a specific snapshot by ID."""
        for snapshot in repository.snapshots:
            if snapshot.id == snapshot_id:
                return snapshot
        return None

    def _load_snapshots(self, root: Path) -> List[RepositorySnapshot]:
        """Load all snapshots from disk."""
        snapshots = []
        snapshots_dir = root / ".tellus" / "snapshots"
        
        if not snapshots_dir.exists():
            return snapshots
        
        for snapshot_file in snapshots_dir.glob("*.json"):
            try:
                with open(snapshot_file, 'r') as f:
                    data = json.load(f)
                
                changes = []
                for change_data in data.get("changes", []):
                    old_hash = None
                    if change_data.get("old_hash"):
                        old_hash = FileHash(
                            algorithm=change_data["old_hash"]["algorithm"],
                            value=change_data["old_hash"]["value"]
                        )
                    
                    new_hash = None
                    if change_data.get("new_hash"):
                        new_hash = FileHash(
                            algorithm=change_data["new_hash"]["algorithm"],
                            value=change_data["new_hash"]["value"]
                        )
                    
                    change = FileChange(
                        file_path=change_data["file_path"],
                        change_type=FileChangeType(change_data["change_type"]),
                        old_hash=old_hash,
                        new_hash=new_hash,
                        old_path=change_data.get("old_path"),
                        timestamp=datetime.fromisoformat(change_data["timestamp"])
                    )
                    changes.append(change)
                
                snapshot = RepositorySnapshot(
                    id=data["id"],
                    timestamp=datetime.fromisoformat(data["timestamp"]),
                    message=data["message"],
                    author=data["author"],
                    changes=changes,
                    parent_id=data.get("parent_id")
                )
                snapshots.append(snapshot)
                
            except (json.JSONDecodeError, KeyError, ValueError):
                continue  # Skip invalid snapshots
        
        # Sort by timestamp
        snapshots.sort(key=lambda s: s.timestamp)
        return snapshots