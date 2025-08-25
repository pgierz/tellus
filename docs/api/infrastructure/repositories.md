# Infrastructure Repositories

Infrastructure repositories provide concrete implementations of domain repository interfaces, handling data persistence and retrieval using specific storage technologies.

## JSON-Based Repositories

### JSON Simulation Repository

```{eval-rst}
.. currentmodule:: tellus.infrastructure.repositories.json_simulation_repository

.. autoclass:: JsonSimulationRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Features:**
- File-based persistence using JSON format
- Atomic writes with backup and recovery
- Thread-safe read/write operations
- Schema validation and migration support
- Human-readable storage format

### JSON Location Repository

```{eval-rst}
.. currentmodule:: tellus.infrastructure.repositories.json_location_repository

.. autoclass:: JsonLocationRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### JSON Archive Repository

```{eval-rst}
.. currentmodule:: tellus.infrastructure.repositories.json_archive_repository

.. autoclass:: JsonArchiveRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### JSON File Tracking Repository

```{eval-rst}
.. currentmodule:: tellus.infrastructure.repositories.json_file_tracking_repository

.. autoclass:: JsonFileTrackingRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### JSON Progress Tracking Repository

```{eval-rst}
.. currentmodule:: tellus.infrastructure.repositories.json_progress_tracking_repository

.. autoclass:: JsonProgressTrackingRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### JSON Workflow Repository

```{eval-rst}
.. currentmodule:: tellus.infrastructure.repositories.json_workflow_repository

.. autoclass:: JsonWorkflowRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Repository Configuration

### File Path Configuration

```python
from tellus.infrastructure.repositories import (
    JsonSimulationRepository,
    JsonLocationRepository, 
    JsonArchiveRepository
)
from pathlib import Path

# Configure repository storage locations
config_dir = Path("~/.tellus").expanduser()
config_dir.mkdir(exist_ok=True)

# Create repositories with custom paths
simulation_repo = JsonSimulationRepository(config_dir / "simulations.json")
location_repo = JsonLocationRepository(config_dir / "locations.json") 
archive_repo = JsonArchiveRepository(config_dir / "archives.json")

# Repositories handle file creation automatically
simulations = simulation_repo.find_all()  # Creates file if needed
```

### Environment-Specific Configuration

```python
import os
from pathlib import Path

# Environment-aware repository configuration
def create_repositories():
    """Create repositories based on environment configuration."""
    
    # Get configuration directory from environment
    config_dir = Path(
        os.environ.get("TELLUS_CONFIG_DIR", "~/.tellus")
    ).expanduser()
    
    # Create environment-specific subdirectories
    env = os.environ.get("TELLUS_ENV", "development")
    env_dir = config_dir / env
    env_dir.mkdir(parents=True, exist_ok=True)
    
    # Create repositories for this environment
    repos = {
        'simulation': JsonSimulationRepository(env_dir / "simulations.json"),
        'location': JsonLocationRepository(env_dir / "locations.json"),
        'archive': JsonArchiveRepository(env_dir / "archives.json"),
        'file_tracking': JsonFileTrackingRepository(env_dir / "file_tracking.json"),
        'progress': JsonProgressTrackingRepository(env_dir / "progress.json")
    }
    
    return repos

# Usage
repos = create_repositories()
simulation_service = SimulationApplicationService(repos['simulation'])
```

## JSON Storage Format

### Simulation Storage Schema

```json
{
  "simulations": {
    "cesm2-historical-r1i1p1f1": {
      "simulation_id": "cesm2-historical-r1i1p1f1",
      "model_id": "CESM2.1",
      "attrs": {
        "experiment": "historical",
        "time_period": "1850-2014",
        "resolution": "f09_g17",
        "ensemble_member": "r1i1p1f1",
        "atmospheric_model": "CAM6",
        "ocean_model": "POP2"
      },
      "associated_locations": ["hpc-scratch", "long-term-archive"],
      "location_contexts": {
        "hpc-scratch": {
          "path_prefix": "/scratch/cesm2/{simulation_id}",
          "file_pattern": "*.nc",
          "stage": "processing"
        },
        "long-term-archive": {
          "path_prefix": "/archive/cesm2/{model_id}/{experiment}",
          "compression": "lz4",
          "retention": "10_years"
        }
      },
      "metadata": {
        "created_at": "2024-03-15T10:00:00Z",
        "updated_at": "2024-03-15T14:30:00Z"
      }
    }
  },
  "schema_version": "1.0",
  "last_modified": "2024-03-15T14:30:00Z"
}
```

### Location Storage Schema

```json
{
  "locations": {
    "hpc-scratch": {
      "name": "hpc-scratch",
      "kinds": ["COMPUTE"],
      "protocol": "ssh",
      "config": {
        "host": "hpc.edu",
        "username": "researcher",
        "path": "/scratch/researcher"
      },
      "description": "HPC scratch space for active computation",
      "metadata": {
        "institution": "University HPC Center",
        "scheduler": "SLURM",
        "max_walltime": "48:00:00",
        "created_at": "2024-03-15T09:00:00Z"
      }
    },
    "cloud-archive": {
      "name": "cloud-archive",
      "kinds": ["FILESERVER"],
      "protocol": "s3",
      "config": {
        "bucket": "climate-data-archive",
        "region": "us-west-2",
        "prefix": "cesm2-data/"
      },
      "description": "AWS S3 long-term archive",
      "metadata": {
        "storage_class": "GLACIER",
        "cost_per_gb_month": 0.004,
        "created_at": "2024-03-15T09:15:00Z"
      }
    }
  },
  "schema_version": "1.0",
  "last_modified": "2024-03-15T14:30:00Z"
}
```

### Archive Storage Schema

```json
{
  "archives": {
    "cesm2-output-v1": {
      "archive_id": "cesm2-output-v1",
      "location": "cloud-archive",
      "archive_type": "COMPRESSED",
      "simulation_id": "cesm2-historical-r1i1p1f1",
      "version": "1.0",
      "description": "CESM2 model output files",
      "tags": ["cesm2", "historical", "output"],
      "files": [
        {
          "relative_path": "atmosphere/monthly/tas_Amon_CESM2_historical_r1i1p1f1_185001-201412.nc",
          "size": 524288000,
          "content_type": "MODEL_OUTPUT",
          "file_role": "PRIMARY",
          "checksum": "sha256:abc123def456...",
          "metadata": {
            "variable": "tas",
            "frequency": "monthly",
            "time_range": "1850-2014"
          }
        }
      ],
      "metadata": {
        "created_at": "2024-03-15T12:00:00Z",
        "total_size": 5242880000,
        "file_count": 24,
        "compression_ratio": 0.65
      }
    }
  },
  "schema_version": "1.0",
  "last_modified": "2024-03-15T14:30:00Z"
}
```

## Usage Examples

### Basic Repository Operations

```python
from tellus.infrastructure.repositories import JsonSimulationRepository
from tellus.domain.entities.simulation import SimulationEntity

# Create repository with custom file path
repo = JsonSimulationRepository("my_simulations.json")

# Create and save simulation
simulation = SimulationEntity(
    simulation_id="test-simulation",
    model_id="CESM2.1",
    attrs={"experiment": "historical"}
)

repo.save(simulation)

# Repository automatically creates and manages the JSON file
# File content is human-readable and version-controlled friendly

# Retrieve simulation
retrieved = repo.find_by_id("test-simulation")
assert retrieved.simulation_id == "test-simulation"

# List all simulations
all_sims = repo.find_all()
print(f"Total simulations: {len(all_sims)}")

# Check existence
exists = repo.exists("test-simulation")
print(f"Simulation exists: {exists}")
```

### Bulk Operations

```python
from tellus.infrastructure.repositories import JsonSimulationRepository

repo = JsonSimulationRepository("simulations.json")

# Create multiple simulations
simulations = [
    SimulationEntity(f"sim-{i:03d}", "CESM2.1", {"experiment": "historical"})
    for i in range(100)
]

# Bulk save (more efficient than individual saves)
repo.save_all(simulations)

# Bulk operations are atomic - all succeed or all fail
print(f"Saved {len(simulations)} simulations")

# Efficient bulk retrieval
sim_ids = [f"sim-{i:03d}" for i in range(0, 100, 10)]
subset = repo.find_by_ids(sim_ids)
print(f"Retrieved {len(subset)} simulations")
```

### Concurrent Access Handling

```python
import threading
from tellus.infrastructure.repositories import JsonSimulationRepository

repo = JsonSimulationRepository("concurrent_simulations.json")

def worker_thread(thread_id: int):
    """Worker thread that modifies simulations concurrently."""
    
    # Each thread creates its own simulations
    for i in range(10):
        simulation = SimulationEntity(
            simulation_id=f"thread-{thread_id}-sim-{i}",
            model_id="CESM2.1",
            attrs={"thread_id": thread_id, "iteration": i}
        )
        
        # Repository handles concurrent writes safely
        repo.save(simulation)
        
        # Read operations are also thread-safe
        retrieved = repo.find_by_id(simulation.simulation_id)
        assert retrieved is not None

# Start multiple worker threads
threads = []
for thread_id in range(5):
    thread = threading.Thread(target=worker_thread, args=(thread_id,))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Verify all data was saved correctly
all_simulations = repo.find_all()
print(f"Total simulations from all threads: {len(all_simulations)}")
```

### Repository Migration and Schema Evolution

```python
from tellus.infrastructure.repositories.json_simulation_repository import (
    JsonSimulationRepository,
    SchemaVersion
)

# Repository handles schema migration automatically
class MigratableJsonSimulationRepository(JsonSimulationRepository):
    """Repository with custom migration logic."""
    
    def _migrate_schema(self, data: dict) -> dict:
        """Handle schema migrations."""
        current_version = data.get("schema_version", "0.0")
        
        if current_version == "0.0":
            # Migrate from version 0.0 to 1.0
            data = self._migrate_v0_to_v1(data)
            data["schema_version"] = "1.0"
        
        if current_version == "1.0":
            # Future migration to 2.0
            # data = self._migrate_v1_to_v2(data)
            # data["schema_version"] = "2.0"
            pass
        
        return data
    
    def _migrate_v0_to_v1(self, data: dict) -> dict:
        """Migrate from schema v0.0 to v1.0."""
        
        # Example: convert old 'model' field to 'model_id'
        simulations = data.get("simulations", {})
        for sim_data in simulations.values():
            if "model" in sim_data and "model_id" not in sim_data:
                sim_data["model_id"] = sim_data.pop("model")
        
        return data

# Repository automatically handles migration on load
repo = MigratableJsonSimulationRepository("legacy_simulations.json")
simulations = repo.find_all()  # Triggers migration if needed
```

### Custom Serialization

```python
import json
from datetime import datetime
from tellus.infrastructure.repositories import JsonSimulationRepository

class CustomJsonSimulationRepository(JsonSimulationRepository):
    """Repository with custom JSON serialization."""
    
    def _serialize_entity(self, entity: SimulationEntity) -> dict:
        """Custom serialization logic."""
        data = super()._serialize_entity(entity)
        
        # Add custom fields
        data["serialized_at"] = datetime.now().isoformat()
        data["serialization_version"] = "custom-1.0"
        
        # Custom attribute handling
        if "created_date" in entity.attrs:
            # Convert date strings to ISO format
            date_str = entity.attrs["created_date"]
            if isinstance(date_str, str):
                data["attrs"]["created_date_iso"] = date_str
        
        return data
    
    def _deserialize_entity(self, data: dict) -> SimulationEntity:
        """Custom deserialization logic."""
        
        # Handle custom fields during deserialization
        if "serialization_version" in data:
            version = data["serialization_version"]
            if version == "custom-1.0":
                # Handle custom version logic
                pass
        
        return super()._deserialize_entity(data)

# Use custom repository
repo = CustomJsonSimulationRepository("custom_simulations.json")
```

### Repository Backup and Recovery

```python
import shutil
from pathlib import Path
from datetime import datetime

class BackupJsonSimulationRepository(JsonSimulationRepository):
    """Repository with automatic backup functionality."""
    
    def __init__(self, file_path: str, backup_dir: str = None):
        super().__init__(file_path)
        self.backup_dir = Path(backup_dir or "./backups")
        self.backup_dir.mkdir(exist_ok=True)
    
    def save(self, entity: SimulationEntity):
        """Save with automatic backup."""
        
        # Create backup before modification
        if self.file_path.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.backup_dir / f"simulations_backup_{timestamp}.json"
            shutil.copy2(self.file_path, backup_path)
        
        # Perform the actual save
        return super().save(entity)
    
    def restore_from_backup(self, backup_timestamp: str):
        """Restore from a specific backup."""
        backup_path = self.backup_dir / f"simulations_backup_{backup_timestamp}.json"
        
        if backup_path.exists():
            shutil.copy2(backup_path, self.file_path)
            # Clear any cached data
            self._clear_cache()
        else:
            raise FileNotFoundError(f"Backup not found: {backup_path}")
    
    def list_backups(self) -> list:
        """List available backups."""
        backup_files = list(self.backup_dir.glob("simulations_backup_*.json"))
        return sorted([f.stem.split("_")[-1] for f in backup_files])

# Usage
repo = BackupJsonSimulationRepository("simulations.json", "./backups")

# Automatic backup on save
simulation = SimulationEntity("test", "CESM2.1")
repo.save(simulation)  # Creates backup automatically

# List available backups
backups = repo.list_backups()
print(f"Available backups: {backups}")

# Restore from backup if needed
if backups:
    latest_backup = backups[-1]
    repo.restore_from_backup(latest_backup)
```

## Performance Optimization

### Lazy Loading and Caching

```python
from typing import Dict, Optional
from datetime import datetime, timedelta

class OptimizedJsonSimulationRepository(JsonSimulationRepository):
    """Repository with performance optimizations."""
    
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._cache: Dict[str, SimulationEntity] = {}
        self._cache_expiry: Dict[str, datetime] = {}
        self._cache_ttl = timedelta(minutes=5)
    
    def find_by_id(self, simulation_id: str) -> Optional[SimulationEntity]:
        """Find with caching support."""
        
        # Check cache first
        if simulation_id in self._cache:
            expiry = self._cache_expiry.get(simulation_id)
            if expiry and datetime.now() < expiry:
                return self._cache[simulation_id]
        
        # Cache miss - load from file
        entity = super().find_by_id(simulation_id)
        
        if entity:
            # Cache the result
            self._cache[simulation_id] = entity
            self._cache_expiry[simulation_id] = datetime.now() + self._cache_ttl
        
        return entity
    
    def save(self, entity: SimulationEntity):
        """Save and update cache."""
        result = super().save(entity)
        
        # Update cache
        self._cache[entity.simulation_id] = entity
        self._cache_expiry[entity.simulation_id] = datetime.now() + self._cache_ttl
        
        return result
    
    def _clear_cache(self):
        """Clear the entity cache."""
        self._cache.clear()
        self._cache_expiry.clear()
```

### Batch Processing Optimization

```python
class BatchOptimizedJsonSimulationRepository(JsonSimulationRepository):
    """Repository optimized for batch operations."""
    
    def save_all(self, entities: list[SimulationEntity]) -> list[SimulationEntity]:
        """Optimized batch save operation."""
        
        # Load current data once
        data = self._load_data()
        simulations_data = data.setdefault("simulations", {})
        
        # Process all entities in memory
        for entity in entities:
            entity_data = self._serialize_entity(entity)
            simulations_data[entity.simulation_id] = entity_data
        
        # Update metadata
        data["last_modified"] = datetime.now().isoformat()
        
        # Single write operation for all entities
        self._save_data(data)
        
        return entities
    
    def find_by_ids(self, simulation_ids: list[str]) -> list[SimulationEntity]:
        """Optimized batch find operation."""
        
        # Single read operation
        data = self._load_data()
        simulations_data = data.get("simulations", {})
        
        # Process all requested IDs
        results = []
        for sim_id in simulation_ids:
            if sim_id in simulations_data:
                entity = self._deserialize_entity(simulations_data[sim_id])
                results.append(entity)
        
        return results
```

## Error Handling and Reliability

```python
import json
import tempfile
import logging
from pathlib import Path

class ReliableJsonSimulationRepository(JsonSimulationRepository):
    """Repository with enhanced error handling and reliability."""
    
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.logger = logging.getLogger(__name__)
    
    def _save_data(self, data: dict):
        """Save data with atomic write operations."""
        
        # Write to temporary file first
        with tempfile.NamedTemporaryFile(
            mode='w', 
            suffix='.json', 
            delete=False,
            dir=self.file_path.parent
        ) as temp_file:
            try:
                json.dump(data, temp_file, indent=2, ensure_ascii=False)
                temp_path = Path(temp_file.name)
                
                # Atomic move to final location
                temp_path.replace(self.file_path)
                
                self.logger.info(f"Successfully saved data to {self.file_path}")
                
            except Exception as e:
                # Clean up temp file on error
                temp_path.unlink(missing_ok=True)
                self.logger.error(f"Failed to save data: {e}")
                raise
    
    def _load_data(self) -> dict:
        """Load data with error recovery."""
        
        try:
            return super()._load_data()
        
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error in {self.file_path}: {e}")
            
            # Try to recover from backup
            backup_path = self.file_path.with_suffix('.json.backup')
            if backup_path.exists():
                self.logger.info("Attempting recovery from backup")
                try:
                    with backup_path.open('r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Restore from backup
                    shutil.copy2(backup_path, self.file_path)
                    return data
                    
                except Exception as backup_error:
                    self.logger.error(f"Backup recovery failed: {backup_error}")
            
            # Return empty structure if recovery fails
            self.logger.warning("Using empty data structure")
            return {"simulations": {}, "schema_version": "1.0"}
        
        except Exception as e:
            self.logger.error(f"Unexpected error loading data: {e}")
            raise
```

## Testing Support

JSON repositories are ideal for testing due to their simplicity and human-readable format:

```python
import pytest
import tempfile
from pathlib import Path

@pytest.fixture
def temp_simulation_repo():
    """Create temporary simulation repository for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        repo_path = Path(temp_dir) / "test_simulations.json"
        repo = JsonSimulationRepository(str(repo_path))
        yield repo

def test_simulation_crud_operations(temp_simulation_repo):
    """Test CRUD operations on simulation repository."""
    repo = temp_simulation_repo
    
    # Create
    simulation = SimulationEntity("test-sim", "CESM2.1")
    repo.save(simulation)
    
    # Read
    retrieved = repo.find_by_id("test-sim")
    assert retrieved.simulation_id == "test-sim"
    
    # Update
    retrieved.attrs["experiment"] = "historical"
    repo.save(retrieved)
    
    updated = repo.find_by_id("test-sim")
    assert updated.attrs["experiment"] == "historical"
    
    # Delete
    repo.delete("test-sim")
    assert not repo.exists("test-sim")
```