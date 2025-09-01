"""
Tests for JsonSimulationRepository.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.tellus.domain.entities.simulation import SimulationEntity
from src.tellus.domain.repositories.exceptions import RepositoryError
from src.tellus.infrastructure.repositories.json_simulation_repository import \
    JsonSimulationRepository


@pytest.fixture
def temp_json_file(tmp_path):
    """Create a temporary JSON file for testing."""
    return tmp_path / "test_simulations.json"


@pytest.fixture
def sample_simulation():
    """Create a sample simulation for testing."""
    return SimulationEntity(
        simulation_id="test-sim-001",
        model_id="FESOM2",
        path="/data/simulations/test-sim-001",
        attrs={"experiment": "historical", "resolution": "T63"},
        namelists={"ocean": {"dt": 3600}},
        snakemakes={"preprocess": "rules/preprocess.smk"}
    )


class TestJsonSimulationRepository:
    """Test suite for JsonSimulationRepository."""
    
    def test_initialize_creates_empty_file(self, temp_json_file):
        """Test that initializing repository creates empty JSON file."""
        repo = JsonSimulationRepository(temp_json_file)
        
        assert temp_json_file.exists()
        with open(temp_json_file) as f:
            data = json.load(f)
        assert data == {}
    
    def test_save_and_get_simulation(self, temp_json_file, sample_simulation):
        """Test saving and retrieving a simulation."""
        repo = JsonSimulationRepository(temp_json_file)
        
        # Save simulation
        repo.save(sample_simulation)
        
        # Retrieve simulation
        retrieved = repo.get_by_id("test-sim-001")
        
        assert retrieved is not None
        assert retrieved.simulation_id == sample_simulation.simulation_id
        assert retrieved.model_id == sample_simulation.model_id
        assert retrieved.path == sample_simulation.path
        assert retrieved.attrs == sample_simulation.attrs
        assert retrieved.namelists == sample_simulation.namelists
        assert retrieved.snakemakes == sample_simulation.snakemakes
    
    def test_get_nonexistent_simulation_returns_none(self, temp_json_file):
        """Test getting a simulation that doesn't exist returns None."""
        repo = JsonSimulationRepository(temp_json_file)
        
        result = repo.get_by_id("nonexistent-sim")
        assert result is None
    
    def test_list_all_simulations(self, temp_json_file, sample_simulation):
        """Test listing all simulations."""
        repo = JsonSimulationRepository(temp_json_file)
        
        # Initially empty
        assert repo.list_all() == []
        
        # Add simulations
        repo.save(sample_simulation)
        
        sim2 = SimulationEntity(simulation_id="test-sim-002", model_id="AWI-CM")
        repo.save(sim2)
        
        # List all
        all_sims = repo.list_all()
        assert len(all_sims) == 2
        
        sim_ids = [sim.simulation_id for sim in all_sims]
        assert "test-sim-001" in sim_ids
        assert "test-sim-002" in sim_ids
    
    def test_delete_simulation(self, temp_json_file, sample_simulation):
        """Test deleting a simulation."""
        repo = JsonSimulationRepository(temp_json_file)
        
        # Save simulation
        repo.save(sample_simulation)
        assert repo.exists("test-sim-001")
        
        # Delete simulation
        result = repo.delete("test-sim-001")
        assert result is True
        assert not repo.exists("test-sim-001")
        
        # Try to delete again
        result = repo.delete("test-sim-001")
        assert result is False
    
    def test_exists_simulation(self, temp_json_file, sample_simulation):
        """Test checking if simulation exists."""
        repo = JsonSimulationRepository(temp_json_file)
        
        # Initially doesn't exist
        assert not repo.exists("test-sim-001")
        
        # Save simulation
        repo.save(sample_simulation)
        assert repo.exists("test-sim-001")
        
        # Still doesn't exist for different ID
        assert not repo.exists("nonexistent")
    
    def test_count_simulations(self, temp_json_file, sample_simulation):
        """Test counting simulations."""
        repo = JsonSimulationRepository(temp_json_file)
        
        # Initially zero
        assert repo.count() == 0
        
        # Add one simulation
        repo.save(sample_simulation)
        assert repo.count() == 1
        
        # Add another simulation
        sim2 = SimulationEntity(simulation_id="test-sim-002")
        repo.save(sim2)
        assert repo.count() == 2
        
        # Delete one
        repo.delete("test-sim-001")
        assert repo.count() == 1
    
    def test_save_overwrites_existing_simulation(self, temp_json_file, sample_simulation):
        """Test that saving overwrites existing simulation with same ID."""
        repo = JsonSimulationRepository(temp_json_file)
        
        # Save original
        repo.save(sample_simulation)
        original = repo.get_by_id("test-sim-001")
        
        # Modify and save again
        sample_simulation.model_id = "Modified-Model"
        sample_simulation.attrs["modified"] = True
        repo.save(sample_simulation)
        
        # Should be updated
        updated = repo.get_by_id("test-sim-001")
        assert updated.model_id == "Modified-Model"
        assert updated.attrs["modified"] is True
        assert repo.count() == 1  # Still only one simulation
    
    def test_atomic_file_operations(self, temp_json_file, sample_simulation):
        """Test that file operations are atomic (uses temporary file)."""
        repo = JsonSimulationRepository(temp_json_file)
        
        # Mock to simulate failure during write
        original_open = open
        
        def mock_open(*args, **kwargs):
            if str(args[0]).endswith('.tmp') and 'w' in args[1]:
                # Simulate write failure to temp file
                raise IOError("Simulated write failure")
            return original_open(*args, **kwargs)
        
        with patch('builtins.open', side_effect=mock_open):
            with pytest.raises(RepositoryError, match="Failed to save data"):
                repo.save(sample_simulation)
        
        # Original file should be unchanged
        with open(temp_json_file) as f:
            data = json.load(f)
        assert data == {}  # Still empty
    
    def test_thread_safety(self, temp_json_file):
        """Test thread safety with concurrent operations."""
        import threading
        import time
        
        repo = JsonSimulationRepository(temp_json_file)
        results = []
        errors = []
        
        def save_simulation(sim_id):
            try:
                sim = SimulationEntity(simulation_id=f"sim-{sim_id}")
                repo.save(sim)
                results.append(sim_id)
            except Exception as e:
                errors.append(e)
        
        # Create multiple threads that save simulations concurrently
        threads = []
        for i in range(10):
            thread = threading.Thread(target=save_simulation, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check results
        assert len(errors) == 0
        assert len(results) == 10
        assert repo.count() == 10
    
    def test_corrupted_json_file_raises_error(self, temp_json_file):
        """Test that corrupted JSON file raises appropriate error."""
        # Write invalid JSON
        with open(temp_json_file, 'w') as f:
            f.write("{ invalid json")
        
        repo = JsonSimulationRepository(temp_json_file)
        
        with pytest.raises(RepositoryError, match="Invalid JSON"):
            repo.list_all()
    
    def test_migrate_from_legacy_format(self, temp_json_file, tmp_path):
        """Test migrating from legacy simulation format."""
        # Create legacy format file
        legacy_file = tmp_path / "legacy_simulations.json"
        legacy_data = {
            "sim-001": {
                "simulation_id": "sim-001",
                "uid": "legacy-uid-001",
                "model_id": "FESOM2",
                "path": "/data/sim-001",
                "attrs": {"experiment": "historical"},
                "namelists": {},
                "snakemakes": {}
            },
            "sim-002": {
                "simulation_id": "sim-002",
                "uid": "legacy-uid-002",
                "model_id": "AWI-CM",
                "path": "/data/sim-002",
                "attrs": {},
                "namelists": {},
                "snakemakes": {}
            }
        }
        
        with open(legacy_file, 'w') as f:
            json.dump(legacy_data, f)
        
        repo = JsonSimulationRepository(temp_json_file)
        
        # Migrate from legacy
        repo.migrate_from_legacy_format(legacy_file)
        
        # Check migrated data
        assert repo.count() == 2
        
        sim1 = repo.get_by_id("sim-001")
        assert sim1.model_id == "FESOM2"
        assert sim1.uid == "legacy-uid-001"
        
        sim2 = repo.get_by_id("sim-002")
        assert sim2.model_id == "AWI-CM"
        assert sim2.uid == "legacy-uid-002"
    
    def test_preserve_uid_on_save_and_load(self, temp_json_file, sample_simulation):
        """Test that internal UID is preserved across save/load operations."""
        repo = JsonSimulationRepository(temp_json_file)
        
        original_uid = sample_simulation.uid
        
        # Save and reload
        repo.save(sample_simulation)
        reloaded = repo.get_by_id("test-sim-001")
        
        assert reloaded.uid == original_uid