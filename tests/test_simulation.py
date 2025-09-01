"""
Tests for Simulation objects - Test-driven freezing of current behavior
"""

import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from tellus.location import Location, LocationExistsError, LocationKind
from tellus.simulation import (CacheConfig, CacheManager, Simulation,
                               SimulationExistsError)
from tellus.simulation.context import LocationContext


@pytest.fixture
def clean_simulation_registry():
    """Clean simulation registry before and after each test"""
    original_sims = Simulation._simulations.copy()
    Simulation._simulations.clear()
    yield
    Simulation._simulations = original_sims


@pytest.fixture
def clean_location_registry():
    """Clean location registry before and after each test"""
    original_locs = Location._locations.copy()
    Location._locations.clear()
    yield
    Location._locations = original_locs


@pytest.fixture  
def basic_sim(clean_simulation_registry, clean_location_registry):
    """Create a basic simulation for testing"""
    return Simulation(
        simulation_id="test_sim",
        path="/my/path/to/simulation",
        model_id="test_model"
    )


@pytest.fixture
def sample_location(clean_location_registry):
    """Create a sample location for testing"""
    with patch('tellus.location.location.Location._save_locations'):
        return Location(
            name="test_location",
            kinds=[LocationKind.DISK],
            config={"path": "/test/data", "protocol": "file"}
        )


class TestSimulationBasics:
    """Test basic Simulation functionality"""
    
    def test_import_simulation_from_submodule(self):
        """Test importing Simulation from submodule"""
        from tellus.simulation import Simulation
        assert Simulation is not None
    
    def test_import_simulation_from_top_level(self):
        """Test importing Simulation from top level"""
        from tellus import Simulation
        assert Simulation is not None
    
    def test_simulation_initialization_with_id(self, clean_simulation_registry):
        """Test Simulation initialization with provided ID"""
        sim = Simulation(
            simulation_id="test_id",
            path="/test/path",
            model_id="test_model"
        )
        
        assert sim.simulation_id == "test_id"
        assert sim.path == "/test/path"
        assert sim.model_id == "test_model"
        assert sim.attrs == {}
        assert sim.locations == {}
        assert sim.namelists == {}
        assert sim.snakemakes == {}
        
        # Check it was added to registry
        assert "test_id" in Simulation._simulations
        assert Simulation._simulations["test_id"] == sim
    
    def test_simulation_initialization_auto_id(self, clean_simulation_registry):
        """Test Simulation initialization with auto-generated ID"""
        sim = Simulation(path="/test/path")
        
        assert sim.simulation_id is not None
        assert len(sim.simulation_id) > 0
        assert sim.path == "/test/path"
        assert sim.simulation_id in Simulation._simulations
    
    def test_simulation_duplicate_id_raises_error(self, clean_simulation_registry):
        """Test that duplicate simulation IDs raise an error"""
        Simulation(simulation_id="duplicate_id")
        
        with pytest.raises(SimulationExistsError):
            Simulation(simulation_id="duplicate_id")
    
    def test_simulation_basic_properties(self, basic_sim):
        """Test that Simulation has expected basic properties"""
        assert hasattr(basic_sim, "simulation_id")
        assert hasattr(basic_sim, "path")
        assert hasattr(basic_sim, "model_id")
        assert hasattr(basic_sim, "attrs")
        assert hasattr(basic_sim, "data")
        assert hasattr(basic_sim, "namelists")
        assert hasattr(basic_sim, "locations")
        assert hasattr(basic_sim, "snakemakes")
        assert hasattr(basic_sim, "uid")
    
    def test_simulation_uid_property(self, basic_sim):
        """Test simulation UID property"""
        uid = basic_sim.uid
        assert uid is not None
        assert isinstance(uid, str)
        # UUID should be valid
        uuid.UUID(uid)  # This will raise if invalid


class TestSimulationLocationManagement:
    """Test Simulation location management functionality"""
    
    @patch('tellus.simulation.simulation.Simulation.save_simulations')
    def test_add_location_basic(self, mock_save, basic_sim, sample_location):
        """Test adding a location to a simulation"""
        basic_sim.add_location(sample_location, "test_loc")
        
        assert "test_loc" in basic_sim.locations
        loc_entry = basic_sim.locations["test_loc"]
        assert loc_entry["location"] == sample_location
        assert "context" in loc_entry
        mock_save.assert_called_once()
    
    @patch('tellus.simulation.simulation.Simulation.save_simulations')
    def test_add_location_from_dict(self, mock_save, basic_sim):
        """Test adding a location from dictionary"""
        loc_dict = {
            "name": "dict_location",
            "kinds": ["DISK"],
            "config": {"path": "/dict/test", "protocol": "file"}
        }
        
        with patch('tellus.location.location.Location._save_locations'):
            basic_sim.add_location(loc_dict, "dict_loc")
        
        assert "dict_loc" in basic_sim.locations
        loc_entry = basic_sim.locations["dict_loc"]
        assert loc_entry["location"].name == "dict_location"
    
    def test_add_location_duplicate_raises_error(self, basic_sim, sample_location):
        """Test that adding duplicate location raises error"""
        with patch('tellus.simulation.simulation.Simulation.save_simulations'):
            basic_sim.add_location(sample_location, "test_loc")
            
            with pytest.raises(LocationExistsError):
                basic_sim.add_location(sample_location, "test_loc")
    
    @patch('tellus.simulation.simulation.Simulation.save_simulations')
    def test_add_location_with_override(self, mock_save, basic_sim, sample_location):
        """Test adding location with override=True"""
        basic_sim.add_location(sample_location, "test_loc")
        
        # Create different location
        with patch('tellus.location.location.Location._save_locations'):
            new_location = Location(
                name="new_location",
                kinds=[LocationKind.TAPE], 
                config={"path": "/new/test", "protocol": "file"}
            )
        
        # Override should work
        basic_sim.add_location(new_location, "test_loc", override=True)
        
        assert basic_sim.locations["test_loc"]["location"] == new_location
    
    def test_get_location(self, basic_sim, sample_location):
        """Test getting a location by name"""
        with patch('tellus.simulation.simulation.Simulation.save_simulations'):
            basic_sim.add_location(sample_location, "test_loc")
        
        retrieved = basic_sim.get_location("test_loc")
        assert retrieved == sample_location
        assert basic_sim.get_location("nonexistent") is None
    
    def test_list_locations(self, basic_sim, sample_location):
        """Test listing all locations"""
        with patch('tellus.simulation.simulation.Simulation.save_simulations'):
            basic_sim.add_location(sample_location, "test_loc")
        
        locations = basic_sim.list_locations()
        assert locations == ["test_loc"]
    
    def test_remove_location(self, basic_sim, sample_location):
        """Test removing a location"""
        with patch('tellus.simulation.simulation.Simulation.save_simulations'):
            basic_sim.add_location(sample_location, "test_loc")
            assert "test_loc" in basic_sim.locations
            
            basic_sim.remove_location("test_loc")
            assert "test_loc" not in basic_sim.locations


class TestSimulationLocationContext:
    """Test Simulation location context functionality"""
    
    @patch('tellus.simulation.simulation.Simulation.save_simulations')
    def test_get_location_context(self, mock_save, basic_sim, sample_location):
        """Test getting location context"""
        context = LocationContext(path_prefix="/custom/prefix")
        basic_sim.add_location(sample_location, "test_loc", context=context)
        
        retrieved_context = basic_sim.get_location_context("test_loc")
        assert retrieved_context.path_prefix == "/custom/prefix"
        assert basic_sim.get_location_context("nonexistent") is None
    
    @patch('tellus.simulation.simulation.Simulation.save_simulations')
    def test_set_location_context(self, mock_save, basic_sim, sample_location):
        """Test setting location context"""
        basic_sim.add_location(sample_location, "test_loc")
        
        new_context = LocationContext(path_prefix="/new/prefix")
        basic_sim.set_location_context("test_loc", new_context)
        
        retrieved = basic_sim.get_location_context("test_loc")
        assert retrieved.path_prefix == "/new/prefix"
    
    def test_set_location_context_nonexistent_raises_error(self, basic_sim):
        """Test setting context for nonexistent location raises error"""
        context = LocationContext(path_prefix="/test")
        
        with pytest.raises(ValueError, match="Location 'nonexistent' not found"):
            basic_sim.set_location_context("nonexistent", context)
    
    @patch('tellus.simulation.simulation.Simulation.save_simulations')
    def test_get_location_path_with_context(self, mock_save, basic_sim, sample_location):
        """Test getting location path with context applied"""
        context = LocationContext(path_prefix="/{{model_id}}/{{simulation_id}}")
        basic_sim.add_location(sample_location, "test_loc", context=context)
        
        path = basic_sim.get_location_path("test_loc", "subdir", "file.txt")
        
        # Should include template substitution
        expected = "/test_model/test_sim/test/data/subdir/file.txt"
        assert path == expected
    
    def test_get_location_path_nonexistent_raises_error(self, basic_sim):
        """Test getting path for nonexistent location raises error"""
        with pytest.raises(ValueError, match="Location 'nonexistent' not in simulation"):
            basic_sim.get_location_path("nonexistent")


class TestSimulationPersistence:
    """Test Simulation persistence functionality"""
    
    def test_to_dict(self, basic_sim, sample_location):
        """Test converting simulation to dictionary"""
        with patch('tellus.simulation.simulation.Simulation.save_simulations'):
            basic_sim.add_location(sample_location, "test_loc")
        
        result = basic_sim.to_dict()
        
        assert result["simulation_id"] == "test_sim"
        assert result["path"] == "/my/path/to/simulation"
        assert result["model_id"] == "test_model"
        assert "uid" in result
        assert "locations" in result
        assert "test_loc" in result["locations"]
    
    def test_from_dict(self, clean_simulation_registry, clean_location_registry):
        """Test creating simulation from dictionary"""
        data = {
            "simulation_id": "from_dict_test",
            "path": "/test/path",
            "model_id": "test_model",
            "attrs": {"test_attr": "value"},
            "namelists": {"test.nml": "content"},
            "snakemakes": {"rule1": "file.smk"},
            "locations": {}
        }
        
        sim = Simulation.from_dict(data)
        
        assert sim.simulation_id == "from_dict_test"
        assert sim.path == "/test/path"
        assert sim.model_id == "test_model"
        assert sim.attrs == {"test_attr": "value"}
        assert sim.namelists == {"test.nml": "content"}
        assert sim.snakemakes == {"rule1": "file.smk"}
    
    def test_to_json(self, basic_sim):
        """Test converting simulation to JSON"""
        json_str = basic_sim.to_json()
        
        # Should be valid JSON
        data = json.loads(json_str)
        assert data["simulation_id"] == "test_sim"
    
    def test_from_json(self, clean_simulation_registry):
        """Test creating simulation from JSON"""
        json_data = {
            "simulation_id": "from_json_test",
            "path": "/test/path",
            "model_id": "test_model",
            "attrs": {},
            "locations": {},
            "namelists": {},
            "snakemakes": {}
        }
        json_str = json.dumps(json_data)
        
        sim = Simulation.from_json(json_str)
        assert sim.simulation_id == "from_json_test"


class TestSimulationClassMethods:
    """Test Simulation class-level methods"""
    
    def test_get_simulation(self, basic_sim):
        """Test getting simulation by ID"""
        retrieved = Simulation.get_simulation("test_sim")
        assert retrieved == basic_sim
        assert Simulation.get_simulation("nonexistent") is None
    
    def test_list_simulations(self, basic_sim):
        """Test listing all simulations"""
        simulations = Simulation.list_simulations()
        assert basic_sim in simulations
    
    def test_delete_simulation(self, basic_sim):
        """Test deleting simulation"""
        sim_id = basic_sim.simulation_id
        assert sim_id in Simulation._simulations
        
        result = Simulation.delete_simulation(sim_id)
        assert result is True
        assert sim_id not in Simulation._simulations
        
        # Test deleting nonexistent
        result = Simulation.delete_simulation("nonexistent")
        assert result is False


class TestSimulationSnakemake:
    """Test Simulation Snakemake integration"""
    
    def test_add_snakemake(self, basic_sim):
        """Test adding Snakemake rule"""
        basic_sim.add_snakemake("test_rule", "/path/to/rule.smk")
        
        assert "test_rule" in basic_sim.snakemakes
        assert basic_sim.snakemakes["test_rule"] == "/path/to/rule.smk"
    
    def test_add_duplicate_snakemake_raises_error(self, basic_sim):
        """Test adding duplicate Snakemake rule raises error"""
        basic_sim.add_snakemake("test_rule", "/path/to/rule.smk")
        
        with pytest.raises(ValueError, match="Snakemake rule test_rule already exists"):
            basic_sim.add_snakemake("test_rule", "/path/to/other.smk")
    
    def test_run_snakemake(self, basic_sim, capsys):
        """Test running Snakemake rule"""
        basic_sim.add_snakemake("test_rule", "/path/to/rule.smk")
        
        basic_sim.run_snakemake("test_rule")
        
        captured = capsys.readouterr()
        assert "Running snakemake rule test_rule" in captured.out
    
    def test_run_nonexistent_snakemake_raises_error(self, basic_sim):
        """Test running nonexistent Snakemake rule raises error"""
        with pytest.raises(ValueError, match="Snakemake rule nonexistent not found"):
            basic_sim.run_snakemake("nonexistent")


class TestCacheConfig:
    """Test CacheConfig dataclass"""
    
    def test_cache_config_defaults(self):
        """Test CacheConfig default values"""
        config = CacheConfig()
        
        assert config.cache_dir == Path.home() / ".cache" / "tellus"
        assert config.archive_cache_size_limit == 50 * 1024**3  # 50 GB
        assert config.file_cache_size_limit == 10 * 1024**3     # 10 GB
        assert config.unified_cache is False
        
        # Check computed paths
        assert config.archive_cache_dir == config.cache_dir / "archives"
        assert config.file_cache_dir == config.cache_dir / "files"
    
    def test_cache_config_custom_path(self):
        """Test CacheConfig with custom cache directory"""
        custom_path = Path("/custom/cache")
        config = CacheConfig(cache_dir=custom_path)
        
        assert config.cache_dir == custom_path
        assert config.archive_cache_dir == custom_path / "archives"
        assert config.file_cache_dir == custom_path / "files"