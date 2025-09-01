"""
Tests for SimulationEntity domain entity.
"""

import uuid

import pytest

from src.tellus.domain.entities.simulation import SimulationEntity


class TestSimulationEntity:
    """Test suite for SimulationEntity."""
    
    def test_create_simulation_with_required_fields(self):
        """Test creating a simulation with only required fields."""
        sim = SimulationEntity(simulation_id="test-sim-001")
        
        assert sim.simulation_id == "test-sim-001"
        assert sim.model_id is None
        assert sim.path is None
        assert sim.attrs == {}
        assert sim.namelists == {}
        assert sim.snakemakes == {}
        assert sim.uid is not None
        assert isinstance(sim.uid, str)
    
    def test_create_simulation_with_all_fields(self):
        """Test creating a simulation with all fields."""
        attrs = {"experiment": "historical", "resolution": "T63"}
        namelists = {"namelist.ocean": {"dt": 3600}}
        snakemakes = {"preprocess": "rules/preprocess.smk"}
        
        sim = SimulationEntity(
            simulation_id="fesom2-hist-001",
            model_id="FESOM2",
            path="/data/simulations/fesom2-hist-001",
            attrs=attrs,
            namelists=namelists,
            snakemakes=snakemakes
        )
        
        assert sim.simulation_id == "fesom2-hist-001"
        assert sim.model_id == "FESOM2"
        assert sim.path == "/data/simulations/fesom2-hist-001"
        assert sim.attrs == attrs
        assert sim.namelists == namelists
        assert sim.snakemakes == snakemakes
    
    def test_validation_empty_simulation_id_fails(self):
        """Test that empty simulation ID fails validation."""
        with pytest.raises(ValueError, match="Simulation ID is required"):
            SimulationEntity(simulation_id="")
    
    def test_validation_none_simulation_id_fails(self):
        """Test that None simulation ID fails validation."""
        with pytest.raises(ValueError, match="Simulation ID is required"):
            SimulationEntity(simulation_id=None)
    
    def test_validation_invalid_types(self):
        """Test validation with invalid types."""
        with pytest.raises(ValueError, match="Simulation ID must be a string"):
            SimulationEntity(simulation_id=123)
        
        with pytest.raises(ValueError, match="Model ID must be a string"):
            SimulationEntity(simulation_id="test", model_id=456)
        
        with pytest.raises(ValueError, match="Attributes must be a dictionary"):
            SimulationEntity(simulation_id="test", attrs="not a dict")
    
    def test_add_attribute(self):
        """Test adding attributes to a simulation."""
        sim = SimulationEntity(simulation_id="test-sim")
        
        sim.add_attribute("resolution", "T63")
        sim.add_attribute("experiment", "historical")
        
        assert sim.attrs["resolution"] == "T63"
        assert sim.attrs["experiment"] == "historical"
    
    def test_add_attribute_invalid_key_fails(self):
        """Test that adding attribute with invalid key fails."""
        sim = SimulationEntity(simulation_id="test-sim")
        
        with pytest.raises(ValueError, match="Attribute key must be a string"):
            sim.add_attribute(123, "value")
    
    def test_remove_attribute(self):
        """Test removing attributes from a simulation."""
        sim = SimulationEntity(simulation_id="test-sim")
        sim.add_attribute("temp_attr", "temp_value")
        
        # Remove existing attribute
        assert sim.remove_attribute("temp_attr") is True
        assert "temp_attr" not in sim.attrs
        
        # Try to remove non-existing attribute
        assert sim.remove_attribute("non_existing") is False
    
    def test_add_namelist(self):
        """Test adding namelists to a simulation."""
        sim = SimulationEntity(simulation_id="test-sim")
        
        namelist_data = {"dt": 3600, "output_freq": 86400}
        sim.add_namelist("ocean", namelist_data)
        
        assert sim.namelists["ocean"] == namelist_data
    
    def test_add_snakemake_rule(self):
        """Test adding Snakemake rules."""
        sim = SimulationEntity(simulation_id="test-sim")
        
        sim.add_snakemake_rule("preprocess", "rules/preprocess.smk")
        sim.add_snakemake_rule("postprocess", "rules/postprocess.smk")
        
        assert sim.snakemakes["preprocess"] == "rules/preprocess.smk"
        assert sim.snakemakes["postprocess"] == "rules/postprocess.smk"
    
    def test_add_duplicate_snakemake_rule_fails(self):
        """Test that adding duplicate Snakemake rule fails."""
        sim = SimulationEntity(simulation_id="test-sim")
        sim.add_snakemake_rule("preprocess", "rules/preprocess.smk")
        
        with pytest.raises(ValueError, match="already exists"):
            sim.add_snakemake_rule("preprocess", "rules/another.smk")
    
    def test_add_snakemake_rule_invalid_parameters_fail(self):
        """Test that invalid parameters for Snakemake rules fail."""
        sim = SimulationEntity(simulation_id="test-sim")
        
        with pytest.raises(ValueError, match="Rule name must be a non-empty string"):
            sim.add_snakemake_rule("", "rules/test.smk")
        
        with pytest.raises(ValueError, match="Rule name must be a non-empty string"):
            sim.add_snakemake_rule(None, "rules/test.smk")
        
        with pytest.raises(ValueError, match="Snakemake file path must be a non-empty string"):
            sim.add_snakemake_rule("test", "")
    
    def test_remove_snakemake_rule(self):
        """Test removing Snakemake rules."""
        sim = SimulationEntity(simulation_id="test-sim")
        sim.add_snakemake_rule("temp_rule", "rules/temp.smk")
        
        # Remove existing rule
        assert sim.remove_snakemake_rule("temp_rule") is True
        assert "temp_rule" not in sim.snakemakes
        
        # Try to remove non-existing rule
        assert sim.remove_snakemake_rule("non_existing") is False
    
    def test_get_context_variables(self):
        """Test getting context variables for template rendering."""
        sim = SimulationEntity(
            simulation_id="fesom2-hist-001",
            model_id="FESOM2",
            attrs={"experiment": "historical", "resolution": "T63"}
        )
        
        context = sim.get_context_variables()
        
        assert context["simulation_id"] == "fesom2-hist-001"
        assert context["model_id"] == "FESOM2"
        assert context["uid"] == sim.uid
        assert context["experiment"] == "historical"
        assert context["resolution"] == "T63"
    
    def test_get_context_variables_handles_none_values(self):
        """Test context variables with None values."""
        sim = SimulationEntity(simulation_id="test-sim")
        
        context = sim.get_context_variables()
        
        assert context["simulation_id"] == "test-sim"
        assert context["model_id"] == ""  # None converted to empty string
        assert context["uid"] == sim.uid
    
    def test_equality_and_hashing(self):
        """Test equality and hashing based on simulation_id."""
        sim1 = SimulationEntity(simulation_id="test-sim")
        sim2 = SimulationEntity(simulation_id="test-sim")
        sim3 = SimulationEntity(simulation_id="different-sim")
        
        # Test equality
        assert sim1 == sim2
        assert sim1 != sim3
        assert sim1 != "not a simulation"
        
        # Test hashing (allows use in sets and as dict keys)
        sim_set = {sim1, sim2, sim3}
        assert len(sim_set) == 2  # sim1 and sim2 are considered equal
    
    def test_string_representations(self):
        """Test string representations of the simulation."""
        sim = SimulationEntity(
            simulation_id="fesom2-hist-001",
            model_id="FESOM2"
        )
        
        str_repr = str(sim)
        assert "fesom2-hist-001" in str_repr
        assert "FESOM2" in str_repr
        
        repr_str = repr(sim)
        assert "SimulationEntity" in repr_str
        assert "fesom2-hist-001" in repr_str
        assert "FESOM2" in repr_str
        assert sim.uid in repr_str

    def test_associate_location(self):
        """Test associating locations with simulation."""
        simulation = SimulationEntity(simulation_id="test-sim")
        
        # Associate location without context
        simulation.associate_location("compute-cluster")
        assert simulation.is_location_associated("compute-cluster")
        assert "compute-cluster" in simulation.get_associated_locations()
        
        # Associate location with context
        context = {"path_prefix": "/data/{model_id}"}
        simulation.associate_location("tape-archive", context)
        assert simulation.is_location_associated("tape-archive")
        assert simulation.get_location_context("tape-archive") == context

    def test_associate_location_validation(self):
        """Test location association validation."""
        simulation = SimulationEntity(simulation_id="test-sim")
        
        # Invalid location name
        with pytest.raises(ValueError, match="Location name must be a non-empty string"):
            simulation.associate_location("")
        
        with pytest.raises(ValueError, match="Location name must be a non-empty string"):
            simulation.associate_location(None)
        
        # Invalid context
        with pytest.raises(ValueError, match="Location context must be a dictionary"):
            simulation.associate_location("test-loc", "invalid-context")

    def test_disassociate_location(self):
        """Test disassociating locations from simulation."""
        simulation = SimulationEntity(simulation_id="test-sim")
        
        # Associate locations first
        simulation.associate_location("loc1")
        simulation.associate_location("loc2", {"key": "value"})
        
        # Disassociate existing location
        result = simulation.disassociate_location("loc1")
        assert result is True
        assert not simulation.is_location_associated("loc1")
        assert "loc1" not in simulation.get_associated_locations()
        
        # Disassociate non-existent location
        result = simulation.disassociate_location("non-existent")
        assert result is False
        
        # Check that context is also removed
        simulation.disassociate_location("loc2")
        assert simulation.get_location_context("loc2") is None

    def test_update_location_context(self):
        """Test updating location context."""
        simulation = SimulationEntity(simulation_id="test-sim")
        
        # Associate location first
        simulation.associate_location("test-loc", {"old": "context"})
        
        # Update context
        new_context = {"new": "context", "path": "/updated/path"}
        simulation.update_location_context("test-loc", new_context)
        assert simulation.get_location_context("test-loc") == new_context
        
        # Try to update context for non-associated location
        with pytest.raises(ValueError, match="Location 'missing-loc' is not associated"):
            simulation.update_location_context("missing-loc", {"key": "value"})
        
        # Invalid context type
        with pytest.raises(ValueError, match="Location context must be a dictionary"):
            simulation.update_location_context("test-loc", "invalid")

    def test_location_context_isolation(self):
        """Test that location contexts are properly isolated."""
        simulation = SimulationEntity(simulation_id="test-sim")
        
        original_context = {"shared": "value", "list": [1, 2, 3]}
        simulation.associate_location("test-loc", original_context)
        
        # Modify original context
        original_context["shared"] = "modified"
        original_context["list"].append(4)
        
        # Check that stored context was not affected
        stored_context = simulation.get_location_context("test-loc")
        assert stored_context["shared"] == "value"
        assert stored_context["list"] == [1, 2, 3]

    def test_get_associated_locations_sorted(self):
        """Test that associated locations are returned sorted."""
        simulation = SimulationEntity(simulation_id="test-sim")
        
        # Associate locations in non-alphabetical order
        simulation.associate_location("zebra")
        simulation.associate_location("alpha")
        simulation.associate_location("beta")
        
        locations = simulation.get_associated_locations()
        assert locations == ["alpha", "beta", "zebra"]

    def test_validation_with_invalid_location_data(self):
        """Test validation fails with invalid location data types."""
        # Invalid associated_locations type
        with pytest.raises(ValueError, match="Associated locations must be a set"):
            SimulationEntity(
                simulation_id="test-sim",
                associated_locations=["not", "a", "set"]  # Should be a set
            )
        
        # Invalid location_contexts type
        with pytest.raises(ValueError, match="Location contexts must be a dictionary"):
            SimulationEntity(
                simulation_id="test-sim",
                location_contexts="not-a-dict"  # Should be a dict
            )