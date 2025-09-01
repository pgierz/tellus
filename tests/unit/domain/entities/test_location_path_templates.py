"""
Unit tests for LocationEntity path template functionality.

Tests the clean architecture solution for path template resolution,
ensuring locations can suggest paths without coupling to simulations.
"""

import pytest

from tellus.domain.entities.location import (LocationEntity, LocationKind,
                                             PathTemplate)


class TestPathTemplate:
    """Test PathTemplate value object."""
    
    def test_path_template_creation(self):
        """Test basic path template creation."""
        template = PathTemplate(
            name="basic",
            pattern="{model}/{experiment}",
            description="Basic pattern"
        )
        
        assert template.name == "basic"
        assert template.pattern == "{model}/{experiment}"
        assert template.description == "Basic pattern"
        assert set(template.required_attributes) == {"model", "experiment"}
    
    def test_path_template_auto_extracts_attributes(self):
        """Test automatic attribute extraction from pattern."""
        template = PathTemplate(
            name="complex",
            pattern="{institution}/{model}/{experiment}/{variant}/{table}",
            description="Complex CMIP6 pattern"
        )
        
        expected_attrs = {"institution", "model", "experiment", "variant", "table"}
        assert set(template.required_attributes) == expected_attrs
        assert template.get_complexity_score() == 5
    
    def test_path_template_manual_attributes(self):
        """Test manual specification of required attributes."""
        template = PathTemplate(
            name="manual",
            pattern="{model}/{experiment}",
            description="Manual pattern",
            required_attributes=["model", "experiment", "custom"]
        )
        
        # Manual attributes take precedence
        assert set(template.required_attributes) == {"model", "experiment", "custom"}
        assert template.get_complexity_score() == 3
    
    def test_path_template_attribute_compatibility(self):
        """Test attribute compatibility checking."""
        template = PathTemplate(
            name="test",
            pattern="{model}/{experiment}/{variant}",
            description="Test pattern"
        )
        
        full_attrs = {"model": "CESM2", "experiment": "historical", "variant": "r1i1p1f1"}
        partial_attrs = {"model": "CESM2", "experiment": "historical"}
        extra_attrs = {"model": "CESM2", "experiment": "historical", "variant": "r1i1p1f1", "extra": "value"}
        
        assert template.has_all_required_attributes(full_attrs) is True
        assert template.has_all_required_attributes(partial_attrs) is False
        assert template.has_all_required_attributes(extra_attrs) is True


class TestLocationEntityPathTemplates:
    """Test LocationEntity path template management."""
    
    @pytest.fixture
    def basic_location(self):
        """Create a basic location for testing."""
        return LocationEntity(
            name="test-location",
            kinds=[LocationKind.COMPUTE],
            config={'protocol': 'file', 'path': '/test'}
        )
    
    def test_location_path_template_management(self, basic_location):
        """Test adding, removing, and retrieving path templates."""
        template = PathTemplate(
            name="test-template",
            pattern="{model}/{experiment}",
            description="Test template"
        )
        
        # Add template
        basic_location.add_path_template(template)
        assert len(basic_location.path_templates) == 1
        assert basic_location.get_path_template("test-template") == template
        
        # Try to add duplicate
        with pytest.raises(ValueError, match="already exists"):
            basic_location.add_path_template(template)
        
        # Remove template
        assert basic_location.remove_path_template("test-template") is True
        assert len(basic_location.path_templates) == 0
        assert basic_location.get_path_template("test-template") is None
        
        # Remove non-existent template
        assert basic_location.remove_path_template("nonexistent") is False
    
    def test_location_list_path_templates(self, basic_location):
        """Test listing all path templates."""
        template1 = PathTemplate("t1", "{model}", "Template 1")
        template2 = PathTemplate("t2", "{experiment}", "Template 2")
        
        basic_location.add_path_template(template1)
        basic_location.add_path_template(template2)
        
        templates = basic_location.list_path_templates()
        assert len(templates) == 2
        assert template1 in templates
        assert template2 in templates
        
        # Ensure it returns a copy
        templates.clear()
        assert len(basic_location.path_templates) == 2
    
    def test_location_create_default_templates(self):
        """Test creation of default templates based on location kinds."""
        # Compute location
        compute_location = LocationEntity(
            name="compute",
            kinds=[LocationKind.COMPUTE],
            config={'protocol': 'file', 'path': '/compute'}
        )
        compute_location.create_default_templates()
        
        template_names = [t.name for t in compute_location.path_templates]
        assert "simple" in template_names
        assert "model_experiment" in template_names
        assert "detailed" in template_names
        
        # Tape location
        tape_location = LocationEntity(
            name="tape",
            kinds=[LocationKind.TAPE],
            config={'protocol': 'file', 'path': '/tape'}
        )
        tape_location.create_default_templates()
        
        template_names = [t.name for t in tape_location.path_templates]
        assert "archive_basic" in template_names
        assert "archive_dated" in template_names
        
        # Multi-kind location (should not have duplicates)
        multi_location = LocationEntity(
            name="multi",
            kinds=[LocationKind.COMPUTE, LocationKind.DISK],
            config={'protocol': 'file', 'path': '/multi'}
        )
        multi_location.create_default_templates()
        
        # Check no duplicate names
        template_names = [t.name for t in multi_location.path_templates]
        assert len(template_names) == len(set(template_names))


class TestLocationEntityPathSuggestion:
    """Test LocationEntity path suggestion functionality."""
    
    @pytest.fixture
    def configured_location(self):
        """Create a location with multiple path templates."""
        location = LocationEntity(
            name="test-location",
            kinds=[LocationKind.COMPUTE],
            config={'protocol': 'file', 'path': '/test'}
        )
        
        # Add templates of varying complexity
        location.add_path_template(PathTemplate(
            "simple", "{simulation_id}", "Just simulation ID"
        ))
        location.add_path_template(PathTemplate(
            "basic", "{model}/{simulation_id}", "Model and simulation"
        ))
        location.add_path_template(PathTemplate(
            "detailed", "{model}/{experiment}/{simulation_id}", "Full hierarchy"
        ))
        location.add_path_template(PathTemplate(
            "cmip6", "{institution}/{model}/{experiment}/{variant}", "CMIP6 structure"
        ))
        
        return location
    
    def test_suggest_path_template(self, configured_location):
        """Test path template suggestion based on available attributes."""
        # Full CMIP6 attributes - should suggest most specific template
        cmip6_attrs = {
            "simulation_id": "historical_r1i1p1f1",
            "model": "CESM2", 
            "experiment": "historical",
            "institution": "NCAR",
            "variant": "r1i1p1f1"
        }
        
        suggested = configured_location.suggest_path_template(cmip6_attrs)
        assert suggested.name == "cmip6"  # Most attributes used
        
        # Minimal attributes - should suggest simpler template
        minimal_attrs = {"simulation_id": "test-run"}
        
        suggested = configured_location.suggest_path_template(minimal_attrs)
        assert suggested.name == "simple"  # Only compatible template
        
        # Partial attributes - should suggest appropriate template
        partial_attrs = {"model": "AWI-ESM", "simulation_id": "test-run"}
        
        suggested = configured_location.suggest_path_template(partial_attrs)
        assert suggested.name == "basic"  # Uses both available attributes
    
    def test_suggest_path_template_no_compatible(self, configured_location):
        """Test path template suggestion when no templates are compatible."""
        # No simulation_id attribute (required by all templates)
        incompatible_attrs = {"model": "CESM2", "experiment": "historical"}
        
        suggested = configured_location.suggest_path_template(incompatible_attrs)
        assert suggested is None
    
    def test_suggest_path_template_empty_templates(self):
        """Test path template suggestion with no templates."""
        location = LocationEntity(
            name="empty",
            kinds=[LocationKind.DISK],
            config={'protocol': 'file', 'path': '/empty'}
        )
        
        attrs = {"simulation_id": "test"}
        suggested = location.suggest_path_template(attrs)
        assert suggested is None
    
    def test_suggest_path_resolution(self, configured_location):
        """Test complete path suggestion with template resolution."""
        attrs = {
            "simulation_id": "historical_r1i1p1f1",
            "model": "CESM2",
            "experiment": "historical", 
            "institution": "NCAR",
            "variant": "r1i1p1f1"
        }
        
        # Auto-select template and resolve path
        suggested_path = configured_location.suggest_path(attrs)
        assert suggested_path == "NCAR/CESM2/historical/r1i1p1f1"
        
        # Specify template explicitly
        basic_path = configured_location.suggest_path(attrs, "basic")
        assert basic_path == "CESM2/historical_r1i1p1f1"
        
        # Request non-existent template
        none_path = configured_location.suggest_path(attrs, "nonexistent")
        assert none_path is None
        
        # Request template with missing attributes
        insufficient_attrs = {"simulation_id": "test"}
        none_path = configured_location.suggest_path(insufficient_attrs, "cmip6")
        assert none_path is None
    
    def test_get_template_suggestions(self, configured_location):
        """Test getting all compatible template suggestions."""
        attrs = {
            "simulation_id": "test-run",
            "model": "CESM2",
            "experiment": "historical"
        }
        
        suggestions = configured_location.get_template_suggestions(attrs)
        
        # Should have 3 compatible templates (simple, basic, detailed)
        assert len(suggestions) == 3
        
        # Check sorting by complexity (simplest first)
        complexity_scores = [s['complexity_score'] for s in suggestions]
        assert complexity_scores == sorted(complexity_scores)
        
        # Check structure of suggestions
        for suggestion in suggestions:
            assert 'template_name' in suggestion
            assert 'template_pattern' in suggestion
            assert 'description' in suggestion
            assert 'resolved_path' in suggestion
            assert 'complexity_score' in suggestion
            assert 'required_attributes' in suggestion
        
        # Check specific resolved paths
        paths_by_name = {s['template_name']: s['resolved_path'] for s in suggestions}
        assert paths_by_name['simple'] == "test-run"
        assert paths_by_name['basic'] == "CESM2/test-run"
        assert paths_by_name['detailed'] == "CESM2/historical/test-run"


class TestLocationEntityValidation:
    """Test validation of path templates in LocationEntity."""
    
    def test_location_validates_path_templates(self):
        """Test that LocationEntity validates path templates."""
        # Valid creation
        location = LocationEntity(
            name="test",
            kinds=[LocationKind.DISK],
            config={'protocol': 'file', 'path': '/test'},
            path_templates=[
                PathTemplate("test", "{model}", "Test template")
            ]
        )
        assert len(location.validate()) == 0
        
        # Invalid path_templates type
        with pytest.raises(ValueError):
            LocationEntity(
                name="test",
                kinds=[LocationKind.DISK],
                config={'protocol': 'file', 'path': '/test'},
                path_templates="not_a_list"
            )
        
        # Invalid template type in list
        with pytest.raises(ValueError):
            LocationEntity(
                name="test",
                kinds=[LocationKind.DISK],
                config={'protocol': 'file', 'path': '/test'},
                path_templates=["not_a_template"]
            )
    
    def test_add_path_template_validation(self):
        """Test validation when adding path templates."""
        location = LocationEntity(
            name="test",
            kinds=[LocationKind.DISK],
            config={'protocol': 'file', 'path': '/test'}
        )
        
        # Valid template
        valid_template = PathTemplate("test", "{model}", "Test")
        location.add_path_template(valid_template)
        assert len(location.path_templates) == 1
        
        # Invalid type
        with pytest.raises(ValueError, match="must be a PathTemplate instance"):
            location.add_path_template("not_a_template")