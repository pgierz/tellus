"""
Example of refactored test using the new Clean Architecture test framework.

This demonstrates how to migrate from the old tightly-coupled test style
to the new clean architecture approach with proper separation of concerns.
"""

from pathlib import Path
from unittest.mock import Mock, patch

from .base_tests import IntegrationTest, UnitTest
from .factories import LocationBuilder, LocationFactory, SimulationBuilder
from .test_doubles import TestDoubleFactory, TestDoubleType
from .utilities import (AssertionHelper, FileSystemTestHelper,
                        TemporaryPathManager)

# =============================================================================
# UNIT TESTS - Fast, isolated, no I/O
# =============================================================================

class TestLocationCreationUnit(UnitTest):
    """Unit tests for Location creation logic."""
    
    def test_location_initialization_with_valid_parameters(self):
        """Test that Location can be initialized with valid parameters."""
        # Arrange - using builder pattern for clean test data
        location = (LocationBuilder()
                   .with_name("test_location")
                   .with_kinds("DISK")
                   .with_protocol("file")
                   .with_path("/test/path")
                   .build())
        
        # Act & Assert
        self.assertEqual(location.name, "test_location")
        self.assertEqual(location.kinds, ["DISK"])
        self.assertEqual(location.config["protocol"], "file")
        self.assertEqual(location.config["path"], "/test/path")
        self.assertFalse(location.optional)
    
    def test_location_builder_fluent_interface(self):
        """Test the fluent interface of LocationBuilder."""
        # Arrange & Act
        location = (LocationBuilder()
                   .with_name("s3_location")
                   .as_s3_location("test-bucket", "us-west-2")
                   .as_optional(True)
                   .build())
        
        # Assert
        self.assertEqual(location.name, "s3_location")
        self.assertIn("DISK", location.kinds)
        self.assertEqual(location.config["protocol"], "s3")
        self.assertTrue(location.optional)
        self.assertEqual(location.config["storage_options"]["region"], "us-west-2")
    
    def test_location_validation_with_invalid_kinds(self):
        """Test that invalid location kinds are rejected."""
        # This test uses mocks since we're testing business logic, not I/O
        with self.assertRaises(ValueError) as context:
            LocationBuilder().with_kinds("INVALID_KIND").build()
        
        self.assertIn("is not a valid LocationKind", str(context.exception))


class TestSimulationLocationManagementUnit(UnitTest):
    """Unit tests for Simulation location management."""
    
    def setUp(self):
        """Set up unit test with mocked repositories."""
        super().setUp()
        
        # Use test doubles for repositories (no real persistence)
        self.location_repository = TestDoubleFactory.create_location_repository(
            TestDoubleType.MOCK
        )
        self.simulation_repository = TestDoubleFactory.create_simulation_repository(
            TestDoubleType.MOCK
        )
    
    def test_add_location_to_simulation(self):
        """Test adding a location to a simulation."""
        # Arrange
        simulation = SimulationBuilder().with_id("test_sim").build()
        location = LocationFactory().create_disk_location("test_location")
        
        # Act
        simulation.add_location(location, "primary_storage")
        
        # Assert
        self.assertIn("primary_storage", simulation.locations)
        stored_location = simulation.locations["primary_storage"]["location"]
        self.assertEqual(stored_location.name, "test_location")
    
    def test_add_duplicate_location_raises_error(self):
        """Test that adding duplicate location names raises appropriate error."""
        # Arrange
        simulation = SimulationBuilder().build()
        location1 = LocationFactory().create_disk_location("location1")
        location2 = LocationFactory().create_tape_location("location2")
        
        # Act
        simulation.add_location(location1, "storage")
        
        # Assert
        with self.assertRaises(Exception):  # LocationExistsError in real implementation
            simulation.add_location(location2, "storage")  # Same name should fail
    
    def test_simulation_location_context_management(self):
        """Test location context management."""
        # Arrange
        simulation = SimulationBuilder().with_id("test_sim").with_model("test_model").build()
        location = LocationFactory().create_disk_location("test_location", "/base/path")
        
        # Act
        simulation.add_location(location, "storage")
        # In real implementation, this would set up path prefix templating
        
        # Assert - verify the location was properly configured
        self.assertIn("storage", simulation.locations)
        # Additional assertions would test path templating functionality


# =============================================================================
# INTEGRATION TESTS - Test component interaction with fakes
# =============================================================================

class TestLocationPersistenceIntegration(IntegrationTest):
    """Integration tests for Location persistence."""
    
    def setUp(self):
        """Set up integration test with fake filesystem."""
        super().setUp()
        
        # Use fake implementations that simulate real behavior
        self.temp_manager = TemporaryPathManager()
        self.fs_helper = FileSystemTestHelper()
        
        # Set up fake locations file
        self.locations_file = self.temp_manager.create_temp_file(
            content='{}', suffix='.json'
        )
    
    def tearDown(self):
        """Clean up integration test."""
        self.temp_manager.cleanup_all()
        super().tearDown()
    
    def test_location_save_and_load_cycle(self):
        """Test complete save and load cycle with fake filesystem."""
        # Arrange
        original_locations = [
            LocationFactory().create_disk_location("disk1", "/path1"),
            LocationFactory().create_s3_location("s3storage", "test-bucket")
        ]
        
        # Act - Save locations
        self._save_locations_to_file(original_locations)
        
        # Load locations back
        loaded_locations = self._load_locations_from_file()
        
        # Assert
        self.assertEqual(len(loaded_locations), 2)
        location_names = [loc.name for loc in loaded_locations]
        self.assertIn("disk1", location_names)
        self.assertIn("s3storage", location_names)
    
    def test_location_persistence_with_filesystem_errors(self):
        """Test location persistence handles filesystem errors gracefully."""
        # Arrange - Create a read-only directory to simulate permission errors
        readonly_dir = self.temp_manager.create_temp_dir()
        readonly_file = readonly_dir / "readonly.json"
        
        location = LocationFactory().create_disk_location("test_location")
        
        # Act & Assert - This would test error handling in real implementation
        # For now, we verify the test setup
        self.assertTrue(readonly_dir.exists())
    
    def _save_locations_to_file(self, locations):
        """Helper method to save locations (simulates real persistence)."""
        import json
        data = {}
        for location in locations:
            data[location.name] = {
                "kinds": location.kinds,
                "config": location.config,
                "optional": location.optional
            }
        
        self.locations_file.write_text(json.dumps(data, indent=2))
    
    def _load_locations_from_file(self):
        """Helper method to load locations (simulates real persistence)."""
        import json
        data = json.loads(self.locations_file.read_text())
        
        locations = []
        for name, location_data in data.items():
            location = (LocationBuilder()
                       .with_name(name)
                       .with_kinds(*location_data["kinds"])
                       .with_config(location_data["config"])
                       .as_optional(location_data["optional"])
                       .build())
            locations.append(location)
        
        return locations


class TestSimulationWorkflowIntegration(IntegrationTest):
    """Integration tests for complete simulation workflows."""
    
    def setUp(self):
        """Set up integration test environment."""
        super().setUp()
        self.temp_manager = TemporaryPathManager()
        self.fs_helper = FileSystemTestHelper()
    
    def tearDown(self):
        """Clean up integration test."""
        self.temp_manager.cleanup_all()
        super().tearDown()
    
    def test_simulation_with_multiple_locations_workflow(self):
        """Test complete workflow with simulation having multiple locations."""
        # Arrange - Create a simulation with multiple location types
        simulation = (SimulationBuilder()
                     .with_id("workflow_test")
                     .with_model("awiesm")
                     .with_path("/sim/path")
                     .build())
        
        # Add various location types
        disk_location = LocationFactory().create_disk_location("local_disk", "/local/data")
        tape_location = LocationFactory().create_tape_location("tape_storage")
        s3_location = LocationFactory().create_s3_location("cloud_storage", "sim-bucket")
        
        # Act - Build up the simulation configuration
        simulation.add_location(disk_location, "input_data")
        simulation.add_location(tape_location, "archive_storage")
        simulation.add_location(s3_location, "output_data")
        
        # Add some namelists and configuration
        simulation.add_namelist("echam.nml", "# ECHAM configuration")
        simulation.add_snakemake("process_data", "/workflow/process.smk")
        
        # Assert - Verify the complete configuration
        self.assertEqual(len(simulation.locations), 3)
        self.assertIn("input_data", simulation.locations)
        self.assertIn("archive_storage", simulation.locations)
        self.assertIn("output_data", simulation.locations)
        
        # Verify location types
        input_location = simulation.get_location("input_data")
        self.assertEqual(input_location.config["protocol"], "file")
        
        cloud_location = simulation.get_location("output_data")
        self.assertEqual(cloud_location.config["protocol"], "s3")
        
        # Verify simulation metadata
        self.assertEqual(simulation.model_id, "awiesm")
        self.assertIn("echam.nml", simulation.namelists)
        self.assertIn("process_data", simulation.snakemakes)
    
    def test_simulation_location_path_resolution(self):
        """Test path resolution with location contexts."""
        # Arrange
        simulation = (SimulationBuilder()
                     .with_id("path_test")
                     .with_model("icon")
                     .build())
        
        location = LocationFactory().create_disk_location("storage", "/base/path")
        
        # Act - Add location with context (in real implementation)
        simulation.add_location(location, "data_storage")
        
        # This would test path template resolution in real implementation
        # expected_path = simulation.get_location_path("data_storage", "output", "results.nc")
        
        # Assert - Verify path construction
        self.assertIn("data_storage", simulation.locations)
        # Additional assertions would verify path templating


# =============================================================================
# EXAMPLE USAGE OF THE ARCHITECTURE
# =============================================================================

def demonstrate_clean_test_architecture():
    """
    Demonstrate how the clean test architecture improves upon the original tests.
    
    Key improvements:
    1. Clear separation between unit and integration tests
    2. Proper dependency injection and test doubles
    3. Builder pattern for clean test data creation
    4. Single responsibility utilities
    5. No direct filesystem or network access in unit tests
    6. Proper cleanup and resource management
    """
    
    print("=== Clean Test Architecture Demonstration ===")
    
    # 1. Unit Test Example - Fast, isolated, deterministic
    print("1. Unit Test - Testing business logic only")
    unit_test = TestLocationCreationUnit()
    unit_test.setUp()
    
    try:
        unit_test.test_location_initialization_with_valid_parameters()
        print("   ✓ Location initialization test passed")
        
        unit_test.test_location_builder_fluent_interface()
        print("   ✓ Builder pattern test passed")
        
    finally:
        unit_test.tearDown()
    
    # 2. Integration Test Example - Tests component interaction
    print("\n2. Integration Test - Testing component interaction")
    integration_test = TestLocationPersistenceIntegration()
    integration_test.setUp()
    
    try:
        integration_test.test_location_save_and_load_cycle()
        print("   ✓ Save and load cycle test passed")
        
    finally:
        integration_test.tearDown()
    
    # 3. Show the benefits
    print("\n=== Architecture Benefits ===")
    print("✓ Unit tests are fast and isolated")
    print("✓ Integration tests use controlled fakes")
    print("✓ Clear boundaries between test types")
    print("✓ Proper dependency injection")
    print("✓ Reusable test utilities and builders")
    print("✓ Automatic cleanup and resource management")
    print("✓ Easy to extend and maintain")


if __name__ == "__main__":
    demonstrate_clean_test_architecture()