"""
Test organization following Clean Architecture principles.

This module provides clear separation between unit, integration, and end-to-end
test concerns, with proper boundaries and responsibilities.
"""

import unittest
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Type

from .base_tests import (EndToEndTest, IntegrationTest, PerformanceTest,
                         UnitTest)
from .configuration import TestConfiguration, TestEnvironmentType
from .dependency_injection import ContainerFactory, TestContainer
from .test_doubles import TestDoubleRegistry, create_unit_test_doubles


class TestLayer(ABC):
    """Abstract base class defining test layer responsibilities."""
    
    @abstractmethod
    def get_test_environment_type(self) -> TestEnvironmentType:
        """Get the environment type for this test layer."""
        pass
    
    @abstractmethod
    def setup_dependencies(self, container: TestContainer) -> None:
        """Set up dependencies for this test layer."""
        pass
    
    @abstractmethod
    def get_allowed_dependencies(self) -> List[str]:
        """Get list of allowed external dependencies for this layer."""
        pass
    
    @abstractmethod
    def should_isolate_filesystem(self) -> bool:
        """Whether this layer should isolate filesystem operations."""
        pass
    
    @abstractmethod
    def should_isolate_network(self) -> bool:
        """Whether this layer should isolate network operations."""
        pass


class UnitTestLayer(TestLayer):
    """Test layer for unit tests - highest isolation, fastest execution."""
    
    def get_test_environment_type(self) -> TestEnvironmentType:
        """Unit tests use unit environment."""
        return TestEnvironmentType.UNIT
    
    def setup_dependencies(self, container: TestContainer) -> None:
        """Set up dependencies for unit tests - all mocked/stubbed."""
        # All dependencies should be mocked or stubbed
        doubles = create_unit_test_doubles()
        
        # Register test doubles in container
        container.register_location_repository(doubles.get("location_repository"))
        # Additional registrations as needed
    
    def get_allowed_dependencies(self) -> List[str]:
        """Unit tests should not depend on external systems."""
        return []  # No external dependencies allowed
    
    def should_isolate_filesystem(self) -> bool:
        """Unit tests must isolate filesystem."""
        return True
    
    def should_isolate_network(self) -> bool:
        """Unit tests must isolate network."""
        return True


class IntegrationTestLayer(TestLayer):
    """Test layer for integration tests - moderate isolation, tests component interaction."""
    
    def get_test_environment_type(self) -> TestEnvironmentType:
        """Integration tests use integration environment."""
        return TestEnvironmentType.INTEGRATION
    
    def setup_dependencies(self, container: TestContainer) -> None:
        """Set up dependencies for integration tests - fakes and in-memory implementations."""
        from .test_doubles import create_integration_test_doubles
        doubles = create_integration_test_doubles()
        
        # Register test doubles in container
        container.register_location_repository(doubles.get("location_repository"))
        # Additional registrations as needed
    
    def get_allowed_dependencies(self) -> List[str]:
        """Integration tests can use temporary filesystem and in-memory databases."""
        return ["temp_filesystem", "in_memory_database", "fake_network"]
    
    def should_isolate_filesystem(self) -> bool:
        """Integration tests use temporary filesystem."""
        return True  # Use temp filesystem, not real filesystem
    
    def should_isolate_network(self) -> bool:
        """Integration tests use fake network."""
        return True  # Use fake network, not real network


class EndToEndTestLayer(TestLayer):
    """Test layer for end-to-end tests - minimal isolation, tests complete workflows."""
    
    def get_test_environment_type(self) -> TestEnvironmentType:
        """E2E tests use end-to-end environment."""
        return TestEnvironmentType.END_TO_END
    
    def setup_dependencies(self, container: TestContainer) -> None:
        """Set up dependencies for E2E tests - real implementations where appropriate."""
        # Use real implementations for comprehensive testing
        # But still controlled/test-specific configurations
        pass
    
    def get_allowed_dependencies(self) -> List[str]:
        """E2E tests can use real filesystem and controlled external services."""
        return ["real_filesystem", "test_database", "controlled_network"]
    
    def should_isolate_filesystem(self) -> bool:
        """E2E tests can use real filesystem in controlled directories."""
        return False  # Use real filesystem but in controlled test directories
    
    def should_isolate_network(self) -> bool:
        """E2E tests may use controlled network access."""
        return False  # May use real network to test environments


@dataclass
class TestBoundary:
    """Defines boundaries and constraints for a test layer."""
    layer_name: str
    max_execution_time_ms: int
    max_memory_usage_mb: int
    allowed_file_operations: List[str]
    allowed_network_operations: List[str]
    required_isolation_level: str


class TestLayerRegistry:
    """Registry for test layers with boundary enforcement."""
    
    def __init__(self):
        """Initialize registry."""
        self._layers: Dict[str, TestLayer] = {}
        self._boundaries: Dict[str, TestBoundary] = {}
        self._register_default_layers()
    
    def _register_default_layers(self) -> None:
        """Register default test layers."""
        # Unit test layer
        self.register_layer("unit", UnitTestLayer())
        self.register_boundary("unit", TestBoundary(
            layer_name="unit",
            max_execution_time_ms=100,  # 100ms max per unit test
            max_memory_usage_mb=50,     # 50MB max memory
            allowed_file_operations=[],  # No file operations
            allowed_network_operations=[],  # No network operations
            required_isolation_level="complete"
        ))
        
        # Integration test layer
        self.register_layer("integration", IntegrationTestLayer())
        self.register_boundary("integration", TestBoundary(
            layer_name="integration",
            max_execution_time_ms=5000,  # 5 seconds max
            max_memory_usage_mb=200,      # 200MB max memory
            allowed_file_operations=["temp_file", "temp_dir"],
            allowed_network_operations=["fake_network"],
            required_isolation_level="moderate"
        ))
        
        # End-to-end test layer
        self.register_layer("e2e", EndToEndTestLayer())
        self.register_boundary("e2e", TestBoundary(
            layer_name="e2e",
            max_execution_time_ms=30000,  # 30 seconds max
            max_memory_usage_mb=500,       # 500MB max memory
            allowed_file_operations=["controlled_filesystem"],
            allowed_network_operations=["controlled_network"],
            required_isolation_level="minimal"
        ))
    
    def register_layer(self, name: str, layer: TestLayer) -> None:
        """Register a test layer."""
        self._layers[name] = layer
    
    def register_boundary(self, layer_name: str, boundary: TestBoundary) -> None:
        """Register boundary constraints for a test layer."""
        self._boundaries[layer_name] = boundary
    
    def get_layer(self, name: str) -> TestLayer:
        """Get a test layer by name."""
        if name not in self._layers:
            raise ValueError(f"Test layer '{name}' not registered")
        return self._layers[name]
    
    def get_boundary(self, layer_name: str) -> TestBoundary:
        """Get boundary constraints for a test layer."""
        if layer_name not in self._boundaries:
            raise ValueError(f"Test boundary for '{layer_name}' not registered")
        return self._boundaries[layer_name]
    
    def validate_test_class(self, test_class: Type[unittest.TestCase], layer_name: str) -> List[str]:
        """Validate that a test class adheres to layer boundaries."""
        violations = []
        layer = self.get_layer(layer_name)
        boundary = self.get_boundary(layer_name)
        
        # Check test class inheritance
        if layer_name == "unit" and not issubclass(test_class, UnitTest):
            violations.append(f"Unit test class {test_class.__name__} must inherit from UnitTest")
        elif layer_name == "integration" and not issubclass(test_class, IntegrationTest):
            violations.append(f"Integration test class {test_class.__name__} must inherit from IntegrationTest")
        elif layer_name == "e2e" and not issubclass(test_class, EndToEndTest):
            violations.append(f"E2E test class {test_class.__name__} must inherit from EndToEndTest")
        
        # Additional validations could be added here
        # - Check for forbidden imports
        # - Check for filesystem/network operations
        # - Check test method complexity
        
        return violations


class TestCategorizer:
    """Categorizes tests based on their characteristics and dependencies."""
    
    def __init__(self):
        """Initialize categorizer."""
        self._layer_registry = TestLayerRegistry()
    
    def categorize_test_class(self, test_class: Type[unittest.TestCase]) -> str:
        """Categorize a test class into the appropriate layer."""
        # Check inheritance hierarchy
        if issubclass(test_class, UnitTest):
            return "unit"
        elif issubclass(test_class, IntegrationTest):
            return "integration"
        elif issubclass(test_class, EndToEndTest):
            return "e2e"
        elif issubclass(test_class, PerformanceTest):
            return "performance"
        
        # Analyze test class characteristics if inheritance is not clear
        return self._analyze_test_characteristics(test_class)
    
    def _analyze_test_characteristics(self, test_class: Type[unittest.TestCase]) -> str:
        """Analyze test class to determine appropriate layer."""
        # This could analyze:
        # - Import statements
        # - Method complexity
        # - External dependencies
        # - File system access patterns
        
        # Default to unit test for now
        return "unit"
    
    def validate_test_categorization(self, test_class: Type[unittest.TestCase]) -> Dict[str, Any]:
        """Validate that a test is properly categorized."""
        category = self.categorize_test_class(test_class)
        violations = self._layer_registry.validate_test_class(test_class, category)
        
        return {
            "test_class": test_class.__name__,
            "category": category,
            "is_valid": len(violations) == 0,
            "violations": violations
        }


class TestExecutionStrategy:
    """Defines execution strategies for different test layers."""
    
    def __init__(self):
        """Initialize execution strategy."""
        self._layer_registry = TestLayerRegistry()
    
    def get_execution_order(self) -> List[str]:
        """Get the recommended execution order for test layers."""
        return ["unit", "integration", "e2e", "performance"]
    
    def should_run_in_parallel(self, layer_name: str) -> bool:
        """Determine if tests in a layer should run in parallel."""
        if layer_name == "unit":
            return True  # Unit tests are isolated and can run in parallel
        elif layer_name == "integration":
            return False  # Integration tests may have shared state
        elif layer_name == "e2e":
            return False  # E2E tests often have dependencies
        else:
            return False
    
    def get_timeout_for_layer(self, layer_name: str) -> int:
        """Get timeout in milliseconds for a test layer."""
        boundary = self._layer_registry.get_boundary(layer_name)
        return boundary.max_execution_time_ms
    
    def get_retry_strategy(self, layer_name: str) -> Dict[str, Any]:
        """Get retry strategy for a test layer."""
        if layer_name == "unit":
            return {"enabled": False}  # Unit tests should not need retries
        elif layer_name == "integration":
            return {"enabled": True, "max_retries": 2, "delay_ms": 100}
        elif layer_name == "e2e":
            return {"enabled": True, "max_retries": 3, "delay_ms": 1000}
        else:
            return {"enabled": False}


class TestSuiteOrganizer:
    """Organizes test suites based on clean architecture principles."""
    
    def __init__(self):
        """Initialize organizer."""
        self._categorizer = TestCategorizer()
        self._execution_strategy = TestExecutionStrategy()
        self._test_classes: Dict[str, List[Type[unittest.TestCase]]] = {
            "unit": [],
            "integration": [],
            "e2e": [],
            "performance": []
        }
    
    def add_test_class(self, test_class: Type[unittest.TestCase]) -> None:
        """Add a test class to the appropriate category."""
        category = self._categorizer.categorize_test_class(test_class)
        self._test_classes[category].append(test_class)
    
    def get_test_suite_for_layer(self, layer_name: str) -> unittest.TestSuite:
        """Get test suite for a specific layer."""
        suite = unittest.TestSuite()
        loader = unittest.TestLoader()
        
        for test_class in self._test_classes[layer_name]:
            suite.addTest(loader.loadTestsFromTestCase(test_class))
        
        return suite
    
    def get_fast_feedback_suite(self) -> unittest.TestSuite:
        """Get a suite optimized for fast feedback (unit tests only)."""
        return self.get_test_suite_for_layer("unit")
    
    def get_comprehensive_suite(self) -> unittest.TestSuite:
        """Get a comprehensive suite with all test types."""
        suite = unittest.TestSuite()
        
        for layer_name in self._execution_strategy.get_execution_order():
            layer_suite = self.get_test_suite_for_layer(layer_name)
            suite.addTest(layer_suite)
        
        return suite
    
    def get_smoke_test_suite(self) -> unittest.TestSuite:
        """Get a smoke test suite with critical tests from each layer."""
        # This would select a subset of tests from each layer
        # that represent the most critical functionality
        suite = unittest.TestSuite()
        
        # Implementation would select specific tests marked as "smoke tests"
        # For now, return a subset from each layer
        
        return suite
    
    def validate_organization(self) -> Dict[str, Any]:
        """Validate the test organization."""
        results = {
            "layers": {},
            "violations": [],
            "recommendations": []
        }
        
        for layer_name, test_classes in self._test_classes.items():
            layer_results = {
                "test_count": len(test_classes),
                "violations": []
            }
            
            for test_class in test_classes:
                validation = self._categorizer.validate_test_categorization(test_class)
                if not validation["is_valid"]:
                    layer_results["violations"].extend(validation["violations"])
            
            results["layers"][layer_name] = layer_results
        
        # Add recommendations
        if results["layers"]["unit"]["test_count"] == 0:
            results["recommendations"].append("Consider adding unit tests for faster feedback")
        
        if results["layers"]["integration"]["test_count"] > results["layers"]["unit"]["test_count"]:
            results["recommendations"].append("Consider having more unit tests than integration tests")
        
        return results


# Utility functions for test organization
def organize_test_classes(*test_classes: Type[unittest.TestCase]) -> TestSuiteOrganizer:
    """Organize multiple test classes into appropriate layers."""
    organizer = TestSuiteOrganizer()
    
    for test_class in test_classes:
        organizer.add_test_class(test_class)
    
    return organizer


def create_layered_test_runner() -> 'LayeredTestRunner':
    """Create a test runner that respects layer boundaries."""
    return LayeredTestRunner()


class LayeredTestRunner:
    """Test runner that executes tests according to layer boundaries and strategies."""
    
    def __init__(self):
        """Initialize layered test runner."""
        self._execution_strategy = TestExecutionStrategy()
        self._organizer = TestSuiteOrganizer()
    
    def run_layer(self, layer_name: str, verbosity: int = 1) -> unittest.TestResult:
        """Run tests for a specific layer."""
        suite = self._organizer.get_test_suite_for_layer(layer_name)
        runner = unittest.TextTestRunner(verbosity=verbosity)
        
        # Apply layer-specific settings
        timeout = self._execution_strategy.get_timeout_for_layer(layer_name)
        # Set timeout on runner if supported
        
        return runner.run(suite)
    
    def run_all_layers(self, verbosity: int = 1) -> Dict[str, unittest.TestResult]:
        """Run all test layers in the correct order."""
        results = {}
        
        for layer_name in self._execution_strategy.get_execution_order():
            print(f"\n--- Running {layer_name} tests ---")
            results[layer_name] = self.run_layer(layer_name, verbosity)
            
            # Stop on first failure if configured
            if results[layer_name].failures or results[layer_name].errors:
                if self._should_fail_fast(layer_name):
                    break
        
        return results
    
    def _should_fail_fast(self, layer_name: str) -> bool:
        """Determine if execution should stop on failure for this layer."""
        # Unit tests should fail fast, others might continue
        return layer_name == "unit"