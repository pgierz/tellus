"""
Test doubles strategy with clear boundaries following the Test Double patterns.

This module provides different types of test doubles (Mocks, Stubs, Fakes, Spies)
with clear separation of concerns and well-defined usage patterns.
"""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
from unittest.mock import MagicMock, Mock

from .interfaces import (CacheInterface, FileSystemInterface,
                         LocationRepository, LoggerInterface, NetworkInterface,
                         ProgressTracker, SimulationRepository)


class TestDoubleType:
    """Constants for test double types."""

    DUMMY = "dummy"
    STUB = "stub"
    FAKE = "fake"
    MOCK = "mock"
    SPY = "spy"


@dataclass
class CallRecord:
    """Record of a method call for verification."""

    method_name: str
    args: tuple
    kwargs: dict
    timestamp: float = field(default_factory=time.time)
    return_value: Any = None
    exception: Optional[Exception] = None


class Spy:
    """Base class for spy test doubles that record method calls."""

    def __init__(self):
        """Initialize spy with empty call history."""
        self._call_history: List[CallRecord] = []

    def _record_call(
        self,
        method_name: str,
        args: tuple,
        kwargs: dict,
        return_value: Any = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Record a method call."""
        self._call_history.append(
            CallRecord(
                method_name=method_name,
                args=args,
                kwargs=kwargs,
                return_value=return_value,
                exception=exception,
            )
        )

    def get_call_history(self) -> List[CallRecord]:
        """Get the complete call history."""
        return self._call_history.copy()

    def get_calls_for_method(self, method_name: str) -> List[CallRecord]:
        """Get call history for a specific method."""
        return [call for call in self._call_history if call.method_name == method_name]

    def was_called(self, method_name: str) -> bool:
        """Check if a method was called."""
        return any(call.method_name == method_name for call in self._call_history)

    def call_count(self, method_name: str) -> int:
        """Get the number of times a method was called."""
        return len(self.get_calls_for_method(method_name))

    def clear_history(self) -> None:
        """Clear the call history."""
        self._call_history.clear()


class StubLocationRepository(LocationRepository):
    """Stub implementation of LocationRepository with predefined responses."""

    def __init__(self):
        """Initialize stub with configurable responses."""
        self._locations: Dict[str, Any] = {}
        self._save_responses: Dict[str, Optional[Exception]] = {}
        self._find_responses: Dict[str, Any] = {}
        self._delete_responses: Dict[str, bool] = {}
        self._exists_responses: Dict[str, bool] = {}

    def configure_save_response(
        self, location_name: str, exception: Optional[Exception] = None
    ) -> None:
        """Configure response for save method."""
        self._save_responses[location_name] = exception

    def configure_find_response(self, location_name: str, location: Any) -> None:
        """Configure response for find_by_name method."""
        self._find_responses[location_name] = location

    def configure_delete_response(self, location_name: str, success: bool) -> None:
        """Configure response for delete method."""
        self._delete_responses[location_name] = success

    def configure_exists_response(self, location_name: str, exists: bool) -> None:
        """Configure response for exists method."""
        self._exists_responses[location_name] = exists

    def save(self, location: Any) -> None:
        """Save with configured response."""
        location_name = getattr(location, "name", "unknown")
        if (
            location_name in self._save_responses
            and self._save_responses[location_name]
        ):
            raise self._save_responses[location_name]
        self._locations[location_name] = location

    def find_by_name(self, name: str) -> Optional[Any]:
        """Find with configured response."""
        if name in self._find_responses:
            return self._find_responses[name]
        return self._locations.get(name)

    def find_all(self) -> List[Any]:
        """Return all configured locations."""
        return list(self._locations.values())

    def delete(self, name: str) -> bool:
        """Delete with configured response."""
        if name in self._delete_responses:
            result = self._delete_responses[name]
            if result and name in self._locations:
                del self._locations[name]
            return result

        if name in self._locations:
            del self._locations[name]
            return True
        return False

    def exists(self, name: str) -> bool:
        """Check existence with configured response."""
        if name in self._exists_responses:
            return self._exists_responses[name]
        return name in self._locations

    def clear(self) -> None:
        """Clear all locations."""
        self._locations.clear()


class SpyLocationRepository(LocationRepository, Spy):
    """Spy implementation of LocationRepository that records all method calls."""

    def __init__(self, base_repository: Optional[LocationRepository] = None):
        """Initialize spy with optional base repository."""
        LocationRepository.__init__(self)
        Spy.__init__(self)
        self._base_repository = base_repository or StubLocationRepository()

    def save(self, location: Any) -> None:
        """Save and record the call."""
        try:
            result = self._base_repository.save(location)
            self._record_call("save", (location,), {}, result)
            return result
        except Exception as e:
            self._record_call("save", (location,), {}, exception=e)
            raise

    def find_by_name(self, name: str) -> Optional[Any]:
        """Find and record the call."""
        try:
            result = self._base_repository.find_by_name(name)
            self._record_call("find_by_name", (name,), {}, result)
            return result
        except Exception as e:
            self._record_call("find_by_name", (name,), {}, exception=e)
            raise

    def find_all(self) -> List[Any]:
        """Find all and record the call."""
        try:
            result = self._base_repository.find_all()
            self._record_call("find_all", (), {}, result)
            return result
        except Exception as e:
            self._record_call("find_all", (), {}, exception=e)
            raise

    def delete(self, name: str) -> bool:
        """Delete and record the call."""
        try:
            result = self._base_repository.delete(name)
            self._record_call("delete", (name,), {}, result)
            return result
        except Exception as e:
            self._record_call("delete", (name,), {}, exception=e)
            raise

    def exists(self, name: str) -> bool:
        """Check existence and record the call."""
        try:
            result = self._base_repository.exists(name)
            self._record_call("exists", (name,), {}, result)
            return result
        except Exception as e:
            self._record_call("exists", (name,), {}, exception=e)
            raise

    def clear(self) -> None:
        """Clear and record the call."""
        try:
            result = self._base_repository.clear()
            self._record_call("clear", (), {}, result)
            return result
        except Exception as e:
            self._record_call("clear", (), {}, exception=e)
            raise


class FakeProgressTracker(ProgressTracker):
    """Fake implementation of ProgressTracker that simulates real behavior."""

    def __init__(self):
        """Initialize fake progress tracker."""
        self._current_operation: Optional[str] = None
        self._total: Optional[int] = None
        self._current: int = 0
        self._is_finished: bool = False
        self._messages: List[str] = []

    def start(self, operation: str, total: Optional[int] = None) -> None:
        """Start tracking an operation."""
        self._current_operation = operation
        self._total = total
        self._current = 0
        self._is_finished = False
        self._messages.append(f"Started: {operation}")

    def update(self, amount: int = 1, message: Optional[str] = None) -> None:
        """Update progress."""
        if self._current_operation is None:
            raise RuntimeError("No operation started")

        self._current += amount
        if message:
            self._messages.append(message)

    def finish(self, message: Optional[str] = None) -> None:
        """Finish tracking."""
        if self._current_operation is None:
            raise RuntimeError("No operation started")

        self._is_finished = True
        if message:
            self._messages.append(message)
        self._messages.append(f"Finished: {self._current_operation}")

    def set_total(self, total: int) -> None:
        """Set total progress amount."""
        self._total = total

    # Additional methods for testing
    def get_current_operation(self) -> Optional[str]:
        """Get current operation name."""
        return self._current_operation

    def get_progress(self) -> tuple[int, Optional[int]]:
        """Get current progress (current, total)."""
        return self._current, self._total

    def is_finished(self) -> bool:
        """Check if operation is finished."""
        return self._is_finished

    def get_messages(self) -> List[str]:
        """Get all progress messages."""
        return self._messages.copy()


class StubLogger(LoggerInterface):
    """Stub implementation of LoggerInterface."""

    def __init__(self):
        """Initialize stub logger."""
        self._logs: List[Dict[str, Any]] = []
        self._enabled_levels = {"DEBUG", "INFO", "WARNING", "ERROR"}

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        if "DEBUG" in self._enabled_levels:
            self._logs.append({"level": "DEBUG", "message": message, "kwargs": kwargs})

    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        if "INFO" in self._enabled_levels:
            self._logs.append({"level": "INFO", "message": message, "kwargs": kwargs})

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        if "WARNING" in self._enabled_levels:
            self._logs.append(
                {"level": "WARNING", "message": message, "kwargs": kwargs}
            )

    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        if "ERROR" in self._enabled_levels:
            self._logs.append({"level": "ERROR", "message": message, "kwargs": kwargs})

    def get_logs(self, level: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get logged messages."""
        if level:
            return [log for log in self._logs if log["level"] == level]
        return self._logs.copy()

    def clear_logs(self) -> None:
        """Clear all logs."""
        self._logs.clear()

    def set_enabled_levels(self, *levels: str) -> None:
        """Set which log levels are enabled."""
        self._enabled_levels = set(levels)


class MockLocationRepository:
    """Mock implementation of LocationRepository using unittest.mock."""

    def __init__(self):
        """Initialize mock repository."""
        self.save = Mock()
        self.find_by_name = Mock(return_value=None)
        self.find_all = Mock(return_value=[])
        self.delete = Mock(return_value=False)
        self.exists = Mock(return_value=False)
        self.clear = Mock()

    def configure_find_by_name(self, name_location_mapping: Dict[str, Any]) -> None:
        """Configure find_by_name responses."""
        self.find_by_name.side_effect = lambda name: name_location_mapping.get(name)

    def configure_exists(self, name_exists_mapping: Dict[str, bool]) -> None:
        """Configure exists responses."""
        self.exists.side_effect = lambda name: name_exists_mapping.get(name, False)

    def configure_delete(self, name_success_mapping: Dict[str, bool]) -> None:
        """Configure delete responses."""
        self.delete.side_effect = lambda name: name_success_mapping.get(name, False)


class DummyLocation:
    """Dummy location object that does nothing but satisfies interface."""

    def __init__(self, name: str = "dummy_location"):
        """Initialize dummy location."""
        self.name = name
        self.kinds = ["DISK"]
        self.config = {"protocol": "file", "path": "/dummy/path"}
        self.optional = False


class DummySimulation:
    """Dummy simulation object that does nothing but satisfies interface."""

    def __init__(self, simulation_id: str = "dummy_simulation"):
        """Initialize dummy simulation."""
        self.simulation_id = simulation_id
        self.path = "/dummy/path"
        self.model_id = "dummy_model"
        self.attrs = {}
        self.locations = {}
        self.namelists = {}
        self.snakemakes = {}


class TestDoubleFactory:
    """Factory for creating appropriate test doubles based on test requirements."""

    @staticmethod
    def create_location_repository(double_type: str, **kwargs) -> LocationRepository:
        """Create location repository test double."""
        if double_type == TestDoubleType.STUB:
            return StubLocationRepository()
        elif double_type == TestDoubleType.SPY:
            base = kwargs.get("base_repository")
            return SpyLocationRepository(base)
        elif double_type == TestDoubleType.MOCK:
            return MockLocationRepository()
        else:
            raise ValueError(f"Unsupported test double type: {double_type}")

    @staticmethod
    def create_progress_tracker(double_type: str, **kwargs) -> ProgressTracker:
        """Create progress tracker test double."""
        if double_type == TestDoubleType.FAKE:
            return FakeProgressTracker()
        elif double_type == TestDoubleType.MOCK:
            mock = Mock(spec=ProgressTracker)
            return mock
        else:
            raise ValueError(f"Unsupported test double type: {double_type}")

    @staticmethod
    def create_logger(double_type: str, **kwargs) -> LoggerInterface:
        """Create logger test double."""
        if double_type == TestDoubleType.STUB:
            return StubLogger()
        elif double_type == TestDoubleType.MOCK:
            mock = Mock(spec=LoggerInterface)
            return mock
        else:
            raise ValueError(f"Unsupported test double type: {double_type}")

    @staticmethod
    def create_dummy_location(**kwargs) -> Any:
        """Create dummy location object."""
        name = kwargs.get("name", "dummy_location")
        return DummyLocation(name)

    @staticmethod
    def create_dummy_simulation(**kwargs) -> Any:
        """Create dummy simulation object."""
        simulation_id = kwargs.get("simulation_id", "dummy_simulation")
        return DummySimulation(simulation_id)


class TestDoubleRegistry:
    """Registry for managing test doubles in a test environment."""

    def __init__(self):
        """Initialize empty registry."""
        self._doubles: Dict[str, Any] = {}
        self._spy_history: List[Spy] = []

    def register(self, name: str, test_double: Any) -> None:
        """Register a test double."""
        self._doubles[name] = test_double

        # Track spies for history management
        if isinstance(test_double, Spy):
            self._spy_history.append(test_double)

    def get(self, name: str) -> Any:
        """Get a registered test double."""
        if name not in self._doubles:
            raise KeyError(f"Test double '{name}' not registered")
        return self._doubles[name]

    def unregister(self, name: str) -> None:
        """Unregister a test double."""
        if name in self._doubles:
            double = self._doubles[name]
            if isinstance(double, Spy) and double in self._spy_history:
                self._spy_history.remove(double)
            del self._doubles[name]

    def clear_all_spy_history(self) -> None:
        """Clear history for all registered spies."""
        for spy in self._spy_history:
            spy.clear_history()

    def get_all_spy_calls(self) -> Dict[str, List[CallRecord]]:
        """Get call history from all registered spies."""
        result = {}
        for name, double in self._doubles.items():
            if isinstance(double, Spy):
                result[name] = double.get_call_history()
        return result

    def clear(self) -> None:
        """Clear all registered test doubles."""
        self._doubles.clear()
        self._spy_history.clear()


class TestDoubleBuilder:
    """Builder for creating complex test double configurations."""

    def __init__(self):
        """Initialize builder."""
        self._registry = TestDoubleRegistry()

    def with_stub_location_repository(
        self, name: str = "location_repository"
    ) -> "TestDoubleBuilder":
        """Add stub location repository."""
        repository = TestDoubleFactory.create_location_repository(TestDoubleType.STUB)
        self._registry.register(name, repository)
        return self

    def with_spy_location_repository(
        self,
        name: str = "location_repository",
        base: Optional[LocationRepository] = None,
    ) -> "TestDoubleBuilder":
        """Add spy location repository."""
        repository = TestDoubleFactory.create_location_repository(
            TestDoubleType.SPY, base_repository=base
        )
        self._registry.register(name, repository)
        return self

    def with_mock_location_repository(
        self, name: str = "location_repository"
    ) -> "TestDoubleBuilder":
        """Add mock location repository."""
        repository = TestDoubleFactory.create_location_repository(TestDoubleType.MOCK)
        self._registry.register(name, repository)
        return self

    def with_fake_progress_tracker(
        self, name: str = "progress_tracker"
    ) -> "TestDoubleBuilder":
        """Add fake progress tracker."""
        tracker = TestDoubleFactory.create_progress_tracker(TestDoubleType.FAKE)
        self._registry.register(name, tracker)
        return self

    def with_stub_logger(self, name: str = "logger") -> "TestDoubleBuilder":
        """Add stub logger."""
        logger = TestDoubleFactory.create_logger(TestDoubleType.STUB)
        self._registry.register(name, logger)
        return self

    def build(self) -> TestDoubleRegistry:
        """Build the test double registry."""
        return self._registry


# Convenience functions for common test double patterns
def create_unit_test_doubles() -> TestDoubleRegistry:
    """Create test doubles appropriate for unit tests (mocks and stubs)."""
    return (
        TestDoubleBuilder().with_mock_location_repository().with_stub_logger().build()
    )


def create_integration_test_doubles() -> TestDoubleRegistry:
    """Create test doubles appropriate for integration tests (fakes and spies)."""
    return (
        TestDoubleBuilder()
        .with_spy_location_repository()
        .with_fake_progress_tracker()
        .with_stub_logger()
        .build()
    )


def create_behavior_verification_doubles() -> TestDoubleRegistry:
    """Create test doubles for behavior verification (spies and mocks)."""
    return (
        TestDoubleBuilder()
        .with_spy_location_repository()
        .with_fake_progress_tracker()
        .build()
    )
