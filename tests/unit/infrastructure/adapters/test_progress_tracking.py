"""
Tests for progress tracking adapters.

Tests the infrastructure adapter for progress tracking utilities,
including fsspec callback integration, Rich progress display, and progress configuration.
"""

import time
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from rich.console import Console
from rich.progress import Progress, TaskID

from tellus.infrastructure.adapters.progress_tracking import (
    FSSpecProgressCallback, ProgressConfig, ProgressTracker,
    get_default_progress, get_progress_callback, set_progress_config)


@pytest.fixture
def mock_progress():
    """Create a mock Progress instance for testing."""
    mock_progress = Mock(spec=Progress)
    mock_progress.add_task = Mock(return_value=TaskID(1))
    mock_progress.update = Mock()
    mock_progress.start = Mock()
    mock_progress.stop_task = Mock()
    return mock_progress


@pytest.fixture
def mock_console():
    """Create a mock Console instance for testing."""
    return Mock(spec=Console)


class TestFSSpecProgressCallback:
    """Test suite for FSSpecProgressCallback."""
    
    def test_init_with_size(self, mock_progress):
        """Test initialization with known size."""
        callback = FSSpecProgressCallback(
            description="Test operation",
            size=1024,
            value=0,
            progress=mock_progress
        )
        
        assert callback.description == "Test operation"
        assert callback.size == 1024
        assert callback.value == 0
        
        # Should have added a task
        mock_progress.add_task.assert_called_once_with(
            "Test operation",
            total=1024,
            start=False,
            completed=0
        )
    
    def test_init_without_size(self, mock_progress):
        """Test initialization without known size."""
        callback = FSSpecProgressCallback(
            description="Test operation",
            size=None,
            progress=mock_progress
        )
        
        assert callback.description == "Test operation"
        assert callback.size is None
        
        # Should not have added a task
        mock_progress.add_task.assert_not_called()
    
    def test_init_with_initial_value(self, mock_progress):
        """Test initialization with initial progress value."""
        callback = FSSpecProgressCallback(
            description="Test operation",
            size=1024,
            value=256,
            progress=mock_progress
        )
        
        # Should have added a task with start=True
        mock_progress.add_task.assert_called_once_with(
            "Test operation",
            total=1024,
            start=True,
            completed=256
        )
    
    def test_init_creates_default_progress(self):
        """Test initialization creates default progress when none provided."""
        with patch('tellus.infrastructure.adapters.progress_tracking.get_default_progress') as mock_get_progress:
            mock_default_progress = Mock(spec=Progress)
            mock_get_progress.return_value = mock_default_progress
            
            callback = FSSpecProgressCallback(
                description="Test operation",
                size=1024
            )
            
            mock_get_progress.assert_called_once()
            mock_default_progress.start.assert_called_once()
    
    def test_context_manager(self, mock_progress):
        """Test context manager functionality."""
        with FSSpecProgressCallback(
            description="Test operation",
            size=1024,
            progress=mock_progress
        ) as callback:
            assert callback is not None
            assert isinstance(callback, FSSpecProgressCallback)
        
        # Should have cleaned up (stop_task called in close)
        mock_progress.update.assert_called()
        mock_progress.stop_task.assert_called()
    
    def test_set_description(self, mock_progress):
        """Test updating the description."""
        callback = FSSpecProgressCallback(
            description="Initial description",
            size=1024,
            progress=mock_progress
        )
        
        # Reset to track only new calls
        mock_progress.update.reset_mock()
        
        callback.set_description("New description")
        
        assert callback.description == "New description"
        mock_progress.update.assert_called_once_with(
            TaskID(1),
            description="New description"
        )
    
    def test_relative_update(self, mock_progress):
        """Test relative progress updates."""
        callback = FSSpecProgressCallback(
            description="Test operation",
            size=1024,
            progress=mock_progress
        )
        
        # Reset to track only new calls
        mock_progress.update.reset_mock()
        
        callback.relative_update(128)
        
        mock_progress.update.assert_called_once_with(
            TaskID(1),
            advance=128
        )
    
    def test_relative_update_default_increment(self, mock_progress):
        """Test relative update with default increment."""
        callback = FSSpecProgressCallback(
            description="Test operation",
            size=1024,
            progress=mock_progress
        )
        
        # Reset to track only new calls
        mock_progress.update.reset_mock()
        
        callback.relative_update()
        
        mock_progress.update.assert_called_once_with(
            TaskID(1),
            advance=1
        )
    
    def test_branch_callback_creation(self, mock_progress):
        """Test creation of branch callbacks."""
        parent_callback = FSSpecProgressCallback(
            description="Parent operation",
            size=1024,
            progress=mock_progress
        )
        
        branch_callback = parent_callback.branch(
            "/source/file.txt", 
            "/dest/file.txt",
            {"size": 512}
        )
        
        assert isinstance(branch_callback, FSSpecProgressCallback)
        assert branch_callback.description == "file.txt → file.txt"
        assert branch_callback.size == 512
        assert branch_callback._progress == mock_progress
    
    def test_call_updates_progress(self, mock_progress):
        """Test that call method updates progress."""
        callback = FSSpecProgressCallback(
            description="Test operation",
            size=1024,
            progress=mock_progress
        )
        
        # Reset to track only new calls
        mock_progress.update.reset_mock()
        
        # Simulate fsspec callback
        callback.size = 1024
        callback.value = 256
        callback.call()
        
        mock_progress.update.assert_called_with(
            TaskID(1),
            completed=256,
            total=1024,
            refresh=True
        )
    
    def test_close_cleanup(self, mock_progress):
        """Test proper cleanup on close."""
        callback = FSSpecProgressCallback(
            description="Test operation",
            size=1024,
            progress=mock_progress
        )
        
        callback.close()
        
        mock_progress.update.assert_called_with(TaskID(1), visible=False)
        mock_progress.stop_task.assert_called_once_with(TaskID(1))
        assert callback._task_id is None
    
    def test_close_without_task(self, mock_progress):
        """Test close when no task exists."""
        callback = FSSpecProgressCallback(
            description="Test operation",
            size=None,  # No size means no task
            progress=mock_progress
        )
        
        # Should not raise exception
        callback.close()
        
        # Should not have called stop_task since no task was created
        mock_progress.stop_task.assert_not_called()
    
    def test_disabled_callback(self, mock_progress):
        """Test callback with enable=False."""
        callback = FSSpecProgressCallback(
            description="Test operation",
            size=1024,
            enable=False,
            progress=mock_progress
        )
        
        # Should still initialize normally
        assert callback.enable is False
        assert callback.size == 1024


class TestProgressConfig:
    """Test suite for ProgressConfig."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = ProgressConfig()
        
        assert config.enabled is True
        assert config.show_speed is True
        assert config.show_eta is True
        assert config.show_bar is True
        assert config.show_value is True
        assert config.show_total is True
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = ProgressConfig(
            enabled=False,
            show_speed=False,
            show_eta=False,
            show_bar=False,
            show_value=False,
            show_total=False
        )
        
        assert config.enabled is False
        assert config.show_speed is False
        assert config.show_eta is False
        assert config.show_bar is False
        assert config.show_value is False
        assert config.show_total is False


class TestProgressConfiguration:
    """Test progress configuration management."""
    
    def test_set_progress_config(self):
        """Test updating progress configuration."""
        # Test updating specific settings
        set_progress_config(enabled=False, show_speed=False)
        
        # Get new progress to check configuration was applied
        with patch('tellus.infrastructure.adapters.progress_tracking._progress_config') as mock_config:
            mock_config.show_bar = True
            mock_config.show_value = True
            mock_config.show_speed = False  # Should be updated
            mock_config.show_eta = True
            mock_config.show_total = True
            
            progress = get_default_progress()
            
            # Should have created progress instance
            assert progress is not None
    
    def test_get_default_progress_all_columns(self):
        """Test default progress with all columns enabled."""
        with patch('tellus.infrastructure.adapters.progress_tracking._progress_config') as mock_config:
            mock_config.show_bar = True
            mock_config.show_value = True
            mock_config.show_speed = True
            mock_config.show_eta = True
            mock_config.show_total = True
            
            progress = get_default_progress()
            
            assert progress is not None
            # Should have created progress with multiple columns
            assert len(progress.columns) >= 5
    
    def test_get_default_progress_minimal_columns(self):
        """Test default progress with minimal columns."""
        with patch('tellus.infrastructure.adapters.progress_tracking._progress_config') as mock_config:
            mock_config.show_bar = False
            mock_config.show_value = False
            mock_config.show_speed = False
            mock_config.show_eta = False
            mock_config.show_total = False
            
            progress = get_default_progress()
            
            assert progress is not None
            # Should have at least the description column
            assert len(progress.columns) >= 1
    
    def test_get_progress_callback_factory(self):
        """Test progress callback factory function."""
        callback = get_progress_callback(
            description="Test callback",
            size=2048,
            enable=True
        )
        
        assert isinstance(callback, FSSpecProgressCallback)
        assert callback.description == "Test callback"
        assert callback.size == 2048
        assert callback.enable is True


class TestProgressTracker:
    """Test suite for ProgressTracker."""
    
    def test_init_default_values(self):
        """Test initialization with default values."""
        tracker = ProgressTracker()
        
        assert tracker._max_log_entries == 1000
        assert tracker._log_entries == {}
    
    def test_init_custom_max_entries(self):
        """Test initialization with custom max entries."""
        tracker = ProgressTracker(max_log_entries=500)
        
        assert tracker._max_log_entries == 500
    
    def test_log_progress_basic(self):
        """Test basic progress logging."""
        tracker = ProgressTracker()
        
        with patch('time.monotonic', return_value=12345.0), \
             patch('time.strftime', return_value='2023-01-01 12:00:00'):
            
            tracker.log_progress(
                identifier="test_op",
                progress=0.5,
                message="Processing files",
                metadata={"file_count": 10}
            )
        
        entries = tracker._log_entries["test_op"]
        assert len(entries) == 1
        
        entry = entries[0]
        assert entry["timestamp"] == 12345.0
        assert entry["datetime"] == "2023-01-01 12:00:00"
        assert entry["progress"] == 0.5
        assert entry["message"] == "Processing files"
        assert entry["metadata"] == {"file_count": 10}
    
    def test_log_progress_multiple_entries(self):
        """Test logging multiple progress entries."""
        tracker = ProgressTracker()
        
        with patch('time.monotonic', side_effect=[1.0, 2.0, 3.0]), \
             patch('time.strftime', side_effect=['12:00:00', '12:00:01', '12:00:02']):
            
            tracker.log_progress("test_op", 0.2, "Starting")
            tracker.log_progress("test_op", 0.5, "Halfway")
            tracker.log_progress("test_op", 1.0, "Complete")
        
        entries = tracker._log_entries["test_op"]
        assert len(entries) == 3
        
        assert entries[0]["progress"] == 0.2
        assert entries[1]["progress"] == 0.5
        assert entries[2]["progress"] == 1.0
    
    def test_log_progress_without_metadata(self):
        """Test logging progress without metadata."""
        tracker = ProgressTracker()
        
        tracker.log_progress("test_op", 0.5, "Processing")
        
        entry = tracker._log_entries["test_op"][0]
        assert entry["metadata"] == {}
    
    def test_log_progress_exceeds_limit(self):
        """Test that old entries are trimmed when limit is exceeded."""
        tracker = ProgressTracker(max_log_entries=3)
        
        # Add more entries than the limit
        for i in range(5):
            tracker.log_progress(f"test_op", i/4, f"Step {i}")
        
        entries = tracker._log_entries["test_op"]
        assert len(entries) == 3  # Should be trimmed to limit
        
        # Should keep the most recent entries
        assert entries[0]["message"] == "Step 2"
        assert entries[1]["message"] == "Step 3"
        assert entries[2]["message"] == "Step 4"
    
    def test_get_recent_log_entries(self):
        """Test retrieving recent log entries."""
        tracker = ProgressTracker()
        
        with patch('time.strftime', side_effect=[f'12:00:0{i}' for i in range(5)]):
            for i in range(5):
                tracker.log_progress("test_op", i/4, f"Step {i}")
        
        # Get recent entries
        recent = tracker.get_recent_log_entries("test_op", limit=3)
        
        assert len(recent) == 3
        assert "[12:00:02] Step 2 (50.0%)" in recent[0]
        assert "[12:00:03] Step 3 (75.0%)" in recent[1]
        assert "[12:00:04] Step 4 (100.0%)" in recent[2]
    
    def test_get_recent_log_entries_nonexistent(self):
        """Test retrieving log entries for nonexistent operation."""
        tracker = ProgressTracker()
        
        recent = tracker.get_recent_log_entries("nonexistent_op")
        
        assert recent == []
    
    def test_get_recent_log_entries_empty(self):
        """Test retrieving log entries for empty operation."""
        tracker = ProgressTracker()
        tracker._log_entries["empty_op"] = []
        
        recent = tracker.get_recent_log_entries("empty_op")
        
        assert recent == []
    
    def test_get_current_progress(self):
        """Test getting current progress value."""
        tracker = ProgressTracker()
        
        tracker.log_progress("test_op", 0.3, "Initial")
        tracker.log_progress("test_op", 0.7, "Updated")
        
        current = tracker.get_current_progress("test_op")
        
        assert current == 0.7  # Should be the most recent
    
    def test_get_current_progress_nonexistent(self):
        """Test getting current progress for nonexistent operation."""
        tracker = ProgressTracker()
        
        current = tracker.get_current_progress("nonexistent_op")
        
        assert current is None
    
    def test_get_current_progress_empty(self):
        """Test getting current progress for empty operation."""
        tracker = ProgressTracker()
        tracker._log_entries["empty_op"] = []
        
        current = tracker.get_current_progress("empty_op")
        
        assert current is None
    
    def test_clear_progress(self):
        """Test clearing progress entries."""
        tracker = ProgressTracker()
        
        tracker.log_progress("test_op", 0.5, "Processing")
        assert "test_op" in tracker._log_entries
        
        tracker.clear_progress("test_op")
        
        assert "test_op" not in tracker._log_entries
    
    def test_clear_progress_nonexistent(self):
        """Test clearing progress for nonexistent operation."""
        tracker = ProgressTracker()
        
        # Should not raise exception
        tracker.clear_progress("nonexistent_op")
        
        assert "nonexistent_op" not in tracker._log_entries


class TestIntegrationScenarios:
    """Test integration scenarios and real-world usage patterns."""
    
    def test_file_transfer_simulation(self, mock_progress):
        """Test simulating a file transfer with progress updates."""
        callback = FSSpecProgressCallback(
            description="Transferring large_file.nc",
            size=1024 * 1024,  # 1MB
            progress=mock_progress
        )
        
        # Simulate transfer progress
        for i in range(0, 1024 * 1024, 1024):
            callback.value = i
            callback.call()
        
        # Should have called update multiple times
        assert mock_progress.update.call_count >= 1000
    
    def test_multiple_concurrent_operations(self):
        """Test tracking multiple concurrent operations."""
        tracker = ProgressTracker()
        
        # Simulate multiple operations running concurrently
        operations = ["upload_1", "upload_2", "download_1"]
        
        for op in operations:
            for i in range(3):
                tracker.log_progress(op, (i + 1) / 3, f"{op} step {i + 1}")
        
        # Check all operations are tracked
        for op in operations:
            assert op in tracker._log_entries
            assert len(tracker._log_entries[op]) == 3
            assert tracker.get_current_progress(op) == 1.0
    
    def test_progress_callback_with_real_fsspec_pattern(self):
        """Test progress callback following real fsspec usage patterns."""
        # Create a callback that would be used with real fsspec
        callback = FSSpecProgressCallback(
            description="Downloading data.nc",
            size=10240,
            value=0
        )
        
        # Simulate fsspec calling the callback during transfer
        transfer_chunks = [1024, 2048, 1024, 2048, 2048, 2048]
        total_transferred = 0
        
        for chunk_size in transfer_chunks:
            total_transferred += chunk_size
            callback.size = 10240
            callback.value = total_transferred
            callback.call()
        
        assert callback.value == 10240  # Full transfer completed
    
    def test_branch_callbacks_hierarchy(self, mock_progress):
        """Test hierarchical branch callback creation."""
        parent_callback = FSSpecProgressCallback(
            description="Batch transfer",
            size=None,
            progress=mock_progress
        )
        
        # Create branch callbacks for individual files
        file_callbacks = []
        files = [
            ("data/file1.nc", "backup/file1.nc", 1024),
            ("data/file2.nc", "backup/file2.nc", 2048),
            ("data/file3.nc", "backup/file3.nc", 512)
        ]
        
        for src, dst, size in files:
            branch = parent_callback.branch(src, dst, {"size": size})
            file_callbacks.append(branch)
        
        assert len(file_callbacks) == 3
        for callback in file_callbacks:
            assert isinstance(callback, FSSpecProgressCallback)
            assert callback._progress == mock_progress
    
    def test_error_handling_during_progress(self, mock_progress):
        """Test error handling during progress updates."""
        callback = FSSpecProgressCallback(
            description="Error prone operation",
            size=1024,
            progress=mock_progress
        )
        
        # Simulate an error in progress update
        mock_progress.update.side_effect = Exception("Progress update failed")
        
        # Should not raise exception even if progress update fails
        try:
            callback.relative_update(100)
            callback.call()
        except Exception:
            pytest.fail("Progress callback should handle update errors gracefully")
    
    def test_large_scale_progress_tracking(self):
        """Test tracking progress for large-scale operations."""
        tracker = ProgressTracker(max_log_entries=10)
        
        # Simulate a large operation with many progress updates
        operation_id = "large_scale_operation"
        
        # Add many progress entries (more than max_log_entries)
        for i in range(100):
            tracker.log_progress(
                operation_id,
                i / 99,  # Progress from 0 to 1
                f"Processing batch {i}",
                metadata={"batch_id": i, "files_processed": i * 10}
            )
        
        # Should only keep the most recent entries
        entries = tracker._log_entries[operation_id]
        assert len(entries) == 10
        
        # Check that the most recent entries are kept
        assert entries[0]["message"] == "Processing batch 90"
        assert entries[-1]["message"] == "Processing batch 99"
        
        # Current progress should be 1.0
        assert tracker.get_current_progress(operation_id) == 1.0


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_progress_callback_zero_size(self, mock_progress):
        """Test progress callback with zero size."""
        callback = FSSpecProgressCallback(
            description="Zero size operation",
            size=0,
            progress=mock_progress
        )
        
        # Should still work but not create a task
        mock_progress.add_task.assert_not_called()
    
    def test_progress_callback_negative_values(self, mock_progress):
        """Test progress callback with negative values."""
        callback = FSSpecProgressCallback(
            description="Test operation",
            size=1024,
            value=-1,
            progress=mock_progress
        )
        
        # Should handle negative values gracefully
        assert callback.value == -1
    
    def test_progress_tracker_extreme_values(self):
        """Test progress tracker with extreme values."""
        tracker = ProgressTracker()
        
        # Test with extreme progress values
        tracker.log_progress("test_op", -0.5, "Negative progress")
        tracker.log_progress("test_op", 2.0, "Over 100% progress")
        tracker.log_progress("test_op", float('inf'), "Infinite progress")
        
        entries = tracker.get_recent_log_entries("test_op")
        assert len(entries) == 3
    
    def test_progress_callback_empty_description(self, mock_progress):
        """Test progress callback with empty description."""
        callback = FSSpecProgressCallback(
            description="",
            size=1024,
            progress=mock_progress
        )
        
        assert callback.description == ""
        mock_progress.add_task.assert_called_once_with(
            "",
            total=1024,
            start=False,
            completed=0
        )
    
    def test_progress_tracker_max_entries_zero(self):
        """Test progress tracker with zero max entries."""
        tracker = ProgressTracker(max_log_entries=0)
        
        tracker.log_progress("test_op", 0.5, "Test message")
        
        # Should still store at least one entry
        assert len(tracker._log_entries["test_op"]) == 0
    
    def test_progress_tracker_unicode_messages(self):
        """Test progress tracker with Unicode messages."""
        tracker = ProgressTracker()
        
        unicode_messages = [
            "Processing 文件.nc",
            "Downloading données.nc", 
            "Uploading файл.nc",
            "Progress: 🚀 100%"
        ]
        
        for i, message in enumerate(unicode_messages):
            tracker.log_progress("unicode_op", (i + 1) / len(unicode_messages), message)
        
        entries = tracker.get_recent_log_entries("unicode_op")
        assert len(entries) == 4
        
        # Should handle Unicode properly in formatting
        for entry in entries:
            assert isinstance(entry, str)