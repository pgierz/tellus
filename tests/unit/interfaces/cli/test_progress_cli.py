"""
Unit tests for progress tracking CLI commands.

These tests verify the progress tracking CLI interface layer,
including operation listing, detailed status, real-time monitoring,
operation control, summary statistics, and cleanup functionality.
"""

import asyncio
import json
from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
from click.testing import CliRunner

from tellus.application.dtos import (FilterOptions, OperationContextDto,
                                     OperationControlDto, PaginationInfo,
                                     ProgressMetricsDto, ThroughputMetricsDto)
from tellus.application.services.progress_tracking_service import \
    ProgressTrackingService
from tellus.interfaces.cli.progress import progress


class TestProgressListCommand:
    """Test cases for progress list command."""
    
    @pytest.fixture
    def mock_progress_service(self):
        """Mock progress tracking service."""
        service = Mock(spec=ProgressTrackingService)
        service.list_operations = AsyncMock()
        return service
    
    @pytest.fixture
    def sample_operations(self):
        """Sample progress tracking operations."""
        operations = []
        for i in range(3):
            op = Mock()
            op.operation_id = f"op-{i:04d}-abcd-efgh"
            op.operation_type = "file_transfer"
            op.operation_name = f"Transfer Operation {i}"
            op.status = ["pending", "running", "completed"][i]
            op.priority = "normal"
            op.created_time = 1672574400.0 + i * 3600
            op.last_update_time = 1672574500.0 + i * 3600
            op.duration_seconds = 120.5 + i * 60 if i > 0 else None
            op.current_metrics = Mock()
            op.current_metrics.percentage = 25.5 + i * 30
            operations.append(op)
        return operations
    
    @pytest.fixture
    def list_result(self, sample_operations):
        """Mock list operations result."""
        result = Mock()
        result.operations = sample_operations
        result.pagination = Mock()
        result.pagination.page = 1
        result.pagination.has_next = False
        result.pagination.has_previous = False
        return result
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_list_operations_success(self, mock_get_container, mock_progress_service, list_result):
        """Test successful listing of progress operations."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.list_operations.return_value = list_result
        
        runner = CliRunner()
        result = runner.invoke(progress, ['list-operations'])
        
        assert result.exit_code == 0
        assert "Progress Tracking Operations" in result.output
        assert "op-0000" in result.output
        assert "op-0001" in result.output
        assert "op-0002" in result.output
        assert "Transfer" in result.output and "Operation" in result.output
        assert "Pending" in result.output
        assert "Running" in result.output
        assert "Completed" in result.output
        
        # Verify service was called with correct parameters
        mock_progress_service.list_operations.assert_called_once()
        call_args = mock_progress_service.list_operations.call_args[0]
        assert isinstance(call_args[0], FilterOptions)
        assert isinstance(call_args[1], PaginationInfo)
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_list_operations_with_filters(self, mock_get_container, mock_progress_service, list_result):
        """Test listing operations with filters."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.list_operations.return_value = list_result
        
        runner = CliRunner()
        result = runner.invoke(progress, [
            'list-operations',
            '--status', 'running',
            '--operation-type', 'file_transfer',
            '--user-id', 'testuser',
            '--limit', '10',
            '--page', '2'
        ])
        
        assert result.exit_code == 0
        
        # Verify filters were applied to service call
        call_args = mock_progress_service.list_operations.call_args[0]
        filters = call_args[0]
        pagination = call_args[1]
        assert filters.search_term == 'file_transfer'
        assert pagination.page == 2
        assert pagination.page_size == 10
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_list_operations_json_output(self, mock_get_container, mock_progress_service, list_result):
        """Test listing operations with JSON output."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.list_operations.return_value = list_result
        
        runner = CliRunner()
        result = runner.invoke(progress, ['list-operations', '--json-output'])
        
        assert result.exit_code == 0
        
        # Verify JSON output is valid
        output_data = json.loads(result.output)
        assert isinstance(output_data, list)
        assert len(output_data) == 3
        assert output_data[0]['operation_id'] == 'op-0000-abcd-efgh'
        assert output_data[0]['operation_type'] == 'file_transfer'
        assert output_data[0]['operation_name'] == 'Transfer Operation 0'
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_list_operations_empty(self, mock_get_container, mock_progress_service):
        """Test listing when no operations exist."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        
        empty_result = Mock()
        empty_result.operations = []
        empty_result.pagination = Mock()
        empty_result.pagination.has_next = False
        empty_result.pagination.has_previous = False
        mock_progress_service.list_operations.return_value = empty_result
        
        runner = CliRunner()
        result = runner.invoke(progress, ['list-operations'])
        
        assert result.exit_code == 0
        # Should show empty table with header
        assert "Progress Tracking Operations" in result.output


class TestProgressShowCommand:
    """Test cases for progress show command."""
    
    @pytest.fixture
    def mock_progress_service(self):
        """Mock progress tracking service."""
        service = Mock(spec=ProgressTrackingService)
        service.get_operation = AsyncMock()
        return service
    
    @pytest.fixture
    def sample_operation(self):
        """Sample detailed operation for testing."""
        op = Mock()
        op.operation_id = "op-detailed-1234-5678"
        op.operation_type = "archive_extraction"
        op.operation_name = "Extract Climate Data Archive"
        op.status = "running"
        op.priority = "high"
        op.created_time = 1672574400.0
        op.started_time = 1672574500.0
        op.completed_time = None
        op.last_update_time = 1672575000.0
        op.duration_seconds = 600.5
        op.current_metrics = Mock()
        op.current_metrics.percentage = 67.5
        op.current_metrics.current_value = 675
        op.current_metrics.total_value = 1000
        op.current_metrics.bytes_processed = 2048000000  # 2GB
        op.current_metrics.total_bytes = 3072000000      # 3GB
        op.current_metrics.files_processed = 150
        op.current_metrics.total_files = 200
        op.current_throughput = Mock()
        op.current_throughput.bytes_per_second = 3407872.0  # ~3.25MB/s
        op.current_throughput.files_per_second = 0.25
        op.context = Mock()
        op.context.user_id = "testuser"
        op.context.session_id = "session-123"
        op.context.simulation_id = "sim-climate-2023"
        op.context.location_name = "hpc-cluster"
        op.context.tags = {"priority", "climate-data"}
        op.error_message = None
        op.warnings = ["Large file detected", "Network latency high"]
        return op
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_show_operation_success(self, mock_get_container, mock_progress_service, sample_operation):
        """Test successful display of operation details."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.get_operation.return_value = sample_operation
        
        runner = CliRunner()
        result = runner.invoke(progress, ['show', 'op-detailed-1234-5678'])
        
        assert result.exit_code == 0
        assert "Operation Details: Extract Climate Data Archive" in result.output
        assert "op-detailed-1234-5678" in result.output
        assert "archive_extraction" in result.output
        assert "🔄 Running" in result.output
        assert "67.5%" in result.output
        assert "675/1000" in result.output
        assert "150/200" in result.output
        assert "testuser" in result.output
        assert "sim-climate-2023" in result.output
        assert "hpc-cluster" in result.output
        assert "priority, climate-data" in result.output
        assert "Large file detected" in result.output
        assert "Network latency high" in result.output
        
        mock_progress_service.get_operation.assert_called_once_with("op-detailed-1234-5678")
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_show_operation_json_output(self, mock_get_container, mock_progress_service, sample_operation):
        """Test showing operation details with JSON output."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.get_operation.return_value = sample_operation
        
        runner = CliRunner()
        result = runner.invoke(progress, ['show', 'op-detailed-1234-5678', '--json-output'])
        
        assert result.exit_code == 0
        
        # Verify JSON output is valid
        output_data = json.loads(result.output)
        assert output_data['operation_id'] == 'op-detailed-1234-5678'
        assert output_data['operation_type'] == 'archive_extraction'
        assert output_data['status'] == 'running'
        assert output_data['current_metrics']['percentage'] == 67.5
        assert output_data['context']['user_id'] == 'testuser'
        assert output_data['context']['simulation_id'] == 'sim-climate-2023'
        assert len(output_data['warnings']) == 2
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_show_operation_not_found(self, mock_get_container, mock_progress_service):
        """Test showing non-existent operation."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.get_operation.return_value = None
        
        runner = CliRunner()
        result = runner.invoke(progress, ['show', 'nonexistent'])
        
        assert result.exit_code == 1
        assert "Operation nonexistent not found" in result.output


class TestProgressControlCommand:
    """Test cases for progress control command."""
    
    @pytest.fixture
    def mock_progress_service(self):
        """Mock progress tracking service."""
        service = Mock(spec=ProgressTrackingService)
        service.control_operation = AsyncMock()
        return service
    
    @pytest.fixture
    def successful_control_result(self):
        """Mock successful control result."""
        result = Mock()
        result.success = True
        result.message = "Operation paused successfully"
        result.previous_status = "running"
        result.new_status = "paused"
        return result
    
    @pytest.fixture
    def failed_control_result(self):
        """Mock failed control result."""
        result = Mock()
        result.success = False
        result.message = "Operation cannot be cancelled - already completed"
        return result
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_control_operation_success(self, mock_get_container, mock_progress_service, successful_control_result):
        """Test successful operation control."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.control_operation.return_value = successful_control_result
        
        runner = CliRunner()
        result = runner.invoke(progress, ['control', 'op-1234', 'pause'])
        
        assert result.exit_code == 0
        assert "Operation paused successfully" in result.output
        assert "running" in result.output
        assert "paused" in result.output
        
        # Verify correct DTO was passed
        mock_progress_service.control_operation.assert_called_once()
        call_args = mock_progress_service.control_operation.call_args[0][0]
        assert isinstance(call_args, OperationControlDto)
        assert call_args.operation_id == "op-1234"
        assert call_args.command == "pause"
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_control_operation_with_reason(self, mock_get_container, mock_progress_service, successful_control_result):
        """Test operation control with reason."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        successful_control_result.message = "Operation cancelled: User requested"
        successful_control_result.new_status = "cancelled"
        mock_progress_service.control_operation.return_value = successful_control_result
        
        runner = CliRunner()
        result = runner.invoke(progress, [
            'control', 'op-1234', 'cancel', 
            '--reason', 'User requested'
        ])
        
        assert result.exit_code == 0
        assert "Operation cancelled: User requested" in result.output
        
        # Verify reason was passed
        call_args = mock_progress_service.control_operation.call_args[0][0]
        assert call_args.reason == "User requested"
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_control_operation_failure(self, mock_get_container, mock_progress_service, failed_control_result):
        """Test failed operation control."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.control_operation.return_value = failed_control_result
        
        runner = CliRunner()
        result = runner.invoke(progress, ['control', 'op-1234', 'cancel'])
        
        assert result.exit_code == 1
        assert "Operation cannot be cancelled - already completed" in result.output


class TestProgressMonitorCommand:
    """Test cases for progress monitor command."""
    
    @pytest.fixture
    def mock_progress_service(self):
        """Mock progress tracking service."""
        service = Mock(spec=ProgressTrackingService)
        service.get_operation = AsyncMock()
        return service
    
    @pytest.fixture
    def sample_operation_for_monitoring(self):
        """Sample operation for monitoring."""
        op = Mock()
        op.operation_id = "op-monitor-1234"
        op.operation_name = "File Transfer Monitor Test"
        op.status = "running"
        op.current_metrics = Mock()
        op.current_metrics.percentage = 45.0
        op.last_update_time = 1672574500.0
        return op
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_monitor_operation_single_check(self, mock_get_container, mock_progress_service, sample_operation_for_monitoring):
        """Test single status check (non-following mode)."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.get_operation.return_value = sample_operation_for_monitoring
        
        runner = CliRunner()
        result = runner.invoke(progress, ['monitor', 'op-monitor-1234'])
        
        assert result.exit_code == 0
        assert "File Transfer Monitor Test" in result.output
        assert "running" in result.output
        assert "45.0%" in result.output
        
        mock_progress_service.get_operation.assert_called_once_with("op-monitor-1234")
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_monitor_operation_not_found(self, mock_get_container, mock_progress_service):
        """Test monitoring non-existent operation."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.get_operation.return_value = None
        
        runner = CliRunner()
        result = runner.invoke(progress, ['monitor', 'nonexistent'])
        
        assert result.exit_code == 1
        assert "Operation nonexistent not found" in result.output


class TestProgressSummaryCommand:
    """Test cases for progress summary command."""
    
    @pytest.fixture
    def mock_progress_service(self):
        """Mock progress tracking service."""
        service = Mock(spec=ProgressTrackingService)
        service.get_summary = AsyncMock()
        return service
    
    @pytest.fixture
    def sample_summary(self):
        """Sample summary statistics."""
        summary = Mock()
        summary.total_operations = 150
        summary.active_operations = 5
        summary.completed_operations = 140
        summary.failed_operations = 3
        summary.cancelled_operations = 2
        summary.operations_by_type = {
            'file_transfer': 80,
            'archive_extraction': 45,
            'data_processing': 25
        }
        summary.operations_by_status = {
            'completed': 140,
            'running': 3,
            'failed': 3,
            'pending': 2,
            'cancelled': 2
        }
        summary.total_bytes_processed = 5368709120  # 5GB
        summary.average_completion_time = 300.5     # 5 minutes
        return summary
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_summary_success(self, mock_get_container, mock_progress_service, sample_summary):
        """Test successful summary display."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.get_summary.return_value = sample_summary
        
        runner = CliRunner()
        result = runner.invoke(progress, ['summary'])
        
        assert result.exit_code == 0
        assert "Progress Tracking Summary" in result.output
        assert "150" in result.output  # Total operations
        assert "5" in result.output    # Active operations
        assert "140" in result.output  # Completed operations
        assert "5.0 GB" in result.output  # Total data processed
        assert "5.0 minutes" in result.output  # Average completion time
        assert "Operations by Type:" in result.output
        assert "file_transfer: 80" in result.output
        assert "archive_extraction: 45" in result.output
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_summary_json_output(self, mock_get_container, mock_progress_service, sample_summary):
        """Test summary with JSON output."""
        mock_container = Mock()
        mock_container.progress_tracking_service = mock_progress_service
        mock_get_container.return_value = mock_container
        mock_progress_service.get_summary.return_value = sample_summary
        
        runner = CliRunner()
        result = runner.invoke(progress, ['summary', '--json-output'])
        
        assert result.exit_code == 0
        
        # Verify JSON output is valid
        output_data = json.loads(result.output)
        assert output_data['total_operations'] == 150
        assert output_data['active_operations'] == 5
        assert output_data['completed_operations'] == 140
        assert output_data['total_bytes_processed'] == 5368709120
        assert output_data['operations_by_type']['file_transfer'] == 80


class TestProgressCleanupCommand:
    """Test cases for progress cleanup command."""
    
    @pytest.fixture
    def mock_service_container(self):
        """Mock service container with repository access."""
        container = Mock()
        service_factory = Mock()
        progress_service = Mock()
        repository = Mock()
        repository.cleanup_completed_operations = AsyncMock()
        
        progress_service._repository = repository
        service_factory._progress_tracking_service = progress_service
        container.service_factory = service_factory
        return container, repository
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_cleanup_operations_success(self, mock_get_container, mock_service_container):
        """Test successful cleanup of operations."""
        container, repository = mock_service_container
        mock_get_container.return_value = container
        repository.cleanup_completed_operations.return_value = 25
        
        runner = CliRunner()
        result = runner.invoke(progress, ['cleanup', '--older-than-hours', '48'])
        
        assert result.exit_code == 0
        assert "Cleaned up 25 completed operations" in result.output
        
        # Verify cleanup was called with correct parameters
        repository.cleanup_completed_operations.assert_called_once_with(172800.0, True)  # 48 hours in seconds
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_cleanup_operations_none_needed(self, mock_get_container, mock_service_container):
        """Test cleanup when no operations need cleaning."""
        container, repository = mock_service_container
        mock_get_container.return_value = container
        repository.cleanup_completed_operations.return_value = 0
        
        runner = CliRunner()
        result = runner.invoke(progress, ['cleanup'])
        
        assert result.exit_code == 0
        assert "No operations needed cleanup" in result.output
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_cleanup_operations_dry_run(self, mock_get_container, mock_service_container):
        """Test cleanup in dry run mode."""
        container, repository = mock_service_container
        mock_get_container.return_value = container
        
        runner = CliRunner()
        result = runner.invoke(progress, ['cleanup', '--dry-run'])
        
        assert result.exit_code == 0
        assert "Dry run: would clean up operations" in result.output
        
        # Verify cleanup was NOT called in dry run mode
        repository.cleanup_completed_operations.assert_not_called()


class TestProgressCLIErrorHandling:
    """Test CLI error handling scenarios."""
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_service_unavailable_list(self, mock_get_container):
        """Test CLI behavior when progress service is unavailable for list."""
        mock_get_container.side_effect = Exception("Service initialization failed")
        
        runner = CliRunner()
        # Test that the exception is propagated
        with pytest.raises(Exception, match="Service initialization failed"):
            runner.invoke(progress, ['list-operations'], catch_exceptions=False)
    
    @patch('tellus.interfaces.cli.progress.get_service_container')
    def test_service_error_show(self, mock_get_container):
        """Test error handling in show command."""
        mock_container = Mock()
        mock_service = Mock(spec=ProgressTrackingService)
        mock_service.get_operation = AsyncMock()
        mock_service.get_operation.side_effect = Exception("Database connection failed")
        mock_container.progress_tracking_service = mock_service
        mock_get_container.return_value = mock_container
        
        runner = CliRunner()
        result = runner.invoke(progress, ['show', 'op-1234'])
        
        assert result.exit_code == 1
        assert "Error getting operation details:" in result.output
        assert "Database connection failed" in result.output


class TestProgressCLIHelpAndValidation:
    """Test CLI help and argument validation."""
    
    def test_progress_cli_help_commands(self):
        """Test help output for progress CLI commands."""
        runner = CliRunner()
        
        # Test main progress help
        result = runner.invoke(progress, ['--help'])
        assert result.exit_code == 0
        assert "progress" in result.output.lower()
        
        # Test subcommand help
        subcommands = ['list-operations', 'show', 'control', 'monitor', 'summary', 'cleanup']
        for subcmd in subcommands:
            result = runner.invoke(progress, [subcmd, '--help'])
            assert result.exit_code == 0
    
    def test_show_command_argument_validation(self):
        """Test show command argument validation."""
        runner = CliRunner()
        
        # Test missing required argument
        result = runner.invoke(progress, ['show'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
    
    def test_control_command_argument_validation(self):
        """Test control command argument validation."""
        runner = CliRunner()
        
        # Test missing operation ID
        result = runner.invoke(progress, ['control'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
        
        # Test invalid command choice
        result = runner.invoke(progress, ['control', 'op-1234', 'invalid-command'])
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "Choice" in result.output
        
    def test_monitor_command_argument_validation(self):
        """Test monitor command argument validation."""
        runner = CliRunner()
        
        # Test missing operation ID
        result = runner.invoke(progress, ['monitor'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
    
    def test_status_filter_validation(self):
        """Test status filter validation in list command."""
        runner = CliRunner()
        
        # Test invalid status filter
        result = runner.invoke(progress, ['list-operations', '--status', 'invalid-status'])
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "Choice" in result.output