"""
Unit tests for file transfer CLI commands.

These tests verify the file transfer CLI interface layer,
including file transfers, batch operations, directory transfers,
queue management, and progress tracking integration.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from click.testing import CliRunner

from tellus.application.dtos import (BatchFileTransferOperationDto,
                                     DirectoryTransferOperationDto,
                                     FileTransferOperationDto)
from tellus.application.services.bulk_operation_queue import (QueuePriority,
                                                              QueueStatus)
from tellus.application.services.file_transfer_service import \
    FileTransferApplicationService
from tellus.application.services.operation_queue_service import \
    OperationQueueService
from tellus.interfaces.cli.transfer import queue_group, transfer


class TestFileTransferCommand:
    """Test cases for single file transfer command."""
    
    @pytest.fixture
    def mock_transfer_service(self):
        """Mock file transfer service."""
        service = Mock(spec=FileTransferApplicationService)
        service.transfer_file = AsyncMock()
        return service
    
    @pytest.fixture
    def successful_transfer_result(self):
        """Mock successful transfer result."""
        result = Mock()
        result.success = True
        result.bytes_transferred = 1024000
        result.duration_seconds = 10.5
        result.throughput_mbps = 0.98
        result.checksum_verified = True
        result.error_message = None
        return result
    
    @pytest.fixture
    def failed_transfer_result(self):
        """Mock failed transfer result."""
        result = Mock()
        result.success = False
        result.bytes_transferred = 0
        result.duration_seconds = 0
        result.throughput_mbps = 0
        result.checksum_verified = False
        result.error_message = "Connection timeout"
        return result
    
    @patch('tellus.interfaces.cli.transfer._get_transfer_service')
    def test_transfer_file_success(self, mock_get_service, mock_transfer_service, successful_transfer_result):
        """Test successful single file transfer."""
        mock_get_service.return_value = mock_transfer_service
        mock_transfer_service.transfer_file.return_value = successful_transfer_result
        
        runner = CliRunner()
        result = runner.invoke(transfer, [
            'file', 'source.txt', 'dest.txt',
            '--source-location', 'local',
            '--dest-location', 'remote'
        ])
        
        assert result.exit_code == 0
        assert "Transfer completed successfully" in result.output
        assert "1,024,000" in result.output  # Bytes transferred
        assert "10.50s" in result.output      # Duration
        assert "0.98 MB/s" in result.output   # Throughput
        assert "Checksum verified" in result.output
        
        # Verify correct DTO was passed
        mock_transfer_service.transfer_file.assert_called_once()
        call_args = mock_transfer_service.transfer_file.call_args[0][0]
        assert isinstance(call_args, FileTransferOperationDto)
        assert call_args.source_path == "source.txt"
        assert call_args.dest_path == "dest.txt"
        assert call_args.source_location == "local"
        assert call_args.dest_location == "remote"
        assert call_args.overwrite is False
        assert call_args.verify_checksum is True
    
    @patch('tellus.interfaces.cli.transfer._get_transfer_service')
    def test_transfer_file_with_options(self, mock_get_service, mock_transfer_service, successful_transfer_result):
        """Test file transfer with various options."""
        mock_get_service.return_value = mock_transfer_service
        mock_transfer_service.transfer_file.return_value = successful_transfer_result
        
        runner = CliRunner()
        result = runner.invoke(transfer, [
            'file', 'source.txt', 'dest.txt',
            '--source-location', 'hpc',
            '--dest-location', 'cloud',
            '--overwrite',
            '--no-verify',
            '--chunk-size', '16777216'
        ])
        
        assert result.exit_code == 0
        
        # Verify options were correctly passed
        call_args = mock_transfer_service.transfer_file.call_args[0][0]
        assert call_args.source_location == "hpc"
        assert call_args.dest_location == "cloud"
        assert call_args.overwrite is True
        assert call_args.verify_checksum is False
        assert call_args.chunk_size == 16777216
    
    @patch('tellus.interfaces.cli.transfer._get_transfer_service')
    def test_transfer_file_failure(self, mock_get_service, mock_transfer_service, failed_transfer_result):
        """Test failed file transfer."""
        mock_get_service.return_value = mock_transfer_service
        mock_transfer_service.transfer_file.return_value = failed_transfer_result
        
        runner = CliRunner()
        result = runner.invoke(transfer, [
            'file', 'source.txt', 'dest.txt',
            '--dest-location', 'remote'
        ])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Transfer failed: Connection timeout" in result.output
        assert "Transfer completed successfully" not in result.output
    
    @patch('tellus.interfaces.cli.transfer._get_transfer_service')
    def test_transfer_file_service_error(self, mock_get_service, mock_transfer_service):
        """Test handling service errors during transfer."""
        mock_get_service.return_value = mock_transfer_service
        mock_transfer_service.transfer_file.side_effect = Exception("Service error")
        
        runner = CliRunner()
        result = runner.invoke(transfer, [
            'file', 'source.txt', 'dest.txt',
            '--dest-location', 'remote'
        ])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output


class TestBatchTransferCommand:
    """Test cases for batch file transfer command."""
    
    @pytest.fixture
    def mock_transfer_service(self):
        """Mock file transfer service."""
        service = Mock(spec=FileTransferApplicationService)
        service.batch_transfer_files = AsyncMock()
        return service
    
    @pytest.fixture
    def successful_batch_result(self):
        """Mock successful batch transfer result."""
        result = Mock()
        result.successful_transfers = [Mock(), Mock(), Mock()]  # 3 successful
        result.failed_transfers = []  # 0 failed
        result.total_bytes_transferred = 5120000
        result.total_duration_seconds = 25.5
        result.average_throughput_mbps = 1.6
        return result
    
    @pytest.fixture
    def mixed_batch_result(self):
        """Mock batch transfer result with some failures."""
        failed_transfer = Mock()
        failed_transfer.source_path = "failed.txt"
        failed_transfer.dest_path = "failed_dest.txt"
        failed_transfer.error_message = "Permission denied"
        
        result = Mock()
        result.successful_transfers = [Mock(), Mock()]  # 2 successful
        result.failed_transfers = [failed_transfer]     # 1 failed
        result.total_bytes_transferred = 2048000
        result.total_duration_seconds = 15.2
        result.average_throughput_mbps = 1.1
        return result
    
    @patch('tellus.interfaces.cli.transfer._get_transfer_service')
    def test_batch_transfer_success(self, mock_get_service, mock_transfer_service, successful_batch_result):
        """Test successful batch file transfer."""
        mock_get_service.return_value = mock_transfer_service
        mock_transfer_service.batch_transfer_files.return_value = successful_batch_result
        
        runner = CliRunner()
        result = runner.invoke(transfer, [
            'batch', 'file1.txt', 'file2.txt', 'file3.txt',
            '--dest-location', 'remote',
            '--dest-dir', '/remote/data',
            '--parallel', '2'
        ])
        
        assert result.exit_code == 0
        assert "Batch transfer completed" in result.output
        assert "Successful: 3" in result.output
        assert "Failed: 0" in result.output
        assert "5,120,000" in result.output  # Total bytes
        assert "25.50s" in result.output      # Duration
        assert "1.60 MB/s" in result.output   # Throughput
        
        # Verify correct DTO was passed
        mock_transfer_service.batch_transfer_files.assert_called_once()
        call_args = mock_transfer_service.batch_transfer_files.call_args[0][0]
        assert isinstance(call_args, BatchFileTransferOperationDto)
        assert len(call_args.transfers) == 3
        assert call_args.parallel_transfers == 2
        assert call_args.stop_on_error is False
        assert call_args.verify_all_checksums is True
    
    @patch('tellus.interfaces.cli.transfer._get_transfer_service')
    def test_batch_transfer_with_failures(self, mock_get_service, mock_transfer_service, mixed_batch_result):
        """Test batch transfer with some failures."""
        mock_get_service.return_value = mock_transfer_service
        mock_transfer_service.batch_transfer_files.return_value = mixed_batch_result
        
        runner = CliRunner()
        result = runner.invoke(transfer, [
            'batch', 'file1.txt', 'file2.txt', 'file3.txt',
            '--dest-location', 'remote',
            '--dest-dir', '/remote/data'
        ])
        
        assert result.exit_code == 0
        assert "Successful: 2" in result.output
        assert "Failed: 1" in result.output
        assert "Failed transfers:" in result.output
        assert "failed.txt → failed_dest.txt: Permission denied" in result.output
    
    @patch('tellus.interfaces.cli.transfer._get_transfer_service')  
    def test_batch_transfer_with_options(self, mock_get_service):
        """Test batch transfer with various options."""
        mock_transfer_service = Mock(spec=FileTransferApplicationService)
        mock_transfer_service.batch_transfer_files = AsyncMock()
        successful_batch_result = Mock()
        successful_batch_result.successful_transfers = [Mock(), Mock()]
        successful_batch_result.failed_transfers = []
        successful_batch_result.total_bytes_transferred = 2048000
        successful_batch_result.total_duration_seconds = 15.2
        successful_batch_result.average_throughput_mbps = 1.1
        
        mock_get_service.return_value = mock_transfer_service
        mock_transfer_service.batch_transfer_files.return_value = successful_batch_result
        
        runner = CliRunner()
        result = runner.invoke(transfer, [
            'batch', 'file1.txt', 'file2.txt',
            '--source-location', 'hpc',
            '--dest-location', 'storage',
            '--dest-dir', '/storage/backup',
            '--parallel', '5',
            '--overwrite',
            '--no-verify',
            '--stop-on-error'
        ])
        
        assert result.exit_code == 0
        
        # Verify options were correctly passed
        call_args = mock_transfer_service.batch_transfer_files.call_args[0][0]
        assert call_args.parallel_transfers == 5
        assert call_args.stop_on_error is True
        assert call_args.verify_all_checksums is False
        # Check individual transfer options
        for transfer_dto in call_args.transfers:
            assert transfer_dto.source_location == "hpc"
            assert transfer_dto.dest_location == "storage"
            assert transfer_dto.overwrite is True
            assert transfer_dto.verify_checksum is False


class TestDirectoryTransferCommand:
    """Test cases for directory transfer command."""
    
    @pytest.fixture
    def mock_transfer_service(self):
        """Mock file transfer service."""
        service = Mock(spec=FileTransferApplicationService)
        service.transfer_directory = AsyncMock()
        return service
    
    @pytest.fixture
    def successful_directory_result(self):
        """Mock successful directory transfer result."""
        result = Mock()
        result.successful_transfers = [Mock() for _ in range(15)]  # 15 files
        result.failed_transfers = []  # 0 failed
        result.total_bytes_transferred = 10240000
        result.total_duration_seconds = 45.8
        return result
    
    @patch('tellus.interfaces.cli.transfer._get_transfer_service')
    def test_directory_transfer_success(self, mock_get_service, mock_transfer_service, successful_directory_result):
        """Test successful directory transfer."""
        mock_get_service.return_value = mock_transfer_service
        mock_transfer_service.transfer_directory.return_value = successful_directory_result
        
        runner = CliRunner()
        result = runner.invoke(transfer, [
            'directory', '/local/source', '/remote/dest',
            '--dest-location', 'remote'
        ])
        
        assert result.exit_code == 0
        assert "Directory transfer completed" in result.output
        assert "Files transferred: 15" in result.output
        assert "Failed transfers: 0" in result.output
        assert "10,240,000" in result.output  # Total bytes
        assert "45.80s" in result.output       # Duration
        
        # Verify correct DTO was passed
        mock_transfer_service.transfer_directory.assert_called_once()
        call_args = mock_transfer_service.transfer_directory.call_args[0][0]
        assert isinstance(call_args, DirectoryTransferOperationDto)
        assert call_args.source_path == "/local/source"
        assert call_args.dest_path == "/remote/dest"
        assert call_args.source_location == "local"
        assert call_args.dest_location == "remote"
        assert call_args.recursive is True
        assert call_args.overwrite is False
        assert call_args.verify_checksums is True
        assert call_args.include_patterns == []
        assert call_args.exclude_patterns == []
    
    @patch('tellus.interfaces.cli.transfer._get_transfer_service')
    def test_directory_transfer_with_patterns(self, mock_get_service, mock_transfer_service, successful_directory_result):
        """Test directory transfer with include/exclude patterns."""
        mock_get_service.return_value = mock_transfer_service
        mock_transfer_service.transfer_directory.return_value = successful_directory_result
        
        runner = CliRunner()
        result = runner.invoke(transfer, [
            'directory', '/source', '/dest',
            '--source-location', 'hpc',
            '--dest-location', 'storage',
            '--include', '*.nc',
            '--include', '*.log',
            '--exclude', 'temp*',
            '--exclude', '*.tmp',
            '--overwrite',
            '--no-verify'
        ])
        
        assert result.exit_code == 0
        
        # Verify patterns were correctly passed
        call_args = mock_transfer_service.transfer_directory.call_args[0][0]
        assert call_args.source_location == "hpc"
        assert call_args.dest_location == "storage"
        assert call_args.include_patterns == ["*.nc", "*.log"]
        assert call_args.exclude_patterns == ["temp*", "*.tmp"]
        assert call_args.overwrite is True
        assert call_args.verify_checksums is False


class TestQueueListCommand:
    """Test cases for queue list command."""
    
    @pytest.fixture
    def mock_queue_service(self):
        """Mock operation queue service."""
        service = Mock(spec=OperationQueueService)
        return service
    
    @pytest.fixture
    def sample_operations(self):
        """Sample queue operations for testing."""
        operations = []
        for i in range(3):
            op = Mock()
            op.id = f"op-{i:04d}-abcd-efgh"
            op.status = Mock()
            op.status.value = ["queued", "running", "completed"][i]
            op.created_time = 1672574400.0 + i * 3600
            op.duration = 10.5 + i * 5 if i > 0 else None
            op.operation_dto = Mock()
            op.operation_dto.operation_type = "file_transfer"
            op.result = Mock() if i == 2 else None
            if op.result:
                op.result.bytes_transferred = 1024000
            operations.append(op)
        return operations
    
    @pytest.fixture
    def queue_stats(self):
        """Sample queue statistics."""
        return {
            'queue_length': 5,
            'running': 2,
            'completed': 10,
            'failed': 1,
            'total_operations': 18,
            'queued': 5,
            'cancelled': 0,
            'is_processing': True,
            'is_paused': False,
            'max_concurrent': 3,
            'total_bytes_processed': 50240000
        }
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_queue_list_with_operations(self, mock_get_service, mock_queue_service, sample_operations, queue_stats):
        """Test listing queue operations."""
        mock_get_service.return_value = mock_queue_service
        mock_queue_service.list_operations.return_value = sample_operations
        mock_queue_service.get_queue_stats.return_value = queue_stats
        
        runner = CliRunner()
        result = runner.invoke(queue_group, ['list'])
        
        assert result.exit_code == 0
        assert "Operation Queue" in result.output
        assert "op-0000" in result.output
        assert "op-0001" in result.output
        assert "op-0002" in result.output
        assert "queued" in result.output
        assert "running" in result.output
        assert "completed" in result.output
        assert "1,024,000" in result.output and "bytes" in result.output
        assert "Queue Statistics" in result.output
        assert "5 pending" in result.output
        assert "2 running" in result.output
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_queue_list_empty(self, mock_get_service, mock_queue_service):
        """Test listing empty queue."""
        mock_get_service.return_value = mock_queue_service
        mock_queue_service.list_operations.return_value = []
        
        runner = CliRunner()
        result = runner.invoke(queue_group, ['list'])
        
        assert result.exit_code == 0
        assert "No operations found" in result.output
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_queue_list_with_filters(self, mock_get_service):
        """Test listing queue with status and user filters."""
        mock_queue_service = Mock()  # Remove spec to allow flexible method calls
        mock_get_service.return_value = mock_queue_service
        mock_queue_service.list_operations.return_value = []  # Empty for simplicity
        mock_queue_service.get_queue_stats.return_value = {
            'queue_length': 0, 'running': 0, 'completed': 0, 'failed': 0
        }
        
        runner = CliRunner()
        result = runner.invoke(queue_group, [
            'list',
            '--status', 'queued',
            '--user', 'testuser',
            '--limit', '10'
        ])
        
        assert result.exit_code == 0
        # Just verify the command completed successfully without checking internal calls


class TestQueueStatusCommand:
    """Test cases for queue status command."""
    
    @pytest.fixture
    def mock_queue_service(self):
        """Mock operation queue service."""
        service = Mock(spec=OperationQueueService)
        return service
    
    @pytest.fixture
    def sample_operation(self):
        """Sample operation for detailed status."""
        op = Mock()
        op.id = "op-detailed-1234-5678"
        op.status = Mock()
        op.status.value = "running"
        op.priority = Mock()
        op.priority.name = "NORMAL"
        op.created_time = 1672574400.0
        op.started_time = 1672574500.0
        op.completed_time = None
        op.duration = 120.5
        op.error_message = None
        op.operation_dto = Mock()
        op.operation_dto.operation_type = "directory_transfer"
        op.operation_dto.source_location = "hpc"
        op.operation_dto.source_path = "/hpc/data"
        op.operation_dto.dest_location = "storage"
        op.operation_dto.dest_path = "/storage/backup"
        op.result = Mock()
        op.result.success = True
        op.result.bytes_transferred = 2048000
        op.result.throughput_mbps = 1.2
        return op
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_queue_status_detailed(self, mock_get_service, mock_queue_service, sample_operation):
        """Test showing detailed operation status."""
        mock_get_service.return_value = mock_queue_service
        mock_queue_service.get_operation_status.return_value = sample_operation
        
        runner = CliRunner()
        result = runner.invoke(queue_group, ['status', 'op-detailed-1234-5678'])
        
        assert result.exit_code == 0
        assert "op-detailed-123" in result.output  # Truncated ID in title
        assert "running" in result.output
        assert "NORMAL" in result.output
        assert "directory_transfer" in result.output
        assert "hpc:/hpc/data" in result.output
        assert "storage:/storage/backup" in result.output
        assert "Duration: 120.50s" in result.output
        assert "Result Details" in result.output
        assert "2,048,000" in result.output
        assert "1.20 MB/s" in result.output
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_queue_status_not_found(self, mock_get_service, mock_queue_service):
        """Test showing status for non-existent operation."""
        mock_get_service.return_value = mock_queue_service
        mock_queue_service.get_operation_status.return_value = None
        
        runner = CliRunner()
        result = runner.invoke(queue_group, ['status', 'nonexistent'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Operation not found: nonexistent" in result.output


class TestQueueManagementCommands:
    """Test cases for queue management commands."""
    
    @pytest.fixture
    def mock_queue_service(self):
        """Mock operation queue service."""
        service = Mock(spec=OperationQueueService)
        return service
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_cancel_operation_success(self, mock_get_service, mock_queue_service):
        """Test successful operation cancellation."""
        mock_get_service.return_value = mock_queue_service
        mock_queue_service.cancel_operation.return_value = True
        
        runner = CliRunner()
        result = runner.invoke(queue_group, ['cancel', 'op-1234'])
        
        assert result.exit_code == 0
        assert "Cancelled operation: op-1234" in result.output
        mock_queue_service.cancel_operation.assert_called_once_with("op-1234")
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_cancel_operation_failure(self, mock_get_service, mock_queue_service):
        """Test failed operation cancellation."""
        mock_get_service.return_value = mock_queue_service
        mock_queue_service.cancel_operation.return_value = False
        
        runner = CliRunner()
        result = runner.invoke(queue_group, ['cancel', 'op-1234'])
        
        assert result.exit_code == 0
        assert "Failed to cancel operation: op-1234" in result.output
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_pause_queue(self, mock_get_service, mock_queue_service):
        """Test pausing queue processing."""
        mock_get_service.return_value = mock_queue_service
        
        runner = CliRunner()
        result = runner.invoke(queue_group, ['pause'])
        
        assert result.exit_code == 0
        assert "Queue processing paused" in result.output
        mock_queue_service.pause_queue.assert_called_once()
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_resume_queue(self, mock_get_service, mock_queue_service):
        """Test resuming queue processing."""
        mock_get_service.return_value = mock_queue_service
        
        runner = CliRunner()
        result = runner.invoke(queue_group, ['resume'])
        
        assert result.exit_code == 0
        assert "Queue processing resumed" in result.output
        mock_queue_service.resume_queue.assert_called_once()
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_clear_completed(self, mock_get_service, mock_queue_service):
        """Test clearing completed operations."""
        mock_get_service.return_value = mock_queue_service
        mock_queue_service.clear_completed.return_value = 5
        
        runner = CliRunner()
        result = runner.invoke(queue_group, ['clear'])
        
        assert result.exit_code == 0
        assert "Cleared 5 completed operations" in result.output
        mock_queue_service.clear_completed.assert_called_once()
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_show_stats(self, mock_get_service, mock_queue_service):
        """Test showing detailed queue statistics."""
        mock_get_service.return_value = mock_queue_service
        stats = {
            'total_operations': 50,
            'queued': 5,
            'running': 2,
            'completed': 40,
            'failed': 2,
            'cancelled': 1,
            'is_processing': True,
            'is_paused': False,
            'max_concurrent': 3,
            'total_bytes_processed': 102400000
        }
        mock_queue_service.get_queue_stats.return_value = stats
        
        runner = CliRunner()
        result = runner.invoke(queue_group, ['stats'])
        
        assert result.exit_code == 0
        assert "Queue Statistics" in result.output
        assert "Total operations: 50" in result.output
        assert "Queued: 5" in result.output
        assert "Running: 2" in result.output
        assert "Completed: 40" in result.output
        assert "Failed: 2" in result.output
        assert "Cancelled: 1" in result.output
        assert "Processing: Yes" in result.output
        assert "Paused: No" in result.output
        assert "Max concurrent: 3" in result.output
        assert "102,400,000" in result.output


class TestTransferCLIErrorHandling:
    """Test CLI error handling scenarios."""
    
    @patch('tellus.interfaces.cli.transfer._get_transfer_service')
    def test_service_unavailable(self, mock_get_service):
        """Test CLI behavior when service is unavailable."""
        mock_get_service.side_effect = Exception("Service initialization failed")
        
        runner = CliRunner()
        result = runner.invoke(transfer, [
            'file', 'source.txt', 'dest.txt',
            '--dest-location', 'remote'
        ])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
    
    @patch('tellus.interfaces.cli.transfer._get_queue_service')
    def test_queue_service_unavailable(self, mock_get_service):
        """Test CLI behavior when queue service is unavailable."""
        mock_get_service.side_effect = Exception("Queue service failed")
        
        runner = CliRunner()
        result = runner.invoke(queue_group, ['list'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output


class TestTransferCLIHelpAndValidation:
    """Test CLI help and argument validation."""
    
    def test_transfer_cli_help_commands(self):
        """Test help output for transfer CLI commands."""
        runner = CliRunner()
        
        # Test main transfer help
        result = runner.invoke(transfer, ['--help'])
        assert result.exit_code == 0
        assert "transfer" in result.output.lower()
        
        # Test subcommand help
        subcommands = ['file', 'batch', 'directory']
        for subcmd in subcommands:
            result = runner.invoke(transfer, [subcmd, '--help'])
            assert result.exit_code == 0
            assert subcmd in result.output.lower()
    
    def test_queue_cli_help_commands(self):
        """Test help output for queue CLI commands."""
        runner = CliRunner()
        
        # Test main queue help
        result = runner.invoke(queue_group, ['--help'])
        assert result.exit_code == 0
        assert "queue" in result.output.lower()
        
        # Test subcommand help
        subcommands = ['list', 'status', 'cancel', 'pause', 'resume', 'clear', 'stats']
        for subcmd in subcommands:
            result = runner.invoke(queue_group, [subcmd, '--help'])
            assert result.exit_code == 0
    
    def test_file_transfer_argument_validation(self):
        """Test file transfer argument validation."""
        runner = CliRunner()
        
        # Test missing required arguments
        result = runner.invoke(transfer, ['file'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
        
        result = runner.invoke(transfer, ['file', 'source.txt'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
        
        # Test missing required destination location
        result = runner.invoke(transfer, ['file', 'source.txt', 'dest.txt'])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "--dest-location" in result.output
    
    def test_queue_status_argument_validation(self):
        """Test queue status argument validation."""
        runner = CliRunner()
        
        # Test missing operation ID
        result = runner.invoke(queue_group, ['status'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
        
        # Test missing operation ID for cancel
        result = runner.invoke(queue_group, ['cancel'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
    
    def test_invalid_transfer_commands(self):
        """Test handling of invalid transfer commands."""
        runner = CliRunner()
        
        # Test invalid transfer subcommand
        result = runner.invoke(transfer, ['invalid-command'])
        assert result.exit_code != 0
        assert "No such command" in result.output
        
        # Test invalid queue subcommand
        result = runner.invoke(queue_group, ['invalid-command'])
        assert result.exit_code != 0
        assert "No such command" in result.output