"""
Test factories and builders for file transfer system components.

This module extends the existing factory patterns to support the new 
file transfer system, operation queue, and progress tracking features.
"""

import random
import string
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union

# Import new domain DTOs 
from ...application.dtos import (BatchFileTransferOperationDto,
                                 BatchFileTransferResultDto,
                                 CreateProgressTrackingDto,
                                 DirectoryTransferOperationDto,
                                 DirectoryTransferResultDto,
                                 FileTransferOperationDto,
                                 FileTransferResultDto,
                                 ProgressTrackingResultDto,
                                 UpdateProgressTrackingDto)
# Import existing factory patterns
from .factories import Builder, Factory, TestLocation, TestSimulation


# Test representations for new domain objects
@dataclass
class TestFileTransferOperation:
    """Test representation of file transfer operation."""
    operation_id: str
    source_location: str
    source_path: str
    dest_location: str
    dest_path: str
    operation_type: str = "file_transfer"
    overwrite: bool = False
    verify_checksum: bool = True
    chunk_size: int = 8 * 1024 * 1024
    status: str = "pending"
    progress: float = 0.0
    bytes_transferred: int = 0
    throughput_mbps: float = 0.0
    error_message: Optional[str] = None


@dataclass
class TestProgressTracking:
    """Test representation of progress tracking."""
    operation_id: str
    operation_type: str
    operation_name: str
    status: str = "in_progress"
    progress_percentage: float = 0.0
    bytes_processed: int = 0
    total_bytes: Optional[int] = None
    throughput_mbps: Optional[float] = None
    eta_seconds: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass  
class TestOperationQueue:
    """Test representation of operation queue."""
    queue_id: str
    operations: List[TestFileTransferOperation] = field(default_factory=list)
    max_concurrent: int = 3
    is_paused: bool = False
    is_processing: bool = False
    statistics: Dict[str, int] = field(default_factory=lambda: {
        'total_operations': 0,
        'queued': 0,
        'running': 0,
        'completed': 0,
        'failed': 0,
        'cancelled': 0
    })


class FileTransferOperationBuilder(Builder[TestFileTransferOperation]):
    """Builder for creating test file transfer operations."""
    
    def __init__(self):
        """Initialize file transfer operation builder."""
        self.reset()
    
    def reset(self) -> 'FileTransferOperationBuilder':
        """Reset builder to default state."""
        self._operation_id = f"transfer_{uuid.uuid4().hex[:8]}"
        self._source_location = "local"
        self._source_path = "/test/source/file.txt"
        self._dest_location = "remote"
        self._dest_path = "/test/dest/file.txt"
        self._operation_type = "file_transfer"
        self._overwrite = False
        self._verify_checksum = True
        self._chunk_size = 8 * 1024 * 1024
        self._status = "pending"
        self._progress = 0.0
        self._bytes_transferred = 0
        self._throughput_mbps = 0.0
        self._error_message = None
        return self
    
    def with_id(self, operation_id: str) -> 'FileTransferOperationBuilder':
        """Set operation ID."""
        self._operation_id = operation_id
        return self
    
    def with_source(self, location: str, path: str) -> 'FileTransferOperationBuilder':
        """Set source location and path."""
        self._source_location = location
        self._source_path = path
        return self
    
    def with_destination(self, location: str, path: str) -> 'FileTransferOperationBuilder':
        """Set destination location and path."""
        self._dest_location = location
        self._dest_path = path
        return self
    
    def with_options(self, overwrite: bool = False, verify_checksum: bool = True, 
                    chunk_size: int = 8*1024*1024) -> 'FileTransferOperationBuilder':
        """Set transfer options."""
        self._overwrite = overwrite
        self._verify_checksum = verify_checksum
        self._chunk_size = chunk_size
        return self
    
    def with_status(self, status: str) -> 'FileTransferOperationBuilder':
        """Set operation status."""
        self._status = status
        return self
    
    def with_progress(self, progress: float, bytes_transferred: int = 0, 
                     throughput_mbps: float = 0.0) -> 'FileTransferOperationBuilder':
        """Set progress information."""
        self._progress = progress
        self._bytes_transferred = bytes_transferred
        self._throughput_mbps = throughput_mbps
        return self
    
    def with_error(self, error_message: str) -> 'FileTransferOperationBuilder':
        """Set error information."""
        self._error_message = error_message
        self._status = "failed"
        return self
    
    def as_batch_transfer(self) -> 'FileTransferOperationBuilder':
        """Configure as batch transfer operation."""
        self._operation_type = "batch_transfer"
        return self
    
    def as_directory_transfer(self) -> 'FileTransferOperationBuilder':
        """Configure as directory transfer operation."""
        self._operation_type = "directory_transfer"
        return self
    
    def as_large_file_transfer(self, size_mb: int = 100) -> 'FileTransferOperationBuilder':
        """Configure as large file transfer."""
        self._bytes_transferred = size_mb * 1024 * 1024
        self._chunk_size = 32 * 1024 * 1024  # 32MB chunks for large files
        return self
    
    def as_completed(self, duration_seconds: float = 10.0) -> 'FileTransferOperationBuilder':
        """Configure as completed transfer."""
        self._status = "completed"
        self._progress = 100.0
        if self._bytes_transferred == 0:
            self._bytes_transferred = 1024 * 1024  # Default 1MB
        self._throughput_mbps = (self._bytes_transferred / (1024 * 1024)) / duration_seconds
        return self
    
    def as_failed(self, error_message: str = "Transfer failed") -> 'FileTransferOperationBuilder':
        """Configure as failed transfer."""
        return self.with_error(error_message)
    
    def build(self) -> TestFileTransferOperation:
        """Build the file transfer operation object."""
        return TestFileTransferOperation(
            operation_id=self._operation_id,
            source_location=self._source_location,
            source_path=self._source_path,
            dest_location=self._dest_location,
            dest_path=self._dest_path,
            operation_type=self._operation_type,
            overwrite=self._overwrite,
            verify_checksum=self._verify_checksum,
            chunk_size=self._chunk_size,
            status=self._status,
            progress=self._progress,
            bytes_transferred=self._bytes_transferred,
            throughput_mbps=self._throughput_mbps,
            error_message=self._error_message
        )


class ProgressTrackingBuilder(Builder[TestProgressTracking]):
    """Builder for creating test progress tracking objects."""
    
    def __init__(self):
        """Initialize progress tracking builder."""
        self.reset()
    
    def reset(self) -> 'ProgressTrackingBuilder':
        """Reset builder to default state."""
        self._operation_id = f"progress_{uuid.uuid4().hex[:8]}"
        self._operation_type = "file_transfer"
        self._operation_name = "Test Operation"
        self._status = "in_progress"
        self._progress_percentage = 0.0
        self._bytes_processed = 0
        self._total_bytes = None
        self._throughput_mbps = None
        self._eta_seconds = None
        self._metadata = {}
        return self
    
    def with_operation(self, operation_id: str, operation_type: str, 
                      operation_name: str) -> 'ProgressTrackingBuilder':
        """Set operation details."""
        self._operation_id = operation_id
        self._operation_type = operation_type
        self._operation_name = operation_name
        return self
    
    def with_progress(self, percentage: float, bytes_processed: int = 0,
                     total_bytes: Optional[int] = None) -> 'ProgressTrackingBuilder':
        """Set progress information."""
        self._progress_percentage = percentage
        self._bytes_processed = bytes_processed
        self._total_bytes = total_bytes
        return self
    
    def with_performance(self, throughput_mbps: float, 
                        eta_seconds: Optional[float] = None) -> 'ProgressTrackingBuilder':
        """Set performance metrics."""
        self._throughput_mbps = throughput_mbps
        self._eta_seconds = eta_seconds
        return self
    
    def with_metadata(self, **metadata) -> 'ProgressTrackingBuilder':
        """Add metadata."""
        self._metadata.update(metadata)
        return self
    
    def as_file_transfer_progress(self, file_size: int) -> 'ProgressTrackingBuilder':
        """Configure as file transfer progress."""
        self._operation_type = "file_transfer"
        self._total_bytes = file_size
        return self
    
    def as_archive_operation_progress(self, archive_size: int) -> 'ProgressTrackingBuilder':
        """Configure as archive operation progress."""
        self._operation_type = "archive_extract"
        self._total_bytes = archive_size
        return self
    
    def as_completed(self) -> 'ProgressTrackingBuilder':
        """Configure as completed operation."""
        self._status = "completed"
        self._progress_percentage = 100.0
        if self._total_bytes:
            self._bytes_processed = self._total_bytes
        return self
    
    def build(self) -> TestProgressTracking:
        """Build the progress tracking object."""
        return TestProgressTracking(
            operation_id=self._operation_id,
            operation_type=self._operation_type,
            operation_name=self._operation_name,
            status=self._status,
            progress_percentage=self._progress_percentage,
            bytes_processed=self._bytes_processed,
            total_bytes=self._total_bytes,
            throughput_mbps=self._throughput_mbps,
            eta_seconds=self._eta_seconds,
            metadata=self._metadata.copy()
        )


class OperationQueueBuilder(Builder[TestOperationQueue]):
    """Builder for creating test operation queue objects."""
    
    def __init__(self):
        """Initialize operation queue builder."""
        self.reset()
    
    def reset(self) -> 'OperationQueueBuilder':
        """Reset builder to default state."""
        self._queue_id = f"queue_{uuid.uuid4().hex[:8]}"
        self._operations = []
        self._max_concurrent = 3
        self._is_paused = False
        self._is_processing = False
        self._statistics = {
            'total_operations': 0,
            'queued': 0,
            'running': 0,
            'completed': 0,
            'failed': 0,
            'cancelled': 0
        }
        return self
    
    def with_id(self, queue_id: str) -> 'OperationQueueBuilder':
        """Set queue ID."""
        self._queue_id = queue_id
        return self
    
    def with_operation(self, operation: TestFileTransferOperation) -> 'OperationQueueBuilder':
        """Add an operation to the queue."""
        self._operations.append(operation)
        self._update_statistics()
        return self
    
    def with_operations(self, *operations: TestFileTransferOperation) -> 'OperationQueueBuilder':
        """Add multiple operations to the queue."""
        self._operations.extend(operations)
        self._update_statistics()
        return self
    
    def with_concurrent_limit(self, max_concurrent: int) -> 'OperationQueueBuilder':
        """Set maximum concurrent operations."""
        self._max_concurrent = max_concurrent
        return self
    
    def as_paused(self) -> 'OperationQueueBuilder':
        """Configure queue as paused."""
        self._is_paused = True
        return self
    
    def as_processing(self) -> 'OperationQueueBuilder':
        """Configure queue as processing."""
        self._is_processing = True
        return self
    
    def with_mixed_operations(self, count: int = 10) -> 'OperationQueueBuilder':
        """Add mixed operation types for testing."""
        for i in range(count):
            status = random.choice(['pending', 'running', 'completed', 'failed'])
            operation = (FileTransferOperationBuilder()
                        .with_id(f"mixed_op_{i}")
                        .with_source("local", f"/test/source/file_{i}.txt")
                        .with_destination("remote", f"/test/dest/file_{i}.txt")
                        .with_status(status)
                        .build())
            self._operations.append(operation)
        
        self._update_statistics()
        return self
    
    def _update_statistics(self) -> None:
        """Update queue statistics based on operations."""
        self._statistics = {
            'total_operations': len(self._operations),
            'queued': len([op for op in self._operations if op.status == 'pending']),
            'running': len([op for op in self._operations if op.status == 'running']),
            'completed': len([op for op in self._operations if op.status == 'completed']),
            'failed': len([op for op in self._operations if op.status == 'failed']),
            'cancelled': len([op for op in self._operations if op.status == 'cancelled'])
        }
    
    def build(self) -> TestOperationQueue:
        """Build the operation queue object."""
        return TestOperationQueue(
            queue_id=self._queue_id,
            operations=self._operations.copy(),
            max_concurrent=self._max_concurrent,
            is_paused=self._is_paused,
            is_processing=self._is_processing,
            statistics=self._statistics.copy()
        )


class FileTransferDtoBuilder:
    """Builder for creating real file transfer DTOs."""
    
    def __init__(self):
        """Initialize DTO builder."""
        self.reset()
    
    def reset(self) -> 'FileTransferDtoBuilder':
        """Reset builder to default state."""
        self._source_location = "local"
        self._source_path = "/test/source/file.txt"
        self._dest_location = "remote"
        self._dest_path = "/test/dest/file.txt"
        self._overwrite = False
        self._verify_checksum = True
        self._chunk_size = 8 * 1024 * 1024
        return self
    
    def with_source(self, location: str, path: str) -> 'FileTransferDtoBuilder':
        """Set source location and path."""
        self._source_location = location
        self._source_path = path
        return self
    
    def with_destination(self, location: str, path: str) -> 'FileTransferDtoBuilder':
        """Set destination location and path."""
        self._dest_location = location
        self._dest_path = path
        return self
    
    def with_options(self, overwrite: bool = False, verify_checksum: bool = True,
                    chunk_size: int = 8*1024*1024) -> 'FileTransferDtoBuilder':
        """Set transfer options."""
        self._overwrite = overwrite
        self._verify_checksum = verify_checksum
        self._chunk_size = chunk_size
        return self
    
    def build_single_transfer(self) -> FileTransferOperationDto:
        """Build single file transfer DTO."""
        return FileTransferOperationDto(
            source_location=self._source_location,
            source_path=self._source_path,
            dest_location=self._dest_location,
            dest_path=self._dest_path,
            overwrite=self._overwrite,
            verify_checksum=self._verify_checksum,
            chunk_size=self._chunk_size
        )
    
    def build_directory_transfer(self, recursive: bool = True,
                               include_patterns: List[str] = None,
                               exclude_patterns: List[str] = None) -> DirectoryTransferOperationDto:
        """Build directory transfer DTO."""
        return DirectoryTransferOperationDto(
            source_location=self._source_location,
            source_path=self._source_path,
            dest_location=self._dest_location,
            dest_path=self._dest_path,
            recursive=recursive,
            overwrite=self._overwrite,
            verify_checksums=self._verify_checksum,
            include_patterns=include_patterns or [],
            exclude_patterns=exclude_patterns or []
        )


class FileTransferFactory(Factory[TestFileTransferOperation]):
    """Factory for creating test file transfer operations."""
    
    def create(self, **kwargs) -> TestFileTransferOperation:
        """Create a file transfer operation with optional parameters."""
        builder = FileTransferOperationBuilder()
        
        # Apply common parameters
        if 'operation_id' in kwargs:
            builder.with_id(kwargs['operation_id'])
        if 'source' in kwargs:
            source = kwargs['source']
            builder.with_source(source.get('location', 'local'), source.get('path', '/test/source'))
        if 'destination' in kwargs:
            dest = kwargs['destination']
            builder.with_destination(dest.get('location', 'remote'), dest.get('path', '/test/dest'))
        if 'status' in kwargs:
            builder.with_status(kwargs['status'])
        if 'progress' in kwargs:
            builder.with_progress(kwargs['progress'])
        
        return builder.build()
    
    def create_completed_transfer(self, size_mb: int = 10) -> TestFileTransferOperation:
        """Create a completed transfer operation."""
        return (FileTransferOperationBuilder()
                .with_progress(100.0, size_mb * 1024 * 1024, 15.5)
                .as_completed()
                .build())
    
    def create_failed_transfer(self, error_message: str = "Network timeout") -> TestFileTransferOperation:
        """Create a failed transfer operation."""
        return (FileTransferOperationBuilder()
                .as_failed(error_message)
                .build())
    
    def create_large_file_transfer(self, size_mb: int = 500) -> TestFileTransferOperation:
        """Create a large file transfer operation."""
        return (FileTransferOperationBuilder()
                .as_large_file_transfer(size_mb)
                .build())


class ProgressTrackingFactory(Factory[TestProgressTracking]):
    """Factory for creating test progress tracking objects."""
    
    def create(self, **kwargs) -> TestProgressTracking:
        """Create progress tracking with optional parameters."""
        builder = ProgressTrackingBuilder()
        
        if 'operation_id' in kwargs:
            builder.with_operation(
                kwargs['operation_id'],
                kwargs.get('operation_type', 'file_transfer'),
                kwargs.get('operation_name', 'Test Operation')
            )
        if 'progress' in kwargs:
            builder.with_progress(kwargs['progress'])
        if 'throughput' in kwargs:
            builder.with_performance(kwargs['throughput'])
        
        return builder.build()
    
    def create_file_transfer_progress(self, file_size: int, 
                                    progress_percentage: float = 50.0) -> TestProgressTracking:
        """Create file transfer progress tracking."""
        return (ProgressTrackingBuilder()
                .as_file_transfer_progress(file_size)
                .with_progress(progress_percentage, int(file_size * progress_percentage / 100))
                .build())


class OperationQueueFactory(Factory[TestOperationQueue]):
    """Factory for creating test operation queues."""
    
    def create(self, **kwargs) -> TestOperationQueue:
        """Create operation queue with optional parameters."""
        builder = OperationQueueBuilder()
        
        if 'queue_id' in kwargs:
            builder.with_id(kwargs['queue_id'])
        if 'max_concurrent' in kwargs:
            builder.with_concurrent_limit(kwargs['max_concurrent'])
        if 'operations' in kwargs:
            builder.with_operations(*kwargs['operations'])
        
        return builder.build()
    
    def create_busy_queue(self, operation_count: int = 20) -> TestOperationQueue:
        """Create a busy queue with many operations."""
        return (OperationQueueBuilder()
                .with_mixed_operations(operation_count)
                .as_processing()
                .build())
    
    def create_empty_queue(self) -> TestOperationQueue:
        """Create an empty queue."""
        return OperationQueueBuilder().build()


# Convenience functions for quick object creation
def file_transfer_operation(**kwargs) -> TestFileTransferOperation:
    """Create a test file transfer operation with optional parameters."""
    return FileTransferFactory().create(**kwargs)


def progress_tracking(**kwargs) -> TestProgressTracking:
    """Create a test progress tracking with optional parameters."""
    return ProgressTrackingFactory().create(**kwargs)


def operation_queue(**kwargs) -> TestOperationQueue:
    """Create a test operation queue with optional parameters."""
    return OperationQueueFactory().create(**kwargs)


def file_transfer_dto(**kwargs) -> FileTransferOperationDto:
    """Create a real file transfer DTO with optional parameters."""
    builder = FileTransferDtoBuilder()
    
    if 'source' in kwargs:
        source = kwargs['source']
        builder.with_source(source.get('location', 'local'), source.get('path', '/test/source'))
    if 'destination' in kwargs:
        dest = kwargs['destination']
        builder.with_destination(dest.get('location', 'remote'), dest.get('path', '/test/dest'))
    if 'options' in kwargs:
        builder.with_options(**kwargs['options'])
    
    return builder.build_single_transfer()