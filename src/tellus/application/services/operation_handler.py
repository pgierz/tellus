"""
Operation Handler Interface for Generic Queue System.

Defines the interface for handling different types of operations in the generic
operation queue, supporting both archive operations and file transfer operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Union

from ..dtos import (
    BulkArchiveOperationDto,
    BulkOperationResultDto,
    FileTransferOperationDto,
    BatchFileTransferOperationDto,
    DirectoryTransferOperationDto,
    FileTransferResultDto,
    BatchFileTransferResultDto,
)


# Union type for all supported operation DTOs
OperationDto = Union[
    BulkArchiveOperationDto,
    FileTransferOperationDto,
    BatchFileTransferOperationDto,
    DirectoryTransferOperationDto,
]

# Union type for all supported result DTOs
OperationResultDto = Union[
    BulkOperationResultDto,
    FileTransferResultDto,
    BatchFileTransferResultDto,
]


class IOperationHandler(ABC):
    """Interface for operation handlers in the generic queue system."""
    
    @abstractmethod
    def can_handle(self, operation_dto: OperationDto) -> bool:
        """Check if this handler can process the given operation DTO."""
        pass
    
    @abstractmethod
    async def execute_operation(self, operation_dto: OperationDto) -> OperationResultDto:
        """Execute the operation and return the result."""
        pass
    
    @abstractmethod
    def get_operation_type(self) -> str:
        """Get the operation type string this handler supports."""
        pass


class ArchiveOperationHandler(IOperationHandler):
    """Handler for archive operations (bulk_copy, bulk_move, bulk_extract)."""
    
    def __init__(self, archive_service):
        self.archive_service = archive_service
    
    def can_handle(self, operation_dto: OperationDto) -> bool:
        """Check if this is a bulk archive operation."""
        return isinstance(operation_dto, BulkArchiveOperationDto)
    
    async def execute_operation(self, operation_dto: OperationDto) -> OperationResultDto:
        """Execute bulk archive operation."""
        if not isinstance(operation_dto, BulkArchiveOperationDto):
            raise ValueError(f"Expected BulkArchiveOperationDto, got {type(operation_dto)}")
        
        return await self.archive_service.execute_bulk_operation(operation_dto)
    
    def get_operation_type(self) -> str:
        return "bulk_archive"


class FileTransferOperationHandler(IOperationHandler):
    """Handler for file transfer operations."""
    
    def __init__(self, file_transfer_service):
        self.file_transfer_service = file_transfer_service
    
    def can_handle(self, operation_dto: OperationDto) -> bool:
        """Check if this is a file transfer operation."""
        return isinstance(operation_dto, (
            FileTransferOperationDto,
            BatchFileTransferOperationDto,
            DirectoryTransferOperationDto
        ))
    
    async def execute_operation(self, operation_dto: OperationDto) -> OperationResultDto:
        """Execute file transfer operation."""
        if isinstance(operation_dto, FileTransferOperationDto):
            return await self.file_transfer_service.transfer_file(operation_dto)
        elif isinstance(operation_dto, BatchFileTransferOperationDto):
            return await self.file_transfer_service.batch_transfer_files(operation_dto)
        elif isinstance(operation_dto, DirectoryTransferOperationDto):
            return await self.file_transfer_service.transfer_directory(operation_dto)
        else:
            raise ValueError(f"Unsupported file transfer operation: {type(operation_dto)}")
    
    def get_operation_type(self) -> str:
        return "file_transfer"


class OperationRouter:
    """Routes operations to appropriate handlers."""
    
    def __init__(self):
        self._handlers = []
    
    def register_handler(self, handler: IOperationHandler) -> None:
        """Register an operation handler."""
        self._handlers.append(handler)
    
    def get_handler(self, operation_dto: OperationDto) -> IOperationHandler:
        """Get the appropriate handler for the operation."""
        for handler in self._handlers:
            if handler.can_handle(operation_dto):
                return handler
        
        raise ValueError(f"No handler found for operation type: {type(operation_dto)}")
    
    async def execute_operation(self, operation_dto: OperationDto) -> OperationResultDto:
        """Route and execute operation."""
        handler = self.get_handler(operation_dto)
        return await handler.execute_operation(operation_dto)