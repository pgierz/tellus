"""
Application layer exceptions.

These exceptions represent business errors that occur at the application service layer,
wrapping domain exceptions and adding application-specific context.
"""

from typing import List, Optional


class ApplicationError(Exception):
    """Base exception for application layer errors."""
    pass


class ValidationError(ApplicationError):
    """Raised when application-level validation fails."""
    
    def __init__(self, message: str, field: Optional[str] = None, errors: Optional[List[str]] = None):
        self.field = field
        self.errors = errors or []
        super().__init__(message)


class EntityNotFoundError(ApplicationError):
    """Raised when a requested entity is not found."""
    
    def __init__(self, entity_type: str, identifier: str):
        self.entity_type = entity_type
        self.identifier = identifier
        super().__init__(f"{entity_type} with identifier '{identifier}' not found")


class EntityAlreadyExistsError(ApplicationError):
    """Raised when trying to create an entity that already exists."""
    
    def __init__(self, entity_type: str, identifier: str):
        self.entity_type = entity_type
        self.identifier = identifier
        super().__init__(f"{entity_type} with identifier '{identifier}' already exists")


class BusinessRuleViolationError(ApplicationError):
    """Raised when a business rule is violated."""
    
    def __init__(self, rule: str, message: str):
        self.rule = rule
        super().__init__(f"Business rule '{rule}' violated: {message}")


class OperationNotAllowedError(ApplicationError):
    """Raised when an operation is not allowed in the current state."""
    
    def __init__(self, operation: str, reason: str):
        self.operation = operation
        self.reason = reason
        super().__init__(f"Operation '{operation}' not allowed: {reason}")


class ExternalServiceError(ApplicationError):
    """Raised when an external service operation fails."""
    
    def __init__(self, service: str, operation: str, error: str):
        self.service = service
        self.operation = operation
        self.error = error
        super().__init__(f"External service '{service}' failed during '{operation}': {error}")


class ConcurrentModificationError(ApplicationError):
    """Raised when a concurrent modification conflict occurs."""
    
    def __init__(self, entity_type: str, identifier: str):
        self.entity_type = entity_type
        self.identifier = identifier
        super().__init__(f"Concurrent modification detected for {entity_type} '{identifier}'")


class InsufficientPermissionsError(ApplicationError):
    """Raised when the user lacks permissions for an operation."""
    
    def __init__(self, operation: str, resource: str):
        self.operation = operation
        self.resource = resource
        super().__init__(f"Insufficient permissions for '{operation}' on '{resource}'")


class ResourceLimitExceededError(ApplicationError):
    """Raised when a resource limit is exceeded."""
    
    def __init__(self, resource: str, limit: str, current: str):
        self.resource = resource
        self.limit = limit
        self.current = current
        super().__init__(f"Resource limit exceeded for '{resource}': {current} > {limit}")


class ConfigurationError(ApplicationError):
    """Raised when there's a configuration problem."""
    
    def __init__(self, component: str, issue: str):
        self.component = component
        self.issue = issue
        super().__init__(f"Configuration error in '{component}': {issue}")


# Earth Science specific exceptions

class SimulationStateError(ApplicationError):
    """Raised when simulation is in an invalid state for the operation."""
    
    def __init__(self, simulation_id: str, current_state: str, required_state: str):
        self.simulation_id = simulation_id
        self.current_state = current_state
        self.required_state = required_state
        super().__init__(
            f"Simulation '{simulation_id}' is in state '{current_state}' "
            f"but requires state '{required_state}'"
        )


class LocationAccessError(ApplicationError):
    """Raised when a storage location cannot be accessed."""
    
    def __init__(self, location_name: str, protocol: str, error: str):
        self.location_name = location_name
        self.protocol = protocol
        self.error = error
        super().__init__(
            f"Cannot access location '{location_name}' (protocol: {protocol}): {error}"
        )


class ArchiveOperationError(ApplicationError):
    """Raised when an archive operation fails."""
    
    def __init__(self, archive_id: str, operation: str, error: str):
        self.archive_id = archive_id
        self.operation = operation
        self.error = error
        super().__init__(f"Archive operation '{operation}' failed for '{archive_id}': {error}")


class CacheOperationError(ApplicationError):
    """Raised when a cache operation fails."""
    
    def __init__(self, operation: str, cache_key: str, error: str):
        self.operation = operation
        self.cache_key = cache_key
        self.error = error
        super().__init__(f"Cache operation '{operation}' failed for key '{cache_key}': {error}")


class DataIntegrityError(ApplicationError):
    """Raised when data integrity checks fail."""
    
    def __init__(self, data_type: str, identifier: str, issue: str):
        self.data_type = data_type
        self.identifier = identifier
        self.issue = issue
        super().__init__(f"Data integrity error in {data_type} '{identifier}': {issue}")


class WorkflowExecutionError(ApplicationError):
    """Raised when workflow execution fails."""
    
    def __init__(self, workflow_id: str, step_id: Optional[str] = None, error: str = ""):
        self.workflow_id = workflow_id
        self.step_id = step_id
        self.error = error
        
        if step_id:
            super().__init__(f"Workflow '{workflow_id}' failed at step '{step_id}': {error}")
        else:
            super().__init__(f"Workflow '{workflow_id}' execution failed: {error}")


class PathResolutionError(ApplicationError):
    """Raised when path resolution fails."""
    
    def __init__(self, message: str, simulation_id: Optional[str] = None, location_name: Optional[str] = None):
        self.simulation_id = simulation_id
        self.location_name = location_name
        super().__init__(message)