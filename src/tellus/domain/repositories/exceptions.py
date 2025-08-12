"""
Repository-related exceptions.
"""


class RepositoryError(Exception):
    """Base exception for repository operations."""
    pass


class SimulationExistsError(RepositoryError):
    """Raised when trying to create a simulation that already exists."""
    
    def __init__(self, simulation_id: str):
        self.simulation_id = simulation_id
        super().__init__(f"Simulation with ID '{simulation_id}' already exists")


class SimulationNotFoundError(RepositoryError):
    """Raised when a requested simulation is not found."""
    
    def __init__(self, simulation_id: str):
        self.simulation_id = simulation_id
        super().__init__(f"Simulation with ID '{simulation_id}' not found")


class LocationExistsError(RepositoryError):
    """Raised when trying to create a location that already exists."""
    
    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Location with name '{name}' already exists")


class LocationNotFoundError(RepositoryError):
    """Raised when a requested location is not found."""
    
    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Location with name '{name}' not found")


class ValidationError(RepositoryError):
    """Raised when entity validation fails."""
    
    def __init__(self, errors: list):
        self.errors = errors
        error_msg = '; '.join(errors) if isinstance(errors, list) else str(errors)
        super().__init__(f"Validation failed: {error_msg}")