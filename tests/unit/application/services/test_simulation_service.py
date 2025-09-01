"""
Unit tests for SimulationApplicationService.

Tests the application service layer for simulation management,
including CRUD operations, location associations, and validation.
"""

from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import pytest

from tellus.application.dtos import (CreateSimulationDto, FilterOptions,
                                     PaginationInfo, SimulationDto,
                                     SimulationListDto,
                                     SimulationLocationAssociationDto,
                                     UpdateSimulationDto)
from tellus.application.exceptions import (BusinessRuleViolationError,
                                           EntityAlreadyExistsError,
                                           EntityNotFoundError,
                                           OperationNotAllowedError,
                                           ValidationError)
from tellus.application.services.simulation_service import \
    SimulationApplicationService
from tellus.domain.entities.location import LocationEntity, LocationKind
from tellus.domain.entities.simulation import SimulationEntity
from tellus.domain.repositories.exceptions import (LocationNotFoundError,
                                                   RepositoryError,
                                                   SimulationExistsError,
                                                   SimulationNotFoundError)


@pytest.fixture
def mock_simulation_repo():
    """Mock simulation repository."""
    return Mock()


@pytest.fixture
def mock_location_repo():
    """Mock location repository."""
    return Mock()


@pytest.fixture
def service(mock_simulation_repo, mock_location_repo):
    """Create service instance with mocked dependencies."""
    return SimulationApplicationService(
        simulation_repository=mock_simulation_repo,
        location_repository=mock_location_repo
    )


@pytest.fixture
def sample_simulation_entity():
    """Create a sample simulation entity for testing."""
    return SimulationEntity(
        simulation_id="test-sim",
        model_id="test-model",
        path="/test/path",
        attrs={"experiment": "test"},
        namelists={"namelist1": "/path/to/namelist"},
        snakemakes={"rule1": "/path/to/snakefile"}
    )


@pytest.fixture
def sample_location_entity():
    """Create a sample location entity for testing."""
    return LocationEntity(
        name="test-location",
        kinds=[LocationKind.DISK],
        optional=False,
        config={"protocol": "file", "path": "/test/location"}
    )


class TestSimulationApplicationService:
    """Test suite for SimulationApplicationService."""


class TestCreateSimulation:
    """Test simulation creation operations."""
    
    def test_create_simulation_success(self, service, mock_simulation_repo):
        """Test successful simulation creation."""
        # Arrange
        dto = CreateSimulationDto(
            simulation_id="test-sim",
            model_id="test-model",
            path="/test/path",
            attrs={"experiment": "test"},
            namelists={"namelist1": "/path/to/namelist"},
            snakemakes={"rule1": "/path/to/snakefile"}
        )
        
        mock_simulation_repo.exists.return_value = False
        mock_simulation_repo.save.return_value = None
        
        # Act
        result = service.create_simulation(dto)
        
        # Assert
        assert isinstance(result, SimulationDto)
        assert result.simulation_id == "test-sim"
        assert result.model_id == "test-model"
        assert result.path == "/test/path"
        assert result.attrs == {"experiment": "test"}
        
        mock_simulation_repo.exists.assert_called_once_with("test-sim")
        mock_simulation_repo.save.assert_called_once()
    
    def test_create_simulation_already_exists(self, service, mock_simulation_repo):
        """Test creating a simulation that already exists."""
        # Arrange
        dto = CreateSimulationDto(simulation_id="existing-sim")
        mock_simulation_repo.exists.return_value = True
        
        # Act & Assert
        with pytest.raises(EntityAlreadyExistsError) as exc_info:
            service.create_simulation(dto)
        
        assert "existing-sim" in str(exc_info.value)
        mock_simulation_repo.save.assert_not_called()
    
    def test_create_simulation_repository_error(self, service, mock_simulation_repo):
        """Test simulation creation with repository error."""
        # Arrange
        dto = CreateSimulationDto(simulation_id="test-sim")
        mock_simulation_repo.exists.return_value = False
        mock_simulation_repo.save.side_effect = SimulationExistsError("test-sim")
        
        # Act & Assert
        with pytest.raises(EntityAlreadyExistsError):
            service.create_simulation(dto)
    
    def test_create_simulation_validation_error(self, service, mock_simulation_repo):
        """Test simulation creation with invalid data."""
        # Arrange
        dto = CreateSimulationDto(simulation_id="")  # Invalid empty ID
        mock_simulation_repo.exists.return_value = False
        mock_simulation_repo.save.side_effect = ValueError("Invalid simulation ID")
        
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            service.create_simulation(dto)
        
        assert "Invalid simulation data" in str(exc_info.value)


class TestGetSimulation:
    """Test simulation retrieval operations."""
    
    def test_get_simulation_success(self, service, mock_simulation_repo, sample_simulation_entity):
        """Test successful simulation retrieval."""
        # Arrange
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        
        # Act
        result = service.get_simulation("test-sim")
        
        # Assert
        assert isinstance(result, SimulationDto)
        assert result.simulation_id == "test-sim"
        assert result.model_id == "test-model"
        mock_simulation_repo.get_by_id.assert_called_once_with("test-sim")
    
    def test_get_simulation_not_found(self, service, mock_simulation_repo):
        """Test retrieving a non-existent simulation."""
        # Arrange
        mock_simulation_repo.get_by_id.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.get_simulation("nonexistent")
        
        assert "nonexistent" in str(exc_info.value)
    
    def test_get_simulation_repository_error(self, service, mock_simulation_repo):
        """Test simulation retrieval with repository error."""
        # Arrange
        mock_simulation_repo.get_by_id.side_effect = SimulationNotFoundError("test-sim")
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError):
            service.get_simulation("test-sim")


class TestUpdateSimulation:
    """Test simulation update operations."""
    
    def test_update_simulation_success(self, service, mock_simulation_repo, sample_simulation_entity):
        """Test successful simulation update."""
        # Arrange
        dto = UpdateSimulationDto(
            model_id="updated-model",
            path="/updated/path",
            attrs={"updated": "true"}
        )
        
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        mock_simulation_repo.save.return_value = None
        
        # Mock validation to return no errors
        with patch.object(sample_simulation_entity, 'validate', return_value=[]):
            # Act
            result = service.update_simulation("test-sim", dto)
        
        # Assert
        assert isinstance(result, SimulationDto)
        assert result.simulation_id == "test-sim"
        mock_simulation_repo.get_by_id.assert_called_once_with("test-sim")
        mock_simulation_repo.save.assert_called_once()
    
    def test_update_simulation_not_found(self, service, mock_simulation_repo):
        """Test updating a non-existent simulation."""
        # Arrange
        dto = UpdateSimulationDto(model_id="updated-model")
        mock_simulation_repo.get_by_id.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError):
            service.update_simulation("nonexistent", dto)
        
        mock_simulation_repo.save.assert_not_called()
    
    def test_update_simulation_validation_error(self, service, mock_simulation_repo, sample_simulation_entity):
        """Test simulation update with validation errors."""
        # Arrange
        dto = UpdateSimulationDto(model_id="invalid")
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        
        # Mock validation to return errors
        with patch.object(sample_simulation_entity, 'validate', return_value=["Invalid model ID"]):
            # Act & Assert
            with pytest.raises(ValidationError) as exc_info:
                service.update_simulation("test-sim", dto)
        
        assert "Simulation validation failed" in str(exc_info.value)
        mock_simulation_repo.save.assert_not_called()


class TestDeleteSimulation:
    """Test simulation deletion operations."""
    
    def test_delete_simulation_success(self, service, mock_simulation_repo):
        """Test successful simulation deletion."""
        # Arrange
        mock_simulation_repo.exists.return_value = True
        mock_simulation_repo.delete.return_value = True
        
        # Act
        result = service.delete_simulation("test-sim")
        
        # Assert
        assert result is True
        mock_simulation_repo.exists.assert_called_once_with("test-sim")
        mock_simulation_repo.delete.assert_called_once_with("test-sim")
    
    def test_delete_simulation_not_found(self, service, mock_simulation_repo):
        """Test deleting a non-existent simulation."""
        # Arrange
        mock_simulation_repo.exists.return_value = False
        
        # Act
        result = service.delete_simulation("nonexistent")
        
        # Assert
        assert result is False
        mock_simulation_repo.delete.assert_not_called()
    
    def test_delete_simulation_repository_failure(self, service, mock_simulation_repo):
        """Test simulation deletion that fails at repository level."""
        # Arrange
        mock_simulation_repo.exists.return_value = True
        mock_simulation_repo.delete.return_value = False
        
        # Act
        result = service.delete_simulation("test-sim")
        
        # Assert
        assert result is False


class TestListSimulations:
    """Test simulation listing operations."""
    
    def test_list_simulations_success(self, service, mock_simulation_repo, sample_simulation_entity):
        """Test successful simulation listing."""
        # Arrange
        simulations = [sample_simulation_entity]
        mock_simulation_repo.list_all.return_value = simulations
        
        # Act
        result = service.list_simulations()
        
        # Assert
        assert isinstance(result, SimulationListDto)
        assert len(result.simulations) == 1
        assert result.simulations[0].simulation_id == "test-sim"
        assert isinstance(result.pagination, PaginationInfo)
        assert result.pagination.total_count == 1
    
    def test_list_simulations_with_pagination(self, service, mock_simulation_repo):
        """Test simulation listing with pagination."""
        # Arrange
        simulations = [
            SimulationEntity(simulation_id=f"sim-{i}", model_id="test-model")
            for i in range(10)
        ]
        mock_simulation_repo.list_all.return_value = simulations
        
        # Act
        result = service.list_simulations(page=2, page_size=3)
        
        # Assert
        assert len(result.simulations) == 3
        assert result.pagination.page == 2
        assert result.pagination.page_size == 3
        assert result.pagination.total_count == 10
        assert result.pagination.has_next is True
        assert result.pagination.has_previous is True
    
    def test_list_simulations_with_filters(self, service, mock_simulation_repo):
        """Test simulation listing with search filters."""
        # Arrange
        simulations = [
            SimulationEntity(simulation_id="test-sim-1", model_id="model-a"),
            SimulationEntity(simulation_id="prod-sim-2", model_id="model-b"),
            SimulationEntity(simulation_id="test-sim-3", model_id="model-c")
        ]
        mock_simulation_repo.list_all.return_value = simulations
        
        filters = FilterOptions(search_term="test")
        
        # Act
        result = service.list_simulations(filters=filters)
        
        # Assert
        assert len(result.simulations) == 2  # Only test-sim-1 and test-sim-3
        assert all("test" in sim.simulation_id for sim in result.simulations)
    
    def test_list_simulations_empty(self, service, mock_simulation_repo):
        """Test simulation listing with no simulations."""
        # Arrange
        mock_simulation_repo.list_all.return_value = []
        
        # Act
        result = service.list_simulations()
        
        # Assert
        assert len(result.simulations) == 0
        assert result.pagination.total_count == 0


class TestLocationAssociations:
    """Test simulation-location association operations."""
    
    def test_associate_locations_success(self, service, mock_simulation_repo, mock_location_repo, 
                                       sample_simulation_entity, sample_location_entity):
        """Test successful location association."""
        # Arrange
        dto = SimulationLocationAssociationDto(
            simulation_id="test-sim",
            location_names=["test-location"],
            context_overrides={"test-location": {"path_prefix": "/custom"}}
        )
        
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        mock_location_repo.get_by_name.return_value = sample_location_entity
        mock_simulation_repo.save.return_value = None
        
        # Act
        service.associate_locations(dto)
        
        # Assert
        mock_simulation_repo.get_by_id.assert_called_once_with("test-sim")
        mock_location_repo.get_by_name.assert_called_once_with("test-location")
        mock_simulation_repo.save.assert_called_once()
    
    def test_associate_locations_simulation_not_found(self, service, mock_simulation_repo):
        """Test location association with non-existent simulation."""
        # Arrange
        dto = SimulationLocationAssociationDto(
            simulation_id="nonexistent",
            location_names=["test-location"]
        )
        mock_simulation_repo.get_by_id.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.associate_locations(dto)
        
        assert "nonexistent" in str(exc_info.value)
    
    def test_associate_locations_location_not_found(self, service, mock_simulation_repo, 
                                                  mock_location_repo, sample_simulation_entity):
        """Test location association with non-existent location."""
        # Arrange
        dto = SimulationLocationAssociationDto(
            simulation_id="test-sim",
            location_names=["nonexistent-location"]
        )
        
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        mock_location_repo.get_by_name.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.associate_locations(dto)
        
        assert "nonexistent-location" in str(exc_info.value)
    
    def test_associate_simulation_with_locations_success(self, service, mock_simulation_repo, 
                                                       mock_location_repo, sample_simulation_entity):
        """Test successful simulation-location association using the new method."""
        # Arrange
        dto = SimulationLocationAssociationDto(
            simulation_id="test-sim",
            location_names=["test-location"],
            context_overrides={"test-location": {"path_prefix": "/custom"}}
        )
        
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        mock_location_repo.exists.return_value = True
        mock_simulation_repo.save.return_value = None
        
        # Mock the location association method
        with patch.object(sample_simulation_entity, 'associate_location') as mock_associate:
            # Act
            result = service.associate_simulation_with_locations(dto)
        
        # Assert
        assert isinstance(result, SimulationDto)
        mock_associate.assert_called_once_with("test-location", {"path_prefix": "/custom"})
        mock_simulation_repo.save.assert_called_once()
    
    def test_disassociate_simulation_from_location_success(self, service, mock_simulation_repo, 
                                                         sample_simulation_entity):
        """Test successful simulation-location disassociation."""
        # Arrange
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        mock_simulation_repo.save.return_value = None
        
        # Mock the location methods
        with patch.object(sample_simulation_entity, 'is_location_associated', return_value=True), \
             patch.object(sample_simulation_entity, 'disassociate_location') as mock_disassociate:
            
            # Act
            result = service.disassociate_simulation_from_location("test-sim", "test-location")
        
        # Assert
        assert isinstance(result, SimulationDto)
        mock_disassociate.assert_called_once_with("test-location")
        mock_simulation_repo.save.assert_called_once()
    
    def test_disassociate_simulation_from_location_not_associated(self, service, mock_simulation_repo, 
                                                                sample_simulation_entity):
        """Test disassociation when location is not associated."""
        # Arrange
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        
        # Mock the location methods
        with patch.object(sample_simulation_entity, 'is_location_associated', return_value=False):
            
            # Act & Assert
            with pytest.raises(ValidationError) as exc_info:
                service.disassociate_simulation_from_location("test-sim", "test-location")
            
            assert "is not associated" in str(exc_info.value)


class TestSimulationAttributes:
    """Test simulation attribute management operations."""
    
    def test_add_simulation_attribute_success(self, service, mock_simulation_repo, sample_simulation_entity):
        """Test successful attribute addition."""
        # Arrange
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        mock_simulation_repo.save.return_value = None
        
        # Mock the add_attribute method
        with patch.object(sample_simulation_entity, 'add_attribute') as mock_add_attr:
            # Act
            service.add_simulation_attribute("test-sim", "new_key", "new_value")
        
        # Assert
        mock_add_attr.assert_called_once_with("new_key", "new_value")
        mock_simulation_repo.save.assert_called_once()
    
    def test_add_simulation_attribute_validation_error(self, service, mock_simulation_repo, 
                                                     sample_simulation_entity):
        """Test attribute addition with validation error."""
        # Arrange
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        
        # Mock the add_attribute method to raise ValueError
        with patch.object(sample_simulation_entity, 'add_attribute', 
                         side_effect=ValueError("Invalid attribute")):
            
            # Act & Assert
            with pytest.raises(ValidationError) as exc_info:
                service.add_simulation_attribute("test-sim", "invalid_key", "value")
            
            assert "Invalid attribute" in str(exc_info.value)
    
    def test_get_simulation_context_success(self, service, mock_simulation_repo, sample_simulation_entity):
        """Test successful context retrieval."""
        # Arrange
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        
        # Act
        result = service.get_simulation_context("test-sim")
        
        # Assert
        assert isinstance(result, dict)
        assert "experiment" in result
        assert result["experiment"] == "test"


class TestSnakemakeRules:
    """Test Snakemake rule management operations."""
    
    def test_add_snakemake_rule_success(self, service, mock_simulation_repo, sample_simulation_entity):
        """Test successful Snakemake rule addition."""
        # Arrange
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        mock_simulation_repo.save.return_value = None
        
        # Mock the add_snakemake_rule method
        with patch.object(sample_simulation_entity, 'add_snakemake_rule') as mock_add_rule:
            # Act
            service.add_snakemake_rule("test-sim", "new_rule", "/path/to/rule.smk")
        
        # Assert
        mock_add_rule.assert_called_once_with("new_rule", "/path/to/rule.smk")
        mock_simulation_repo.save.assert_called_once()
    
    def test_add_snakemake_rule_duplicate(self, service, mock_simulation_repo, sample_simulation_entity):
        """Test adding a duplicate Snakemake rule."""
        # Arrange
        mock_simulation_repo.get_by_id.return_value = sample_simulation_entity
        
        # Mock the add_snakemake_rule method to raise ValueError for duplicate
        with patch.object(sample_simulation_entity, 'add_snakemake_rule', 
                         side_effect=ValueError("Rule already exists")):
            
            # Act & Assert
            with pytest.raises(BusinessRuleViolationError) as exc_info:
                service.add_snakemake_rule("test-sim", "existing_rule", "/path/to/rule.smk")
            
            assert "already exists" in str(exc_info.value)


class TestPrivateHelperMethods:
    """Test private helper methods."""
    
    def test_entity_to_dto_conversion(self, service, sample_simulation_entity):
        """Test entity to DTO conversion."""
        # Arrange
        with patch.object(sample_simulation_entity, 'get_associated_locations', return_value={"test-location"}), \
             patch.object(sample_simulation_entity, 'get_location_context', return_value={"path_prefix": "/test"}):
            
            # Act
            result = service._entity_to_dto(sample_simulation_entity)
        
        # Assert
        assert isinstance(result, SimulationDto)
        assert result.simulation_id == "test-sim"
        assert result.model_id == "test-model"
        assert result.path == "/test/path"
        assert result.attrs == {"experiment": "test"}
        assert "LocationContext" in result.contexts
    
    def test_apply_filters_search_term(self, service):
        """Test applying search term filters."""
        # Arrange
        simulations = [
            SimulationEntity(simulation_id="test-sim-1", model_id="model-a"),
            SimulationEntity(simulation_id="prod-sim-2", model_id="model-b"),
            SimulationEntity(simulation_id="test-sim-3", model_id="model-c")
        ]
        filters = FilterOptions(search_term="test")
        
        # Act
        result = service._apply_filters(simulations, filters)
        
        # Assert
        assert len(result) == 2
        assert all("test" in sim.simulation_id for sim in result)
    
    def test_apply_filters_no_matches(self, service):
        """Test applying filters with no matches."""
        # Arrange
        simulations = [
            SimulationEntity(simulation_id="prod-sim-1", model_id="model-a"),
            SimulationEntity(simulation_id="prod-sim-2", model_id="model-b")
        ]
        filters = FilterOptions(search_term="test")
        
        # Act
        result = service._apply_filters(simulations, filters)
        
        # Assert
        assert len(result) == 0
    
    def test_validate_location_associations_success(self, service, sample_simulation_entity, sample_location_entity):
        """Test successful location association validation."""
        # Arrange
        locations = [sample_location_entity]
        
        # Mock the location methods
        with patch.object(sample_location_entity, 'get_protocol', return_value="file"):
            # Act & Assert - Should not raise any exception
            service._validate_location_associations(sample_simulation_entity, locations)
    
    def test_validate_location_associations_no_required_locations(self, service, sample_simulation_entity):
        """Test location association validation with no required locations."""
        # Arrange
        optional_location = LocationEntity(
            name="optional-location",
            kinds=[LocationKind.DISK],
            optional=True,
            config={"protocol": "file", "path": "/test"}
        )
        locations = [optional_location]
        
        # Act & Assert
        with pytest.raises(BusinessRuleViolationError) as exc_info:
            service._validate_location_associations(sample_simulation_entity, locations)
        
        assert "required" in str(exc_info.value)
    
    def test_validate_location_associations_conflicting_protocols(self, service, sample_simulation_entity):
        """Test location association validation with conflicting protocols."""
        # Arrange
        location1 = LocationEntity(
            name="location1",
            kinds=[LocationKind.DISK],
            optional=False,
            config={"protocol": "file", "path": "/test1"}
        )
        location2 = LocationEntity(
            name="location2", 
            kinds=[LocationKind.DISK],
            optional=False,
            config={"protocol": "ssh", "storage_options": {"host": "remote"}}
        )
        locations = [location1, location2]
        
        # Mock the get_protocol methods
        with patch.object(location1, 'get_protocol', return_value="file"), \
             patch.object(location2, 'get_protocol', return_value="ssh"):
            
            # Act & Assert
            with pytest.raises(BusinessRuleViolationError) as exc_info:
                service._validate_location_associations(sample_simulation_entity, locations)
            
            assert "Conflicting protocols" in str(exc_info.value)


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_repository_error_propagation(self, service, mock_simulation_repo):
        """Test that repository errors are properly propagated."""
        # Arrange
        mock_simulation_repo.list_all.side_effect = RepositoryError("Database connection failed")
        
        # Act & Assert
        with pytest.raises(RepositoryError) as exc_info:
            service.list_simulations()
        
        assert "Database connection failed" in str(exc_info.value)
    
    def test_unexpected_error_handling(self, service, mock_simulation_repo):
        """Test handling of unexpected errors."""
        # Arrange
        dto = CreateSimulationDto(simulation_id="test-sim")
        mock_simulation_repo.exists.return_value = False
        mock_simulation_repo.save.side_effect = RuntimeError("Unexpected error")
        
        # Act & Assert
        with pytest.raises(RuntimeError):
            service.create_simulation(dto)