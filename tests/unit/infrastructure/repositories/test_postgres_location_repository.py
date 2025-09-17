"""
Tests for PostgreSQL location repository.

Uses mocked database infrastructure for unit testing without real database connections.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from sqlalchemy.exc import IntegrityError

from tellus.infrastructure.repositories.postgres_location_repository import PostgresLocationRepository
from tellus.domain.entities.location import LocationEntity, LocationKind, PathTemplate
from tellus.domain.repositories.exceptions import LocationExistsError, RepositoryError


@pytest.fixture
def repository():
    """Create repository instance for testing."""
    return PostgresLocationRepository()


@pytest.fixture
def repository_with_session(mock_session):
    """Create repository instance with provided session for testing."""
    return PostgresLocationRepository(session=mock_session)


@pytest.mark.asyncio
class TestPostgresLocationRepository:
    """Test PostgreSQL location repository functionality."""

    async def test_save_new_location(
        self, repository, mock_session, sample_location_entity, mock_query_result
    ):
        """Test saving a new location."""
        # Mock that location doesn't exist
        mock_session.execute.return_value = mock_query_result(scalar_result=None)

        await repository.save(sample_location_entity)

        # Verify that we checked for existing location
        assert mock_session.execute.called
        # Verify that we added the new location
        assert mock_session.add.called

    async def test_save_update_existing_location(
        self, repository, mock_session, sample_location_entity, sample_location_model, mock_query_result
    ):
        """Test updating an existing location."""
        # Mock that location exists
        mock_session.execute.return_value = mock_query_result(scalar_result=sample_location_model)

        await repository.save(sample_location_entity)

        # Verify that we checked for existing location
        assert mock_session.execute.called
        # Verify that we didn't add (since it already exists)
        assert not mock_session.add.called

    async def test_save_with_provided_session(
        self, repository_with_session, mock_session, sample_location_entity, mock_query_result
    ):
        """Test saving with a provided session."""
        # Mock that location doesn't exist
        mock_session.execute.return_value = mock_query_result(scalar_result=None)

        await repository_with_session.save(sample_location_entity)

        # Should not commit when using provided session
        assert not mock_session.commit.called

    async def test_save_integrity_error(
        self, repository, mock_session, sample_location_entity, mock_query_result
    ):
        """Test handling of integrity errors during save."""
        # Mock that location doesn't exist initially
        mock_session.execute.return_value = mock_query_result(scalar_result=None)
        # But then raise IntegrityError on add
        mock_session.add.side_effect = IntegrityError("duplicate key", None, None)

        with pytest.raises(LocationExistsError):
            await repository.save(sample_location_entity)

    async def test_get_by_name_existing(
        self, repository, mock_session, sample_location_model, sample_location_entity, mock_query_result
    ):
        """Test retrieving an existing location."""
        # Mock the location exists
        mock_session.execute.return_value = mock_query_result(scalar_result=sample_location_model)

        result = await repository.get_by_name(sample_location_entity.name)

        assert result is not None
        assert result.name == sample_location_entity.name
        assert result.config['protocol'] == sample_location_entity.config['protocol']

    async def test_get_by_name_nonexistent(
        self, repository, mock_session, mock_query_result
    ):
        """Test retrieving a location that doesn't exist."""
        # Mock that location doesn't exist
        mock_session.execute.return_value = mock_query_result(scalar_result=None)

        result = await repository.get_by_name("nonexistent")

        assert result is None

    async def test_list_all_locations(
        self, repository, mock_session, sample_location_model, mock_query_result
    ):
        """Test listing all locations."""
        # Mock the locations exist
        models = [sample_location_model]
        mock_session.execute.return_value = mock_query_result(models=models)

        results = await repository.list_all()

        assert len(results) == 1
        assert results[0].name == sample_location_model.name

    async def test_list_all_empty(
        self, repository, mock_session, mock_query_result
    ):
        """Test listing locations when none exist."""
        # Mock empty result
        mock_session.execute.return_value = mock_query_result(models=[])

        results = await repository.list_all()

        assert len(results) == 0

    async def test_delete_existing_location(
        self, repository, mock_session, mock_query_result
    ):
        """Test deleting an existing location."""
        # Mock successful delete
        result_mock = MagicMock()
        result_mock.rowcount = 1
        mock_session.execute.return_value = result_mock

        result = await repository.delete("test_location")

        assert result is True

    async def test_delete_nonexistent_location(
        self, repository, mock_session, mock_query_result
    ):
        """Test deleting a location that doesn't exist."""
        # Mock no rows affected
        result_mock = MagicMock()
        result_mock.rowcount = 0
        mock_session.execute.return_value = result_mock

        result = await repository.delete("nonexistent")

        assert result is False

    async def test_exists_location_exists(
        self, repository, mock_session, mock_query_result
    ):
        """Test checking existence of an existing location."""
        # Mock that location exists
        mock_session.execute.return_value = mock_query_result(scalar_result="test_location")

        result = await repository.exists("test_location")

        assert result is True

    async def test_exists_location_does_not_exist(
        self, repository, mock_session, mock_query_result
    ):
        """Test checking existence of a non-existing location."""
        # Mock that location doesn't exist
        mock_session.execute.return_value = mock_query_result(scalar_result=None)

        result = await repository.exists("nonexistent")

        assert result is False

    async def test_find_by_kind(
        self, repository, mock_session, mock_query_result
    ):
        """Test finding locations by kind."""
        # Create mock location models with different kinds
        compute_location = MagicMock()
        compute_location.name = "compute1"
        compute_location.kinds = ["COMPUTE"]
        compute_location.protocol = "ssh"
        compute_location.path = "/work"
        compute_location.storage_options = {}
        compute_location.additional_config = {}
        compute_location.is_remote = True
        compute_location.is_accessible = True
        compute_location.last_verified = None
        compute_location.path_templates = {}

        # Mock the query result
        mock_session.execute.return_value = mock_query_result(models=[compute_location])

        results = await repository.find_by_kind(LocationKind.COMPUTE)

        assert len(results) == 1
        assert results[0].name == "compute1"

    async def test_find_by_protocol(
        self, repository, mock_session, mock_query_result
    ):
        """Test finding locations by protocol."""
        # Create mock location model
        ssh_location = MagicMock()
        ssh_location.name = "ssh1"
        ssh_location.kinds = ["COMPUTE"]
        ssh_location.protocol = "ssh"
        ssh_location.path = "/work"
        ssh_location.storage_options = {}
        ssh_location.additional_config = {}
        ssh_location.is_remote = True
        ssh_location.is_accessible = True
        ssh_location.last_verified = None
        ssh_location.path_templates = {}

        # Mock the query result
        mock_session.execute.return_value = mock_query_result(models=[ssh_location])

        results = await repository.find_by_protocol("ssh")

        assert len(results) == 1
        assert results[0].name == "ssh1"

    async def test_count_locations(
        self, repository, mock_session, sample_location_model, mock_query_result
    ):
        """Test counting locations."""
        # Mock 3 locations exist
        models = [sample_location_model] * 3
        mock_session.execute.return_value = mock_query_result(models=models)

        result = await repository.count()

        assert result == 3

    async def test_count_no_locations(
        self, repository, mock_session, mock_query_result
    ):
        """Test counting when no locations exist."""
        # Mock empty result
        mock_session.execute.return_value = mock_query_result(models=[])

        result = await repository.count()

        assert result == 0

    async def test_path_templates_conversion(
        self, repository, sample_location_entity
    ):
        """Test path templates conversion to/from dict."""
        # Test entity to model conversion
        location_model = repository._entity_to_model(sample_location_entity)

        assert isinstance(location_model.path_templates, dict)

        # Test model to entity conversion back
        location_entity = repository._model_to_entity(location_model)

        assert len(location_entity.path_templates) == len(sample_location_entity.path_templates)

    async def test_kind_conversion(
        self, repository, sample_location_entity
    ):
        """Test LocationKind enum conversion to/from strings."""
        # Test entity to model conversion
        location_model = repository._entity_to_model(sample_location_entity)

        assert all(isinstance(kind, str) for kind in location_model.kinds)
        assert "COMPUTE" in location_model.kinds
        assert "DISK" in location_model.kinds

        # Test model to entity conversion back
        location_entity = repository._model_to_entity(location_model)

        assert LocationKind.COMPUTE in location_entity.kinds
        assert LocationKind.DISK in location_entity.kinds

    async def test_database_error_handling(
        self, repository, mock_session, sample_location_entity
    ):
        """Test handling of database errors."""
        # Mock database error
        mock_session.execute.side_effect = Exception("Database connection error")

        with pytest.raises(RepositoryError):
            await repository.save(sample_location_entity)


@pytest.mark.asyncio
class TestPostgresLocationRepositoryErrorHandling:
    """Test error handling in PostgreSQL location repository."""

    async def test_get_by_name_database_error(
        self, repository, mock_session
    ):
        """Test handling of database errors during get_by_name."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError):
            await repository.get_by_name("test_location")

    async def test_list_all_database_error(
        self, repository, mock_session
    ):
        """Test handling of database errors during list_all."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError):
            await repository.list_all()

    async def test_delete_database_error(
        self, repository, mock_session
    ):
        """Test handling of database errors during delete."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError):
            await repository.delete("test_location")

    async def test_exists_database_error(
        self, repository, mock_session
    ):
        """Test handling of database errors during exists check."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError):
            await repository.exists("test_location")

    async def test_find_by_kind_database_error(
        self, repository, mock_session
    ):
        """Test handling of database errors during find_by_kind."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError):
            await repository.find_by_kind(LocationKind.COMPUTE)

    async def test_find_by_protocol_database_error(
        self, repository, mock_session
    ):
        """Test handling of database errors during find_by_protocol."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError):
            await repository.find_by_protocol("ssh")

    async def test_count_database_error(
        self, repository, mock_session
    ):
        """Test handling of database errors during count."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError):
            await repository.count()


@pytest.mark.asyncio
class TestAsyncLocationRepositoryWrapper:
    """Test the async to sync wrapper functionality."""

    def test_wrapper_creation(self):
        """Test creating the wrapper."""
        from tellus.infrastructure.repositories.postgres_location_repository import AsyncLocationRepositoryWrapper

        async_repo = PostgresLocationRepository()
        wrapper = AsyncLocationRepositoryWrapper(async_repo)

        assert wrapper.async_repo is async_repo

    @patch('asyncio.run')
    def test_wrapper_save(self, mock_asyncio_run, sample_location_entity):
        """Test sync wrapper for save operation."""
        from tellus.infrastructure.repositories.postgres_location_repository import AsyncLocationRepositoryWrapper

        async_repo = PostgresLocationRepository()
        wrapper = AsyncLocationRepositoryWrapper(async_repo)

        wrapper.save(sample_location_entity)

        mock_asyncio_run.assert_called_once()

    @patch('asyncio.run')
    def test_wrapper_get_by_name(self, mock_asyncio_run):
        """Test sync wrapper for get_by_name operation."""
        from tellus.infrastructure.repositories.postgres_location_repository import AsyncLocationRepositoryWrapper

        async_repo = PostgresLocationRepository()
        wrapper = AsyncLocationRepositoryWrapper(async_repo)

        wrapper.get_by_name("test_location")

        mock_asyncio_run.assert_called_once()

    @patch('asyncio.run')
    def test_wrapper_list_all(self, mock_asyncio_run):
        """Test sync wrapper for list_all operation."""
        from tellus.infrastructure.repositories.postgres_location_repository import AsyncLocationRepositoryWrapper

        async_repo = PostgresLocationRepository()
        wrapper = AsyncLocationRepositoryWrapper(async_repo)

        wrapper.list_all()

        mock_asyncio_run.assert_called_once()

    @patch('asyncio.run')
    def test_wrapper_delete(self, mock_asyncio_run):
        """Test sync wrapper for delete operation."""
        from tellus.infrastructure.repositories.postgres_location_repository import AsyncLocationRepositoryWrapper

        async_repo = PostgresLocationRepository()
        wrapper = AsyncLocationRepositoryWrapper(async_repo)

        wrapper.delete("test_location")

        mock_asyncio_run.assert_called_once()

    @patch('asyncio.run')
    def test_wrapper_exists(self, mock_asyncio_run):
        """Test sync wrapper for exists operation."""
        from tellus.infrastructure.repositories.postgres_location_repository import AsyncLocationRepositoryWrapper

        async_repo = PostgresLocationRepository()
        wrapper = AsyncLocationRepositoryWrapper(async_repo)

        wrapper.exists("test_location")

        mock_asyncio_run.assert_called_once()

    @patch('asyncio.run')
    def test_wrapper_find_by_kind(self, mock_asyncio_run):
        """Test sync wrapper for find_by_kind operation."""
        from tellus.infrastructure.repositories.postgres_location_repository import AsyncLocationRepositoryWrapper

        async_repo = PostgresLocationRepository()
        wrapper = AsyncLocationRepositoryWrapper(async_repo)

        wrapper.find_by_kind(LocationKind.COMPUTE)

        mock_asyncio_run.assert_called_once()

    @patch('asyncio.run')
    def test_wrapper_find_by_protocol(self, mock_asyncio_run):
        """Test sync wrapper for find_by_protocol operation."""
        from tellus.infrastructure.repositories.postgres_location_repository import AsyncLocationRepositoryWrapper

        async_repo = PostgresLocationRepository()
        wrapper = AsyncLocationRepositoryWrapper(async_repo)

        wrapper.find_by_protocol("ssh")

        mock_asyncio_run.assert_called_once()

    @patch('asyncio.run')
    def test_wrapper_count(self, mock_asyncio_run):
        """Test sync wrapper for count operation."""
        from tellus.infrastructure.repositories.postgres_location_repository import AsyncLocationRepositoryWrapper

        async_repo = PostgresLocationRepository()
        wrapper = AsyncLocationRepositoryWrapper(async_repo)

        wrapper.count()

        mock_asyncio_run.assert_called_once()


@pytest.mark.asyncio
class TestLocationEntityConversions:
    """Test entity to model conversions in isolation."""

    def test_path_templates_to_dict_empty(self):
        """Test converting empty path templates."""
        repository = PostgresLocationRepository()
        result = repository._path_templates_to_dict([])
        assert result == {}

    def test_path_templates_to_dict_with_data(self):
        """Test converting path templates with data."""
        repository = PostgresLocationRepository()
        templates = [
            PathTemplate(
                name="test_template",
                pattern="{model}/{experiment}",
                description="Test template",
                required_attributes=["model", "experiment"]
            )
        ]

        result = repository._path_templates_to_dict(templates)

        assert "test_template" in result
        assert result["test_template"]["pattern"] == "{model}/{experiment}"
        assert result["test_template"]["description"] == "Test template"
        assert result["test_template"]["required_attributes"] == ["model", "experiment"]

    def test_dict_to_path_templates_empty(self):
        """Test converting empty dict to path templates."""
        repository = PostgresLocationRepository()
        result = repository._dict_to_path_templates({})
        assert result == []

    def test_dict_to_path_templates_with_data(self):
        """Test converting dict to path templates with data."""
        repository = PostgresLocationRepository()
        templates_dict = {
            "test_template": {
                "pattern": "{model}/{experiment}",
                "description": "Test template",
                "required_attributes": ["model", "experiment"]
            }
        }

        result = repository._dict_to_path_templates(templates_dict)

        assert len(result) == 1
        assert result[0].name == "test_template"
        assert result[0].pattern == "{model}/{experiment}"
        assert result[0].description == "Test template"
        assert result[0].required_attributes == ["model", "experiment"]