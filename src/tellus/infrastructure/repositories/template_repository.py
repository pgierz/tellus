"""
Repository implementation for simulation templates.

Provides persistence layer for simulation templates with support for JSON file storage.
"""

import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Dict, Any
from loguru import logger

from ...domain.entities.simulation_template import SimulationTemplate


class TemplateRepository(ABC):
    """Abstract base class for template repositories."""

    @abstractmethod
    def save(self, template: SimulationTemplate) -> SimulationTemplate:
        """Save a template."""
        pass

    @abstractmethod
    def get_by_name(self, name: str) -> Optional[SimulationTemplate]:
        """Get template by name."""
        pass

    @abstractmethod
    def get_by_id(self, template_id: str) -> Optional[SimulationTemplate]:
        """Get template by ID."""
        pass

    @abstractmethod
    def list_all(self) -> List[SimulationTemplate]:
        """List all templates."""
        pass

    @abstractmethod
    def exists_by_name(self, name: str) -> bool:
        """Check if template exists by name."""
        pass

    @abstractmethod
    def delete(self, name: str) -> bool:
        """Delete template by name."""
        pass


class JsonTemplateRepository(TemplateRepository):
    """
    JSON file-based template repository.

    Stores templates in a JSON file for simple persistence without external dependencies.
    """

    def __init__(self, file_path: Optional[str] = None):
        """
        Initialize repository with JSON file path.

        Args:
            file_path: Path to JSON file for template storage.
                      Defaults to ~/.local/tellus/templates.json
        """
        if file_path is None:
            home_dir = Path.home()
            config_dir = home_dir / '.local' / 'tellus'
            config_dir.mkdir(parents=True, exist_ok=True)
            file_path = str(config_dir / 'templates.json')

        self._file_path = Path(file_path)
        self._logger = logger.bind(repository="JsonTemplateRepository")

        # Ensure parent directory exists
        self._file_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize empty file if it doesn't exist
        if not self._file_path.exists():
            self._save_templates([])

    def save(self, template: SimulationTemplate) -> SimulationTemplate:
        """Save a template to the JSON file."""
        templates = self._load_templates()

        # Remove existing template with same name
        templates = [t for t in templates if t.name != template.name]

        # Add the new/updated template
        templates.append(template)

        self._save_templates(templates)
        self._logger.debug(f"Saved template: {template.name}")

        return template

    def get_by_name(self, name: str) -> Optional[SimulationTemplate]:
        """Get template by name."""
        templates = self._load_templates()
        for template in templates:
            if template.name == name:
                return template
        return None

    def get_by_id(self, template_id: str) -> Optional[SimulationTemplate]:
        """Get template by ID."""
        templates = self._load_templates()
        for template in templates:
            if template.template_id == template_id:
                return template
        return None

    def list_all(self) -> List[SimulationTemplate]:
        """List all templates."""
        return self._load_templates()

    def exists_by_name(self, name: str) -> bool:
        """Check if template exists by name."""
        return self.get_by_name(name) is not None

    def delete(self, name: str) -> bool:
        """Delete template by name."""
        templates = self._load_templates()
        original_count = len(templates)

        templates = [t for t in templates if t.name != name]

        if len(templates) < original_count:
            self._save_templates(templates)
            self._logger.debug(f"Deleted template: {name}")
            return True

        return False

    def _load_templates(self) -> List[SimulationTemplate]:
        """Load templates from JSON file."""
        try:
            if not self._file_path.exists():
                return []

            with open(self._file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            templates = []
            for template_data in data:
                try:
                    template = SimulationTemplate(**template_data)
                    templates.append(template)
                except Exception as e:
                    self._logger.warning(f"Failed to load template: {e}")

            return templates

        except json.JSONDecodeError as e:
            self._logger.error(f"JSON decode error loading templates: {e}")
            return []
        except Exception as e:
            self._logger.error(f"Error loading templates: {e}")
            return []

    def _save_templates(self, templates: List[SimulationTemplate]) -> None:
        """Save templates to JSON file."""
        try:
            # Convert templates to dictionaries
            template_dicts = []
            for template in templates:
                template_dict = template.model_dump()
                # Convert sets to lists for JSON serialization
                if 'tags' in template_dict and isinstance(template_dict['tags'], set):
                    template_dict['tags'] = list(template_dict['tags'])
                template_dicts.append(template_dict)

            # Write to file with proper formatting
            with open(self._file_path, 'w', encoding='utf-8') as f:
                json.dump(template_dicts, f, indent=2, ensure_ascii=False)

        except Exception as e:
            self._logger.error(f"Error saving templates: {e}")
            raise


class PostgresTemplateRepository(TemplateRepository):
    """
    PostgreSQL-based template repository.

    Future implementation for database-backed template storage.
    """

    def __init__(self, db_connection):
        """Initialize with database connection."""
        self._db = db_connection
        self._logger = logger.bind(repository="PostgresTemplateRepository")
        # TODO: Implement database schema and operations

    def save(self, template: SimulationTemplate) -> SimulationTemplate:
        """Save template to database."""
        # TODO: Implement database save
        raise NotImplementedError("PostgreSQL template repository not yet implemented")

    def get_by_name(self, name: str) -> Optional[SimulationTemplate]:
        """Get template by name from database."""
        # TODO: Implement database query
        raise NotImplementedError("PostgreSQL template repository not yet implemented")

    def get_by_id(self, template_id: str) -> Optional[SimulationTemplate]:
        """Get template by ID from database."""
        # TODO: Implement database query
        raise NotImplementedError("PostgreSQL template repository not yet implemented")

    def list_all(self) -> List[SimulationTemplate]:
        """List all templates from database."""
        # TODO: Implement database query
        raise NotImplementedError("PostgreSQL template repository not yet implemented")

    def exists_by_name(self, name: str) -> bool:
        """Check if template exists by name in database."""
        # TODO: Implement database query
        raise NotImplementedError("PostgreSQL template repository not yet implemented")

    def delete(self, name: str) -> bool:
        """Delete template by name from database."""
        # TODO: Implement database delete
        raise NotImplementedError("PostgreSQL template repository not yet implemented")