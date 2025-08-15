"""
Bridge adapters between legacy classes and new application services.

This module provides compatibility layers that maintain backward compatibility
while delegating operations to the new clean architecture.
"""

from typing import Dict, List, Optional, Any
import logging
import warnings
import inspect
from dataclasses import asdict

from ..application.service_factory import ApplicationServiceFactory
from ..application.dtos import (
    CreateSimulationDto,
    SimulationDto,
    UpdateSimulationDto,
    SimulationListDto,
    CreateLocationDto,
    LocationDto,
    SimulationLocationAssociationDto,
    CreateArchiveDto,
    ArchiveDto,
    ArchiveListDto,
    ArchiveOperationDto,
)
from ..application.exceptions import EntityNotFoundError, EntityAlreadyExistsError
from ..domain.entities.simulation import SimulationEntity

logger = logging.getLogger(__name__)


class SimulationBridge:
    """Bridge between legacy Simulation class and new application services."""

    def __init__(self, service_factory: ApplicationServiceFactory):
        self._service_factory = service_factory
        self._simulation_service = service_factory.simulation_service
        self._location_service = service_factory.location_service

    def create_simulation_from_legacy_data(
        self,
        simulation_id: str,
        model_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        base_path: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> SimulationDto:
        """Create simulation using new service from legacy-style parameters."""
        create_dto = CreateSimulationDto(
            simulation_id=simulation_id,
            model_id=model_id,
            experiment_id=experiment_id,
            base_path=base_path,
            attributes=attributes or {},
            description=f"Created via legacy bridge interface",
        )

        logger.debug(f"Creating simulation via bridge: {simulation_id}")
        return self._simulation_service.create_simulation(create_dto)

    def get_simulation_legacy_format(
        self, simulation_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get simulation data in legacy format compatible with old CLI."""
        try:
            sim_dto = self._simulation_service.get_simulation(simulation_id)

            # Convert to legacy format that old CLI expects
            legacy_format = {
                "simulation_id": sim_dto.simulation_id,
                "model_id": sim_dto.model_id,
                "experiment_id": sim_dto.simulation_id,  # Use simulation_id for experiment_id in legacy format
                "path": getattr(
                    sim_dto, "base_path", sim_dto.path
                ),  # Handle both base_path and path attributes
                "attrs": getattr(sim_dto, "attributes", sim_dto.attrs) or {},
                "locations": self._convert_contexts_to_legacy_locations(
                    sim_dto.contexts
                ),
                # Additional legacy fields - use defaults for missing fields
                "created_at": None,  # Not available in current DTO
                "updated_at": None,  # Not available in current DTO
                "description": f"Simulation {sim_dto.simulation_id}",  # Generate default description
                "context_variables": self._extract_context_variables_from_contexts(
                    sim_dto.contexts
                ),
            }

            return legacy_format

        except EntityNotFoundError:
            logger.debug(f"Simulation not found in new architecture: {simulation_id}")
            return None

    def list_simulations_legacy_format(self) -> List[Dict[str, Any]]:
        """List all simulations in legacy format."""
        try:
            sim_list = self._simulation_service.list_simulations()
            legacy_list = []

            for sim in sim_list.simulations:
                legacy_sim = self.get_simulation_legacy_format(sim.simulation_id)
                if legacy_sim:
                    legacy_list.append(legacy_sim)

            logger.debug(f"Listed {len(legacy_list)} simulations via bridge")
            return legacy_list

        except Exception as e:
            logger.error(f"Error listing simulations via bridge: {e}")
            return []

    def update_simulation_attributes(
        self, simulation_id: str, attributes: Dict[str, Any]
    ) -> bool:
        """Update simulation attributes using new service."""
        try:
            update_dto = UpdateSimulationDto(
                attrs=attributes
            )

            updated_sim = self._simulation_service.update_simulation(simulation_id, update_dto)
            logger.debug(f"Updated simulation attributes via bridge: {simulation_id}")
            return True

        except EntityNotFoundError:
            logger.warning(f"Cannot update non-existent simulation: {simulation_id}")
            return False
        except Exception as e:
            logger.error(f"Error updating simulation via bridge: {e}")
            return False

    def update_simulation(
        self,
        simulation_id: str,
        *,
        path: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
        model_id: Optional[str] = None,
        namelists: Optional[Dict[str, Any]] = None,
        snakemakes: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Update simulation fields using new service.

        Supports updating path and attrs (and optionally model_id, namelists, snakemakes).
        """
        try:
            update_dto = UpdateSimulationDto(
                model_id=model_id,
                path=path,
                attrs=attributes,
                namelists=namelists,
                snakemakes=snakemakes,
            )

            self._simulation_service.update_simulation(simulation_id, update_dto)
            logger.debug(f"Updated simulation via bridge: {simulation_id}")
            return True
        except EntityNotFoundError:
            logger.warning(f"Cannot update non-existent simulation: {simulation_id}")
            return False
        except Exception as e:
            logger.error(f"Error updating simulation via bridge: {e}")
            return False

    def delete_simulation(self, simulation_id: str) -> bool:
        """Delete simulation using new service."""
        try:
            self._simulation_service.delete_simulation(simulation_id)
            logger.debug(f"Deleted simulation via bridge: {simulation_id}")
            return True

        except EntityNotFoundError:
            logger.warning(f"Cannot delete non-existent simulation: {simulation_id}")
            return False
        except Exception as e:
            logger.error(f"Error deleting simulation via bridge: {e}")
            return False

    def _convert_locations_to_legacy_format(
        self, location_names: List[str]
    ) -> Dict[str, Any]:
        """Convert location associations to legacy format."""
        locations = {}

        for name in location_names:
            try:
                loc_dto = self._location_service.get_location(name)
                locations[name] = {
                    "location": {
                        "name": loc_dto.name,
                        "kinds": [kind.name for kind in loc_dto.kinds],
                        "protocol": loc_dto.protocol,
                        "config": loc_dto.configuration,
                        "optional": loc_dto.optional,
                    },
                    "context": {
                        "path_prefix": loc_dto.path_prefix or "",
                        "overrides": {},
                        "metadata": {},
                    },
                }
            except EntityNotFoundError:
                logger.warning(f"Location {name} not found during legacy conversion")
                continue

        return locations

    def _convert_contexts_to_legacy_locations(
        self, contexts: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Convert contexts system to legacy locations format."""
        locations = {}

        # Extract location contexts
        location_contexts = contexts.get("LocationContext", {})

        for location_name, context_data in location_contexts.items():
            try:
                # Get location details from location service
                loc_dto = self._location_service.get_location(location_name)
                locations[location_name] = {
                    "location": {
                        "name": loc_dto.name,
                        "kinds": [kind.name for kind in loc_dto.kinds],
                        "protocol": loc_dto.protocol,
                        "config": loc_dto.configuration,
                        "optional": loc_dto.optional,
                    },
                    "context": context_data,
                }
            except EntityNotFoundError:
                logger.warning(
                    f"Location {location_name} not found during context conversion"
                )
                # Include context even if location not found
                locations[location_name] = {
                    "location": {
                        "name": location_name,
                        "kinds": [],
                        "protocol": "unknown",
                        "config": {},
                        "optional": False,
                    },
                    "context": context_data,
                }

        return locations

    def _extract_context_variables_from_contexts(
        self, contexts: Dict[str, Dict[str, Any]]
    ) -> Dict[str, str]:
        """Extract simple context variables from contexts system for legacy compatibility."""
        context_variables = {}

        # Extract path_prefix values as context variables
        location_contexts = contexts.get("LocationContext", {})
        for location_name, context_data in location_contexts.items():
            path_prefix = context_data.get("path_prefix", "")
            if path_prefix:
                context_variables[f"{location_name}_path_prefix"] = path_prefix

        return context_variables

    def _dto_to_entity(self, sim_dto: SimulationDto) -> SimulationEntity:
        """Convert SimulationDto to SimulationEntity for legacy code."""
        # Extract location contexts from the new contexts system
        location_contexts = sim_dto.contexts.get("LocationContext", {})

        # Create SimulationEntity with the data
        entity = SimulationEntity(
            simulation_id=sim_dto.simulation_id,
            model_id=sim_dto.model_id,
            path=sim_dto.path,
            attrs=sim_dto.attrs.copy(),
            namelists=sim_dto.namelists.copy(),
            snakemakes=sim_dto.snakemakes.copy(),
            associated_locations=set(location_contexts.keys()),
            location_contexts=location_contexts.copy(),
        )

        # Set the internal UID
        entity._uid = sim_dto.uid

        return entity

    def _entity_to_create_dto(self, entity: SimulationEntity) -> CreateSimulationDto:
        """Convert SimulationEntity to CreateSimulationDto (OLD→NEW direction with warning)."""
        return CreateSimulationDto(
            simulation_id=entity.simulation_id,
            model_id=entity.model_id,
            path=entity.path,
            attrs=entity.attrs.copy(),
            namelists=entity.namelists.copy(),
            snakemakes=entity.snakemakes.copy(),
        )

    def get_simulation(self, simulation_id: str) -> Optional[SimulationEntity]:
        """
        NEW→OLD: Get SimulationEntity from new service for legacy CLI code.
        ✅ APPROVED DIRECTION: New service → Old domain objects
        """
        try:
            sim_dto = self._simulation_service.get_simulation(simulation_id)
            return self._dto_to_entity(sim_dto)

        except EntityNotFoundError:
            logger.debug(f"Simulation not found in new architecture: {simulation_id}")
            return None
        except Exception as e:
            logger.error(f"Error getting simulation via bridge: {e}")
            return None

    def associate_location_to_simulation(
        self,
        simulation_id: str,
        location_name: str,
        context_data: Dict[str, Any] = None,
    ) -> bool:
        """Associate a location with a simulation using the new service architecture."""
        try:
            # Use the service to add location association
            # The service expects context_overrides to be a mapping from
            # location_name -> context dict. Wrap the provided context accordingly.
            association_dto = SimulationLocationAssociationDto(
                simulation_id=simulation_id,
                location_names=[location_name],
                context_overrides={location_name: (context_data or {})},
            )

            self._simulation_service.associate_simulation_with_locations(
                association_dto
            )
            logger.info(
                f"Successfully associated location {location_name} to simulation {simulation_id}"
            )
            return True

        except Exception as e:
            logger.error(f"Error associating location via bridge: {e}")
            return False

    def create_simulation_legacy(self, simulation_entity: SimulationEntity) -> bool:
        """
        OLD→NEW: Accept SimulationEntity and save via new service.

        WARNING: This is the LEGACY→NEW direction!
        Only use during transition period!
        Migrate your code to use SimulationService directly!
        ⚠️  WARNING: This is the LEGACY→NEW direction!
        ⚠️  Only use during transition period!
        ⚠️  Migrate your code to use SimulationService directly!
        """
        warnings.warn(
            "🚨 BRIDGE WARNING: Using OLD→NEW direction! "
            "This creates technical debt. Migrate to use SimulationService directly. "
            "This bridge direction will be removed in v0.2.0",
            DeprecationWarning,
            stacklevel=2,
        )
        caller_info = inspect.stack()[1]
        logger.warning(
            "🚨 Legacy bridge used in OLD→NEW direction from %s:%s in function %s",
            caller_info.filename,
            caller_info.lineno,
            caller_info.function,
        )

        try:
            create_dto = self._entity_to_create_dto(simulation_entity)
            self._simulation_service.create_simulation(create_dto)
            logger.info(
                f"Created simulation {simulation_entity.simulation_id} via legacy bridge"
            )
            return True
        except Exception as e:
            logger.error(f"Error creating simulation via legacy bridge: {e}")
            return False

    def update_simulation_legacy(self, simulation_entity: SimulationEntity) -> bool:
        """
        OLD→NEW: Update simulation using SimulationEntity.

        WARNING: This is the LEGACY→NEW direction!
        Only use during transition period!
        Migrate your code to use SimulationService directly!
        ⚠️  WARNING: This is the LEGACY→NEW direction!
        ⚠️  Only use during transition period!
        ⚠️  Migrate your code to use SimulationService directly!
        """
        warnings.warn(
            "🚨 BRIDGE WARNING: Using OLD→NEW direction! "
            "This creates technical debt. Migrate to use SimulationService directly. "
            "This bridge direction will be removed in v0.2.0",
            DeprecationWarning,
            stacklevel=2,
        )
        caller_info = inspect.stack()[1]
        logger.warning(
            "🚨 Legacy bridge used in OLD→NEW direction from %s:%s in function %s",
            caller_info.filename,
            caller_info.lineno,
            caller_info.function,
        )

        try:
            update_dto = UpdateSimulationDto(
                model_id=simulation_entity.model_id,
                path=simulation_entity.path,
                attrs=simulation_entity.attrs.copy(),
                namelists=simulation_entity.namelists.copy(),
                snakemakes=simulation_entity.snakemakes.copy(),
            )
            self._simulation_service.update_simulation(
                simulation_entity.simulation_id, update_dto
            )
            logger.info(
                f"Updated simulation {simulation_entity.simulation_id} via legacy bridge"
            )
            return True
        except Exception as e:
            logger.error(f"Error updating simulation via legacy bridge: {e}")
            return False


class LocationBridge:
    """Bridge for location operations between legacy and new architecture."""

    def __init__(self, service_factory: ApplicationServiceFactory):
        self._location_service = service_factory.location_service

    def list_locations_legacy_format(self) -> Dict[str, Any]:
        """List locations in legacy format."""
        try:
            location_list = self._location_service.list_locations()
            legacy_format = {}

            for loc in location_list.locations:
                legacy_format[loc.name] = {
                    "name": loc.name,
                    "kinds": loc.kinds,  # Already strings in DTO
                    "protocol": loc.protocol,
                    "config": loc.storage_options,  # Use storage_options from DTO
                    "path_prefix": loc.path,
                    "optional": loc.optional,
                    "description": getattr(
                        loc, "description", ""
                    ),  # May not exist in DTO
                }

            return legacy_format

        except Exception as e:
            logger.error(f"Error listing locations via bridge: {e}")
            return {}

    def get_location_legacy_format(
        self, location_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get single location in legacy format."""
        try:
            loc_dto = self._location_service.get_location(location_name)
            return {
                "name": loc_dto.name,
                "kinds": loc_dto.kinds,  # Already strings in DTO
                "protocol": loc_dto.protocol,
                "config": loc_dto.storage_options,  # Use storage_options from DTO
                "path_prefix": loc_dto.path,
                "optional": loc_dto.optional,
                "description": getattr(
                    loc_dto, "description", ""
                ),  # May not exist in DTO
            }
        except EntityNotFoundError:
            return None

    def create_location_from_legacy_data(
        self,
        name: str,
        protocol: str,
        kinds: List[str],
        config: Dict[str, Any],
        path_prefix: Optional[str] = None,
        optional: bool = False,
        description: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Create location using new service from legacy-style parameters."""
        try:
            from ..domain.entities.location import LocationKind as DomainLocationKind

            # Convert string kinds to domain kinds
            domain_kinds = []
            for kind_str in kinds:
                try:
                    domain_kinds.append(DomainLocationKind[kind_str.upper()])
                except KeyError:
                    logger.warning(f"Unknown location kind: {kind_str}")
                    continue

            create_dto = CreateLocationDto(
                name=name,
                kinds=[kind.name for kind in domain_kinds],
                protocol=protocol,
                path=path_prefix,
                storage_options=config.get("storage_options", {}),
                optional=optional,
                additional_config=config,
            )

            logger.debug(f"Creating location via bridge: {name}")
            loc_dto = self._location_service.create_location(create_dto)

            # Return in legacy format
            return self.get_location_legacy_format(name)

        except EntityAlreadyExistsError:
            logger.warning(f"Location already exists: {name}")
            return None
        except Exception as e:
            logger.error(f"Error creating location via bridge: {e}")
            return None

    def update_location_from_legacy_data(
        self,
        name: str,
        protocol: Optional[str] = None,
        kinds: Optional[List[str]] = None,
        config: Optional[Dict[str, Any]] = None,
        path_prefix: Optional[str] = None,
        optional: Optional[bool] = None,
        description: Optional[str] = None,
    ) -> bool:
        """Update location using new service from legacy-style parameters."""
        try:
            from ..application.dtos import UpdateLocationDto
            from ..domain.entities.location import LocationKind as DomainLocationKind

            # Convert string kinds to domain kinds if provided
            domain_kinds = None
            if kinds is not None:
                domain_kinds = []
                for kind_str in kinds:
                    try:
                        domain_kinds.append(DomainLocationKind[kind_str.upper()])
                    except KeyError:
                        logger.warning(f"Unknown location kind: {kind_str}")
                        continue
                domain_kinds = [kind.name for kind in domain_kinds]

            update_dto = UpdateLocationDto(
                kinds=domain_kinds,
                protocol=protocol,
                path=path_prefix,
                storage_options=config.get("storage_options", {}) if config else None,
                optional=optional,
                additional_config=config,
            )

            logger.debug(f"Updating location via bridge: {name}")
            self._location_service.update_location(name, update_dto)
            return True

        except EntityNotFoundError:
            logger.warning(f"Cannot update non-existent location: {name}")
            return False
        except Exception as e:
            logger.error(f"Error updating location via bridge: {e}")
            return False

    def delete_location(self, name: str) -> bool:
        """Delete location using new service."""
        try:
            self._location_service.delete_location(name)
            logger.debug(f"Deleted location via bridge: {name}")
            return True

        except EntityNotFoundError:
            logger.warning(f"Cannot delete non-existent location: {name}")
            return False
        except Exception as e:
            logger.error(f"Error deleting location via bridge: {e}")
            return False


class ArchiveBridge:
    """Bridge for archive operations between legacy and new architecture."""

    def __init__(self, service_factory: ApplicationServiceFactory):
        self._archive_service = service_factory.archive_service

    def create_archive_from_legacy_data(
        self,
        archive_id: str,
        simulation_id: str,
        archive_path: str,
        location_name: Optional[str] = None,
        name: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Create archive using new service from legacy-style parameters."""
        try:
            create_dto = CreateArchiveDto(
                archive_id=archive_id,
                location_name=location_name or "localhost",
                archive_type="compressed",  # Use enum value, not key
                simulation_id=simulation_id,  # Now properly supported
                description=f"Created via legacy bridge interface: {name or archive_id}",
                tags=set(tags or []),
            )

            logger.debug(f"Creating archive via bridge: {archive_id}")
            archive_dto = self._archive_service.create_archive_metadata(create_dto)

            # Return in legacy format
            return {
                "archive_id": archive_dto.archive_id,
                "location": archive_dto.location,
                "archive_type": archive_dto.archive_type,
                "simulation_id": archive_dto.simulation_id,
                "size": archive_dto.size,
                "created": archive_dto.created_time,
                "checksum": archive_dto.checksum,
                "checksum_algorithm": archive_dto.checksum_algorithm,
                "simulation_date": archive_dto.simulation_date,
                "version": archive_dto.version,
                "tags": list(archive_dto.tags or []),
                "description": archive_dto.description,
                "is_cached": archive_dto.is_cached,
                "cache_path": archive_dto.cache_path,
            }

        except EntityAlreadyExistsError:
            logger.warning(f"Archive already exists: {archive_id}")
            return None
        except Exception as e:
            logger.error(f"Error creating archive via bridge: {e}")
            return None

    def get_archive_legacy_format(self, archive_id: str) -> Optional[Dict[str, Any]]:
        """Get single archive in legacy format."""
        try:
            archive_dto = self._archive_service.get_archive_metadata(archive_id)
            return {
                "archive_id": archive_dto.archive_id,
                "location": archive_dto.location,
                "archive_type": archive_dto.archive_type,
                "simulation_id": archive_dto.simulation_id,
                "size": archive_dto.size,
                "created": archive_dto.created_time,
                "checksum": archive_dto.checksum,
                "checksum_algorithm": archive_dto.checksum_algorithm,
                "simulation_date": archive_dto.simulation_date,
                "version": archive_dto.version,
                "tags": list(archive_dto.tags or []),
                "description": archive_dto.description,
                "is_cached": archive_dto.is_cached,
                "cache_path": archive_dto.cache_path,
            }
        except EntityNotFoundError:
            return None

    def list_archives_for_simulation_legacy_format(
        self, simulation_id: str, cached_only: bool = False
    ) -> List[Dict[str, Any]]:
        """List archives for a simulation in legacy format."""
        try:
            from ..application.dtos import FilterOptions

            filters = FilterOptions()
            # Add simulation ID filtering if needed - this may require service modification
            archive_list = self._archive_service.list_archives(filters=filters)

            legacy_archives = []
            for archive in archive_list.archives:
                # Filter by simulation if specified
                if simulation_id and archive.simulation_id != simulation_id:
                    continue

                # Filter by cache status if specified
                if cached_only and not archive.is_cached:
                    continue

                legacy_archive = {
                    "archive_id": archive.archive_id,
                    "location": archive.location,
                    "archive_type": archive.archive_type,
                    "simulation_id": archive.simulation_id,
                    "size": archive.size,
                    "created": archive.created_time,
                    "checksum": archive.checksum,
                    "checksum_algorithm": archive.checksum_algorithm,
                    "simulation_date": archive.simulation_date,
                    "version": archive.version,
                    "tags": list(archive.tags or []),
                    "description": archive.description,
                    "is_cached": archive.is_cached,
                    "cache_path": archive.cache_path,
                }
                legacy_archives.append(legacy_archive)

            return legacy_archives

        except Exception as e:
            logger.error(f"Error listing archives via bridge: {e}")
            return []

    def delete_archive(self, archive_id: str) -> bool:
        """Delete archive using new service."""
        try:
            # Archive service may not have delete method - this might need implementation
            # For now, return False to indicate deletion not supported
            logger.warning(
                f"Archive deletion not yet implemented in new service: {archive_id}"
            )
            return False

        except EntityNotFoundError:
            logger.warning(f"Cannot delete non-existent archive: {archive_id}")
            return False
        except Exception as e:
            logger.error(f"Error deleting archive via bridge: {e}")
            return False

    def extract_archive(
        self,
        archive_id: str,
        target_path: str,
        file_path: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        """Extract archive using new service."""
        try:
            operation_dto = ArchiveOperationDto(
                archive_id=archive_id,
                operation="extract",
                destination_path=target_path,
                include_patterns=[file_path] if file_path else [],
            )

            result_path = self._archive_service.extract_archive(operation_dto)
            logger.debug(f"Extracted archive via bridge: {archive_id} to {result_path}")
            return True

        except EntityNotFoundError:
            logger.warning(f"Archive not found for extraction: {archive_id}")
            return False
        except Exception as e:
            logger.error(f"Error extracting archive via bridge: {e}")
            return False

    def list_archive_files(
        self, 
        archive_id: str, 
        content_type: Optional[str] = None,
        pattern: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """List files in an archive using the new service."""
        try:
            file_list_dto = self._archive_service.list_archive_files(
                archive_id, content_type, pattern, limit
            )
            
            # Convert to legacy format
            legacy_files = []
            for file_dto in file_list_dto.files:
                legacy_file = {
                    "relative_path": file_dto.relative_path,
                    "size": file_dto.size,
                    "checksum": file_dto.checksum,
                    "content_type": file_dto.content_type,
                    "importance": file_dto.importance,
                    "file_role": file_dto.file_role,
                    "simulation_date": file_dto.simulation_date,
                    "created_time": file_dto.created_time,
                    "modified_time": file_dto.modified_time,
                    "source_archive": file_dto.source_archive,
                    "extraction_time": file_dto.extraction_time,
                    "tags": list(file_dto.tags),
                    "attributes": dict(file_dto.attributes)
                }
                legacy_files.append(legacy_file)
                
            return legacy_files
            
        except EntityNotFoundError:
            logger.warning(f"Archive not found for file listing: {archive_id}")
            return []
        except Exception as e:
            logger.error(f"Error listing archive files via bridge: {e}")
            return []

    def associate_files_with_simulation(
        self,
        archive_id: str,
        simulation_id: str,
        files_to_associate: Optional[List[str]] = None,
        content_type_filter: Optional[str] = None,
        pattern_filter: Optional[str] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Associate archive files with a simulation using the new service."""
        try:
            from ..application.dtos import FileAssociationDto
            
            association_dto = FileAssociationDto(
                archive_id=archive_id,
                simulation_id=simulation_id,
                files_to_associate=files_to_associate or [],
                content_type_filter=content_type_filter,
                pattern_filter=pattern_filter,
                dry_run=dry_run
            )
            
            result_dto = self._archive_service.associate_files_with_simulation(association_dto)
            
            # Convert to legacy format
            return {
                "archive_id": result_dto.archive_id,
                "simulation_id": result_dto.simulation_id,
                "files_associated": result_dto.files_associated,
                "files_skipped": result_dto.files_skipped,
                "success": result_dto.success,
                "error_message": result_dto.error_message
            }
            
        except Exception as e:
            logger.error(f"Error associating files via bridge: {e}")
            return {
                "archive_id": archive_id,
                "simulation_id": simulation_id,
                "files_associated": [],
                "files_skipped": files_to_associate or [],
                "success": False,
                "error_message": str(e)
            }

    def copy_archive(
        self,
        archive_id: str,
        source_location: str,
        destination_location: str,
        simulation_id: Optional[str] = None,
        verify_integrity: bool = True,
        overwrite_existing: bool = False
    ) -> Dict[str, Any]:
        """Copy archive to different location using the new service."""
        try:
            from ..application.dtos import ArchiveCopyOperationDto
            
            copy_dto = ArchiveCopyOperationDto(
                archive_id=archive_id,
                source_location=source_location,
                destination_location=destination_location,
                simulation_id=simulation_id,
                verify_integrity=verify_integrity,
                overwrite_existing=overwrite_existing
            )
            
            result_dto = self._archive_service.copy_archive(copy_dto)
            
            # Convert to legacy format
            return {
                "operation_id": result_dto.operation_id,
                "operation_type": result_dto.operation_type,
                "archive_id": result_dto.archive_id,
                "success": result_dto.success,
                "destination_path": result_dto.destination_path,
                "bytes_processed": result_dto.bytes_processed,
                "files_processed": result_dto.files_processed,
                "duration_seconds": result_dto.duration_seconds,
                "checksum_verified": result_dto.checksum_verification,
                "error_message": result_dto.error_message,
                "warnings": result_dto.warnings
            }
            
        except Exception as e:
            logger.error(f"Error copying archive via bridge: {e}")
            return {
                "operation_id": f"copy_{archive_id}_{int(time.time())}",
                "operation_type": "copy",
                "archive_id": archive_id,
                "success": False,
                "error_message": str(e),
                "warnings": []
            }

    def move_archive(
        self,
        archive_id: str,
        source_location: str,
        destination_location: str,
        simulation_id: Optional[str] = None,
        cleanup_source: bool = True,
        verify_integrity: bool = True
    ) -> Dict[str, Any]:
        """Move archive to different location using the new service."""
        try:
            from ..application.dtos import ArchiveMoveOperationDto
            
            move_dto = ArchiveMoveOperationDto(
                archive_id=archive_id,
                source_location=source_location,
                destination_location=destination_location,
                simulation_id=simulation_id,
                cleanup_source=cleanup_source,
                verify_integrity=verify_integrity
            )
            
            result_dto = self._archive_service.move_archive(move_dto)
            
            # Convert to legacy format
            return {
                "operation_id": result_dto.operation_id,
                "operation_type": result_dto.operation_type,
                "archive_id": result_dto.archive_id,
                "success": result_dto.success,
                "destination_path": result_dto.destination_path,
                "bytes_processed": result_dto.bytes_processed,
                "files_processed": result_dto.files_processed,
                "duration_seconds": result_dto.duration_seconds,
                "checksum_verified": result_dto.checksum_verification,
                "error_message": result_dto.error_message,
                "warnings": result_dto.warnings
            }
            
        except Exception as e:
            logger.error(f"Error moving archive via bridge: {e}")
            return {
                "operation_id": f"move_{archive_id}_{int(time.time())}",
                "operation_type": "move", 
                "archive_id": archive_id,
                "success": False,
                "error_message": str(e),
                "warnings": []
            }

    def extract_archive_to_location(
        self,
        archive_id: str,
        destination_location: str,
        simulation_id: Optional[str] = None,
        file_filters: Optional[List[str]] = None,
        content_type_filter: Optional[str] = None,
        pattern_filter: Optional[str] = None,
        preserve_directory_structure: bool = True,
        overwrite_existing: bool = False,
        create_manifest: bool = True
    ) -> Dict[str, Any]:
        """Extract archive to location using the new service."""
        try:
            from ..application.dtos import ArchiveExtractionDto
            
            extract_dto = ArchiveExtractionDto(
                archive_id=archive_id,
                destination_location=destination_location,
                simulation_id=simulation_id,
                file_filters=file_filters,
                content_type_filter=content_type_filter,
                pattern_filter=pattern_filter,
                preserve_directory_structure=preserve_directory_structure,
                overwrite_existing=overwrite_existing,
                create_manifest=create_manifest
            )
            
            result_dto = self._archive_service.extract_archive_to_location(extract_dto)
            
            # Convert to legacy format
            return {
                "operation_id": result_dto.operation_id,
                "operation_type": result_dto.operation_type,
                "archive_id": result_dto.archive_id,
                "success": result_dto.success,
                "destination_path": result_dto.destination_path,
                "bytes_processed": result_dto.bytes_processed,
                "files_processed": result_dto.files_processed,
                "duration_seconds": result_dto.duration_seconds,
                "manifest_created": result_dto.manifest_created,
                "error_message": result_dto.error_message,
                "warnings": result_dto.warnings
            }
            
        except Exception as e:
            logger.error(f"Error extracting archive via bridge: {e}")
            return {
                "operation_id": f"extract_{archive_id}_{int(time.time())}",
                "operation_type": "extract",
                "archive_id": archive_id,
                "success": False,
                "error_message": str(e),
                "warnings": []
            }

    def resolve_location_path(
        self,
        location_name: str,
        simulation_id: str,
        path_template: Optional[str] = None
    ) -> Dict[str, Any]:
        """Resolve location path template with simulation context."""
        try:
            from ..application.dtos import LocationContextResolutionDto
            
            resolution_dto = LocationContextResolutionDto(
                location_name=location_name,
                simulation_id=simulation_id,
                path_template=path_template or ""
            )
            
            resolved_dto = self._archive_service.resolve_location_context(resolution_dto)
            
            # Convert to legacy format
            return {
                "location_name": resolved_dto.location_name,
                "simulation_id": resolved_dto.simulation_id,
                "path_template": resolved_dto.path_template,
                "resolved_path": resolved_dto.resolved_path,
                "context_variables": resolved_dto.context_variables,
                "resolution_errors": resolved_dto.resolution_errors,
                "success": len(resolved_dto.resolution_errors) == 0
            }
            
        except Exception as e:
            logger.error(f"Error resolving location path via bridge: {e}")
            return {
                "location_name": location_name,
                "simulation_id": simulation_id,
                "path_template": path_template or "",
                "resolved_path": None,
                "context_variables": {},
                "resolution_errors": [str(e)],
                "success": False
            }

    def transfer_archive(
        self,
        archive_id: str,
        source_location: str,
        destination_location: str,
        operation_type: str = "copy",
        simulation_id: Optional[str] = None,
        overwrite: bool = False,
        verify_integrity: bool = True
    ) -> Dict[str, Any]:
        """
        Transfer an archive between locations using the new service.
        
        Args:
            archive_id: Archive to transfer
            source_location: Source location name
            destination_location: Destination location name
            operation_type: "copy" or "move"
            simulation_id: Simulation ID for path template resolution
            overwrite: Whether to overwrite existing files
            verify_integrity: Whether to verify transfer integrity
            
        Returns:
            Transfer operation result in legacy format
        """
        try:
            from ..application.dtos import ArchiveTransferDto
            
            transfer_dto = ArchiveTransferDto(
                archive_id=archive_id,
                source_location_name=source_location,
                destination_location_name=destination_location,
                operation_type=operation_type,
                simulation_id=simulation_id,
                overwrite_existing=overwrite,
                verify_integrity=verify_integrity
            )
            
            operation_id = self._archive_service.transfer_archive(transfer_dto)
            
            return {
                "operation_id": operation_id,
                "archive_id": archive_id,
                "operation_type": operation_type,
                "source_location": source_location,
                "destination_location": destination_location,
                "status": "started",
                "success": True
            }
            
        except Exception as e:
            logger.error(f"Error transferring archive via bridge: {e}")
            return {
                "archive_id": archive_id,
                "operation_type": operation_type,
                "source_location": source_location,
                "destination_location": destination_location,
                "status": "failed",
                "success": False,
                "error_message": str(e)
            }

    def extract_archive_to_location(
        self,
        archive_id: str,
        source_location: str,
        destination_location: str,
        simulation_id: Optional[str] = None,
        extract_all: bool = True,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Extract an archive to a specific location using the new service.
        
        Args:
            archive_id: Archive to extract
            source_location: Source location name
            destination_location: Destination location name
            simulation_id: Simulation ID for path template resolution
            extract_all: Whether to extract all files
            include_patterns: File patterns to include
            exclude_patterns: File patterns to exclude
            overwrite: Whether to overwrite existing files
            
        Returns:
            Extraction operation result in legacy format
        """
        try:
            from ..application.dtos import ArchiveExtractionDto
            
            extraction_dto = ArchiveExtractionDto(
                archive_id=archive_id,
                destination_location=destination_location,
                simulation_id=simulation_id,
                file_filters=include_patterns,
                content_type_filter=None,  # Not passed from this interface
                pattern_filter=None,  # Could use exclude_patterns, but keeping simple for now
                preserve_directory_structure=True,
                overwrite_existing=overwrite,
                create_manifest=True
            )
            
            operation_id = self._archive_service.extract_archive_to_location(extraction_dto)
            
            return {
                "operation_id": operation_id,
                "archive_id": archive_id,
                "source_location": source_location,
                "destination_location": destination_location,
                "status": "started",
                "success": True,
                "extract_all": extract_all
            }
            
        except Exception as e:
            logger.error(f"Error extracting archive via bridge: {e}")
            return {
                "archive_id": archive_id,
                "source_location": source_location,
                "destination_location": destination_location,
                "status": "failed",
                "success": False,
                "error_message": str(e)
            }

    def get_operation_progress(self, operation_id: str) -> Dict[str, Any]:
        """
        Get operation progress using the new service.
        
        Args:
            operation_id: Operation ID to check
            
        Returns:
            Progress information in legacy format
        """
        try:
            # Try to get detailed progress first
            try:
                progress_dto = self._archive_service.get_archive_operation_progress(operation_id)
                
                return {
                    "operation_id": operation_id,
                    "archive_id": progress_dto.archive_id,
                    "operation_type": progress_dto.operation_type,
                    "status": progress_dto.status,
                    "progress_percentage": progress_dto.progress_percentage,
                    "current_step": progress_dto.current_step,
                    "total_steps": progress_dto.total_steps,
                    "completed_steps": progress_dto.completed_steps,
                    "start_time": progress_dto.start_time,
                    "last_update": progress_dto.last_update_time,
                    "current_file": progress_dto.current_file,
                    "files_processed": progress_dto.files_processed,
                    "bytes_processed": progress_dto.bytes_processed,
                    "processing_rate_mbps": progress_dto.processing_rate_mbps,
                    "estimated_completion": progress_dto.estimated_completion_time,
                    "errors_encountered": progress_dto.errors_encountered,
                    "last_error": progress_dto.last_error,
                    "success": True
                }
                
            except EntityNotFoundError:
                # Fall back to basic workflow status
                workflow_dto = self._archive_service.get_operation_status(operation_id)
                
                return {
                    "operation_id": operation_id,
                    "status": workflow_dto.status,
                    "progress_percentage": workflow_dto.progress * 100.0,
                    "current_step": workflow_dto.current_step,
                    "total_steps": workflow_dto.total_steps,
                    "completed_steps": workflow_dto.completed_steps,
                    "error_message": workflow_dto.error_message,
                    "success": True
                }
                
        except EntityNotFoundError:
            return {
                "operation_id": operation_id,
                "status": "not_found",
                "success": False,
                "error_message": f"Operation {operation_id} not found"
            }
        except Exception as e:
            logger.error(f"Error getting operation progress via bridge: {e}")
            return {
                "operation_id": operation_id,
                "status": "error",
                "success": False,
                "error_message": str(e)
            }

    def resolve_location_path(
        self,
        location_name: str,
        simulation_id: str,
        path_template: Optional[str] = None,
        context_overrides: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Resolve location path template using simulation context.
        
        Args:
            location_name: Location name
            simulation_id: Simulation ID for context
            path_template: Optional path template override
            context_overrides: Additional context variables
            
        Returns:
            Resolved path information in legacy format
        """
        try:
            from ..application.dtos import LocationContextResolutionDto
            
            resolution_dto = LocationContextResolutionDto(
                location_name=location_name,
                simulation_id=simulation_id,
                path_template=path_template,
                context_variables=context_overrides or {}
            )
            
            resolved_dto = self._archive_service.resolve_location_context(resolution_dto)
            
            return {
                "location_name": resolved_dto.location_name,
                "simulation_id": resolved_dto.simulation_id,
                "path_template": resolved_dto.path_template,
                "resolved_path": resolved_dto.resolved_path,
                "context_variables": resolved_dto.context_variables,
                "resolution_errors": resolved_dto.resolution_errors,
                "success": len(resolved_dto.resolution_errors) == 0
            }
            
        except Exception as e:
            logger.error(f"Error resolving location path via bridge: {e}")
            return {
                "location_name": location_name,
                "simulation_id": simulation_id,
                "success": False,
                "error_message": str(e)
            }

    def start_bulk_operation(
        self,
        operation_type: str,
        archive_ids: List[str],
        destination_location: str,
        source_location: Optional[str] = None,
        simulation_context: Optional[str] = None,
        parallel_operations: int = 3,
        continue_on_error: bool = True
    ) -> Dict[str, Any]:
        """
        Start a bulk operation on multiple archives.
        
        Args:
            operation_type: Type of operation ("bulk_copy", "bulk_move", "bulk_extract")
            archive_ids: List of archive IDs to process
            destination_location: Destination location name
            source_location: Source location name (for transfers)
            simulation_context: Simulation ID for path resolution
            parallel_operations: Number of parallel operations
            continue_on_error: Whether to continue on individual failures
            
        Returns:
            Bulk operation result in legacy format
        """
        try:
            from ..application.dtos import BulkArchiveOperationDto
            
            bulk_dto = BulkArchiveOperationDto(
                operation_id=f"bulk_{operation_type}_{int(time.time())}",
                operation_type=operation_type,
                archive_ids=archive_ids,
                source_location_name=source_location,
                destination_location_name=destination_location,
                simulation_context=simulation_context,
                parallel_operations=parallel_operations,
                continue_on_error=continue_on_error
            )
            
            operation_id = self._archive_service.start_bulk_archive_operation(bulk_dto)
            
            return {
                "operation_id": operation_id,
                "operation_type": operation_type,
                "archive_count": len(archive_ids),
                "destination_location": destination_location,
                "parallel_operations": parallel_operations,
                "status": "started",
                "success": True
            }
            
        except Exception as e:
            logger.error(f"Error starting bulk operation via bridge: {e}")
            return {
                "operation_type": operation_type,
                "archive_count": len(archive_ids),
                "destination_location": destination_location,
                "status": "failed",
                "success": False,
                "error_message": str(e)
            }
