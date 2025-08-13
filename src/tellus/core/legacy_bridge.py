"""
Bridge adapters between legacy classes and new application services.

This module provides compatibility layers that maintain backward compatibility
while delegating operations to the new clean architecture.
"""

from typing import Dict, List, Optional, Any
import logging
from dataclasses import asdict

from ..application.service_factory import ApplicationServiceFactory
from ..application.dtos import (
    CreateSimulationDto, SimulationDto, UpdateSimulationDto,
    SimulationListDto, CreateLocationDto, LocationDto,
    CreateArchiveDto, ArchiveDto, ArchiveListDto, ArchiveOperationDto
)
from ..application.exceptions import EntityNotFoundError, EntityAlreadyExistsError

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
        attributes: Optional[Dict[str, Any]] = None
    ) -> SimulationDto:
        """Create simulation using new service from legacy-style parameters."""
        create_dto = CreateSimulationDto(
            simulation_id=simulation_id,
            model_id=model_id,
            experiment_id=experiment_id,
            base_path=base_path,
            attributes=attributes or {},
            description=f"Created via legacy bridge interface"
        )
        
        logger.debug(f"Creating simulation via bridge: {simulation_id}")
        return self._simulation_service.create_simulation(create_dto)
    
    def get_simulation_legacy_format(
        self, 
        simulation_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get simulation data in legacy format compatible with old CLI."""
        try:
            sim_dto = self._simulation_service.get_simulation(simulation_id)
            
            # Convert to legacy format that old CLI expects
            legacy_format = {
                'simulation_id': sim_dto.simulation_id,
                'model_id': sim_dto.model_id,
                'experiment_id': sim_dto.experiment_id,
                'path': sim_dto.base_path,
                'attrs': sim_dto.attributes or {},
                'locations': self._convert_locations_to_legacy_format(
                    sim_dto.associated_locations or []
                ),
                # Additional legacy fields
                'created_at': sim_dto.created_at.isoformat() if sim_dto.created_at else None,
                'updated_at': sim_dto.updated_at.isoformat() if sim_dto.updated_at else None,
                'description': sim_dto.description
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
        self, 
        simulation_id: str, 
        attributes: Dict[str, Any]
    ) -> bool:
        """Update simulation attributes using new service."""
        try:
            update_dto = UpdateSimulationDto(
                simulation_id=simulation_id,
                attributes=attributes
            )
            
            updated_sim = self._simulation_service.update_simulation(update_dto)
            logger.debug(f"Updated simulation attributes via bridge: {simulation_id}")
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
        self, 
        location_names: List[str]
    ) -> Dict[str, Any]:
        """Convert location associations to legacy format."""
        locations = {}
        
        for name in location_names:
            try:
                loc_dto = self._location_service.get_location(name)
                locations[name] = {
                    'location': {
                        'name': loc_dto.name,
                        'kinds': [kind.name for kind in loc_dto.kinds],
                        'protocol': loc_dto.protocol,
                        'config': loc_dto.configuration,
                        'optional': loc_dto.optional
                    },
                    'context': {
                        'path_prefix': loc_dto.path_prefix or '',
                        'overrides': {},
                        'metadata': {}
                    }
                }
            except EntityNotFoundError:
                logger.warning(f"Location {name} not found during legacy conversion")
                continue
        
        return locations


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
                    'name': loc.name,
                    'kinds': loc.kinds,  # Already strings in DTO
                    'protocol': loc.protocol,
                    'config': loc.storage_options,  # Use storage_options from DTO
                    'path_prefix': loc.path,
                    'optional': loc.optional,
                    'description': getattr(loc, 'description', '')  # May not exist in DTO
                }
            
            return legacy_format
            
        except Exception as e:
            logger.error(f"Error listing locations via bridge: {e}")
            return {}
    
    def get_location_legacy_format(self, location_name: str) -> Optional[Dict[str, Any]]:
        """Get single location in legacy format."""
        try:
            loc_dto = self._location_service.get_location(location_name)
            return {
                'name': loc_dto.name,
                'kinds': loc_dto.kinds,  # Already strings in DTO
                'protocol': loc_dto.protocol,
                'config': loc_dto.storage_options,  # Use storage_options from DTO
                'path_prefix': loc_dto.path,
                'optional': loc_dto.optional,
                'description': getattr(loc_dto, 'description', '')  # May not exist in DTO
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
        description: Optional[str] = None
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
                storage_options=config.get('storage_options', {}),
                optional=optional,
                additional_config=config
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
        description: Optional[str] = None
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
                storage_options=config.get('storage_options', {}) if config else None,
                optional=optional,
                additional_config=config
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
        tags: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """Create archive using new service from legacy-style parameters."""
        try:
            create_dto = CreateArchiveDto(
                archive_id=archive_id,
                location_name=location_name or "localhost",
                archive_type="COMPRESSED",  # Default archive type
                description=f"Created via legacy bridge interface: {name or archive_id}",
                tags=set(tags or [])
            )
            
            logger.debug(f"Creating archive via bridge: {archive_id}")
            archive_dto = self._archive_service.create_archive_metadata(create_dto)
            
            # Return in legacy format
            return {
                'archive_id': archive_dto.archive_id,
                'location': archive_dto.location,
                'archive_type': archive_dto.archive_type,
                'size': archive_dto.size,
                'created': archive_dto.created_time,
                'tags': list(archive_dto.tags or []),
                'description': archive_dto.description,
                'is_cached': archive_dto.is_cached,
                'cache_path': archive_dto.cache_path
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
                'archive_id': archive_dto.archive_id,
                'location': archive_dto.location,
                'archive_type': archive_dto.archive_type,
                'size': archive_dto.size,
                'created': archive_dto.created_time,
                'tags': list(archive_dto.tags or []),
                'description': archive_dto.description,
                'is_cached': archive_dto.is_cached,
                'cache_path': archive_dto.cache_path
            }
        except EntityNotFoundError:
            return None
    
    def list_archives_for_simulation_legacy_format(
        self, 
        simulation_id: str,
        cached_only: bool = False
    ) -> List[Dict[str, Any]]:
        """List archives for a simulation in legacy format."""
        try:
            from ..application.dtos import FilterOptions
            
            filters = FilterOptions()
            # Add simulation ID filtering if needed - this may require service modification
            archive_list = self._archive_service.list_archives(filters=filters)
            
            legacy_archives = []
            for archive in archive_list.archives:
                # For now, include all archives - simulation filtering may need service enhancement
                if cached_only and not archive.is_cached:
                    continue
                    
                legacy_archive = {
                    'archive_id': archive.archive_id,
                    'location': archive.location,
                    'archive_type': archive.archive_type,
                    'size': archive.size,
                    'created': archive.created_time,
                    'tags': list(archive.tags or []),
                    'description': archive.description,
                    'is_cached': archive.is_cached,
                    'cache_path': archive.cache_path
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
            logger.warning(f"Archive deletion not yet implemented in new service: {archive_id}")
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
        tags: Optional[List[str]] = None
    ) -> bool:
        """Extract archive using new service."""
        try:
            operation_dto = ArchiveOperationDto(
                archive_id=archive_id,
                operation="extract",
                destination_path=target_path,
                include_patterns=[file_path] if file_path else []
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