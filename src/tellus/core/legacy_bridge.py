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
    SimulationListDto, CreateLocationDto, LocationDto
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
                    'kinds': [kind.name for kind in loc.kinds],
                    'protocol': loc.protocol,
                    'config': loc.configuration,
                    'path_prefix': loc.path_prefix,
                    'optional': loc.optional,
                    'description': loc.description
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
                'kinds': [kind.name for kind in loc_dto.kinds],
                'protocol': loc_dto.protocol,
                'config': loc_dto.configuration,
                'path_prefix': loc_dto.path_prefix,
                'optional': loc_dto.optional,
                'description': loc_dto.description
            }
        except EntityNotFoundError:
            return None