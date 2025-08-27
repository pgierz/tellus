# Tellus Architecture Refactoring - Migration Guide

This document outlines the migration strategy for refactoring the Tellus Earth System Model data management system from the current monolithic architecture to a clean architecture pattern.

## Overview

The refactoring introduces:
- **Domain Layer**: Pure business logic (entities, interfaces, domain services)
- **Application Layer**: Use case orchestration (application services, DTOs)
- **Infrastructure Layer**: External system integrations (repositories, adapters)

## Current State Analysis

### Problems Identified
1. **Simulation class (~2000 lines)** violates Single Responsibility Principle
   - Mixed concerns: metadata, caching, archiving, file operations
   - Global state management through class registries
   - Direct JSON persistence in domain logic

2. **Location class** has coupling issues
   - File operations mixed with metadata management
   - Progress reporting scattered across methods
   - Direct fsspec dependencies in domain logic

## New Architecture

### Directory Structure
```
src/tellus/
├── domain/
│   ├── entities/          # Pure business objects
│   ├── repositories/      # Abstract interfaces
│   └── services/          # Domain services
├── application/
│   ├── services/          # Use case orchestration
│   └── dtos/              # Data transfer objects
└── infrastructure/
    ├── repositories/      # Concrete implementations
    └── adapters/          # External system integrations
```

### Key Components Created

#### Domain Layer
- `SimulationEntity`: Pure simulation data with validation
- `LocationEntity`: Storage location configuration
- `ArchiveId`, `Checksum`, `FileMetadata`: Value objects
- `ISimulationRepository`, `ILocationRepository`: Repository interfaces

#### Application Layer
- `SimulationApplicationService`: Simulation CRUD and business workflows
- `LocationApplicationService`: Location management with protocol validation
- `ArchiveApplicationService`: Archive operations coordination

#### Infrastructure Layer
- `JsonSimulationRepository`: JSON persistence implementation
- `JsonLocationRepository`: JSON persistence for locations
- `FSSpecAdapter`: Unified filesystem operations adapter

## Migration Strategy

### Phase 1: Preparation (✅ COMPLETED)
- [x] Create new directory structure
- [x] Implement domain entities and interfaces
- [x] Create repository implementations
- [x] Build application services
- [x] Implement infrastructure adapters
- [x] Add comprehensive tests

### Phase 2: Gradual Integration (IN PROGRESS)
This phase introduces the new architecture alongside the existing system without breaking changes.

#### Step 2.1: Create Adapter Bridge
```python
# src/tellus/legacy/simulation_bridge.py
class SimulationBridge:
    """Bridge between old Simulation class and new architecture."""
    
    def __init__(self):
        self.simulation_service = SimulationApplicationService(
            simulation_repo=JsonSimulationRepository(Path("simulations.json")),
            location_service=LocationApplicationService(...)
        )
    
    def create_simulation(self, simulation_id: str, **kwargs) -> 'LegacySimulation':
        """Create simulation using new architecture but return legacy interface."""
        # Use new service
        entity = self.simulation_service.create_simulation({
            'simulation_id': simulation_id,
            **kwargs
        })
        
        # Wrap in legacy interface
        return LegacySimulationWrapper(entity, self.simulation_service)
```

#### Step 2.2: Update CLI Layer
```python
# src/tellus/core/cli.py - Example migration
class CLI:
    def __init__(self):
        # Feature flag for new architecture
        self.use_new_architecture = os.getenv('TELLUS_USE_NEW_ARCH', 'false').lower() == 'true'
        
        if self.use_new_architecture:
            self.simulation_service = self._create_simulation_service()
        else:
            # Use existing Simulation class
            pass
    
    def create_simulation_command(self, simulation_id: str):
        if self.use_new_architecture:
            return self.simulation_service.create_simulation({
                'simulation_id': simulation_id
            })
        else:
            return Simulation(simulation_id=simulation_id)
```

#### Step 2.3: Migrate Data Persistence
```python
# Migration script: scripts/migrate_to_new_format.py
def migrate_simulations():
    """Migrate existing simulations.json to new format."""
    legacy_repo = LegacyJsonRepository("simulations.json")
    new_repo = JsonSimulationRepository("simulations_new.json")
    
    for sim_id, sim_data in legacy_repo.load_all():
        try:
            entity = SimulationEntity(
                simulation_id=sim_data['simulation_id'],
                # ... map other fields
            )
            new_repo.save(entity)
            print(f"Migrated: {sim_id}")
        except Exception as e:
            print(f"Failed to migrate {sim_id}: {e}")
```

### Phase 3: Full Migration (PLANNED)

#### Step 3.1: Replace Core Components
- Update all CLI commands to use new architecture
- Replace Simulation class usage throughout codebase
- Remove global state registries

#### Step 3.2: Performance Optimization
- Implement caching strategies
- Add connection pooling for remote locations
- Optimize file operations for large datasets

#### Step 3.3: Cleanup
- Remove legacy classes
- Consolidate duplicate functionality
- Update documentation

## Testing Strategy

### Current Test Coverage
- ✅ Domain entities with comprehensive validation
- ✅ Repository implementations with edge cases
- ✅ Application services with business logic
- ✅ Infrastructure adapters with error handling

### Migration Testing
```python
# tests/migration/test_legacy_compatibility.py
class TestLegacyCompatibility:
    """Ensure new architecture maintains compatibility."""
    
    def test_simulation_creation_compatibility(self):
        """Test that new architecture produces same results as legacy."""
        # Create with legacy method
        legacy_sim = Simulation(simulation_id="test-001", model_id="FESOM2")
        
        # Create with new architecture
        new_sim = simulation_service.create_simulation({
            'simulation_id': "test-001",
            'model_id': "FESOM2"
        })
        
        # Assert equivalent behavior
        assert legacy_sim.simulation_id == new_sim.simulation_id
        assert legacy_sim.model_id == new_sim.model_id
```

## Risk Mitigation

### Backward Compatibility
- Feature flags to enable/disable new architecture
- Legacy wrapper classes maintain existing API
- Gradual rollout with ability to rollback

### Data Safety
- Atomic file operations prevent corruption
- Backup creation before migration
- Validation of migrated data

### Performance Monitoring
- Track operation times during migration
- Monitor memory usage patterns
- Benchmark file operations with large datasets

## Rollout Plan

### Week 1-2: Infrastructure Setup
- Deploy new code with feature flags disabled
- Run parallel testing in development
- Validate repository implementations

### Week 3-4: Selective Migration
- Enable new architecture for specific operations
- Monitor performance and error rates
- Gather user feedback

### Week 5-6: Full Migration
- Enable new architecture by default
- Remove feature flags
- Clean up legacy code

### Week 7-8: Optimization
- Performance tuning based on real usage
- Documentation updates
- Training materials

## Benefits Expected

### Immediate Benefits
- **Improved testability**: Isolated components with clear interfaces
- **Better error handling**: Consistent exception patterns
- **Cleaner CLI code**: Separation of concerns

### Long-term Benefits
- **Easier feature development**: Clear extension points
- **Better maintainability**: Smaller, focused classes
- **Enhanced reliability**: Robust error recovery and validation

## Monitoring and Success Metrics

### Technical Metrics
- Code complexity reduction (cyclomatic complexity)
- Test coverage increase (target: >90%)
- Defect rate reduction

### User Experience Metrics
- CLI command response times
- Error message clarity
- Operation success rates

### Maintenance Metrics
- Time to implement new features
- Code review time
- Bug fix time

## Support and Training

### Developer Training
- Clean architecture principles workshop
- Code review guidelines update
- Migration troubleshooting guide

### User Communication
- Migration timeline communication
- Breaking changes (if any) documentation
- Support channel for migration issues

## Conclusion

This migration strategy provides a safe, incremental path to a cleaner architecture while maintaining system stability and user productivity. The new architecture positions Tellus for future growth and makes it easier to handle the complex requirements of Earth System Model data management.

The approach minimizes risk through feature flags, comprehensive testing, and gradual rollout while delivering immediate benefits in code maintainability and testability.