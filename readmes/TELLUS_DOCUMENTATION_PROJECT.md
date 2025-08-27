# Tellus Documentation Project Plan

This document outlines a comprehensive documentation project for the Tellus climate science data management system. It serves as a rehydratable AI project specification that can be used to guide implementation.

## Immediate issues
These issues are currently immediately relevant and should be shown as next steps when the user asks to continue work if these things have not been solved. This list may be edited manually by the user or by an AI, with checkmark style-markdown:
- [x] **Location.fs Path Resolution Bug** (CORE BUG): ✅ **FIXED** - `location.fs` now operates correctly within the location's configured path via PathSandboxedFileSystem wrapper. Local locations are properly sandboxed and secure against path traversal attacks. **Implemented with comprehensive security and performance testing.**
- [x] Download progress bar in the quickstart that is run when simulating creating a netcdf file stops at 96%? ✅ **FIXED** - Progress loop now completes to 100% with proper range and completion logic.
- [x] At this point I get a h5netcdf error, whichprobablymeans I need to install xarray and netcdf4 correctly. ✅ **FIXED** - Added netcdf4, h5netcdf, and nc-time-axis dependencies to test environment.
- [x] There is a mistake in the introduction notebook quickstart for "Perform abasic analysis". In our case, it gets "seek on closed file" ✅ **FIXED** - Simplified data loading to use file path directly instead of file handle, which is the normal pattern for climate scientists anyway.
- [x] Deleting a simulation has no effect in the CLI, it is still listed again. ✅ **FIXED** - The delete_simulation method now calls save_simulations() to persist the deletion to disk.

## Project Overview

**Objective**: Create comprehensive documentation for Tellus, a distributed data management system for Earth System Model simulations, targeting climate scientists, research software engineers, and system administrators.

**Current Status**: Strong architecture documentation exists, but gaps remain in API reference, practical examples, and user onboarding materials.

**Target Deliverables**: Complete documentation suite including user handbook, developer documentation, and maintainer guides with 24 executable examples.

## Architecture Context

Tellus is a sophisticated Python-based system with:
- **Domain-driven architecture** with clean separation of concerns
- **Unified storage interface** across local, SSH, and cloud storage via fsspec
- **Template-based path resolution** using simulation attributes
- **Two-level caching system** (50GB archive-level, 10GB file-level)
- **Rich CLI interface** with interactive wizards using questionary
- **Snakemake integration** for scientific workflows

**Key Components:**
- `SimulationEntity`: Computational experiment management
- `LocationEntity`: Storage backend abstraction
- Context system: Dynamic path templating
- Archive system: Intelligent caching and data lifecycle

## Target Audiences

1. **Climate Scientists**: Need simple, reliable data management for large-scale simulations
2. **Research Software Engineers**: Require technical integration details and extension points  
3. **HPC System Administrators**: Focus on deployment, configuration, and operations
4. **Contributors/Maintainers**: Development setup and contribution workflows

## Documentation Examples to Implement

### 1. User Handbook Examples (Getting Started)

#### Jupyter Notebook Tutorials
- **`01_quickstart_personal_archive.ipynb`**
  - Install Tellus and configure first location
  - Download sample CMIP6 temperature data
  - Basic file operations with progress tracking
  - **Dataset**: 5GB CMIP6 sample data
  - **Duration**: 15 minutes
  - **Learning outcome**: Working personal archive setup

- **`02_cmip6_ensemble_analysis.ipynb`**
  - Multi-model CMIP6 data pipeline configuration
  - Template-based path organization using simulation attributes
  - Xarray integration for ensemble statistics calculation
  - **Dataset**: 500GB CMIP6 temperature projections (tas variable, 5 models)
  - **Focus**: Location-aware processing, template system usage

- **`03_regional_downscaling_workflow.ipynb`**
  - High-resolution WRF simulation data management
  - Hierarchical storage strategy (scratch → disk → tape)
  - Automated data lifecycle policies and retention
  - **Dataset**: 50TB regional climate simulation (3km resolution, Central Europe)
  - **Focus**: HPC integration, storage tiering, job management

### 2. Advanced Workflow Examples

- **`04_paleoclimate_model_data.ipynb`**
  - Multi-archive paleoclimate data management across institutions
  - Proxy data comparison workflows with geological records
  - Tagged file systems for atmosphere/ocean/land model components
  - **Dataset**: PMIP4 Last Glacial Maximum simulations (100GB)
  - **Focus**: Multi-site data federation, scientific validation

- **`05_cloud_zarr_optimization.ipynb`**
  - Cloud-native climate data lake implementation
  - Zarr chunking strategies optimized for different access patterns
  - Intake catalog integration for data discovery
  - **Dataset**: Petabyte-scale cloud-optimized CMIP6 collection
  - **Focus**: Cloud storage, performance optimization, metadata management

- **`06_collaborative_sharing.ipynb`**
  - Multi-institutional data sharing protocols setup
  - CF-compliant metadata workflows for interoperability
  - Access control and security configuration
  - **Use case**: Arctic Climate Futures collaboration project
  - **Focus**: Standards compliance, security, collaborative workflows

### 3. Terminal Usage Documentation

- **`cli_cookbook.md`**
  - Essential CLI commands with realistic climate science scenarios
  - Interactive wizard walkthroughs for complex operations
  - Troubleshooting guide for common configuration issues
  - Progress monitoring and job management patterns
  - **Format**: Step-by-step command sequences with explanations

### 4. Developer Documentation Examples

#### API Reference Code Examples
- **`simulation_api_examples.py`**
  - Complete SimulationEntity usage patterns and lifecycle
  - Context template system with climate-specific variables
  - Archive system integration and cache management
  - Error handling, validation, and recovery patterns

- **`location_api_examples.py`**
  - LocationEntity usage across all storage backends (local, SSH, S3, etc.)
  - fsspec integration patterns and configuration options
  - Custom storage backend development guide
  - Performance optimization techniques for large datasets

- **`workflow_integration_examples.py`**
  - Snakemake workflow integration with Tellus locations
  - Dask-distributed processing configuration
  - Custom workflow template creation and parameterization
  - Monitoring, logging, and error recovery setup

#### Architecture Implementation Examples
- **`storage_backend_plugin.py`**
  - Complete custom storage backend implementation example
  - Plugin registration and configuration system
  - Testing strategies for storage adapters
  - Performance benchmarking and optimization

- **`domain_service_patterns.py`**
  - Clean architecture implementation in Tellus context
  - Domain service composition and dependency management
  - Repository pattern usage for data persistence
  - Dependency injection patterns and testing strategies

### 5. Real-World Case Studies

#### Scientific Workflow Case Studies
- **`case_study_cmip6_pipeline.md`**
  - End-to-end CMIP6 data processing pipeline
  - Multi-location data management strategy
  - Quality control, validation, and metadata compliance
  - Performance metrics, optimization results, and lessons learned

- **`case_study_real_time_monitoring.md`**
  - Operational climate monitoring system architecture
  - Real-time data ingestion from multiple sources
  - Alert system configuration and dashboard integration
  - **Data sources**: DWD OpenData, Copernicus CDS, NASA Earthdata

- **`case_study_hpc_integration.md`**
  - Large-scale HPC workflow management (Levante/Mistral clusters)
  - Slurm integration and automated job scheduling
  - Network-aware data placement strategies
  - Resource usage optimization and cost analysis

#### Integration Pattern Examples
- **`integration_dask_xarray.py`**
  - Scalable climate data analysis with distributed computing
  - Memory management for large ensemble datasets
  - Cloud-optimized processing workflows
  - **Focus**: Performance scaling, resource efficiency

- **`integration_intake_catalogs.py`**
  - Automated metadata cataloging for simulation collections
  - Data discovery and programmatic access patterns
  - Catalog maintenance, versioning, and updates
  - Multi-format dataset support (NetCDF, Zarr, HDF5)

### 6. Maintainer Documentation Examples

#### Deployment Scenarios
- **`deployment_hpc_cluster.md`**
  - Production HPC installation procedures and requirements
  - System configuration, tuning, and security hardening
  - User management, permissions, and resource allocation
  - Monitoring setup and maintenance procedures

- **`deployment_cloud_native.md`**
  - Cloud-based deployment on AWS/GCP/Azure
  - Container orchestration with Kubernetes
  - Auto-scaling configuration and resource management
  - Cost optimization strategies and monitoring

- **`deployment_hybrid_infrastructure.md`**
  - Multi-site deployment patterns and federation
  - Network configuration and security considerations
  - Data synchronization strategies across locations
  - Disaster recovery planning and backup procedures

#### Configuration and Operations
- **`configuration_templates.yaml`**
  - Production-ready configuration examples
  - Security hardening and compliance configurations
  - Performance tuning parameters for different workloads
  - Environment-specific adaptations (development, staging, production)

- **`monitoring_observability.md`**
  - Comprehensive monitoring setup with Prometheus/Grafana
  - Log aggregation and analysis with ELK stack
  - Performance metrics collection and alerting
  - Incident response procedures and escalation

### 7. Testing and Quality Assurance

#### Test Implementation Examples
- **`test_examples_integration.py`**
  - Integration test patterns for multi-location workflows
  - Mock storage backend testing strategies
  - Performance test implementation and benchmarking
  - Property-based testing for data integrity

- **`test_examples_earth_science.py`**
  - Scientific data validation and quality assurance tests
  - CF-compliance testing for metadata standards
  - Workflow correctness verification with sample datasets
  - Regression testing for climate data processing

## Implementation Strategy

### Phase 1: Foundation (Week 1-2)
1. **User Handbook Core** (Examples 1-3)
   - Implement quickstart tutorial with real CMIP6 data
   - Create CMIP6 ensemble analysis workflow
   - Develop regional downscaling example

2. **CLI Documentation** (Example 7)
   - Comprehensive command reference with real scenarios
   - Interactive wizard documentation

### Phase 2: Advanced Workflows (Week 3-4)
1. **Advanced Tutorials** (Examples 4-6)
   - Paleoclimate multi-archive management
   - Cloud-native Zarr optimization
   - Collaborative sharing protocols

2. **Developer Examples** (Examples 8-12)
   - Complete API reference with working code
   - Architecture implementation patterns

### Phase 3: Case Studies and Operations (Week 5-6)
1. **Real-World Case Studies** (Examples 13-17)
   - Production workflow documentation
   - Integration pattern examples

2. **Maintainer Documentation** (Examples 18-22)
   - Deployment scenarios and operations
   - Configuration management

### Phase 4: Testing and Quality (Week 7)
1. **Test Examples** (Examples 23-24)
   - Testing pattern documentation
   - Quality assurance procedures

2. **Documentation Review and Integration**
   - Cross-referencing and navigation
   - Final quality review and testing

## Documentation Standards

### Quality Requirements
- **All code examples tested**: Every example must run against actual or realistic test data
- **Progressive complexity**: Examples build from basic concepts to advanced implementations
- **Cross-referencing**: Related examples link to each other with clear navigation paths
- **Performance documentation**: Include timing, resource usage, and optimization guidance
- **Error handling**: Demonstrate proper exception handling and recovery patterns

### Technical Standards
- **Jupyter notebooks**: Fully executable with clear markdown explanations
- **Python code**: PEP 8 compliant with comprehensive docstrings
- **Documentation format**: Jupyter Book with PyData Sphinx theme
- **Version control**: All examples tested against current Tellus version
- **Dataset specifications**: Clear data requirements and acquisition instructions

### Review Process
1. **Technical accuracy**: Earth science domain expert review
2. **Code quality**: Clean architecture and best practices review
3. **User experience**: Climate scientist perspective review for clarity
4. **API completeness**: Backend architect review for comprehensive coverage

## File Organization

```
examples/
├── getting-started/           # Notebooks 1-3, CLI cookbook
│   ├── 01_quickstart_personal_archive.ipynb
│   ├── 02_cmip6_ensemble_analysis.ipynb
│   ├── 03_regional_downscaling_workflow.ipynb
│   └── cli_cookbook.md
├── advanced-workflows/        # Notebooks 4-6
│   ├── 04_paleoclimate_model_data.ipynb
│   ├── 05_cloud_zarr_optimization.ipynb
│   └── 06_collaborative_sharing.ipynb
├── api-reference/            # Developer code examples 8-12
│   ├── simulation_api_examples.py
│   ├── location_api_examples.py
│   ├── workflow_integration_examples.py
│   ├── storage_backend_plugin.py
│   └── domain_service_patterns.py
├── case-studies/             # Real-world scenarios 13-17
│   ├── case_study_cmip6_pipeline.md
│   ├── case_study_real_time_monitoring.md
│   ├── case_study_hpc_integration.md
│   ├── integration_dask_xarray.py
│   └── integration_intake_catalogs.py
├── deployment/               # Maintainer examples 18-22
│   ├── deployment_hpc_cluster.md
│   ├── deployment_cloud_native.md
│   ├── deployment_hybrid_infrastructure.md
│   ├── configuration_templates.yaml
│   └── monitoring_observability.md
└── testing/                  # Test examples 23-24
    ├── test_examples_integration.py
    └── test_examples_earth_science.py
```

## Success Metrics

1. **Completeness**: All 24 examples implemented and tested
2. **Usability**: New users can complete quickstart in <15 minutes
3. **Coverage**: All major Tellus features demonstrated with real-world examples
4. **Maintainability**: Documentation integrated into CI/CD for automatic testing
5. **Community adoption**: Examples referenced in issues and community discussions

## Agent Coordination Strategy

- **documentation-specialist**: Lead strategy, user experience, and integration
- **earth-science-python-expert**: Scientific accuracy, domain-specific examples, and dataset specifications
- **climate-docs-reviewer**: Review from student/newcomer perspective for clarity
- **backend-api-architect**: API documentation completeness and technical accuracy
- **clean-architecture-engineer**: Code quality, architectural patterns, and best practices

This plan provides a comprehensive, executable roadmap for creating world-class documentation for the Tellus climate science data management system.