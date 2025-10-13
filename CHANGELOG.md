# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-XX

This is the first stable release of Tellus, a domain-driven Python framework for managing climate model simulation data across distributed storage locations. Tellus abstracts climate simulations into semantic, AI-understandable objects and provides comprehensive tools for data discovery, organization, and access.

### Added

#### REST API
- **FastAPI-based REST API** with OpenAPI/Swagger documentation at `/docs` endpoint
- **Dynamic API versioning** system based on git branch detection (currently `/api/prep-release/`)
- **Complete CRUD endpoints** for simulations, locations, and workflows
- **Simulation management endpoints**: create, list, retrieve, update, delete simulations with full metadata
- **Location endpoints**: manage storage locations with multi-protocol support (file, SSH, SFTP, ScoutFS)
- **Simulation-location association endpoints**: link simulations to storage locations with context resolution
- **File management endpoints**: list files, download with streaming support, query by content type
- **Archive management endpoints**: create archives, list contents, extract files, manage compressed data
- **Advanced file operations**: split archive support, batch operations, path template resolution
- **Health check endpoint** (`/api/prep-release/health`) for monitoring and service verification
- **Automatic SQLite fallback** when PostgreSQL is unavailable for flexible deployment

#### Python SDK & Architecture
- **User-friendly convenience aliases**: `Simulation`, `Location`, `SimulationService`, `LocationService` exported at package level
- **Clean architecture implementation** with strict separation of concerns:
  - `domain/` - Pure business logic with entities and repository interfaces
  - `application/` - Use cases and application services with DTOs
  - `infrastructure/` - Implementation details (database, file systems, protocols)
  - `interfaces/` - External interfaces (CLI, REST API)
- **Domain entities**: `SimulationEntity`, `LocationEntity`, `WorkflowEntity` with full type safety
- **Application services**: `SimulationService`, `LocationService` with dependency injection support
- **Repository pattern**: Abstract data access with sync/async wrappers for flexibility
- **DTO layer**: Data transfer objects for clean API contracts and validation

#### Production Deployment
- **Docker Compose production configuration** with multi-container orchestration
- **PostgreSQL database support** with SQLAlchemy 2.0 async support and Alembic migrations
- **Nginx reverse proxy** with SSL termination and security headers
- **Automated daily backups** with configurable retention (7 days, 4 weeks, 6 months)
- **Health check integration** for Docker Compose service monitoring
- **Rate limiting** and security hardening in Nginx configuration
- **Non-root containers** for improved security posture
- **Resource limits** and restart policies for production stability
- **Comprehensive deployment guide** (DEPLOYMENT.md) with step-by-step instructions

#### CLI Interface
- **Rich-based beautiful terminal UI** with tables, panels, and formatted output
- **Multi-command CLI structure** organized by domain (simulation, location, workflow, files, archive)
- **Simulation management commands**:
  - `tellus simulation list` - Display simulations in rich tables
  - `tellus simulation show <id>` - Detailed simulation view with metadata
  - `tellus simulation create` - Interactive creation wizard
  - `tellus simulation update <id>` - Modify simulation attributes
  - `tellus simulation delete <id>` - Remove simulations
  - `tellus simulation location add <sim> <location>` - Associate storage locations
  - `tellus simulation location ls <sim> <location>` - Browse remote files
  - `tellus simulation scan` - Auto-discover simulations from templates
- **Location management commands**:
  - `tellus location list` - View all storage locations
  - `tellus location show <name>` - Detailed location information
  - `tellus location create` - Interactive wizard for location setup
  - `tellus location update <name>` - Modify location configuration
  - `tellus location scan <name>` - Discover simulations on remote storage
- **Workflow management commands**:
  - `tellus workflow list` - Display configured workflows
  - `tellus workflow show <id>` - View workflow details
  - `tellus workflow run <id>` - Execute workflows
- **File operations commands**:
  - `tellus files list <sim> <location>` - Git-style file listing
  - `tellus files download <sim> <location> <path>` - Download with progress tracking
  - `tellus files upload <sim> <location> <path>` - Upload to remote storage
- **Archive management commands**:
  - `tellus archive create` - Create compressed archives
  - `tellus archive list` - Show available archives
  - `tellus archive show <id>` - Archive metadata and contents
  - `tellus archive extract <id>` - Extract archive contents
  - `tellus archive add` - Register existing remote archives
  - `tellus archive delete <id>` - Remove archives
- **Interactive wizards** using questionary for complex operations (simulation creation, location setup)
- **Progress tracking** with visual progress bars for file operations
- **REST API client mode** - CLI can communicate with remote REST API instances
- **Global JSON output flag** (`--json`) for machine-readable output
- **Tab completion** for remote paths and location-aware filesystem navigation

#### Infrastructure & Storage
- **Multi-protocol storage abstraction** with unified fsspec-based interface:
  - `file://` - Local filesystem access
  - `ssh://` - Remote SSH connections
  - `sftp://` - SFTP file transfers
  - `scoutfs://` - ScoutFS/HSM tape storage integration
- **Path template system** with dynamic resolution using simulation attributes:
  - Templates: `{model}/{experiment}/{ensemble}` resolved from simulation metadata
  - Context variable support for location-specific path generation
  - Nested directory structure support for organized data storage
- **Async SQLAlchemy implementation** with PostgreSQL and SQLite support
- **Repository pattern with sync/async wrappers** for flexible usage patterns
- **Path sandboxed filesystem** for secure remote access within configured boundaries
- **Network topology system** for transfer optimization (framework implemented)
- **Simulation template scanning** to auto-discover simulations from directory structures
- **Location-based scanning** for remote filesystem indexing

#### Documentation
- **Comprehensive SDK documentation** with usage examples and API reference
- **REST API documentation** with OpenAPI/Swagger UI at `/docs`
- **Installation guide** with pip and pixi support for HPC environments
- **Production deployment guide** (DEPLOYMENT.md) covering Docker, SSL, backups, monitoring
- **Jupyter notebook tutorials** with executable documentation:
  - `CLI_DEMO.ipynb` - Command-line interface walkthrough
  - `REST_API_DEMO.ipynb` - API integration patterns
- **Architecture documentation** describing domain-driven design patterns
- **Contributing guidelines** and development setup instructions
- **Executable documentation in CI** - notebooks run automatically to verify examples

#### Testing & Quality
- **22 passing tests** covering core functionality
- **Integration tests** for REST API endpoints and CLI commands
- **Executable documentation** as integration tests (notebooks in CI)
- **Test markers** for categorization: unit, integration, performance, earth_science, archive, cache, location, slow, network, large_data, hpc
- **pytest-based test suite** with coverage reporting
- **Factory-based test fixtures** using factory-boy
- **Database testing** with testcontainers for PostgreSQL integration tests
- **CI/CD pipeline** with GitHub Actions for automated testing and deployment
- **Multi-Python version testing** (Python 3.9, 3.10, 3.11, 3.12)
- **Docker image building and testing** in CI

### Changed

- **Migrated from JSON file persistence to PostgreSQL** for production scalability (JSON still supported as fallback)
- **Replaced asyncpg with psycopg** for Python 3.13 compatibility
- **Unified API dependencies** into main `[project]` dependencies for simpler installation
- **Questionary version updated to 2.1.1** to fix VSplit compatibility issues in interactive wizards
- **CLI command structure reorganized** from `tellus.core.cli` to `tellus.cli` for better organization
- **Improved error messages** with context-aware suggestions and troubleshooting guidance
- **Enhanced template resolution** with simulation and location context merging
- **Non-interactive mode detection** with graceful fallback messages for CI/scripts
- **Database connection handling** with retry logic and better error reporting
- **Path resolution** to use location-relative paths instead of absolute paths in UI

### Fixed

- **Template variable resolution** in PathResolutionService for complex path templates
- **Context combination** when merging simulation and location contexts
- **Async/await issues** in network-aware CLI commands with proper event loop handling
- **AsyncIO event loop issues** in PostgreSQL repository wrappers with proper session management
- **Simulation location ls authentication** for remote filesystem access
- **Download progress tracking** callback issues with proper state management
- **Path resolution** with trailing spaces in mget operations
- **Filesystem delegation** in PathSandboxedFileSystem get/put methods
- **Archive extraction** command parameter handling
- **Docker container permissions** for non-root execution
- **API version detection** in Docker builds and CI environments
- **Docker Compose syntax** for PostgreSQL and SQLite configurations
- **CLI version detection** excluding tellus_chat module
- **Database test failures** with improved mocking and session handling
- **Git-style files command** examples and JSON output support
- **Questionary protocol selection** validation errors
- **Tab completion** for clean architecture LocationEntity
- **Archive files command** DTO access and configuration issues
- **Workflow CLI integration** with hybrid data persistence
- **Template expansion** in simulation location ls command
- **ScoutFS connectivity testing** implementation for tape storage verification

### Deprecated

- **JSON file persistence** is now secondary to PostgreSQL (still supported for development and backwards compatibility)
- **Legacy service architecture** replaced by clean architecture with domain/application/infrastructure separation (old code maintained for compatibility)

### Security

- **Rate limiting** in Nginx configuration (100 requests per second per IP)
- **Security headers** in Nginx (X-Frame-Options, X-Content-Type-Options, X-XSS-Protection)
- **Non-root Docker containers** for reduced attack surface
- **SSL/TLS support** with Let's Encrypt integration in deployment guide
- **Path sandboxing** to prevent directory traversal attacks in remote filesystem access
- **Database credential management** through environment variables
- **SSH key mounting** for secure remote storage access
- **Firewall configuration guidance** in deployment documentation

## [0.0.x] - Alpha Releases (2024-2025)

### Pre-release Development Summary

The 0.0.x series (v0.1.0a1 through v0.1.0a7) established the foundational architecture and core features:

- Initial project structure with pixi-based development environment
- Core domain entities: Simulation, Location, File abstractions
- Basic CLI with simulation and location CRUD operations
- fsspec-based multi-protocol storage support
- JSON-based data persistence
- Interactive wizards with questionary
- Archive system with tar-based compression
- Path template resolution system
- ScoutFS/HSM integration for tape storage
- Rich-based terminal UI
- REST API prototype
- Network topology framework
- Workflow management foundation
- Docker containerization
- CI/CD pipeline with GitHub Actions
- Documentation framework with Jupyter notebooks

### Alpha Release History

#### v0.1.0a3 (2025-09-04)
- Added new test suite based on CLI specification
- Clarified THO/SAO variables are MPIOM (not FESOM) ocean variables

#### v0.1.0a2 (2025-09-04)
- Added commitizen and pixi release workflows for conventional commits
- Added automated CI/CD publishing workflows
- Fixed multiple CI/CD configuration issues

#### v0.1.0a1 (2025-09-03)
- Added async filesystem representation generation
- Fixed SSH connection parameter separation in location.fs
- Added ScoutFS protocol support to location creation wizard
- Improved filesystem representation display

[0.1.0]: https://github.com/pgierz/tellus/releases/tag/v0.1.0
