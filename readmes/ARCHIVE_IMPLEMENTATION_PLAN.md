# Archive System Implementation Plan

## **Domain Model & Requirements**

### **Archive Fundamentals**
- **Archives are tarballs of individual SimulationFiles**. You will need to implement a generic SimulationFile layer.
- **Archives are independent entities** with globally unique IDs
- **One archive = parts of one simulation** (never multiple simulations in one archive)
- **Multiple archives can reconstruct one simulation** (fragment assembly)
- **Archive storage**: Tarball + metadata sidecar file (stored together)
- **Extract operation**: Archive → Target Location (any storage type supported by Location layer)

### **Archive-Simulation Relationship**
- **Many-to-One**: Multiple archives can contain parts of one simulation
- **Fragment Assembly**: System must merge multiple archive extractions
- **Independent Existence**: Archives exist without requiring simulation objects
- **Reconstruction**: Archives can recreate simulation file structure at any Location

### **Storage & Location Integration**
- **Location Abstraction**: All storage operations go through Location layer
- **Storage Agnostic**: Extract to local disk, remote SSH, cloud, etc.
- **No Tape Handling**: fsspec/Location layer handles tape staging transparently
- **File Structure Preservation**: Maintain original directory structure during extraction

### **Metadata & Content**
- **Sidecar Files**: Metadata stored alongside tarballs (not embedded)
- **Content Classification**: Track content types (input, output, logs, etc.)
- **Simulation Association**: Track which simulation parts are contained
- **Temporal Information**: Support date ranges and experiment phases

### **Extraction Granularity**
- **Maximum Flexibility**: File patterns, tags, date ranges, directory selection
- **All-or-Nothing**: Complete extraction when desired
- **Selective Extraction**: Individual files or file groups
- **DateTime Selection**: Use strftime syntax for date-based extraction
- **Progress Tracking**: Long operations show progress to user

## **Implementation Phases**

### **Phase 1: Fix Current Broken Infrastructure**
**Problem**: ArchiveBridge uses wrong DTO structure, service calls fail

**Tasks**:
1. Audit actual DTO fields vs bridge assumptions
2. Fix ArchiveBridge to use correct service API
3. Update CreateArchiveDto, ArchiveDto, ArchiveOperationDto usage
4. Test basic CLI operations (create, list, show work)

### **Phase 2: Enhance Domain Model**
**Problem**: Current domain entities too basic for rich archive functionality

**Tasks**:
1. Extend ArchiveMetadata entity with simulation_id, content_types
2. Add fragment tracking (which simulation, what parts)
3. Add content classification (input/output/logs)
4. Design sidecar metadata format
5. Add datetime pattern matching using strftime syntax

### **Phase 3: Implement Core Archive Operations**
**Problem**: Service only has basic CRUD, missing core functionality

**Tasks**:
1. **Archive Creation**: From simulation directories → tarball + metadata
2. **Extract Operations**: Archive → Target Location with progress
3. **Fragment Assembly**: Merge multiple archives into simulation
4. **Content Discovery**: List files, scan content, classify types
5. **Selective Operations**: Extract subsets based on patterns/tags/dates
6. **DateTime Filtering**: Parse strftime patterns for file selection

### **Phase 4: Complete Archive CLI**
**Problem**: CLI infrastructure exists but functionality broken

**Tasks**:
1. Fix basic commands: create, list, show, delete
2. Add extract command with datetime support
3. Add fragment operations: merge, assemble, reconstruct
4. Add content commands: files, scan, classify
5. Rich displays: Tables, Panels, progress bars

### **Phase 5: Integration & Testing**
**Validation**: End-to-end workflows work correctly

**Tasks**:
1. Test archive creation from various simulation layouts
2. Test extraction to different Location types
3. Test fragment assembly workflows
4. Test selective extraction with various criteria including datetime patterns
5. Validate feature flag switching between old/new systems

## **Key Technical Notes**

### **Archive Storage Format**
```
archive_name.tar.gz          # The actual tarball
archive_name.metadata.json   # Sidecar metadata file
```

### **DateTime Pattern Extraction**
Use strftime syntax for flexible date-based file selection:
```bash
# Extract files from specific date
tellus archive extract sim_2024 --date-pattern "%Y-%m-%d" --date "2024-03-15"

# Extract files from specific month
tellus archive extract sim_2024 --date-pattern "%Y-%m" --date "2024-03"

# Extract files matching pattern in filename
tellus archive extract sim_2024 --file-pattern "*%Y%m%d*.nc" --date-range "2024-01-01:2024-12-31"
```

### **Extraction Workflow**
1. `tellus archive extract ARCHIVE_ID --location TARGET_LOCATION`
2. System reads metadata to understand content structure
3. Applies any filters (patterns, tags, dates using strftime)
4. Extracts matching files to TARGET_LOCATION preserving directory structure
5. User can then create/update simulation objects pointing to TARGET_LOCATION

### **Fragment Assembly Workflow**
1. Multiple archives contain parts of same simulation
2. Extract each archive to same target Location (with optional filtering)
3. Files merge together to recreate complete simulation structure
4. Metadata tracks which archives contributed which parts

### **CLI Examples**
```bash
# Create archive from simulation
tellus archive create my_sim_2024 /path/to/simulation --simulation sim_001

# List all archives
tellus archive list

# Show archive details
tellus archive show my_sim_2024

# Extract complete archive to local disk
tellus archive extract my_sim_2024 --location localhost --path /tmp/restored

# Selective extraction with patterns and dates
tellus archive extract my_sim_2024 --location localhost --pattern "*.nc" --tags output

# DateTime-based extraction using strftime
tellus archive extract my_sim_2024 --location localhost --date-pattern "%Y-%m-%d" --date "2024-03-15"

# Extract to remote location
tellus archive extract my_sim_2024 --location compute_cluster --path /scratch/restored

# Complex selective extraction
tellus archive extract my_sim_2024 --location localhost \
  --pattern "output_*.nc" \
  --date-pattern "%Y%m%d" \
  --date-range "20240101:20240331" \
  --tags "monthly,output"
```

This plan incorporates strftime datetime syntax and provides a complete roadmap for archive system implementation.
