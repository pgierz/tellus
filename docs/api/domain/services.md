# Domain Services

Domain services encapsulate business logic that doesn't naturally fit within a single entity and orchestrate complex domain operations.

## Archive Services

### Archive Creation Service

```{eval-rst}
.. currentmodule:: tellus.domain.services.archive_creation

.. autoclass:: ArchiveCreationService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Operations:**
- Create compressed archives from file collections
- Generate archive metadata with file classification
- Validate archive integrity and checksums
- Support multiple compression formats (gzip, bzip2, lz4)

### Archive Extraction Service

```{eval-rst}
.. currentmodule:: tellus.domain.services.archive_extraction

.. autoclass:: ArchiveExtractionService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Operations:**
- Extract files from compressed archives
- Filter extraction by content type or patterns
- Preserve file metadata and permissions
- Handle large archives with streaming extraction

## File Classification Services

### File Classifier

```{eval-rst}
.. currentmodule:: tellus.domain.services.file_classifier

.. autoclass:: FileClassifier
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Classification Features:**
- Pattern-based content type detection
- Metadata extraction from file headers
- Climate data format recognition (NetCDF, GRIB)
- Custom classification rule support

### File Scanner

```{eval-rst}
.. currentmodule:: tellus.domain.services.file_scanner

.. autoclass:: FileScanner
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Scanning Capabilities:**
- Recursive directory scanning
- File metadata collection
- Checksum computation
- Change detection and monitoring

## Metadata Services

### Sidecar Metadata Service

```{eval-rst}
.. currentmodule:: tellus.domain.services.sidecar_metadata

.. autoclass:: SidecarMetadataService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Metadata Operations:**
- Generate sidecar metadata files (.tellus.yml)
- Extract metadata from climate data files
- Maintain metadata consistency across locations
- Support custom metadata schemas

### Fragment Assembly Service

```{eval-rst}
.. currentmodule:: tellus.domain.services.fragment_assembly

.. autoclass:: FragmentAssemblyService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Workflow Template Services

### Workflow Templates

```{eval-rst}
.. currentmodule:: tellus.domain.services.workflow_templates

.. autoclass:: WorkflowTemplateService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: EarthScienceWorkflowTemplates
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Available Templates:**
- CMIP6 data processing workflows
- Climate model output post-processing
- Multi-model ensemble analysis
- Data quality control pipelines
- Archive and distribution workflows

## Usage Examples

### File Classification Workflow

```python
from tellus.domain.services.file_classifier import FileClassifier
from tellus.domain.services.file_scanner import FileScanner
from tellus.domain.entities.file_type_config import FileTypeConfig

# Configure file classification
file_config = FileTypeConfig()
file_config.add_pattern(
    pattern=r".*_Amon_.*\.nc$",
    content_type=FileContentType.MODEL_OUTPUT,
    file_role=FileRole.PRIMARY,
    metadata_template={
        "frequency": "monthly",
        "realm": "atmosphere"
    }
)

# Initialize services
classifier = FileClassifier(file_config)
scanner = FileScanner()

# Scan directory and classify files
files = scanner.scan_directory("/data/cesm2/output", recursive=True)
for file_info in files:
    classification = classifier.classify_file(file_info.path)
    print(f"{file_info.name}: {classification.content_type}")
```

### Archive Creation Workflow

```python
from tellus.domain.services.archive_creation import ArchiveCreationService
from tellus.domain.services.sidecar_metadata import SidecarMetadataService
from tellus.domain.entities.archive import ArchiveMetadata, ArchiveType

# Initialize services
archive_service = ArchiveCreationService()
metadata_service = SidecarMetadataService()

# Create archive metadata
archive_metadata = ArchiveMetadata(
    archive_id="cesm2-historical-analysis-v1",
    location="local-archive",
    archive_type=ArchiveType.COMPRESSED,
    simulation_id="cesm2-historical-r1i1p1f1",
    description="CESM2 historical simulation analysis results"
)

# Generate sidecar metadata for files
file_paths = [
    "/data/analysis/temperature_trends.nc",
    "/data/analysis/precipitation_analysis.nc",
    "/data/analysis/sea_level_pressure.nc"
]

for file_path in file_paths:
    metadata = metadata_service.generate_metadata(file_path)
    metadata_service.write_sidecar(file_path, metadata)

# Create archive with metadata
archive_result = archive_service.create_archive(
    archive_metadata=archive_metadata,
    file_paths=file_paths,
    include_sidecars=True,
    compression="gzip",
    compression_level=6
)

print(f"Archive created: {archive_result.archive_path}")
print(f"Files archived: {len(archive_result.archived_files)}")
```

### Climate Data Processing Template

```python
from tellus.domain.services.workflow_templates import EarthScienceWorkflowTemplates
from tellus.domain.entities.workflow import WorkflowDefinition

# Get workflow templates
templates = EarthScienceWorkflowTemplates()

# Create CMIP6 processing workflow
cmip6_workflow = templates.create_cmip6_processing_workflow(
    model_id="CESM2.1",
    experiment="historical",
    variables=["tas", "pr", "psl"],
    time_period="1850-2014",
    output_frequency="monthly"
)

print(f"Workflow: {cmip6_workflow.name}")
print(f"Steps: {len(cmip6_workflow.steps)}")

for step in cmip6_workflow.steps:
    print(f"  - {step.name}: {step.description}")
```

### Multi-Model Ensemble Processing

```python
from tellus.domain.services.workflow_templates import EarthScienceWorkflowTemplates

# Create ensemble processing workflow
templates = EarthScienceWorkflowTemplates()

ensemble_workflow = templates.create_ensemble_analysis_workflow(
    models=["CESM2.1", "GFDL-CM4", "IPSL-CM6A-LR"],
    experiment="ssp585",
    variables=["tas", "pr"],
    analysis_methods=["ensemble_mean", "ensemble_spread", "agreement"],
    reference_period="1995-2014",
    projection_period="2081-2100"
)

# The workflow includes steps for:
# 1. Data harmonization across models
# 2. Regridding to common grid
# 3. Bias correction (optional)
# 4. Ensemble statistics calculation
# 5. Significance testing
# 6. Output formatting and metadata
```

### Archive Extraction with Filtering

```python
from tellus.domain.services.archive_extraction import ArchiveExtractionService
from tellus.domain.entities.file_type_config import FileContentType

# Initialize extraction service
extractor = ArchiveExtractionService()

# Extract specific content types
extraction_result = extractor.extract_archive(
    archive_path="/archives/cesm2-analysis-v1.tar.gz",
    destination_path="/tmp/extracted",
    content_type_filter=FileContentType.MODEL_OUTPUT,
    file_patterns=["**/tas_*.nc", "**/pr_*.nc"],
    preserve_structure=True,
    verify_checksums=True
)

print(f"Extracted {len(extraction_result.extracted_files)} files")
for file_info in extraction_result.extracted_files:
    print(f"  {file_info.relative_path} ({file_info.size} bytes)")
```

### Custom Workflow Template

```python
from tellus.domain.entities.workflow import (
    WorkflowDefinition, WorkflowStep
)

# Define custom quality control workflow
def create_quality_control_workflow(simulation_id: str) -> WorkflowDefinition:
    """Create quality control workflow for climate model output."""
    
    steps = [
        WorkflowStep(
            name="file_validation",
            description="Validate NetCDF file structure and metadata",
            command="cdo info {input_files}",
            inputs=["raw_output/*.nc"],
            outputs=["validation_report.txt"],
            metadata={"tool": "cdo", "check_type": "structure"}
        ),
        
        WorkflowStep(
            name="data_completeness",
            description="Check for missing values and time gaps",
            command="python check_completeness.py {input_files}",
            inputs=["raw_output/*.nc"],
            outputs=["completeness_report.json"],
            dependencies=["file_validation"]
        ),
        
        WorkflowStep(
            name="physical_plausibility",
            description="Check for physically implausible values",
            command="python check_physics.py {input_files}",
            inputs=["raw_output/*.nc"],
            outputs=["physics_report.json", "flagged_values.nc"],
            dependencies=["data_completeness"]
        ),
        
        WorkflowStep(
            name="generate_summary",
            description="Generate QC summary report",
            command="python generate_qc_summary.py {reports}",
            inputs=["*.json", "*.txt"],
            outputs=["qc_summary.html", "qc_summary.pdf"],
            dependencies=["file_validation", "data_completeness", "physical_plausibility"]
        )
    ]
    
    return WorkflowDefinition(
        name=f"quality_control_{simulation_id}",
        description=f"Quality control workflow for {simulation_id}",
        steps=steps,
        metadata={
            "simulation_id": simulation_id,
            "workflow_type": "quality_control",
            "version": "1.0"
        }
    )

# Use custom workflow
qc_workflow = create_quality_control_workflow("cesm2-historical-r1i1p1f1")
```

## Service Integration Patterns

Domain services work together to provide complex functionality:

```python
from tellus.domain.services.file_scanner import FileScanner
from tellus.domain.services.file_classifier import FileClassifier
from tellus.domain.services.archive_creation import ArchiveCreationService
from tellus.domain.services.sidecar_metadata import SidecarMetadataService

class DataProcessingPipeline:
    """Integrated data processing pipeline using domain services."""
    
    def __init__(self):
        self.scanner = FileScanner()
        self.classifier = FileClassifier()
        self.archiver = ArchiveCreationService()
        self.metadata_service = SidecarMetadataService()
    
    def process_simulation_output(self, output_dir: str, simulation_id: str):
        """Process all output from a simulation."""
        
        # 1. Scan for files
        files = self.scanner.scan_directory(output_dir, recursive=True)
        
        # 2. Classify files
        classified_files = []
        for file_info in files:
            classification = self.classifier.classify_file(file_info.path)
            classified_files.append((file_info, classification))
        
        # 3. Generate metadata
        for file_info, classification in classified_files:
            metadata = self.metadata_service.generate_metadata(
                file_info.path, 
                classification
            )
            self.metadata_service.write_sidecar(file_info.path, metadata)
        
        # 4. Create archives by content type
        archives_created = []
        
        # Group files by content type
        by_content_type = {}
        for file_info, classification in classified_files:
            content_type = classification.content_type
            if content_type not in by_content_type:
                by_content_type[content_type] = []
            by_content_type[content_type].append(file_info)
        
        # Create separate archive for each content type
        for content_type, file_list in by_content_type.items():
            archive_id = f"{simulation_id}_{content_type.value.lower()}"
            
            archive_metadata = ArchiveMetadata(
                archive_id=archive_id,
                simulation_id=simulation_id,
                description=f"{content_type.value} files for {simulation_id}"
            )
            
            file_paths = [f.path for f in file_list]
            result = self.archiver.create_archive(
                archive_metadata, 
                file_paths,
                include_sidecars=True
            )
            
            archives_created.append(result)
        
        return {
            'files_processed': len(files),
            'archives_created': len(archives_created),
            'archive_paths': [r.archive_path for r in archives_created]
        }

# Use integrated pipeline
pipeline = DataProcessingPipeline()
result = pipeline.process_simulation_output(
    "/data/cesm2/output", 
    "cesm2-historical-r1i1p1f1"
)
```

## Error Handling and Validation

Domain services provide comprehensive error handling:

```python
from tellus.domain.services.archive_creation import (
    ArchiveCreationService,
    ArchiveCreationError,
    ValidationError
)

archive_service = ArchiveCreationService()

try:
    # This might fail due to various reasons
    result = archive_service.create_archive(
        archive_metadata=metadata,
        file_paths=file_paths,
        verify_checksums=True
    )
except ValidationError as e:
    print(f"Validation failed: {e}")
except ArchiveCreationError as e:
    print(f"Archive creation failed: {e}")
    # Access detailed error information
    if hasattr(e, 'failed_files'):
        print(f"Failed files: {e.failed_files}")
```

## Performance Considerations

- Services support streaming operations for large files
- Parallel processing where applicable (file scanning, classification)
- Efficient memory usage with generator-based iteration
- Configurable chunk sizes for large operations
- Progress callbacks for long-running operations