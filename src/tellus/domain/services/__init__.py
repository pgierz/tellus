"""Domain services - Business logic that doesn't belong to a specific entity."""

from .file_classifier import FileClassifier
from .file_scanner import FileScanner, FileScanResult
from .sidecar_metadata import SidecarMetadata
from .archive_creation import (
    ArchiveCreationService,
    ArchiveCreationResult,
    ArchiveCreationFilter,
    ArchiveCreationConfig,
    CompressionLevel
)
from .archive_extraction import (
    ArchiveExtractionService,
    ArchiveExtractionFilter,
    ArchiveExtractionConfig,
    ExtractionResult,
    ConflictResolution,
    ExtractionMode,
    DateRange
)
from .fragment_assembly import (
    FragmentAssemblyService,
    FragmentConflictStrategy,
    AssemblyMode,
    AssemblyComplexity,
    AssemblyPlan,
    AssemblyResult,
    FragmentOverlap,
    ConflictResolutionCallback
)

__all__ = [
    'FileClassifier',
    'FileScanner',
    'FileScanResult',
    'SidecarMetadata',
    'ArchiveCreationService',
    'ArchiveCreationResult',
    'ArchiveCreationFilter',
    'ArchiveCreationConfig',
    'CompressionLevel',
    'ArchiveExtractionService',
    'ArchiveExtractionFilter',
    'ArchiveExtractionConfig',
    'ExtractionResult',
    'ConflictResolution',
    'ExtractionMode',
    'DateRange',
    'FragmentAssemblyService',
    'FragmentConflictStrategy',
    'AssemblyMode',
    'AssemblyComplexity',
    'AssemblyPlan',
    'AssemblyResult',
    'FragmentOverlap',
    'ConflictResolutionCallback'
]