"""Domain services - Business logic that doesn't belong to a specific entity."""

from .archive_creation import (ArchiveCreationConfig, ArchiveCreationFilter,
                               ArchiveCreationResult, ArchiveCreationService,
                               CompressionLevel)
from .archive_extraction import (ArchiveExtractionConfig,
                                 ArchiveExtractionFilter,
                                 ArchiveExtractionService, ConflictResolution,
                                 DateRange, ExtractionMode, ExtractionResult)
from .file_classifier import FileClassifier
from .file_scanner import FileScanner, FileScanResult
from .fragment_assembly import (AssemblyComplexity, AssemblyMode, AssemblyPlan,
                                AssemblyResult, ConflictResolutionCallback,
                                FragmentAssemblyService,
                                FragmentConflictStrategy, FragmentOverlap)
from .sidecar_metadata import SidecarMetadata

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