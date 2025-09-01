"""
Sidecar metadata service for archive metadata files.

This service manages the creation, reading, and writing of sidecar metadata
files that accompany archive tarballs, providing detailed information about
archive contents and extraction capabilities.
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..entities.archive import (ArchiveId, ArchiveMetadata, ArchiveType,
                                Checksum)
from ..entities.simulation_file import FileInventory, SimulationFile


class SidecarMetadata:
    """
    Domain service for managing archive sidecar metadata files.
    
    Sidecar files store detailed information about archive contents alongside
    the actual tarball files, enabling advanced querying and selective extraction.
    
    Format: archive_name.metadata.json
    """
    
    METADATA_VERSION = "1.0"
    METADATA_EXTENSION = ".metadata.json"
    
    @classmethod
    def create_sidecar_path(cls, archive_path: Path) -> Path:
        """
        Create the sidecar metadata file path for an archive.
        
        Args:
            archive_path: Path to the archive file
            
        Returns:
            Path to the corresponding metadata sidecar file
        """
        # Remove any existing extensions and add metadata extension
        base_name = archive_path.name
        if base_name.endswith('.tar.gz'):
            base_name = base_name[:-7]
        elif base_name.endswith('.tgz'):
            base_name = base_name[:-4]
        elif base_name.endswith('.tar'):
            base_name = base_name[:-4]
        elif '.' in base_name:
            base_name = base_name.rsplit('.', 1)[0]
        
        return archive_path.parent / f"{base_name}{cls.METADATA_EXTENSION}"
    
    @classmethod
    def generate_metadata_dict(cls, archive_metadata: ArchiveMetadata) -> Dict[str, Any]:
        """
        Generate a complete metadata dictionary for sidecar file.
        
        Args:
            archive_metadata: Archive metadata entity
            
        Returns:
            Dictionary ready for JSON serialization
        """
        metadata = {
            # Sidecar file metadata
            "metadata_version": cls.METADATA_VERSION,
            "generated_at": time.time(),
            "generated_by": "tellus-archive-system",
            
            # Archive identification
            "archive": {
                "archive_id": str(archive_metadata.archive_id),
                "location": archive_metadata.location,
                "archive_type": archive_metadata.archive_type.value,
                "created_time": archive_metadata.created_time,
                "version": archive_metadata.version,
                "description": archive_metadata.description
            },
            
            # Simulation association
            "simulation": {
                "simulation_id": archive_metadata.simulation_id,
                "simulation_date": archive_metadata.simulation_date
            },
            
            # Archive properties
            "properties": {
                "checksum": str(archive_metadata.checksum) if archive_metadata.checksum else None,
                "size": archive_metadata.size,
                "tags": list(archive_metadata.tags)
            },
            
            # Fragment information
            "fragment": archive_metadata.fragment_info if archive_metadata.fragment_info else None,
            
            # File inventory details
            "inventory": cls._generate_inventory_metadata(archive_metadata.file_inventory) if archive_metadata.file_inventory else None,
            
            # Extraction capabilities
            "extraction": cls._generate_extraction_metadata(archive_metadata)
        }
        
        return metadata
    
    @classmethod
    def write_sidecar_file(cls, archive_path: Path, archive_metadata: ArchiveMetadata) -> Path:
        """
        Write a sidecar metadata file for an archive.
        
        Args:
            archive_path: Path to the archive file
            archive_metadata: Archive metadata to write
            
        Returns:
            Path to the created sidecar file
            
        Raises:
            IOError: If writing fails
        """
        sidecar_path = cls.create_sidecar_path(archive_path)
        metadata_dict = cls.generate_metadata_dict(archive_metadata)
        
        try:
            # Ensure parent directory exists
            sidecar_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write with atomic operation
            temp_path = sidecar_path.with_suffix(sidecar_path.suffix + '.tmp')
            
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(metadata_dict, f, indent=2, default=str, ensure_ascii=False)
            
            # Atomic replace
            temp_path.replace(sidecar_path)
            
            return sidecar_path
            
        except Exception as e:
            # Cleanup temp file if it exists
            if temp_path.exists():
                temp_path.unlink()
            raise IOError(f"Failed to write sidecar metadata file: {e}")
    
    @classmethod
    def read_sidecar_file(cls, sidecar_path: Path) -> Optional[Dict[str, Any]]:
        """
        Read and parse a sidecar metadata file.
        
        Args:
            sidecar_path: Path to the sidecar file
            
        Returns:
            Parsed metadata dictionary, or None if file doesn't exist
            
        Raises:
            IOError: If reading fails
            ValueError: If JSON is invalid
        """
        if not sidecar_path.exists():
            return None
        
        try:
            with open(sidecar_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # Validate metadata version
            version = metadata.get('metadata_version')
            if version != cls.METADATA_VERSION:
                raise ValueError(f"Unsupported metadata version: {version}")
            
            return metadata
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in sidecar file: {e}")
        except Exception as e:
            raise IOError(f"Failed to read sidecar metadata file: {e}")
    
    @classmethod
    def find_sidecar_for_archive(cls, archive_path: Path) -> Optional[Path]:
        """
        Find the sidecar metadata file for an archive.
        
        Args:
            archive_path: Path to the archive file
            
        Returns:
            Path to sidecar file if it exists, None otherwise
        """
        sidecar_path = cls.create_sidecar_path(archive_path)
        return sidecar_path if sidecar_path.exists() else None
    
    @classmethod
    def reconstruct_archive_metadata(cls, sidecar_data: Dict[str, Any]) -> ArchiveMetadata:
        """
        Reconstruct an ArchiveMetadata entity from sidecar data.
        
        Args:
            sidecar_data: Parsed sidecar metadata dictionary
            
        Returns:
            Reconstructed ArchiveMetadata entity
        """
        archive_info = sidecar_data.get('archive', {})
        simulation_info = sidecar_data.get('simulation', {})
        properties = sidecar_data.get('properties', {})
        fragment_info = sidecar_data.get('fragment')
        inventory_data = sidecar_data.get('inventory')
        
        # Parse checksum
        checksum = None
        if properties.get('checksum'):
            checksum_str = properties['checksum']
            if ':' in checksum_str:
                algorithm, value = checksum_str.split(':', 1)
                checksum = Checksum(value=value, algorithm=algorithm)
            else:
                checksum = Checksum(value=checksum_str, algorithm='md5')
        
        # Reconstruct file inventory if present
        file_inventory = None
        if inventory_data:
            file_inventory = cls._reconstruct_inventory_from_metadata(inventory_data)
        
        return ArchiveMetadata(
            archive_id=ArchiveId(archive_info.get('archive_id', '')),
            location=archive_info.get('location', ''),
            archive_type=ArchiveType(archive_info.get('archive_type', 'compressed')),
            simulation_id=simulation_info.get('simulation_id'),
            checksum=checksum,
            size=properties.get('size'),
            created_time=archive_info.get('created_time', time.time()),
            simulation_date=simulation_info.get('simulation_date'),
            version=archive_info.get('version'),
            description=archive_info.get('description'),
            tags=set(properties.get('tags', [])),
            file_inventory=file_inventory,
            fragment_info=fragment_info
        )
    
    @classmethod
    def get_extraction_patterns(cls, sidecar_data: Dict[str, Any]) -> List[str]:
        """
        Extract file patterns available for selective extraction.
        
        Args:
            sidecar_data: Parsed sidecar metadata
            
        Returns:
            List of file patterns that can be used for extraction
        """
        extraction_info = sidecar_data.get('extraction', {})
        return extraction_info.get('available_patterns', [])
    
    @classmethod
    def get_content_types(cls, sidecar_data: Dict[str, Any]) -> List[str]:
        """
        Get available content types for filtering.
        
        Args:
            sidecar_data: Parsed sidecar metadata
            
        Returns:
            List of content types present in the archive
        """
        inventory_info = sidecar_data.get('inventory', {})
        summary = inventory_info.get('content_summary', {})
        return list(summary.keys())
    
    @classmethod
    def get_date_patterns(cls, sidecar_data: Dict[str, Any]) -> List[str]:
        """
        Get available date patterns for temporal filtering.
        
        Args:
            sidecar_data: Parsed sidecar metadata
            
        Returns:
            List of date patterns found in filenames
        """
        extraction_info = sidecar_data.get('extraction', {})
        return extraction_info.get('date_patterns', [])
    
    @classmethod
    def _generate_inventory_metadata(cls, inventory: FileInventory) -> Dict[str, Any]:
        """Generate inventory section for metadata."""
        files_detail = []
        
        for file in inventory.list_files():
            file_info = {
                "path": file.relative_path,
                "content_type": file.content_type.value,
                "importance": file.importance.value,
                "size": file.size,
                "checksum": str(file.checksum) if file.checksum else None,
                "role": file.file_role,
                "tags": list(file.tags),
                "created_time": file.created_time,
                "modified_time": file.modified_time,
                "simulation_date": file.simulation_date.isoformat() if file.simulation_date else None
            }
            files_detail.append(file_info)
        
        # Generate summary statistics
        content_summary = inventory.get_content_type_summary()
        size_by_content = inventory.get_size_by_content_type()
        
        return {
            "total_files": inventory.file_count,
            "total_size": inventory.total_size,
            "created_time": inventory.created_time,
            "content_summary": content_summary,
            "size_by_content": size_by_content,
            "files": files_detail
        }
    
    @classmethod
    def _generate_extraction_metadata(cls, archive_metadata: ArchiveMetadata) -> Dict[str, Any]:
        """Generate extraction capabilities metadata."""
        extraction_info = {
            "complexity": archive_metadata.estimate_extraction_complexity(),
            "available_patterns": [],
            "content_types": [],
            "date_patterns": [],
            "directories": set(),
            "extensions": set()
        }
        
        if archive_metadata.file_inventory:
            files = archive_metadata.file_inventory.list_files()
            
            # Collect patterns for selective extraction
            for file in files:
                # Content types
                extraction_info["content_types"].append(file.content_type.value)
                
                # Directories
                extraction_info["directories"].add(file.get_directory())
                
                # Extensions
                ext = file.get_file_extension()
                if ext:
                    extraction_info["extensions"].add(f"*.{ext}")
                
                # Date patterns from filenames
                filename = file.get_filename()
                date_patterns = cls._extract_date_patterns_from_filename(filename)
                extraction_info["date_patterns"].extend(date_patterns)
            
            # Convert sets to sorted lists
            extraction_info["content_types"] = sorted(set(extraction_info["content_types"]))
            extraction_info["directories"] = sorted(extraction_info["directories"])
            extraction_info["extensions"] = sorted(extraction_info["extensions"])
            extraction_info["date_patterns"] = sorted(set(extraction_info["date_patterns"]))
            
            # Generate common file patterns
            extraction_info["available_patterns"] = cls._generate_common_patterns(files)
        
        return extraction_info
    
    @classmethod
    def _extract_date_patterns_from_filename(cls, filename: str) -> List[str]:
        """Extract date patterns from a filename."""
        import re
        
        patterns = []
        
        # Common date patterns in Earth science filenames
        date_regexes = [
            (r'\b(\d{4})\b', '%Y'),                    # Year: 2024
            (r'\b(\d{4})-(\d{2})\b', '%Y-%m'),         # Year-Month: 2024-03
            (r'\b(\d{4})-(\d{2})-(\d{2})\b', '%Y-%m-%d'),  # Date: 2024-03-15
            (r'\b(\d{8})\b', '%Y%m%d'),                # Compact date: 20240315
            (r'\b(\d{4})(\d{2})(\d{2})\b', '%Y%m%d'),  # Compact date: 20240315
            (r'\b(\d{6})\b', '%Y%m'),                  # Year-month: 202403
        ]
        
        for regex, pattern in date_regexes:
            if re.search(regex, filename):
                patterns.append(pattern)
        
        return patterns
    
    @classmethod
    def _generate_common_patterns(cls, files: List[SimulationFile]) -> List[str]:
        """Generate common file patterns for extraction."""
        patterns = set()
        
        for file in files:
            path_parts = Path(file.relative_path).parts
            
            # Add directory patterns
            if len(path_parts) > 1:
                patterns.add(f"{path_parts[0]}/*")
                if len(path_parts) > 2:
                    patterns.add(f"{path_parts[0]}/{path_parts[1]}/*")
            
            # Add extension patterns
            ext = file.get_file_extension()
            if ext:
                patterns.add(f"*.{ext}")
            
            # Add content-type patterns
            if file.content_type.value == 'output':
                patterns.add("*output*")
            elif file.content_type.value == 'input':
                patterns.add("*input*")
            elif file.content_type.value == 'log':
                patterns.add("*.log")
                patterns.add("*log*")
        
        return sorted(patterns)
    
    @classmethod
    def _reconstruct_inventory_from_metadata(cls, inventory_data: Dict[str, Any]) -> FileInventory:
        """Reconstruct FileInventory from metadata."""
        from ..entities.simulation_file import FileContentType, FileImportance
        
        inventory = FileInventory(
            total_size=inventory_data.get('total_size', 0),
            file_count=inventory_data.get('total_files', 0),
            created_time=inventory_data.get('created_time', time.time())
        )
        
        # Reconstruct files
        for file_data in inventory_data.get('files', []):
            # Parse simulation date
            simulation_date = None
            if file_data.get('simulation_date'):
                simulation_date = datetime.fromisoformat(file_data['simulation_date'])
            
            # Parse checksum
            checksum = None
            if file_data.get('checksum'):
                checksum_str = file_data['checksum']
                if ':' in checksum_str:
                    algorithm, value = checksum_str.split(':', 1)
                    checksum = Checksum(value=value, algorithm=algorithm)
                else:
                    checksum = Checksum(value=checksum_str, algorithm='md5')
            
            sim_file = SimulationFile(
                relative_path=file_data.get('path', ''),
                content_type=FileContentType(file_data.get('content_type', 'output')),
                importance=FileImportance(file_data.get('importance', 'important')),
                size=file_data.get('size'),
                checksum=checksum,
                file_role=file_data.get('role'),
                tags=set(file_data.get('tags', [])),
                created_time=file_data.get('created_time'),
                modified_time=file_data.get('modified_time'),
                simulation_date=simulation_date
            )
            
            inventory.add_file(sim_file)
        
        return inventory