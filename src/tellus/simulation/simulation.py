#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Simulation objects for Earth System Models"""

import hashlib
import io
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Set
from dataclasses import dataclass, field
from enum import Enum

# import itertools
import tarfile
import uuid

import fsspec

from ..location import Location, LocationExistsError
from .context import LocationContext

# from snakemake.workflow import Rules, Workflow


class CleanupPolicy(Enum):
    LRU = "lru"
    MANUAL = "manual"
    SIZE_ONLY = "size_only"


class CachePriority(Enum):
    FILES = "files"
    ARCHIVES = "archives"


@dataclass
class CacheConfig:
    cache_dir: Path = field(default_factory=lambda: Path.home() / ".cache" / "tellus")
    
    # Archive-level cache settings
    archive_cache_size_limit: int = 50 * 1024**3  # 50 GB
    archive_cache_cleanup_policy: CleanupPolicy = CleanupPolicy.LRU
    
    # File-level cache settings
    file_cache_size_limit: int = 10 * 1024**3  # 10 GB
    file_cache_cleanup_policy: CleanupPolicy = CleanupPolicy.LRU
    
    # Unified vs separate management
    unified_cache: bool = False
    
    # Cleanup priority
    cache_priority: CachePriority = CachePriority.FILES
    
    def __post_init__(self):
        self.cache_dir = Path(self.cache_dir)
        self.archive_cache_dir = self.cache_dir / "archives"
        self.file_cache_dir = self.cache_dir / "files"


@dataclass
class CacheEntry:
    path: Path
    size: int
    checksum: str
    last_accessed: float = field(default_factory=time.time)
    created: float = field(default_factory=time.time)


class CacheManager:
    def __init__(self, config: Optional[CacheConfig] = None):
        self.config = config or CacheConfig()
        self.config.archive_cache_dir.mkdir(parents=True, exist_ok=True)
        self.config.file_cache_dir.mkdir(parents=True, exist_ok=True)
        
        self._archive_index: Dict[str, CacheEntry] = {}
        self._file_index: Dict[str, CacheEntry] = {}
        self._load_cache_index()
    
    def _load_cache_index(self):
        index_file = self.config.cache_dir / "cache_index.json"
        if index_file.exists():
            try:
                with open(index_file) as f:
                    data = json.load(f)
                    
                for checksum, entry_data in data.get("archives", {}).items():
                    self._archive_index[checksum] = CacheEntry(
                        path=Path(entry_data["path"]),
                        size=entry_data["size"],
                        checksum=checksum,
                        last_accessed=entry_data.get("last_accessed", time.time()),
                        created=entry_data.get("created", time.time())
                    )
                    
                for file_key, entry_data in data.get("files", {}).items():
                    self._file_index[file_key] = CacheEntry(
                        path=Path(entry_data["path"]),
                        size=entry_data["size"],
                        checksum=entry_data["checksum"],
                        last_accessed=entry_data.get("last_accessed", time.time()),
                        created=entry_data.get("created", time.time())
                    )
            except (json.JSONDecodeError, KeyError, ValueError):
                # Handle corrupted cache index by starting fresh
                # This allows recovery from cache index corruption
                pass
    
    def _save_cache_index(self):
        index_file = self.config.cache_dir / "cache_index.json"
        data = {
            "archives": {},
            "files": {}
        }
        
        for checksum, entry in self._archive_index.items():
            data["archives"][checksum] = {
                "path": str(entry.path),
                "size": entry.size,
                "last_accessed": entry.last_accessed,
                "created": entry.created
            }
            
        for file_key, entry in self._file_index.items():
            data["files"][file_key] = {
                "path": str(entry.path),
                "size": entry.size,
                "checksum": entry.checksum,
                "last_accessed": entry.last_accessed,
                "created": entry.created
            }
        
        with open(index_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def _calculate_checksum(self, file_path: Path) -> str:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def get_archive_path(self, checksum: str) -> Optional[Path]:
        if checksum in self._archive_index:
            entry = self._archive_index[checksum]
            if entry.path.exists():
                entry.last_accessed = time.time()
                self._save_cache_index()
                return entry.path
            else:
                del self._archive_index[checksum]
                self._save_cache_index()
        return None
    
    def cache_archive(self, source_path: Path, checksum: str) -> Path:
        cache_path = self.config.archive_cache_dir / f"{checksum}.tar.gz"
        
        if checksum in self._archive_index and cache_path.exists():
            self._archive_index[checksum].last_accessed = time.time()
            self._save_cache_index()
            return cache_path
        
        shutil.copy2(source_path, cache_path)
        size = cache_path.stat().st_size
        
        self._archive_index[checksum] = CacheEntry(
            path=cache_path,
            size=size,
            checksum=checksum
        )
        
        self._cleanup_if_needed()
        self._save_cache_index()
        return cache_path
    
    def get_file_path(self, file_key: str) -> Optional[Path]:
        if file_key in self._file_index:
            entry = self._file_index[file_key]
            if entry.path.exists():
                entry.last_accessed = time.time()
                self._save_cache_index()
                return entry.path
            else:
                del self._file_index[file_key]
                self._save_cache_index()
        return None
    
    def cache_file(self, source_data: bytes, file_key: str, checksum: str) -> Path:
        cache_path = self.config.file_cache_dir / f"{checksum}_{file_key.replace('/', '_')}"
        
        if file_key in self._file_index and cache_path.exists():
            self._file_index[file_key].last_accessed = time.time()
            self._save_cache_index()
            return cache_path
        
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, 'wb') as f:
            f.write(source_data)
        
        self._file_index[file_key] = CacheEntry(
            path=cache_path,
            size=len(source_data),
            checksum=checksum
        )
        
        self._cleanup_if_needed()
        self._save_cache_index()
        return cache_path
    
    def _cleanup_if_needed(self):
        if self.config.unified_cache:
            self._cleanup_unified()
        else:
            self._cleanup_separate()
    
    def _cleanup_unified(self):
        total_size = sum(e.size for e in self._archive_index.values()) + \
                    sum(e.size for e in self._file_index.values())
        
        max_size = self.config.archive_cache_size_limit + self.config.file_cache_size_limit
        
        if total_size > max_size:
            all_entries = []
            for checksum, entry in self._archive_index.items():
                all_entries.append(("archive", checksum, entry))
            for file_key, entry in self._file_index.items():
                all_entries.append(("file", file_key, entry))
            
            all_entries.sort(key=lambda x: x[2].last_accessed)
            
            while total_size > max_size and all_entries:
                entry_type, key, entry = all_entries.pop(0)
                total_size -= entry.size
                
                if entry.path.exists():
                    entry.path.unlink()
                
                if entry_type == "archive":
                    del self._archive_index[key]
                else:
                    del self._file_index[key]
    
    def _cleanup_separate(self):
        self._cleanup_cache_type(
            self._archive_index, 
            self.config.archive_cache_size_limit,
            self.config.archive_cache_cleanup_policy
        )
        self._cleanup_cache_type(
            self._file_index, 
            self.config.file_cache_size_limit,
            self.config.file_cache_cleanup_policy
        )
    
    def _cleanup_cache_type(self, index: Dict[str, CacheEntry], size_limit: int, policy: CleanupPolicy):
        if policy == CleanupPolicy.MANUAL:
            return
        
        total_size = sum(entry.size for entry in index.values())
        
        if total_size > size_limit:
            entries = list(index.items())
            if policy == CleanupPolicy.LRU:
                entries.sort(key=lambda x: x[1].last_accessed)
            elif policy == CleanupPolicy.SIZE_ONLY:
                entries.sort(key=lambda x: x[1].size, reverse=True)
            
            while total_size > size_limit and entries:
                key, entry = entries.pop(0)
                total_size -= entry.size
                
                if entry.path.exists():
                    entry.path.unlink()
                del index[key]
    
    def clear_cache(self, cache_type: Optional[str] = None):
        if cache_type == "archives" or cache_type is None:
            for entry in self._archive_index.values():
                if entry.path.exists():
                    entry.path.unlink()
            self._archive_index.clear()
        
        if cache_type == "files" or cache_type is None:
            for entry in self._file_index.values():
                if entry.path.exists():
                    entry.path.unlink()
            self._file_index.clear()
        
        self._save_cache_index()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        archive_size = sum(entry.size for entry in self._archive_index.values())
        file_size = sum(entry.size for entry in self._file_index.values())
        
        return {
            "archive_count": len(self._archive_index),
            "archive_size": archive_size,
            "file_count": len(self._file_index),
            "file_size": file_size,
            "total_size": archive_size + file_size,
            "cache_dir": str(self.config.cache_dir)
        }


# Placeholder for default tag patterns - to be filled in by user
DEFAULT_TAG_PATTERNS = {
    "input": ["input/*", "forcing/*", "initial/*"],
    "scripts": ["scripts/*", "*.sh", "*.py", "run/*"],
    "output": ["output/*", "outdata/*", "results/*"],
    "namelists": ["namelists/*", "*.nml", "*.cfg", "config/*"],
    "restart": ["restart/*", "checkpoints/*"],
    "logs": ["logs/*", "*.log", "*.out", "*.err"],
    # Add more defaults as needed
}


@dataclass
class TaggedFile:
    path: str
    tags: Set[str] = field(default_factory=set)
    size: Optional[int] = None
    checksum: Optional[str] = None
    modified: Optional[float] = None


class TagSystem:
    def __init__(self, tag_patterns: Optional[Dict[str, List[str]]] = None):
        self.tag_patterns = tag_patterns or DEFAULT_TAG_PATTERNS.copy()
        self._compiled_patterns = {}
        self._compile_patterns()
    
    def _compile_patterns(self):
        import fnmatch
        self._compiled_patterns = {}
        for tag, patterns in self.tag_patterns.items():
            self._compiled_patterns[tag] = patterns
    
    def tag_file(self, file_path: str) -> Set[str]:
        import fnmatch
        tags = set()
        
        for tag, patterns in self._compiled_patterns.items():
            for pattern in patterns:
                if fnmatch.fnmatch(file_path, pattern):
                    tags.add(tag)
                    break
        
        if not tags:
            tags.add("unknown")
        
        return tags
    
    def tag_files(self, file_paths: List[str]) -> Dict[str, TaggedFile]:
        tagged_files = {}
        for path in file_paths:
            tags = self.tag_file(path)
            tagged_files[path] = TaggedFile(path=path, tags=tags)
        return tagged_files
    
    def add_tag_pattern(self, tag: str, pattern: str):
        if tag not in self.tag_patterns:
            self.tag_patterns[tag] = []
        self.tag_patterns[tag].append(pattern)
        self._compile_patterns()
    
    def remove_tag_pattern(self, tag: str, pattern: str):
        if tag in self.tag_patterns:
            try:
                self.tag_patterns[tag].remove(pattern)
                if not self.tag_patterns[tag]:
                    del self.tag_patterns[tag]
                self._compile_patterns()
            except ValueError:
                pass
    
    def get_files_by_tag(self, tagged_files: Dict[str, TaggedFile], *tags: str) -> Dict[str, TaggedFile]:
        if not tags:
            return tagged_files
        
        result = {}
        tag_set = set(tags)
        
        for path, tagged_file in tagged_files.items():
            if tag_set.intersection(tagged_file.tags):
                result[path] = tagged_file
        
        return result
    
    def get_files_by_tags_and(self, tagged_files: Dict[str, TaggedFile], *tags: str) -> Dict[str, TaggedFile]:
        if not tags:
            return tagged_files
        
        result = {}
        tag_set = set(tags)
        
        for path, tagged_file in tagged_files.items():
            if tag_set.issubset(tagged_file.tags):
                result[path] = tagged_file
        
        return result
    
    def list_tags(self, tagged_files: Dict[str, TaggedFile]) -> Dict[str, int]:
        tag_counts = {}
        for tagged_file in tagged_files.values():
            for tag in tagged_file.tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
        return tag_counts
    
    def discover_potential_tags(self, file_paths: List[str]) -> Dict[str, Set[str]]:
        potential_tags = {}
        
        # Analyze directory structure
        directories = set()
        extensions = set()
        
        for path in file_paths:
            parts = Path(path).parts
            if len(parts) > 1:
                directories.add(parts[0])
            
            ext = Path(path).suffix
            if ext:
                extensions.add(ext)
        
        potential_tags["directories"] = directories
        potential_tags["extensions"] = extensions
        
        return potential_tags
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "tag_patterns": self.tag_patterns
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TagSystem":
        return cls(tag_patterns=data.get("tag_patterns"))


@dataclass
class PathMapping:
    strip_prefixes: List[str] = field(default_factory=list)
    add_prefix: Optional[str] = None
    relocations: Dict[str, str] = field(default_factory=dict)
    template_variables: Dict[str, str] = field(default_factory=dict)


class PathMapper:
    def __init__(self, 
                 simulation_mapping: Optional[PathMapping] = None,
                 archive_mappings: Optional[Dict[str, PathMapping]] = None):
        self.simulation_mapping = simulation_mapping or PathMapping()
        self.archive_mappings = archive_mappings or {}
    
    def _apply_template_variables(self, path: str, variables: Dict[str, str]) -> str:
        result = path
        for var, value in variables.items():
            result = result.replace(f"{{{{{var}}}}}", str(value))
        return result
    
    def _apply_mapping(self, path: str, mapping: PathMapping) -> str:
        result = path
        
        # Apply template variables first
        if mapping.template_variables:
            result = self._apply_template_variables(result, mapping.template_variables)
        
        # Strip prefixes
        for prefix in mapping.strip_prefixes:
            if result.startswith(prefix):
                result = result[len(prefix):]
                break
        
        # Apply relocations
        for old_pattern, new_pattern in mapping.relocations.items():
            import fnmatch
            if fnmatch.fnmatch(result, old_pattern):
                # Simple pattern replacement
                if '*' in old_pattern:
                    # Handle glob patterns
                    import re
                    pattern = old_pattern.replace('*', '(.*)')
                    match = re.match(pattern, result)
                    if match:
                        # Replace with captured groups
                        replacement = new_pattern
                        for i, group in enumerate(match.groups(), 1):
                            replacement = replacement.replace(f'\\{i}', group)
                        result = replacement
                        break
                else:
                    # Exact match replacement
                    if result == old_pattern:
                        result = new_pattern
                        break
        
        # Add prefix
        if mapping.add_prefix:
            prefix = self._apply_template_variables(mapping.add_prefix, mapping.template_variables)
            result = str(Path(prefix) / result)
        
        return result
    
    def map_path(self, archive_path: str, archive_id: Optional[str] = None) -> str:
        result = archive_path
        
        # Apply archive-specific mapping first if available
        if archive_id and archive_id in self.archive_mappings:
            result = self._apply_mapping(result, self.archive_mappings[archive_id])
        
        # Then apply simulation-level mapping
        result = self._apply_mapping(result, self.simulation_mapping)
        
        return result
    
    def map_paths(self, paths: Dict[str, str], archive_id: Optional[str] = None) -> Dict[str, str]:
        return {
            archive_path: self.map_path(archive_path, archive_id)
            for archive_path in paths
        }
    
    def set_simulation_mapping(self, mapping: PathMapping):
        self.simulation_mapping = mapping
    
    def set_archive_mapping(self, archive_id: str, mapping: PathMapping):
        self.archive_mappings[archive_id] = mapping
    
    def validate_mappings(self, sample_paths: List[str], archive_id: Optional[str] = None) -> Dict[str, Any]:
        results = {
            "valid": True,
            "warnings": [],
            "errors": [],
            "sample_mappings": {}
        }
        
        for path in sample_paths[:10]:  # Limit to first 10 for validation
            try:
                mapped = self.map_path(path, archive_id)
                results["sample_mappings"][path] = mapped
                
                # Check for potential issues
                if mapped == path:
                    results["warnings"].append(f"Path '{path}' unchanged by mapping")
                
                if not mapped:
                    results["errors"].append(f"Path '{path}' mapped to empty string")
                    results["valid"] = False
                
            except Exception as e:
                results["errors"].append(f"Failed to map '{path}': {str(e)}")
                results["valid"] = False
        
        return results
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "simulation_mapping": {
                "strip_prefixes": self.simulation_mapping.strip_prefixes,
                "add_prefix": self.simulation_mapping.add_prefix,
                "relocations": self.simulation_mapping.relocations,
                "template_variables": self.simulation_mapping.template_variables
            },
            "archive_mappings": {
                archive_id: {
                    "strip_prefixes": mapping.strip_prefixes,
                    "add_prefix": mapping.add_prefix,
                    "relocations": mapping.relocations,
                    "template_variables": mapping.template_variables
                }
                for archive_id, mapping in self.archive_mappings.items()
            }
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PathMapper":
        sim_mapping_data = data.get("simulation_mapping", {})
        sim_mapping = PathMapping(
            strip_prefixes=sim_mapping_data.get("strip_prefixes", []),
            add_prefix=sim_mapping_data.get("add_prefix"),
            relocations=sim_mapping_data.get("relocations", {}),
            template_variables=sim_mapping_data.get("template_variables", {})
        )
        
        archive_mappings = {}
        for archive_id, mapping_data in data.get("archive_mappings", {}).items():
            archive_mappings[archive_id] = PathMapping(
                strip_prefixes=mapping_data.get("strip_prefixes", []),
                add_prefix=mapping_data.get("add_prefix"),
                relocations=mapping_data.get("relocations", {}),
                template_variables=mapping_data.get("template_variables", {})
            )
        
        return cls(simulation_mapping=sim_mapping, archive_mappings=archive_mappings)


@dataclass
class ArchiveMetadata:
    archive_id: str
    location: str
    checksum: str
    size: int
    created: float
    simulation_date: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    tags: Set[str] = field(default_factory=set)


class ArchiveManifest:
    def __init__(self, archive_id: str, archive_metadata: Optional[ArchiveMetadata] = None):
        self.archive_id = archive_id
        self.metadata = archive_metadata
        self.files: Dict[str, TaggedFile] = {}
        self.tag_system = TagSystem()
        self._manifest_path: Optional[Path] = None
    
    @classmethod
    def create_from_archive(cls, archive_id: str, archive_path: Path, 
                          fs: Optional[Any] = None) -> "ArchiveManifest":
        fs = fs or fsspec.filesystem("file")
        
        # Calculate checksum and size
        if fs.exists(archive_path):
            size = fs.size(archive_path)
            # For remote files, we might need to download to calculate checksum
            # For now, use modification time as a proxy
            checksum = hashlib.md5(str(archive_path).encode()).hexdigest()
        else:
            raise FileNotFoundError(f"Archive not found: {archive_path}")
        
        metadata = ArchiveMetadata(
            archive_id=archive_id,
            location=str(archive_path),
            checksum=checksum,
            size=size,
            created=time.time()
        )
        
        manifest = cls(archive_id, metadata)
        manifest.scan_archive(archive_path, fs)
        return manifest
    
    def scan_archive(self, archive_path: Path, fs: Optional[Any] = None):
        fs = fs or fsspec.filesystem("file")
        
        try:
            # For compressed archives, we need to open and scan
            if str(archive_path).endswith(('.tar.gz', '.tgz', '.tar')):
                self._scan_tarball(archive_path, fs)
            else:
                raise ValueError(f"Unsupported archive format: {archive_path}")
        except Exception as e:
            print(f"Warning: Failed to scan archive {archive_path}: {e}")
    
    def _scan_tarball(self, archive_path: Path, fs: Any):
        with fs.open(archive_path, 'rb') as f:
            try:
                with tarfile.open(fileobj=f, mode='r:*') as tar:
                    members = tar.getmembers()
                    file_paths = []
                    
                    for member in members:
                        if member.isfile():
                            file_paths.append(member.name)
                            
                            # Create tagged file entry
                            tags = self.tag_system.tag_file(member.name)
                            tagged_file = TaggedFile(
                                path=member.name,
                                tags=tags,
                                size=member.size,
                                modified=member.mtime
                            )
                            self.files[member.name] = tagged_file
                    
                    print(f"Scanned {len(file_paths)} files from {archive_path}")
                    
            except tarfile.TarError as e:
                print(f"Error reading tarball {archive_path}: {e}")
    
    def add_file(self, file_path: str, size: Optional[int] = None, 
                checksum: Optional[str] = None, modified: Optional[float] = None):
        tags = self.tag_system.tag_file(file_path)
        tagged_file = TaggedFile(
            path=file_path,
            tags=tags,
            size=size,
            checksum=checksum,
            modified=modified
        )
        self.files[file_path] = tagged_file
    
    def remove_file(self, file_path: str):
        if file_path in self.files:
            del self.files[file_path]
    
    def get_files_by_tags(self, *tags: str) -> Dict[str, TaggedFile]:
        return self.tag_system.get_files_by_tag(self.files, *tags)
    
    def get_files_by_pattern(self, pattern: str) -> Dict[str, TaggedFile]:
        import fnmatch
        result = {}
        for path, tagged_file in self.files.items():
            if fnmatch.fnmatch(path, pattern):
                result[path] = tagged_file
        return result
    
    def get_files_by_date_range(self, start_date: Optional[float] = None, 
                               end_date: Optional[float] = None) -> Dict[str, TaggedFile]:
        result = {}
        for path, tagged_file in self.files.items():
            if tagged_file.modified is None:
                continue
            
            if start_date and tagged_file.modified < start_date:
                continue
            if end_date and tagged_file.modified > end_date:
                continue
            
            result[path] = tagged_file
        return result
    
    def list_tags(self) -> Dict[str, int]:
        return self.tag_system.list_tags(self.files)
    
    def get_stats(self) -> Dict[str, Any]:
        total_size = sum(f.size for f in self.files.values() if f.size)
        tag_counts = self.list_tags()
        
        return {
            "archive_id": self.archive_id,
            "file_count": len(self.files),
            "total_size": total_size,
            "tags": tag_counts,
            "created": self.metadata.created if self.metadata else None,
            "location": self.metadata.location if self.metadata else None
        }
    
    def save_manifest(self, manifest_path: Path):
        self._manifest_path = manifest_path
        manifest_data = self.to_dict()
        
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_path, 'w') as f:
            json.dump(manifest_data, f, indent=2, default=str)
    
    @classmethod
    def load_manifest(cls, manifest_path: Path) -> "ArchiveManifest":
        with open(manifest_path) as f:
            data = json.load(f)
        
        manifest = cls.from_dict(data)
        manifest._manifest_path = manifest_path
        return manifest
    
    def update_manifest(self):
        if self._manifest_path:
            self.save_manifest(self._manifest_path)
    
    def validate_against_archive(self, archive_path: Path, fs: Optional[Any] = None) -> Dict[str, Any]:
        fs = fs or fsspec.filesystem("file")
        
        results = {
            "valid": True,
            "missing_files": [],
            "extra_files": [],
            "size_mismatches": [],
            "errors": []
        }
        
        try:
            # Scan the actual archive
            temp_manifest = ArchiveManifest.create_from_archive(
                f"temp_{self.archive_id}", archive_path, fs
            )
            
            # Compare file lists
            manifest_files = set(self.files.keys())
            archive_files = set(temp_manifest.files.keys())
            
            results["missing_files"] = list(archive_files - manifest_files)
            results["extra_files"] = list(manifest_files - archive_files)
            
            # Check sizes for common files
            for file_path in manifest_files & archive_files:
                manifest_size = self.files[file_path].size
                archive_size = temp_manifest.files[file_path].size
                
                if manifest_size != archive_size:
                    results["size_mismatches"].append({
                        "file": file_path,
                        "manifest_size": manifest_size,
                        "archive_size": archive_size
                    })
            
            if results["missing_files"] or results["extra_files"] or results["size_mismatches"]:
                results["valid"] = False
            
        except Exception as e:
            results["valid"] = False
            results["errors"].append(str(e))
        
        return results
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "archive_id": self.archive_id,
            "metadata": {
                "archive_id": self.metadata.archive_id,
                "location": self.metadata.location,
                "checksum": self.metadata.checksum,
                "size": self.metadata.size,
                "created": self.metadata.created,
                "simulation_date": self.metadata.simulation_date,
                "version": self.metadata.version,
                "description": self.metadata.description,
                "tags": list(self.metadata.tags)
            } if self.metadata else None,
            "files": {
                path: {
                    "path": file.path,
                    "tags": list(file.tags),
                    "size": file.size,
                    "checksum": file.checksum,
                    "modified": file.modified
                }
                for path, file in self.files.items()
            },
            "tag_system": self.tag_system.to_dict()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ArchiveManifest":
        archive_id = data["archive_id"]
        
        # Restore metadata
        metadata = None
        if data.get("metadata"):
            md = data["metadata"]
            metadata = ArchiveMetadata(
                archive_id=md["archive_id"],
                location=md["location"],
                checksum=md["checksum"],
                size=md["size"],
                created=md["created"],
                simulation_date=md.get("simulation_date"),
                version=md.get("version"),
                description=md.get("description"),
                tags=set(md.get("tags", []))
            )
        
        manifest = cls(archive_id, metadata)
        
        # Restore tag system
        if "tag_system" in data:
            manifest.tag_system = TagSystem.from_dict(data["tag_system"])
        
        # Restore files
        for path, file_data in data.get("files", {}).items():
            tagged_file = TaggedFile(
                path=file_data["path"],
                tags=set(file_data.get("tags", [])),
                size=file_data.get("size"),
                checksum=file_data.get("checksum"),
                modified=file_data.get("modified")
            )
            manifest.files[path] = tagged_file
        
        return manifest


class SimulationExistsError(Exception):
    pass


class Simulation:
    """A Earth System Model Simulation

    This class represents a simulation in the Tellus system. A simulation can have
    multiple locations associated with it, which are used to store and retrieve data.

    Class Attributes:
        _simulations: Class variable to store all simulation instances
        _simulations_file: Path to the JSON file for persistence
    """

    _simulations: Dict[str, "Simulation"] = {}
    _simulations_file: Path = (
        Path(__file__).parent.parent.parent.parent / "simulations.json"
    )

    def __init__(
        self,
        simulation_id: str | None = None,
        path: str | None = None,
        model_id: str | None = None,
    ):
        """Initialize a new simulation.

        Args:
            simulation_id: Optional unique identifier for the simulation.
                         If not provided, a UUID will be generated.
            path: Optional filesystem path for the simulation data.
            model_id: Optional identifier for the model.
        """
        _uid = str(uuid.uuid4())
        if simulation_id:
            if simulation_id in Simulation._simulations:
                raise SimulationExistsError(
                    f"Simulation with ID '{simulation_id}' already exists"
                )
            self.simulation_id = simulation_id
        else:
            self.simulation_id = _uid

        self._uid = _uid
        self.path = path
        self.model_id = model_id
        self.attrs = {}
        self.data = None
        self.namelists = {}
        self.locations: dict[str, dict[str, object]] = {}
        self.snakemakes: Dict[str, Any] = {}
        """dict: A collection of snakemake rules this simulation knows about"""

        # Add to class registry
        Simulation._simulations[self.simulation_id] = self

    @classmethod
    def save_simulations(cls):
        """Save all simulations to disk."""
        cls._simulations_file.parent.mkdir(parents=True, exist_ok=True)
        data = {}
        for sim_id, sim in cls._simulations.items():
            # Convert locations to serializable format
            locations_data = {}
            for name, loc_data in sim.locations.items():
                loc_dict = {
                    "location": loc_data["location"].to_dict(),
                }
                if "context" in loc_data and loc_data["context"] is not None:
                    loc_dict["context"] = loc_data["context"].to_dict()
                locations_data[name] = loc_dict

            data[sim_id] = {
                "simulation_id": sim.simulation_id,
                "path": sim.path,
                "model_id": sim.model_id,
                "attrs": sim.attrs,
                "locations": locations_data,
                "namelists": sim.namelists,
                "snakemakes": sim.snakemakes,
            }

        # Write to file atomically
        temp_file = cls._simulations_file.with_suffix(".tmp")
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=2, default=str)
        temp_file.replace(cls._simulations_file)

    @classmethod
    def load_simulations(cls):
        """Load simulations from disk."""
        if not cls._simulations_file.exists():
            return

        with open(cls._simulations_file, "r") as f:
            data = json.load(f)

        cls._simulations = {}
        for sim_id, sim_data in data.items():
            sim = Simulation(
                simulation_id=sim_data["simulation_id"],
                path=sim_data.get("path"),
                model_id=sim_data.get("model_id"),
            )

            # Set basic attributes
            sim.attrs = sim_data.get("attrs", {})
            sim.namelists = sim_data.get("namelists", {})
            sim.snakemakes = sim_data.get("snakemakes", {})

            # Handle locations
            sim.locations = {}
            for name, loc_data in sim_data.get("locations", {}).items():
                if not isinstance(loc_data, dict):
                    continue

                # Get location data (support both old and new format)
                location_data = loc_data.get("location", loc_data)
                if not isinstance(location_data, dict):
                    continue

                # Create location and handlers
                location = Location.from_dict(location_data)

                # Create location entry
                entry = {
                    "location": location,
                }

                # Add context if present
                if "context" in loc_data and isinstance(loc_data["context"], dict):
                    entry["context"] = LocationContext.from_dict(loc_data["context"])

                sim.locations[name] = entry

    @classmethod
    def get_simulation(cls, simulation_id: str) -> Optional["Simulation"]:
        """Get a simulation by ID."""
        return cls._simulations.get(simulation_id)

    @classmethod
    def list_simulations(cls) -> List["Simulation"]:
        """List all simulations."""
        return list(cls._simulations.values())

    @classmethod
    def delete_simulation(cls, simulation_id: str) -> bool:
        """Delete a simulation.

        Returns:
            bool: True if the simulation was deleted, False if it didn't exist.
        """
        if simulation_id in cls._simulations:
            del cls._simulations[simulation_id]
            return True
        return False

    # def __del__(self):
    #     """Clean up when a simulation is deleted."""
    #     # Temporarily disabled to avoid segfaults during testing
    #     pass

    @property
    def uid(self):
        return self._uid

    def add_snakemake(self, rule_name, smk_file):
        """Add a snakemake rule to the simulation.

        Args:
            rule_name: The name of the snakemake rule.
            smk_file: The path to the snakemake file.
        """
        if rule_name in self.snakemakes:
            raise ValueError(f"Snakemake rule {rule_name} already exists.")
        self.snakemakes[rule_name] = smk_file

    def run_snakemake(self, rule_name):
        """Run a snakemake rule associated with this simulation."""
        if rule_name not in self.snakemakes:
            raise ValueError(f"Snakemake rule {rule_name} not found.")
        smk_file = self.snakemakes[rule_name]
        # Here you would implement the logic to run the snakemake file
        # For now, we just print a message
        print(f"Running snakemake rule {rule_name} from file {smk_file}")

    def add_location(
        self,
        location: Union[Location, dict],
        name: str = None,
        override: bool = False,
        context: Optional[Union[LocationContext, dict]] = None,
    ) -> None:
        """Add a location to the simulation with optional context.

        Args:
            location: The Location instance or dict to create a Location from
            name: Optional name for the location (defaults to location.name)
            override: If True, overwrite existing location with the same name
            context: Optional LocationContext or dict to initialize the context with
        """
        if isinstance(location, dict):
            location = Location.from_dict(location)

        if name is None:
            name = location.name

        if name in self.locations and not override:
            raise LocationExistsError(
                f"Location '{name}' already exists. Use override=True to replace it."
            )

        # Initialize context
        if context is None:
            context = LocationContext()
        elif isinstance(context, dict):
            context = LocationContext.from_dict(context)

        self.locations[name] = {
            "location": location,
            "context": context,
        }

        # Save changes if this is an existing simulation
        if self.simulation_id in Simulation._simulations:
            self.save_simulations()

    set_location = add_location

    def get_location(self, name: str) -> Optional[Location]:
        """Get a location by name.

        Args:
            name: Name of the location to retrieve

        Returns:
            The Location instance or None if not found
        """
        entry = self.locations.get(name)
        return entry["location"] if entry else None

    def get_location_context(self, name: str) -> Optional[LocationContext]:
        """Get the context for a location.

        Args:
            name: Name of the location

        Returns:
            The LocationContext for the location, or None if not found
        """
        entry = self.locations.get(name)
        return entry.get("context") if entry else None

    def set_location_context(
        self, name: str, context: Union[LocationContext, dict], merge: bool = True
    ) -> None:
        """Set the context for a location.

        Args:
            name: Name of the location
            context: LocationContext or dict to set as context
            merge: If True, merge with existing context (default: True)

        Raises:
            ValueError: If the location doesn't exist
        """
        if name not in self.locations:
            raise ValueError(f"Location '{name}' not found in simulation")

        if isinstance(context, dict):
            context = LocationContext.from_dict(context)

        if merge and "context" in self.locations[name]:
            # Merge with existing context
            existing = self.locations[name]["context"]
            if existing and context:
                # If the new context doesn't specify a path_prefix, keep the existing one
                if not context.path_prefix and existing.path_prefix:
                    context.path_prefix = existing.path_prefix
                # Merge overrides and metadata, with new context taking precedence
                merged_overrides = existing.overrides.copy()
                merged_overrides.update(context.overrides)
                merged_metadata = existing.metadata.copy()
                merged_metadata.update(context.metadata)
                context.overrides = merged_overrides
                context.metadata = merged_metadata

        self.locations[name]["context"] = context
        self.save_simulations()

    def get_location_path(self, name: str, *path_parts: str) -> str:
        """Get the full path for a location, applying any context.

        Args:
            name: Name of the location
            *path_parts: Additional path components to append

        Returns:
            The full path with context applied

        Raises:
            ValueError: If the location doesn't exist
        """
        if name not in self.locations:
            raise ValueError(f"Location '{name}' not in simulation")

        loc_data = self.locations[name]
        base_path = loc_data["location"].config.get("path", "/")  # Default to root...
        context = loc_data.get("context", LocationContext())

        # Start with the base path
        full_path = base_path

        # Apply path prefix if set
        if context and context.path_prefix:
            # Render template variables in path_prefix
            template = context.path_prefix
            if template:
                # Replace template variables
                template = template.replace("{{model_id}}", str(self.model_id or ""))
                template = template.replace(
                    "{{simulation_id}}", str(self.simulation_id or "")
                )
                # Join the template prefix with the base path 
                # If template starts with /, it's absolute (template is the root)
                # If template doesn't start with /, it's relative to base_path
                if template.startswith("/"):
                    full_path = "/" + str(Path(template.strip("/")) / base_path.strip("/"))
                else:
                    full_path = str(Path(base_path) / template.strip("/"))

        # Add any additional path parts
        if path_parts:
            full_path = str(Path(full_path).joinpath(*path_parts))

        return full_path

    def post_to_location(self, name: str, data):
        entry = self.locations.get(name)
        if not entry:
            raise ValueError(f"Location {name} not set.")
        entry["handler"].post(data)

    def fetch_from_location(self, name: str, identifier):
        entry = self.locations.get(name)
        if not entry:
            raise ValueError(f"Location {name} not set.")
        return entry["handler"].fetch(identifier)

    def list_locations(self):
        return list(self.locations.keys())

    def remove_location(self, name: str):
        if name in self.locations:
            del self.locations[name]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Simulation":
        """Create a Simulation instance from a dictionary.

        Args:
            data: Dictionary containing simulation data.
                Expected keys: simulation_id, uid, path, attrs, data,
                             namelists, locations, snakemakes

        Returns:
            Simulation: A new Simulation instance
        """
        # Create a new instance without calling __init__ directly
        sim = cls.__new__(cls)

        # Set basic attributes
        sim.simulation_id = data.get("simulation_id")
        sim._uid = data.get("uid", str(uuid.uuid4()))
        sim.path = data.get("path")
        sim.model_id = data.get("model_id")
        sim.attrs = data.get("attrs", {})
        sim.data = data.get("data")
        sim.namelists = data.get("namelists", {})
        sim.snakemakes = data.get("snakemakes", {})

        # Handle locations - convert dicts back to Location objects
        sim.locations = {}
        for name, loc_data in data.get("locations", {}).items():
            if isinstance(loc_data, dict):
                # Extract location data and context if present
                location_data = loc_data.get(
                    "location", loc_data
                )  # Backward compatible
                location = Location.from_dict(location_data)

                # Create location entry
                entry = {
                    "location": location,
                }

                # Add context if present
                if "context" in loc_data and loc_data["context"] is not None:
                    entry["context"] = LocationContext.from_dict(loc_data["context"])

                sim.locations[name] = entry

        # Set data last as it might depend on other attributes
        sim.data = data.get("data")

        return sim

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Simulation instance to a dictionary.

        Returns:
            Dict containing all serializable attributes of the Simulation
        """
        locations_dict = {}
        for name, loc in self.locations.items():
            loc_dict = {
                "location": loc["location"].to_dict(),
            }
            if "context" in loc and loc["context"] is not None:
                loc_dict["context"] = loc["context"].to_dict()
            locations_dict[name] = loc_dict

        return {
            "simulation_id": self.simulation_id,
            "uid": self.uid,
            "path": self.path,
            "model_id": self.model_id,
            "attrs": self.attrs,
            "data": self.data,
            "namelists": self.namelists,
            "locations": locations_dict,
            "snakemakes": self.snakemakes,
        }

    @classmethod
    def from_json(cls, json_str: str) -> "Simulation":
        """Create a Simulation instance from a JSON string.

        Args:
            json_str: JSON string containing simulation data

        Returns:
            Simulation: A new Simulation instance
        """
        data = json.loads(json_str)
        return cls.from_dict(data)

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert the Simulation instance to a JSON string.

        Args:
            indent: If specified, will pretty-print with the given indent level

        Returns:
            JSON string representation of the Simulation
        """
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def save(self, filepath: str, indent: Optional[int] = 2) -> None:
        """Save the Simulation to a JSON file.

        Args:
            filepath: Path to save the JSON file
            indent: If specified, will pretty-print with the given indent level
        """
        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f, indent=indent, default=str)

    @classmethod
    def load(cls, filepath: str) -> "Simulation":
        """Load a Simulation from a JSON file.

        Args:
            filepath: Path to the JSON file

        Returns:
            Simulation: A new Simulation instance loaded from the file
        """
        with open(filepath, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)


class ProgressCallback:
    """Base class for progress reporting"""
    
    def start_operation(self, operation: str, total_items: int = 0, total_bytes: int = 0):
        pass
    
    def update_progress(self, items_done: int = 0, bytes_done: int = 0, message: str = ""):
        pass
    
    def finish_operation(self, success: bool = True, message: str = ""):
        pass


class CLIProgressCallback(ProgressCallback):
    """Simple CLI progress reporting"""
    
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.current_operation = None
        self.total_items = 0
    
    def start_operation(self, operation: str, total_items: int = 0, total_bytes: int = 0):
        self.current_operation = operation
        self.total_items = total_items
        
        if self.verbose:
            if total_items > 0:
                print(f"Starting {operation} ({total_items} items)...")
            else:
                print(f"Starting {operation}...")
    
    def update_progress(self, items_done: int = 0, bytes_done: int = 0, message: str = ""):
        if self.verbose and message:
            if self.total_items > 0:
                progress = f"[{items_done}/{self.total_items}] "
            else:
                progress = ""
            print(f"  {progress}{message}")
    
    def finish_operation(self, success: bool = True, message: str = ""):
        if self.verbose:
            status = "✓" if success else "✗"
            print(f"{status} {self.current_operation}: {message}")
        self.current_operation = None


class ArchivedSimulation:
    """Enhanced archive interface for simulation data"""
    
    def __init__(self, archive_id: str, 
                 cache_manager: Optional[CacheManager] = None,
                 path_mapper: Optional[PathMapper] = None):
        self.archive_id = archive_id
        self.cache_manager = cache_manager or CacheManager()
        self.path_mapper = path_mapper or PathMapper()
        self.manifest: Optional[ArchiveManifest] = None
        self._progress_callbacks: List[ProgressCallback] = []
    
    def add_progress_callback(self, callback: ProgressCallback):
        self._progress_callbacks.append(callback)
    
    def _notify_progress(self, method: str, *args, **kwargs):
        for callback in self._progress_callbacks:
            getattr(callback, method)(*args, **kwargs)
    
    # Core interface methods (to be implemented by subclasses)
    def list_files(self, tags: Optional[List[str]] = None, 
                   pattern: Optional[str] = None) -> Dict[str, TaggedFile]:
        raise NotImplementedError
    
    def open_file(self, filename: str) -> io.BytesIO:
        raise NotImplementedError
    
    def extract_file(self, filename: str, destination: Path, 
                    apply_path_mapping: bool = True) -> Path:
        raise NotImplementedError
    
    def status(self) -> Dict[str, Any]:
        raise NotImplementedError
    
    # Enhanced interface methods
    def get_files_by_tags(self, *tags: str) -> Dict[str, TaggedFile]:
        if not self.manifest:
            raise ValueError("No manifest loaded")
        return self.manifest.get_files_by_tags(*tags)
    
    def get_files_by_pattern(self, pattern: str) -> Dict[str, TaggedFile]:
        if not self.manifest:
            raise ValueError("No manifest loaded")
        return self.manifest.get_files_by_pattern(pattern)
    
    def get_files_by_date_range(self, start_date: Optional[float] = None, 
                               end_date: Optional[float] = None) -> Dict[str, TaggedFile]:
        if not self.manifest:
            raise ValueError("No manifest loaded")
        return self.manifest.get_files_by_date_range(start_date, end_date)
    
    def extract_files_by_tags(self, destination: Path, *tags: str, 
                             apply_path_mapping: bool = True) -> List[Path]:
        files = self.get_files_by_tags(*tags)
        return self._extract_multiple_files(files, destination, apply_path_mapping)
    
    def extract_files_by_pattern(self, destination: Path, pattern: str,
                                apply_path_mapping: bool = True) -> List[Path]:
        files = self.get_files_by_pattern(pattern)
        return self._extract_multiple_files(files, destination, apply_path_mapping)
    
    def _extract_multiple_files(self, files: Dict[str, TaggedFile], 
                               destination: Path, apply_path_mapping: bool) -> List[Path]:
        self._notify_progress("start_operation", "extract_multiple", len(files))
        
        extracted_paths = []
        for i, (file_path, tagged_file) in enumerate(files.items()):
            try:
                extracted_path = self.extract_file(file_path, destination, apply_path_mapping)
                extracted_paths.append(extracted_path)
                self._notify_progress("update_progress", i + 1, 0, f"Extracted {file_path}")
            except Exception as e:
                self._notify_progress("update_progress", i + 1, 0, f"Failed to extract {file_path}: {e}")
        
        self._notify_progress("finish_operation", True, f"Extracted {len(extracted_paths)} files")
        return extracted_paths
    
    def list_tags(self) -> Dict[str, int]:
        if not self.manifest:
            raise ValueError("No manifest loaded")
        return self.manifest.list_tags()
    
    def get_stats(self) -> Dict[str, Any]:
        if not self.manifest:
            return {"archive_id": self.archive_id, "manifest_loaded": False}
        
        stats = self.manifest.get_stats()
        cache_stats = self.cache_manager.get_cache_stats()
        
        return {
            **stats,
            "cache_stats": cache_stats,
            "manifest_loaded": True
        }
    
    def discover_potential_tags(self) -> Dict[str, Set[str]]:
        if not self.manifest:
            raise ValueError("No manifest loaded")
        
        file_paths = list(self.manifest.files.keys())
        return self.manifest.tag_system.discover_potential_tags(file_paths)
    
    def set_path_mapping(self, mapping: PathMapping):
        self.path_mapper.set_archive_mapping(self.archive_id, mapping)
    
    def validate_manifest(self) -> Dict[str, Any]:
        raise NotImplementedError("Subclasses should implement manifest validation")
    
    def refresh_manifest(self) -> None:
        raise NotImplementedError("Subclasses should implement manifest refresh")
    
    def load_manifest(self, manifest_path: Path) -> None:
        self.manifest = ArchiveManifest.load_manifest(manifest_path)
    
    def save_manifest(self, manifest_path: Path) -> None:
        if not self.manifest:
            raise ValueError("No manifest to save")
        self.manifest.save_manifest(manifest_path)


class CompressedArchive(ArchivedSimulation):
    """Main implementation for compressed archive files (tar.gz, tgz, tar)"""
    
    def __init__(self, archive_id: str, archive_location: str, 
                 location: Optional[Location] = None, fs: Optional[Any] = None,
                 cache_manager: Optional[CacheManager] = None,
                 path_mapper: Optional[PathMapper] = None):
        super().__init__(archive_id, cache_manager, path_mapper)
        self.archive_location = archive_location
        self.location = location
        
        # Use location's filesystem if provided, otherwise fall back to fs or local filesystem
        if location:
            self.fs = location.fs
        else:
            self.fs = fs or fsspec.filesystem("file")
            
        self._archive_path = Path(archive_location)
        
        # Try to load existing manifest
        manifest_path = self._get_manifest_path()
        if manifest_path.exists():
            try:
                self.load_manifest(manifest_path)
            except Exception as e:
                print(f"Warning: Failed to load manifest {manifest_path}: {e}")
                self.manifest = None
    
    def _get_manifest_path(self) -> Path:
        return self._archive_path.with_suffix(self._archive_path.suffix + '.manifest.json')
    
    def _get_cached_archive_path(self) -> Optional[Path]:
        # Calculate archive checksum for cache lookup
        if self.fs.exists(self.archive_location):
            # For now, use a simple checksum based on path and size
            size = self.fs.size(self.archive_location)
            simple_checksum = hashlib.md5(f"{self.archive_location}:{size}".encode()).hexdigest()
            
            cached_path = self.cache_manager.get_archive_path(simple_checksum)
            if cached_path:
                return cached_path
                
            # Cache the archive if it's not already cached
            try:
                if self.fs.protocol == 'file':
                    # Local file - can cache directly
                    return self.cache_manager.cache_archive(Path(self.archive_location), simple_checksum)
                else:
                    # Remote file - download and cache using Location if available
                    self._notify_progress("start_operation", "download_archive", 0, size)
                    temp_path = Path(f"/tmp/{self.archive_id}_{simple_checksum}.tar.gz")
                    
                    if self.location:
                        # Use Location's get method for better progress tracking and error handling
                        self.location.get(self.archive_location, str(temp_path), overwrite=True, show_progress=False)
                    else:
                        # Fallback to direct fsspec usage
                        with self.fs.open(self.archive_location, 'rb') as src:
                            with open(temp_path, 'wb') as dst:
                                shutil.copyfileobj(src, dst)
                    
                    cached_path = self.cache_manager.cache_archive(temp_path, simple_checksum)
                    temp_path.unlink()  # Clean up temp file
                    
                    self._notify_progress("finish_operation", True, "Archive downloaded and cached")
                    return cached_path
            except Exception as e:
                print(f"Warning: Failed to cache archive: {e}")
                return None
        
        return None
    
    def list_files(self, tags: Optional[List[str]] = None, 
                   pattern: Optional[str] = None) -> Dict[str, TaggedFile]:
        if not self.manifest:
            self.refresh_manifest()
        
        files = self.manifest.files.copy()
        
        if tags:
            files = self.manifest.get_files_by_tags(*tags)
        
        if pattern:
            filtered_files = {}
            import fnmatch
            for path, tagged_file in files.items():
                if fnmatch.fnmatch(path, pattern):
                    filtered_files[path] = tagged_file
            files = filtered_files
        
        return files
    
    def open_file(self, filename: str) -> io.BytesIO:
        # Check file-level cache first
        file_key = f"{self.archive_id}:{filename}"
        cached_file_path = self.cache_manager.get_file_path(file_key)
        
        if cached_file_path and cached_file_path.exists():
            with open(cached_file_path, 'rb') as f:
                return io.BytesIO(f.read())
        
        # Get archive from cache or download
        archive_path = self._get_cached_archive_path()
        if not archive_path:
            raise FileNotFoundError(f"Archive {self.archive_location} not accessible")
        
        # Extract file from archive
        try:
            with tarfile.open(archive_path, mode='r:*') as tar:
                try:
                    member = tar.getmember(filename)
                except KeyError:
                    raise FileNotFoundError(f"File {filename} not found in archive")
                
                extracted_file = tar.extractfile(member)
                if extracted_file is None:
                    raise IsADirectoryError(f"{filename} is a directory")
                
                data = extracted_file.read()
                
                # Cache the extracted file
                file_checksum = hashlib.md5(data).hexdigest()
                self.cache_manager.cache_file(data, file_key, file_checksum)
                
                return io.BytesIO(data)
                
        except tarfile.TarError as e:
            raise RuntimeError(f"Failed to read archive {archive_path}: {e}")
    
    def extract_file(self, filename: str, destination: Path, 
                    apply_path_mapping: bool = True) -> Path:
        # Get file data
        file_data = self.open_file(filename)
        
        # Determine output path
        if apply_path_mapping:
            mapped_path = self.path_mapper.map_path(filename, self.archive_id)
        else:
            mapped_path = filename
        
        output_path = destination / mapped_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write file
        with open(output_path, 'wb') as f:
            f.write(file_data.read())
        
        return output_path
    
    def status(self) -> Dict[str, Any]:
        archive_exists = self.fs.exists(self.archive_location)
        archive_size = self.fs.size(self.archive_location) if archive_exists else 0
        
        status = {
            "archive_id": self.archive_id,
            "location": self.archive_location,
            "exists": archive_exists,
            "size": archive_size,
            "cached": self._get_cached_archive_path() is not None,
            "manifest_loaded": self.manifest is not None
        }
        
        # Add location information if available
        if self.location:
            status.update({
                "location_name": self.location.name,
                "location_kinds": [kind.name for kind in self.location.kinds],
                "storage_protocol": self.fs.protocol
            })
        else:
            status["storage_protocol"] = self.fs.protocol
        
        if self.manifest:
            status.update(self.manifest.get_stats())
        
        return status
    
    def refresh_manifest(self) -> None:
        self._notify_progress("start_operation", "refresh_manifest")
        
        try:
            self.manifest = ArchiveManifest.create_from_archive(
                self.archive_id, Path(self.archive_location), self.fs
            )
            
            # Save the updated manifest
            manifest_path = self._get_manifest_path()
            self.save_manifest(manifest_path)
            
            self._notify_progress("finish_operation", True, f"Manifest refreshed with {len(self.manifest.files)} files")
            
        except Exception as e:
            self._notify_progress("finish_operation", False, f"Failed to refresh manifest: {e}")
            raise
    
    def validate_manifest(self) -> Dict[str, Any]:
        if not self.manifest:
            return {"valid": False, "error": "No manifest loaded"}
        
        return self.manifest.validate_against_archive(Path(self.archive_location), self.fs)
    
    @classmethod
    def from_location(cls, archive_id: str, location: "Location", 
                     cache_manager: Optional[CacheManager] = None,
                     path_mapper: Optional[PathMapper] = None) -> "CompressedArchive":
        fs = location.fs
        path = location.config["path"]
        return cls(archive_id, path, fs, cache_manager, path_mapper)


# Keep the old class for backward compatibility, but inherit from new system
class SplitTarballArchivedSimulation(CompressedArchive):
    """Backward compatibility wrapper for split tarball archives"""
    
    def __init__(self, part_files, fs=None, archive_id: str = None):
        # Generate archive_id if not provided
        if not archive_id:
            archive_id = f"split_archive_{hashlib.md5(str(part_files).encode()).hexdigest()[:8]}"
        
        # For split archives, we need special handling
        self.part_files = sorted(part_files)
        
        # Create a virtual location for the split archive
        virtual_location = f"split://{':'.join(part_files)}"
        
        super().__init__(archive_id, virtual_location, fs)
    
    def _get_cached_archive_path(self) -> Optional[Path]:
        # For split archives, we need to assemble them first
        return self._assemble_and_cache_parts()
    
    def _assemble_and_cache_parts(self) -> Optional[Path]:
        # Create a checksum for all parts
        parts_info = []
        for part in self.part_files:
            if self.fs.exists(part):
                size = self.fs.size(part)
                parts_info.append(f"{part}:{size}")
        
        combined_checksum = hashlib.md5(':'.join(parts_info).encode()).hexdigest()
        
        # Check if already assembled and cached
        cached_path = self.cache_manager.get_archive_path(combined_checksum)
        if cached_path:
            return cached_path
        
        # Assemble parts into a single archive
        self._notify_progress("start_operation", "assemble_parts", len(self.part_files))
        
        try:
            temp_assembled = Path(f"/tmp/assembled_{self.archive_id}_{combined_checksum}.tar.gz")
            
            with open(temp_assembled, 'wb') as output:
                for i, part in enumerate(self.part_files):
                    with self.fs.open(part, 'rb') as part_file:
                        shutil.copyfileobj(part_file, output)
                    self._notify_progress("update_progress", i + 1, 0, f"Assembled part {part}")
            
            # Cache the assembled archive
            cached_path = self.cache_manager.cache_archive(temp_assembled, combined_checksum)
            temp_assembled.unlink()  # Clean up
            
            self._notify_progress("finish_operation", True, "Parts assembled and cached")
            return cached_path
            
        except Exception as e:
            self._notify_progress("finish_operation", False, f"Failed to assemble parts: {e}")
            return None
    
    @classmethod
    def from_fs_path(cls, fs, path, archive_id: str = None):
        listing = fs.ls(path)
        return cls(part_files=listing, fs=fs, archive_id=archive_id)
    
    @classmethod
    def from_location(cls, location, archive_id: str = None):
        fs = location.fs
        path = location.config["path"]
        return cls.from_fs_path(fs, path, archive_id)


class OrganizedTarballArchivedSimulation(CompressedArchive):
    """
    Subclass for archives with complex internal organization.
    Provides additional methods for handling nested structures.
    """
    
    def __init__(self, archive_id: str, archive_location: str, 
                 organization_config: Optional[Dict[str, Any]] = None,
                 fs: Optional[Any] = None,
                 cache_manager: Optional[CacheManager] = None,
                 path_mapper: Optional[PathMapper] = None):
        super().__init__(archive_id, archive_location, fs, cache_manager, path_mapper)
        self.organization_config = organization_config or {}
    
    def list_nested_archives(self) -> List[str]:
        """List nested archives within the main archive"""
        if not self.manifest:
            self.refresh_manifest()
        
        nested_archives = []
        for file_path in self.manifest.files.keys():
            if file_path.endswith(('.tar', '.tar.gz', '.tgz')):
                nested_archives.append(file_path)
        
        return nested_archives
    
    def extract_nested_archive(self, nested_archive_path: str, 
                              destination: Path) -> "CompressedArchive":
        """Extract a nested archive and return a new archive instance"""
        # Extract the nested archive file
        extracted_path = self.extract_file(nested_archive_path, destination, False)
        
        # Create a new archive instance for the extracted nested archive
        nested_id = f"{self.archive_id}_nested_{Path(nested_archive_path).stem}"
        return CompressedArchive(nested_id, str(extracted_path), 
                               cache_manager=self.cache_manager,
                               path_mapper=self.path_mapper)


# Archive Registry for managing multiple archives per simulation
class ArchiveRegistry:
    """Manages multiple archives for a simulation"""
    
    def __init__(self, simulation_id: str,
                 cache_manager: Optional[CacheManager] = None,
                 path_mapper: Optional[PathMapper] = None):
        self.simulation_id = simulation_id
        self.cache_manager = cache_manager or CacheManager()
        self.path_mapper = path_mapper or PathMapper()
        self.archives: Dict[str, ArchivedSimulation] = {}
    
    def add_archive(self, archive: ArchivedSimulation, archive_name: Optional[str] = None):
        """Add an archive to the registry"""
        name = archive_name or archive.archive_id
        self.archives[name] = archive
        
        # Set up shared resources
        archive.cache_manager = self.cache_manager
        archive.path_mapper = self.path_mapper
    
    def create_compressed_archive(self, archive_id: str, archive_location: str,
                                 location_name: Optional[str] = None,
                                 archive_name: Optional[str] = None) -> CompressedArchive:
        """Create and add a compressed archive with location support"""
        location = None
        if location_name:
            location = Location.get_location(location_name)
            if not location:
                raise ValueError(f"Location '{location_name}' not found")
        
        archive = CompressedArchive(
            archive_id=archive_id,
            archive_location=archive_location,
            location=location,
            cache_manager=self.cache_manager,
            path_mapper=self.path_mapper
        )
        
        self.add_archive(archive, archive_name)
        return archive
    
    def remove_archive(self, archive_name: str):
        """Remove an archive from the registry"""
        if archive_name in self.archives:
            del self.archives[archive_name]
    
    def get_archive(self, archive_name: str) -> Optional[ArchivedSimulation]:
        """Get an archive by name"""
        return self.archives.get(archive_name)
    
    def list_archives(self) -> List[str]:
        """List all archive names"""
        return list(self.archives.keys())
    
    def find_file(self, filename: str) -> List[Dict[str, Any]]:
        """Find a file across all archives, return list of matches with archive info"""
        matches = []
        
        for archive_name, archive in self.archives.items():
            try:
                files = archive.list_files()
                if filename in files:
                    matches.append({
                        "archive_name": archive_name,
                        "archive_id": archive.archive_id,
                        "file": files[filename],
                        "size": archive.status().get("size", 0)
                    })
            except Exception as e:
                print(f"Error searching archive {archive_name}: {e}")
        
        return matches
    
    def extract_file_smart(self, filename: str, destination: Path, 
                          apply_path_mapping: bool = True) -> Path:
        """Extract a file using network-optimized selection"""
        matches = self.find_file(filename)
        
        if not matches:
            raise FileNotFoundError(f"File {filename} not found in any archive")
        
        # Sort by preference: cached archives first, then by size
        def sort_key(match):
            archive = self.archives[match["archive_name"]]
            is_cached = archive.status().get("cached", False)
            size = match["size"]
            return (not is_cached, size)  # Cached first (False < True), then smaller size
        
        matches.sort(key=sort_key)
        best_match = matches[0]
        
        archive = self.archives[best_match["archive_name"]]
        return archive.extract_file(filename, destination, apply_path_mapping)
    
    def extract_files_by_tags(self, destination: Path, *tags: str,
                             apply_path_mapping: bool = True) -> Dict[str, List[Path]]:
        """Extract files by tags from all archives"""
        results = {}
        
        for archive_name, archive in self.archives.items():
            try:
                files = archive.get_files_by_tags(*tags)
                if files:
                    extracted = archive.extract_files_by_tags(destination, *tags, 
                                                            apply_path_mapping=apply_path_mapping)
                    results[archive_name] = extracted
            except Exception as e:
                print(f"Error extracting from archive {archive_name}: {e}")
        
        return results
    
    def get_combined_stats(self) -> Dict[str, Any]:
        """Get combined statistics for all archives"""
        total_files = 0
        total_size = 0
        cached_archives = 0
        tag_counts = {}
        
        for archive in self.archives.values():
            try:
                stats = archive.get_stats()
                total_files += stats.get("file_count", 0)
                total_size += stats.get("total_size", 0)
                
                if stats.get("cached", False):
                    cached_archives += 1
                
                # Combine tag counts
                for tag, count in stats.get("tags", {}).items():
                    tag_counts[tag] = tag_counts.get(tag, 0) + count
                    
            except Exception as e:
                print(f"Error getting stats for archive: {e}")
        
        return {
            "simulation_id": self.simulation_id,
            "archive_count": len(self.archives),
            "total_files": total_files,
            "total_size": total_size,
            "cached_archives": cached_archives,
            "combined_tags": tag_counts
        }
