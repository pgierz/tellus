"""
Cache abstraction implementations for testing.

This module provides concrete implementations of cache interfaces
for both real cache operations and test doubles.
"""

import hashlib
import time
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

from .interfaces import CacheInterface


class InMemoryCache:
    """In-memory cache implementation for unit tests."""
    
    def __init__(self, max_size: Optional[int] = None):
        """Initialize in-memory cache."""
        self._data: Dict[str, bytes] = {}
        self._timestamps: Dict[str, float] = {}
        self._ttls: Dict[str, float] = {}
        self._max_size = max_size
        self._current_size = 0
    
    def get(self, key: str) -> Optional[bytes]:
        """Get cached data by key."""
        if key not in self._data:
            return None
        
        # Check TTL
        if key in self._ttls and time.time() > self._ttls[key]:
            self.delete(key)
            return None
        
        # Update access time
        self._timestamps[key] = time.time()
        return self._data[key]
    
    def put(self, key: str, data: bytes, ttl: Optional[int] = None) -> None:
        """Put data in cache with optional TTL."""
        # Remove existing data if present
        if key in self._data:
            self._current_size -= len(self._data[key])
        
        # Check size limits
        data_size = len(data)
        if self._max_size and self._current_size + data_size > self._max_size:
            self._evict_lru(data_size)
        
        # Store data
        self._data[key] = data
        self._timestamps[key] = time.time()
        self._current_size += data_size
        
        # Set TTL if provided
        if ttl:
            self._ttls[key] = time.time() + ttl
    
    def delete(self, key: str) -> bool:
        """Delete cached data."""
        if key not in self._data:
            return False
        
        self._current_size -= len(self._data[key])
        del self._data[key]
        del self._timestamps[key]
        
        if key in self._ttls:
            del self._ttls[key]
        
        return True
    
    def clear(self) -> None:
        """Clear all cached data."""
        self._data.clear()
        self._timestamps.clear()
        self._ttls.clear()
        self._current_size = 0
    
    def size(self) -> int:
        """Get cache size in bytes."""
        return self._current_size
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        expired_keys = 0
        current_time = time.time()
        
        for key, expiry_time in self._ttls.items():
            if current_time > expiry_time:
                expired_keys += 1
        
        return {
            "entries": len(self._data),
            "size_bytes": self._current_size,
            "max_size_bytes": self._max_size,
            "expired_entries": expired_keys,
            "oldest_entry": min(self._timestamps.values()) if self._timestamps else None,
            "newest_entry": max(self._timestamps.values()) if self._timestamps else None
        }
    
    def _evict_lru(self, required_space: int) -> None:
        """Evict least recently used entries to make space."""
        if not self._data:
            return
        
        # Sort keys by timestamp (oldest first)
        sorted_keys = sorted(self._timestamps.keys(), key=lambda k: self._timestamps[k])
        
        for key in sorted_keys:
            self.delete(key)
            if self._current_size + required_space <= (self._max_size or float('inf')):
                break


class FileCache:
    """File-based cache implementation for integration tests."""
    
    def __init__(self, cache_dir: Path, max_size: Optional[int] = None):
        """Initialize file cache."""
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._max_size = max_size
        self._index_file = self._cache_dir / "cache_index.json"
        self._index: Dict[str, Dict[str, Any]] = {}
        self._load_index()
    
    def get(self, key: str) -> Optional[bytes]:
        """Get cached data by key."""
        if key not in self._index:
            return None
        
        entry = self._index[key]
        file_path = self._cache_dir / entry["filename"]
        
        # Check if file exists
        if not file_path.exists():
            del self._index[key]
            self._save_index()
            return None
        
        # Check TTL
        if "ttl" in entry and time.time() > entry["ttl"]:
            self.delete(key)
            return None
        
        # Update access time
        entry["last_accessed"] = time.time()
        self._save_index()
        
        return file_path.read_bytes()
    
    def put(self, key: str, data: bytes, ttl: Optional[int] = None) -> None:
        """Put data in cache with optional TTL."""
        # Generate filename from key hash
        key_hash = hashlib.md5(key.encode()).hexdigest()
        filename = f"{key_hash}.cache"
        file_path = self._cache_dir / filename
        
        # Remove existing entry if present
        if key in self._index:
            old_path = self._cache_dir / self._index[key]["filename"]
            if old_path.exists():
                old_path.unlink()
        
        # Check size limits
        if self._max_size:
            current_size = self._calculate_total_size()
            data_size = len(data)
            if current_size + data_size > self._max_size:
                self._evict_lru(data_size)
        
        # Store data
        file_path.write_bytes(data)
        
        # Update index
        entry = {
            "filename": filename,
            "size": len(data),
            "created": time.time(),
            "last_accessed": time.time()
        }
        
        if ttl:
            entry["ttl"] = time.time() + ttl
        
        self._index[key] = entry
        self._save_index()
    
    def delete(self, key: str) -> bool:
        """Delete cached data."""
        if key not in self._index:
            return False
        
        entry = self._index[key]
        file_path = self._cache_dir / entry["filename"]
        
        if file_path.exists():
            file_path.unlink()
        
        del self._index[key]
        self._save_index()
        
        return True
    
    def clear(self) -> None:
        """Clear all cached data."""
        # Remove all cache files
        for entry in self._index.values():
            file_path = self._cache_dir / entry["filename"]
            if file_path.exists():
                file_path.unlink()
        
        # Clear index
        self._index.clear()
        self._save_index()
    
    def size(self) -> int:
        """Get cache size in bytes."""
        return self._calculate_total_size()
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        current_time = time.time()
        expired_entries = 0
        total_size = 0
        oldest_entry = None
        newest_entry = None
        
        for entry in self._index.values():
            total_size += entry["size"]
            
            if "ttl" in entry and current_time > entry["ttl"]:
                expired_entries += 1
            
            created_time = entry["created"]
            if oldest_entry is None or created_time < oldest_entry:
                oldest_entry = created_time
            if newest_entry is None or created_time > newest_entry:
                newest_entry = created_time
        
        return {
            "entries": len(self._index),
            "size_bytes": total_size,
            "max_size_bytes": self._max_size,
            "expired_entries": expired_entries,
            "oldest_entry": oldest_entry,
            "newest_entry": newest_entry,
            "cache_directory": str(self._cache_dir)
        }
    
    def _load_index(self) -> None:
        """Load cache index from file."""
        if self._index_file.exists():
            try:
                import json
                with open(self._index_file) as f:
                    self._index = json.load(f)
            except Exception:
                self._index = {}
        else:
            self._index = {}
    
    def _save_index(self) -> None:
        """Save cache index to file."""
        import json
        with open(self._index_file, 'w') as f:
            json.dump(self._index, f, indent=2)
    
    def _calculate_total_size(self) -> int:
        """Calculate total cache size."""
        return sum(entry["size"] for entry in self._index.values())
    
    def _evict_lru(self, required_space: int) -> None:
        """Evict least recently used entries to make space."""
        if not self._index:
            return
        
        # Sort entries by last accessed time
        sorted_entries = sorted(
            self._index.items(), 
            key=lambda item: item[1]["last_accessed"]
        )
        
        current_size = self._calculate_total_size()
        
        for key, entry in sorted_entries:
            self.delete(key)
            current_size -= entry["size"]
            
            if current_size + required_space <= (self._max_size or float('inf')):
                break


class MockCache:
    """Mock cache for isolated unit tests."""
    
    def __init__(self):
        """Initialize mock cache."""
        self.get = MagicMock(return_value=b"mock cached data")
        self.put = MagicMock()
        self.delete = MagicMock(return_value=True)
        self.clear = MagicMock()
        self.size = MagicMock(return_value=1024)
        self.stats = MagicMock(return_value={
            "entries": 5,
            "size_bytes": 1024,
            "max_size_bytes": 10240
        })
    
    def configure_get(self, key_data_mapping: Dict[str, Optional[bytes]]) -> None:
        """Configure get behavior for specific keys."""
        self.get.side_effect = lambda key: key_data_mapping.get(key, b"default data")
    
    def configure_delete(self, key_success_mapping: Dict[str, bool]) -> None:
        """Configure delete behavior for specific keys."""
        self.delete.side_effect = lambda key: key_success_mapping.get(key, True)
    
    def configure_size(self, size: int) -> None:
        """Configure size return value."""
        self.size.return_value = size
    
    def configure_stats(self, stats: Dict[str, Any]) -> None:
        """Configure stats return value."""
        self.stats.return_value = stats


class NoOpCache:
    """No-operation cache that doesn't actually cache anything."""
    
    def get(self, key: str) -> Optional[bytes]:
        """Always return None (cache miss)."""
        return None
    
    def put(self, key: str, data: bytes, ttl: Optional[int] = None) -> None:
        """Do nothing."""
        pass
    
    def delete(self, key: str) -> bool:
        """Always return False (nothing to delete)."""
        return False
    
    def clear(self) -> None:
        """Do nothing."""
        pass
    
    def size(self) -> int:
        """Always return 0."""
        return 0
    
    def stats(self) -> Dict[str, Any]:
        """Return empty stats."""
        return {
            "entries": 0,
            "size_bytes": 0,
            "max_size_bytes": None
        }


class CacheDecorator:
    """Decorator that adds caching behavior to any cache implementation."""
    
    def __init__(self, base_cache: CacheInterface, key_prefix: str = ""):
        """Initialize cache decorator."""
        self._base_cache = base_cache
        self._key_prefix = key_prefix
        self._hit_count = 0
        self._miss_count = 0
    
    def get(self, key: str) -> Optional[bytes]:
        """Get with hit/miss tracking."""
        prefixed_key = self._add_prefix(key)
        result = self._base_cache.get(prefixed_key)
        
        if result is not None:
            self._hit_count += 1
        else:
            self._miss_count += 1
        
        return result
    
    def put(self, key: str, data: bytes, ttl: Optional[int] = None) -> None:
        """Put with key prefixing."""
        prefixed_key = self._add_prefix(key)
        self._base_cache.put(prefixed_key, data, ttl)
    
    def delete(self, key: str) -> bool:
        """Delete with key prefixing."""
        prefixed_key = self._add_prefix(key)
        return self._base_cache.delete(prefixed_key)
    
    def clear(self) -> None:
        """Clear base cache."""
        self._base_cache.clear()
        self._hit_count = 0
        self._miss_count = 0
    
    def size(self) -> int:
        """Get base cache size."""
        return self._base_cache.size()
    
    def stats(self) -> Dict[str, Any]:
        """Get enhanced stats with hit/miss ratio."""
        base_stats = self._base_cache.stats()
        
        total_requests = self._hit_count + self._miss_count
        hit_rate = self._hit_count / total_requests if total_requests > 0 else 0.0
        
        return {
            **base_stats,
            "hit_count": self._hit_count,
            "miss_count": self._miss_count,
            "hit_rate": hit_rate,
            "key_prefix": self._key_prefix
        }
    
    def _add_prefix(self, key: str) -> str:
        """Add prefix to key."""
        if self._key_prefix:
            return f"{self._key_prefix}:{key}"
        return key


class CacheFactory:
    """Factory for creating cache implementations."""
    
    @staticmethod
    def create_in_memory(max_size: Optional[int] = None) -> InMemoryCache:
        """Create in-memory cache implementation."""
        return InMemoryCache(max_size)
    
    @staticmethod
    def create_file_cache(cache_dir: Path, max_size: Optional[int] = None) -> FileCache:
        """Create file-based cache implementation."""
        return FileCache(cache_dir, max_size)
    
    @staticmethod
    def create_mock() -> MockCache:
        """Create mock cache implementation."""
        return MockCache()
    
    @staticmethod
    def create_no_op() -> NoOpCache:
        """Create no-operation cache implementation."""
        return NoOpCache()
    
    @staticmethod
    def create_decorated(base_cache: CacheInterface, key_prefix: str = "") -> CacheDecorator:
        """Create decorated cache with additional functionality."""
        return CacheDecorator(base_cache, key_prefix)