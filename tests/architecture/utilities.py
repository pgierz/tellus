"""
Test utilities following Single Responsibility Principle.

This module provides focused utility classes and functions,
each with a single, well-defined responsibility.
"""

import hashlib
import io
import json
import shutil
import tarfile
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Union


class TemporaryPathManager:
    """Manages temporary paths for testing with automatic cleanup."""
    
    def __init__(self):
        """Initialize path manager."""
        self._temp_paths: List[Path] = []
    
    def create_temp_dir(self, prefix: str = "test_", suffix: str = "") -> Path:
        """Create a temporary directory."""
        temp_dir = Path(tempfile.mkdtemp(prefix=prefix, suffix=suffix))
        self._temp_paths.append(temp_dir)
        return temp_dir
    
    def create_temp_file(self, content: str = "", suffix: str = ".txt", 
                        prefix: str = "test_") -> Path:
        """Create a temporary file with content."""
        temp_dir = self.create_temp_dir()
        temp_file = temp_dir / f"{prefix}{uuid.uuid4().hex[:8]}{suffix}"
        temp_file.write_text(content)
        return temp_file
    
    def create_temp_binary_file(self, content: bytes = b"", suffix: str = ".bin",
                               prefix: str = "test_") -> Path:
        """Create a temporary binary file with content."""
        temp_dir = self.create_temp_dir()
        temp_file = temp_dir / f"{prefix}{uuid.uuid4().hex[:8]}{suffix}"
        temp_file.write_bytes(content)
        return temp_file
    
    def cleanup_all(self) -> None:
        """Clean up all temporary paths."""
        for path in self._temp_paths:
            if path.exists():
                if path.is_file():
                    path.unlink()
                else:
                    shutil.rmtree(path, ignore_errors=True)
        self._temp_paths.clear()
    
    def __enter__(self):
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and cleanup."""
        self.cleanup_all()


class TestDataGenerator:
    """Generates test data with configurable characteristics."""
    
    @staticmethod
    def generate_random_string(length: int = 10, charset: str = "abcdefghijklmnopqrstuvwxyz") -> str:
        """Generate a random string of specified length."""
        import random
        return ''.join(random.choices(charset, k=length))
    
    @staticmethod
    def generate_random_bytes(size: int = 1024) -> bytes:
        """Generate random bytes of specified size."""
        import random
        return bytes(random.getrandbits(8) for _ in range(size))
    
    @staticmethod
    def generate_sequential_data(pattern: str = "data_{:03d}", count: int = 10) -> List[str]:
        """Generate sequential data based on pattern."""
        return [pattern.format(i) for i in range(count)]
    
    @staticmethod
    def generate_hierarchical_paths(base: str = "test", depth: int = 3, 
                                  width: int = 2) -> List[str]:
        """Generate hierarchical paths for testing directory structures."""
        paths = []
        
        def generate_level(current_path: str, current_depth: int) -> None:
            if current_depth <= 0:
                return
            
            for i in range(width):
                new_path = f"{current_path}/level_{current_depth}_{i}"
                paths.append(new_path)
                generate_level(new_path, current_depth - 1)
        
        generate_level(base, depth)
        return paths
    
    @staticmethod
    def generate_sample_json(structure: Dict[str, Any]) -> str:
        """Generate sample JSON based on structure template."""
        def fill_template(template: Any) -> Any:
            if isinstance(template, dict):
                return {key: fill_template(value) for key, value in template.items()}
            elif isinstance(template, list):
                return [fill_template(item) for item in template]
            elif template == "<string>":
                return TestDataGenerator.generate_random_string()
            elif template == "<int>":
                import random
                return random.randint(1, 1000)
            elif template == "<float>":
                import random
                return random.uniform(0.0, 100.0)
            elif template == "<bool>":
                import random
                return random.choice([True, False])
            else:
                return template
        
        return json.dumps(fill_template(structure), indent=2)


class FileSystemTestHelper:
    """Helper for filesystem-related test operations."""
    
    def __init__(self, base_path: Optional[Path] = None):
        """Initialize with optional base path."""
        self._base_path = base_path
    
    def create_directory_structure(self, structure: Dict[str, Any], 
                                 base_path: Optional[Path] = None) -> Path:
        """Create a directory structure from dictionary specification."""
        if base_path is None:
            base_path = self._base_path or Path(tempfile.mkdtemp())
        
        def create_from_dict(current_path: Path, current_structure: Dict[str, Any]) -> None:
            for name, content in current_structure.items():
                path = current_path / name
                
                if isinstance(content, dict):
                    # It's a directory
                    path.mkdir(parents=True, exist_ok=True)
                    create_from_dict(path, content)
                elif isinstance(content, str):
                    # It's a file with string content
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(content)
                elif isinstance(content, bytes):
                    # It's a file with binary content
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(content)
        
        create_from_dict(base_path, structure)
        return base_path
    
    def assert_directory_structure(self, path: Path, expected_structure: Dict[str, Any]) -> None:
        """Assert that directory matches expected structure."""
        def check_structure(current_path: Path, expected: Dict[str, Any]) -> None:
            for name, content in expected.items():
                item_path = current_path / name
                
                if isinstance(content, dict):
                    # Expected to be a directory
                    assert item_path.is_dir(), f"Expected directory: {item_path}"
                    check_structure(item_path, content)
                else:
                    # Expected to be a file
                    assert item_path.is_file(), f"Expected file: {item_path}"
                    if isinstance(content, str):
                        actual_content = item_path.read_text()
                        assert actual_content == content, f"File content mismatch: {item_path}"
                    elif isinstance(content, bytes):
                        actual_content = item_path.read_bytes()
                        assert actual_content == content, f"File content mismatch: {item_path}"
        
        check_structure(path, expected_structure)
    
    def get_directory_tree(self, path: Path) -> Dict[str, Any]:
        """Get directory structure as dictionary."""
        result = {}
        
        if not path.exists():
            return result
        
        for item in path.iterdir():
            if item.is_dir():
                result[item.name] = self.get_directory_tree(item)
            else:
                try:
                    # Try to read as text first
                    result[item.name] = item.read_text()
                except UnicodeDecodeError:
                    # If it's binary, store as bytes
                    result[item.name] = item.read_bytes()
        
        return result


class ArchiveTestHelper:
    """Helper for archive-related test operations."""
    
    @staticmethod
    def create_tar_archive(files: Dict[str, Union[str, bytes]], 
                          archive_path: Optional[Path] = None) -> Path:
        """Create a tar.gz archive with specified files."""
        if archive_path is None:
            archive_path = Path(tempfile.mktemp(suffix=".tar.gz"))
        
        with tarfile.open(archive_path, 'w:gz') as tar:
            for filename, content in files.items():
                info = tarfile.TarInfo(name=filename)
                
                if isinstance(content, str):
                    content_bytes = content.encode('utf-8')
                else:
                    content_bytes = content
                
                info.size = len(content_bytes)
                tar.addfile(info, io.BytesIO(content_bytes))
        
        return archive_path
    
    @staticmethod
    def extract_tar_archive(archive_path: Path, extract_to: Optional[Path] = None) -> Path:
        """Extract tar archive and return extraction path."""
        if extract_to is None:
            extract_to = Path(tempfile.mkdtemp())
        
        with tarfile.open(archive_path, 'r:*') as tar:
            tar.extractall(path=extract_to)
        
        return extract_to
    
    @staticmethod
    def list_archive_contents(archive_path: Path) -> List[str]:
        """List contents of tar archive."""
        with tarfile.open(archive_path, 'r:*') as tar:
            return [member.name for member in tar.getmembers()]
    
    @staticmethod
    def get_archive_file_content(archive_path: Path, file_path: str) -> bytes:
        """Get content of specific file from archive."""
        with tarfile.open(archive_path, 'r:*') as tar:
            member = tar.getmember(file_path)
            extracted_file = tar.extractfile(member)
            if extracted_file is None:
                raise ValueError(f"File {file_path} is not a regular file")
            return extracted_file.read()


class NetworkTestHelper:
    """Helper for network-related test operations."""
    
    def __init__(self):
        """Initialize network helper."""
        self._fake_responses: Dict[str, bytes] = {}
        self._request_history: List[Dict[str, Any]] = []
    
    def add_fake_response(self, url: str, content: bytes, status_code: int = 200) -> None:
        """Add fake response for URL."""
        self._fake_responses[url] = content
    
    def get_fake_response(self, url: str) -> Optional[bytes]:
        """Get fake response for URL."""
        self._request_history.append({
            "url": url,
            "timestamp": time.time(),
            "method": "GET"
        })
        return self._fake_responses.get(url)
    
    def get_request_history(self) -> List[Dict[str, Any]]:
        """Get history of fake requests."""
        return self._request_history.copy()
    
    def clear_fake_responses(self) -> None:
        """Clear all fake responses."""
        self._fake_responses.clear()
        self._request_history.clear()


class TimeTestHelper:
    """Helper for time-related test operations."""
    
    def __init__(self):
        """Initialize time helper."""
        self._frozen_time: Optional[float] = None
    
    @contextmanager
    def freeze_time(self, timestamp: Optional[float] = None):
        """Context manager to freeze time for testing."""
        if timestamp is None:
            timestamp = time.time()
        
        original_time = time.time
        self._frozen_time = timestamp
        
        def frozen_time():
            return self._frozen_time
        
        try:
            time.time = frozen_time
            yield timestamp
        finally:
            time.time = original_time
            self._frozen_time = None
    
    def advance_time(self, seconds: float) -> None:
        """Advance frozen time by specified seconds."""
        if self._frozen_time is None:
            raise RuntimeError("Time is not frozen. Use freeze_time() context manager.")
        self._frozen_time += seconds
    
    @staticmethod
    def measure_execution_time(func: Callable, *args, **kwargs) -> tuple[Any, float]:
        """Measure execution time of a function."""
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        return result, execution_time


class ChecksumHelper:
    """Helper for checksum operations in tests."""
    
    @staticmethod
    def calculate_md5(data: Union[str, bytes, Path]) -> str:
        """Calculate MD5 checksum."""
        md5_hash = hashlib.md5()
        
        if isinstance(data, str):
            md5_hash.update(data.encode('utf-8'))
        elif isinstance(data, bytes):
            md5_hash.update(data)
        elif isinstance(data, Path):
            with open(data, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    md5_hash.update(chunk)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
        
        return md5_hash.hexdigest()
    
    @staticmethod
    def calculate_sha256(data: Union[str, bytes, Path]) -> str:
        """Calculate SHA256 checksum."""
        sha256_hash = hashlib.sha256()
        
        if isinstance(data, str):
            sha256_hash.update(data.encode('utf-8'))
        elif isinstance(data, bytes):
            sha256_hash.update(data)
        elif isinstance(data, Path):
            with open(data, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
        
        return sha256_hash.hexdigest()
    
    @staticmethod
    def verify_checksum(data: Union[str, bytes, Path], expected_checksum: str, 
                       algorithm: str = "md5") -> bool:
        """Verify checksum against expected value."""
        if algorithm.lower() == "md5":
            actual_checksum = ChecksumHelper.calculate_md5(data)
        elif algorithm.lower() == "sha256":
            actual_checksum = ChecksumHelper.calculate_sha256(data)
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        
        return actual_checksum.lower() == expected_checksum.lower()


class AssertionHelper:
    """Helper for custom assertions in tests."""
    
    @staticmethod
    def assert_files_equal(file1: Path, file2: Path, ignore_whitespace: bool = False) -> None:
        """Assert that two files have equal content."""
        content1 = file1.read_text()
        content2 = file2.read_text()
        
        if ignore_whitespace:
            content1 = ''.join(content1.split())
            content2 = ''.join(content2.split())
        
        assert content1 == content2, f"Files differ: {file1} vs {file2}"
    
    @staticmethod
    def assert_directory_empty(directory: Path) -> None:
        """Assert that directory is empty."""
        if not directory.exists():
            return
        
        contents = list(directory.iterdir())
        assert len(contents) == 0, f"Directory not empty: {directory} contains {contents}"
    
    @staticmethod
    def assert_file_contains(file_path: Path, expected_content: str) -> None:
        """Assert that file contains expected content."""
        actual_content = file_path.read_text()
        assert expected_content in actual_content, \
            f"File {file_path} does not contain expected content: {expected_content}"
    
    @staticmethod
    def assert_dict_subset(subset: Dict[str, Any], superset: Dict[str, Any]) -> None:
        """Assert that subset is contained within superset."""
        for key, value in subset.items():
            assert key in superset, f"Key '{key}' not found in superset"
            assert superset[key] == value, \
                f"Value mismatch for key '{key}': expected {value}, got {superset[key]}"
    
    @staticmethod
    def assert_execution_time_under(max_seconds: float, func: Callable, *args, **kwargs) -> Any:
        """Assert that function executes within time limit."""
        result, execution_time = TimeTestHelper.measure_execution_time(func, *args, **kwargs)
        assert execution_time < max_seconds, \
            f"Function took {execution_time:.3f}s, expected under {max_seconds:.3f}s"
        return result


class TestResourceManager:
    """Manages test resources with proper cleanup."""
    
    def __init__(self):
        """Initialize resource manager."""
        self._resources: List[Any] = []
        self._cleanup_functions: List[Callable] = []
    
    def add_resource(self, resource: Any, cleanup_func: Optional[Callable] = None) -> None:
        """Add a resource with optional cleanup function."""
        self._resources.append(resource)
        if cleanup_func:
            self._cleanup_functions.append(cleanup_func)
    
    def cleanup_all(self) -> None:
        """Clean up all resources."""
        for cleanup_func in reversed(self._cleanup_functions):
            try:
                cleanup_func()
            except Exception:
                # Log error but continue cleanup
                pass
        
        self._resources.clear()
        self._cleanup_functions.clear()
    
    def __enter__(self):
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and cleanup."""
        self.cleanup_all()


# Convenience functions combining multiple utilities
def create_test_environment() -> Dict[str, Any]:
    """Create a complete test environment with all utilities."""
    return {
        "path_manager": TemporaryPathManager(),
        "data_generator": TestDataGenerator(),
        "filesystem_helper": FileSystemTestHelper(),
        "archive_helper": ArchiveTestHelper(),
        "network_helper": NetworkTestHelper(),
        "time_helper": TimeTestHelper(),
        "checksum_helper": ChecksumHelper(),
        "assertion_helper": AssertionHelper(),
        "resource_manager": TestResourceManager()
    }


@contextmanager
def test_environment() -> Generator[Dict[str, Any], None, None]:
    """Context manager for complete test environment with cleanup."""
    env = create_test_environment()
    try:
        yield env
    finally:
        # Cleanup all resources
        if "path_manager" in env:
            env["path_manager"].cleanup_all()
        if "network_helper" in env:
            env["network_helper"].clear_fake_responses()
        if "resource_manager" in env:
            env["resource_manager"].cleanup_all()


def quick_temp_dir() -> Path:
    """Quick way to create a temporary directory."""
    return Path(tempfile.mkdtemp(prefix="tellus_test_"))


def quick_temp_file(content: str = "", suffix: str = ".txt") -> Path:
    """Quick way to create a temporary file."""
    temp_dir = quick_temp_dir()
    temp_file = temp_dir / f"test_file{suffix}"
    temp_file.write_text(content)
    return temp_file


def quick_archive(files: Dict[str, str]) -> Path:
    """Quick way to create a test archive."""
    return ArchiveTestHelper.create_tar_archive(files)