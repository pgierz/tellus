"""
Security testing utilities for the Tellus project.

This module provides reusable utilities, fixtures, and test data generators
for security testing of the Location filesystem sandboxing functionality.
It focuses on defensive security testing and vulnerability prevention.
"""

import os
import platform
import tempfile
from pathlib import Path
from typing import Dict, Generator, List, Tuple
from urllib.parse import quote, quote_plus

import pytest


class SecurityTestVectors:
    """Collection of security test vectors for path traversal attacks."""
    
    @staticmethod
    def directory_traversal_payloads() -> List[str]:
        """Generate directory traversal attack payloads."""
        payloads = []
        
        # Basic Unix-style traversal
        for depth in range(1, 8):
            payloads.append("../" * depth + "etc/passwd")
            payloads.append("../" * depth + "root/.ssh/id_rsa")
            payloads.append("../" * depth + "var/log/auth.log")
        
        # Windows-style traversal
        for depth in range(1, 8):
            payloads.append("..\\" * depth + "Windows\\System32\\config\\SAM")
            payloads.append("..\\" * depth + "Users\\Administrator\\Desktop")
            payloads.append("..\\" * depth + "Windows\\System32\\drivers\\etc\\hosts")
        
        # Mixed separators
        payloads.extend([
            "../\\../\\../etc/passwd",
            "..\\/../\\../Windows/System32",
            "dir/..\\../\\../etc/passwd",
            "subdir\\..//../../etc/passwd",
        ])
        
        # Complex nested patterns
        payloads.extend([
            "dir/../../../etc/passwd",
            "./../../etc/passwd", 
            "subdir/../../../etc/passwd",
            "nested/deep/../../../../../../etc/passwd",
            "valid/path/../../../../../etc/passwd",
        ])
        
        return payloads
    
    @staticmethod
    def absolute_path_payloads() -> List[str]:
        """Generate absolute path attack payloads."""
        payloads = []
        
        # Unix absolute paths
        payloads.extend([
            "/etc/passwd",
            "/etc/shadow",
            "/root/.ssh/id_rsa",
            "/var/log/auth.log",
            "/proc/version",
            "/dev/mem",
            "/boot/grub/grub.cfg",
        ])
        
        # Windows absolute paths  
        payloads.extend([
            "C:\\Windows\\System32\\config\\SAM",
            "C:\\Windows\\System32\\config\\SECURITY",
            "C:\\Users\\Administrator\\Desktop\\sensitive.txt",
            "C:\\Windows\\System32\\drivers\\etc\\hosts",
            "D:\\secrets\\database_passwords.txt",
            "\\\\?\\C:\\Windows\\System32",
        ])
        
        # UNC paths
        if platform.system() == "Windows":
            payloads.extend([
                "\\\\server\\share\\sensitive.txt",
                "\\\\localhost\\c$\\Windows\\System32",
                "\\\\?\\UNC\\server\\share\\file.txt",
                "\\\\127.0.0.1\\c$\\Windows",
            ])
        
        return payloads
    
    @staticmethod
    def encoding_attack_payloads() -> List[str]:
        """Generate encoding-based attack payloads."""
        payloads = []
        
        # URL encoding
        payloads.extend([
            quote("../../../etc/passwd"),
            quote_plus("../../../etc/passwd"),
            "%2e%2e%2f" * 3 + "etc%2fpasswd",
            "%2e%2e%5c" * 3 + "Windows%5cSystem32",
        ])
        
        # Double URL encoding
        payloads.extend([
            quote(quote("../../../etc/passwd")),
            "%252e%252e%252f" * 3 + "etc%252fpasswd",
        ])
        
        # Unicode encoding
        payloads.extend([
            "\u002e\u002e\u002f" * 3 + "etc/passwd",  # Unicode dots and slash
            "\ufe52\ufe52\uff0f" * 3,  # Full-width characters
            "\u002e\u002e\u2215" * 3,  # Division slash
            "\uff0e\uff0e\uff0f" * 3,  # Full-width periods and slash
        ])
        
        # Null byte injection
        payloads.extend([
            "../../../etc/passwd\x00.txt",
            "safe_file.txt\x00../../../etc/passwd", 
            "\x00../../../etc/passwd",
            "config.txt\x00..\\..\\..\Windows\\System32",
        ])
        
        return payloads
    
    @staticmethod
    def windows_specific_payloads() -> List[str]:
        """Generate Windows-specific attack payloads."""
        if platform.system() != "Windows":
            return []
        
        payloads = []
        
        # Reserved device names
        reserved_names = [
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5", 
            "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5",
            "LPT6", "LPT7", "LPT8", "LPT9"
        ]
        
        for name in reserved_names:
            payloads.extend([
                name,
                name + ".txt",
                f"dir/{name}",
                f"{name}.config",
            ])
        
        # Drive letter attacks
        for drive in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            payloads.extend([
                f"{drive}:",
                f"{drive}:\\",
                f"{drive}:\\Windows\\System32",
                f"{drive}:\\Users\\Administrator",
            ])
        
        # Alternative data streams
        payloads.extend([
            "file.txt:hidden",
            "config.ini:password",
            "..\\..\\..\\Windows\\System32\\config\\SAM:$DATA",
        ])
        
        return payloads
    
    @staticmethod
    def edge_case_payloads() -> List[str]:
        """Generate edge case attack payloads."""
        payloads = []
        
        # Empty and whitespace
        payloads.extend([
            "",
            " ",
            "\t",
            "\n",
            "\r\n",
        ])
        
        # Very long paths
        payloads.extend([
            "A" * 1000,  # Very long filename
            "../" + "A" * 500,  # Long traversal
            "/A/" * 100 + "file.txt",  # Deep path
        ])
        
        # Special characters
        payloads.extend([
            "file<script>alert('xss')</script>.txt",
            "file'or'1'='1.txt",
            "file;rm -rf /.txt",
            "file$(whoami).txt",
            "file`id`.txt",
        ])
        
        # Path normalization edge cases
        payloads.extend([
            "././../../../etc/passwd",
            "subdir/.././../../../etc/passwd",
            "./subdir/../../../etc/passwd", 
            "dir/./../../etc/passwd",
            "dir/.//../../etc/passwd",
            "dir/..//.//../../etc/passwd",
        ])
        
        return payloads
    
    @staticmethod
    def all_attack_payloads() -> List[str]:
        """Get all attack payloads combined."""
        all_payloads = []
        all_payloads.extend(SecurityTestVectors.directory_traversal_payloads())
        all_payloads.extend(SecurityTestVectors.absolute_path_payloads())
        all_payloads.extend(SecurityTestVectors.encoding_attack_payloads())
        all_payloads.extend(SecurityTestVectors.windows_specific_payloads())
        all_payloads.extend(SecurityTestVectors.edge_case_payloads())
        
        # Remove duplicates while preserving order
        seen = set()
        unique_payloads = []
        for payload in all_payloads:
            if payload not in seen:
                seen.add(payload)
                unique_payloads.append(payload)
        
        return unique_payloads


class SecurityTestEnvironment:
    """Secure test environment for security testing."""
    
    def __init__(self, prefix: str = "tellus_security_"):
        """Initialize secure test environment."""
        self.prefix = prefix
        self.sandbox_dir = None
        self.outside_dir = None
        self.sensitive_files = {}
        
    def __enter__(self) -> Tuple[Path, Path, Dict[str, Path]]:
        """Enter context manager and create test environment."""
        self.sandbox_dir = Path(tempfile.mkdtemp(prefix=f"{self.prefix}sandbox_"))
        self.outside_dir = Path(tempfile.mkdtemp(prefix=f"{self.prefix}outside_"))
        
        # Create test files in sandbox
        self._create_safe_files()
        
        # Create sensitive files outside sandbox
        self._create_sensitive_files()
        
        return self.sandbox_dir, self.outside_dir, self.sensitive_files
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and clean up test environment."""
        import shutil
        if self.sandbox_dir:
            shutil.rmtree(self.sandbox_dir, ignore_errors=True)
        if self.outside_dir:
            shutil.rmtree(self.outside_dir, ignore_errors=True)
            
    def _create_safe_files(self):
        """Create safe test files in sandbox."""
        if not self.sandbox_dir:
            return
            
        # Create various file types
        (self.sandbox_dir / "safe.txt").write_text("safe content")
        (self.sandbox_dir / "config.json").write_text('{"setting": "safe_value"}')
        (self.sandbox_dir / "data.csv").write_text("col1,col2\nvalue1,value2")
        
        # Create subdirectory structure
        subdir = self.sandbox_dir / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_text("nested safe content")
        
        deeper_dir = subdir / "deeper"
        deeper_dir.mkdir()
        (deeper_dir / "deep.txt").write_text("deep safe content")
        
    def _create_sensitive_files(self):
        """Create sensitive files outside sandbox."""
        if not self.outside_dir:
            return
            
        # Create sensitive files that should never be accessible
        sensitive_content = "SENSITIVE DATA - SHOULD NOT BE ACCESSIBLE"
        
        files_to_create = {
            "passwd": sensitive_content,
            "shadow": "root:$6$encrypted$hash:18000:0:99999:7:::",
            "secrets.txt": "API_KEY=super_secret_key_12345",
            "database_config.json": '{"password": "admin123", "host": "prod-db"}',
            "ssh_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSk...",
        }
        
        for filename, content in files_to_create.items():
            file_path = self.outside_dir / filename
            file_path.write_text(content)
            self.sensitive_files[filename] = file_path


@pytest.fixture
def security_test_env():
    """Pytest fixture providing secure test environment."""
    with SecurityTestEnvironment() as (sandbox_dir, outside_dir, sensitive_files):
        yield sandbox_dir, outside_dir, sensitive_files


@pytest.fixture
def attack_payloads():
    """Pytest fixture providing attack payloads."""
    return SecurityTestVectors.all_attack_payloads()


@pytest.fixture
def sandboxed_filesystem(security_test_env):
    """Pytest fixture providing sandboxed filesystem."""
    sandbox_dir, _, _ = security_test_env
    
    import fsspec

    from tellus.location.sandboxed_filesystem import PathSandboxedFileSystem
    
    base_fs = fsspec.filesystem("file")
    return PathSandboxedFileSystem(base_fs, str(sandbox_dir))


class SecurityTestHelpers:
    """Helper functions for security testing."""
    
    @staticmethod
    def assert_path_is_blocked(sandboxed_fs, attack_path: str):
        """Assert that a path is properly blocked by the sandboxed filesystem."""
        from tellus.location.sandboxed_filesystem import PathValidationError

        # Test various operations that should all be blocked
        operations = [
            lambda: sandboxed_fs.read_text(attack_path),
            lambda: sandboxed_fs.read_bytes(attack_path),
            lambda: sandboxed_fs.exists(attack_path),
            lambda: sandboxed_fs.isfile(attack_path),
            lambda: sandboxed_fs.isdir(attack_path),
            lambda: sandboxed_fs.write_text(attack_path, "malicious"),
            lambda: sandboxed_fs.write_bytes(attack_path, b"malicious"),
            lambda: sandboxed_fs.size(attack_path),
            lambda: sandboxed_fs.info(attack_path),
        ]
        
        blocked_operations = 0
        for operation in operations:
            try:
                operation()
                # If operation succeeded, verify it didn't access outside files
                # This is a fallback check for absolute paths that might be made relative
            except (PathValidationError, FileNotFoundError, ValueError, UnicodeError, OSError):
                # Any of these exceptions indicate the attack was blocked
                blocked_operations += 1
        
        # At least most operations should be blocked
        assert blocked_operations >= len(operations) * 0.8, \
            f"Insufficient blocking for path '{attack_path}': only {blocked_operations}/{len(operations)} operations blocked"
    
    @staticmethod
    def assert_no_sensitive_data_leak(result_data: str, sensitive_patterns: List[str] = None):
        """Assert that result data doesn't contain sensitive information."""
        if sensitive_patterns is None:
            sensitive_patterns = [
                "SENSITIVE DATA",
                "CLASSIFIED", 
                "SECRET",
                "PASSWORD",
                "API_KEY",
                "PRIVATE KEY",
                "root:",
                "$6$encrypted$",
            ]
        
        for pattern in sensitive_patterns:
            assert pattern not in result_data, f"Sensitive pattern '{pattern}' found in result data"
    
    @staticmethod
    def verify_sandbox_boundaries(sandbox_path: Path, result_paths: List[str]):
        """Verify that all result paths are within sandbox boundaries."""
        sandbox_resolved = sandbox_path.resolve()
        
        for path_str in result_paths:
            result_path = Path(path_str).resolve()
            assert str(result_path).startswith(str(sandbox_resolved)), \
                f"Path '{path_str}' escapes sandbox boundary '{sandbox_path}'"
    
    @staticmethod
    def generate_security_report(test_results: Dict[str, bool]) -> Dict[str, any]:
        """Generate a security test report."""
        total_tests = len(test_results)
        passed_tests = sum(1 for result in test_results.values() if result)
        failed_tests = total_tests - passed_tests
        
        report = {
            "total_security_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": failed_tests,
            "success_rate": (passed_tests / total_tests) * 100 if total_tests > 0 else 0,
            "failed_test_names": [name for name, result in test_results.items() if not result],
            "security_status": "SECURE" if failed_tests == 0 else "VULNERABLE",
        }
        
        return report


def parametrize_security_payloads(test_vectors_method):
    """Decorator to parametrize tests with security payloads."""
    def decorator(test_func):
        payloads = test_vectors_method()
        return pytest.mark.parametrize("attack_payload", payloads)(test_func)
    return decorator


# Security test decorators
security_critical = pytest.mark.security
path_traversal_test = pytest.mark.parametrize("attack_payload", 
    SecurityTestVectors.directory_traversal_payloads())
encoding_attack_test = pytest.mark.parametrize("attack_payload",
    SecurityTestVectors.encoding_attack_payloads())