"""
Property-based security testing for Location filesystem sandboxing.

This module uses Hypothesis to generate comprehensive security test cases
and verify that the PathSandboxedFileSystem maintains security properties
across a wide range of inputs and scenarios.
"""

import os
import platform
import shutil
import tempfile
from pathlib import Path
from typing import List

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, initialize, rule

from tellus.location import (Location, LocationKind, PathSandboxedFileSystem,
                             PathValidationError)
from tests.security_utils import SecurityTestEnvironment, SecurityTestHelpers

pytestmark = [
    pytest.mark.unit,
    pytest.mark.security,
    pytest.mark.property,  # Property-based testing marker
]


class TestPathSandboxingProperties:
    """Property-based tests for path sandboxing security properties."""
    
    def setup_method(self):
        """Set up test environment."""
        self.test_env = SecurityTestEnvironment()
        self.sandbox_dir, self.outside_dir, self.sensitive_files = self.test_env.__enter__()
        
        import fsspec
        base_fs = fsspec.filesystem("file")
        self.sandboxed_fs = PathSandboxedFileSystem(base_fs, str(self.sandbox_dir))
    
    def teardown_method(self):
        """Clean up test environment."""
        self.test_env.__exit__(None, None, None)
    
    @given(st.text(min_size=1, max_size=500))
    @settings(max_examples=100)
    def test_arbitrary_paths_cannot_escape_sandbox(self, path_input):
        """Property: No arbitrary path input should escape the sandbox."""
        # Filter out paths that are clearly invalid or would cause issues
        assume(not any(char in path_input for char in ['\x00', '\r', '\n']))
        assume(len(path_input.strip()) > 0)
        
        # Test that arbitrary path input doesn't allow sandbox escape
        try:
            # Try to access the path
            exists = self.sandboxed_fs.exists(path_input)
            
            # If operation succeeded, verify it's within sandbox boundaries
            if exists:
                # If file exists, it must be within sandbox
                resolved_path = self.sandboxed_fs._resolve_path(path_input)
                sandbox_resolved = str(self.sandbox_dir.resolve())
                assert resolved_path.startswith(sandbox_resolved), \
                    f"Path '{path_input}' resolved to '{resolved_path}' outside sandbox '{sandbox_resolved}'"
                    
        except (PathValidationError, ValueError, OSError, UnicodeError):
            # These exceptions indicate proper security blocking
            pass
    
    @given(
        st.lists(
            st.text(alphabet=st.characters(whitelist_categories=('Ll', 'Lu', 'Nd', 'Pc')), 
                   min_size=1, max_size=50),
            min_size=1, max_size=10
        )
    )
    @settings(max_examples=50) 
    def test_path_components_cannot_escape(self, path_components):
        """Property: Path components joined with traversal patterns cannot escape."""
        # Create various path traversal patterns
        traversal_patterns = ["../", "..\\", "../\\", "..\\//"]
        
        for pattern in traversal_patterns:
            # Join components with traversal pattern
            malicious_path = pattern.join(path_components)
            
            try:
                self.sandboxed_fs.read_text(malicious_path)
                # If successful, verify it's accessing sandbox content only
                resolved = self.sandboxed_fs._resolve_path(malicious_path)
                assert str(self.sandbox_dir.resolve()) in resolved
            except (PathValidationError, FileNotFoundError, ValueError, UnicodeError, OSError):
                # Expected - path should be blocked
                pass
    
    @given(
        st.integers(min_value=1, max_value=20),
        st.sampled_from(["../", "..\\"]) 
    )
    @settings(max_examples=30)
    def test_depth_traversal_always_blocked(self, depth, traversal_pattern):
        """Property: Directory traversal of any depth should be blocked."""
        # Create deep traversal path
        traversal_path = traversal_pattern * depth + "sensitive_file.txt"
        
        with pytest.raises(PathValidationError):
            self.sandboxed_fs.read_text(traversal_path)
        
        with pytest.raises(PathValidationError):
            self.sandboxed_fs.write_text(traversal_path, "malicious content")
    
    @given(
        st.text(alphabet=st.characters(min_codepoint=1, max_codepoint=0x10FFFF),
               min_size=1, max_size=100)
    )
    @settings(max_examples=50, deadline=5000)  # Increased deadline for Unicode processing
    def test_unicode_paths_maintain_security(self, unicode_path):
        """Property: Unicode paths should maintain security boundaries."""
        # Skip paths with null bytes or control characters that would be invalid
        assume('\x00' not in unicode_path)
        assume(not any(ord(c) < 32 for c in unicode_path if ord(c) not in [9, 10, 13]))  # Allow tab, LF, CR
        
        try:
            # Unicode paths should either be blocked or properly sandboxed
            result = self.sandboxed_fs.exists(unicode_path)
            
            # If operation succeeded, verify security
            if result or not result:  # Whether exists or not, should be safe
                resolved = self.sandboxed_fs._resolve_path(unicode_path)
                sandbox_path = str(self.sandbox_dir.resolve())
                assert resolved.startswith(sandbox_path), \
                    f"Unicode path '{repr(unicode_path)}' escaped sandbox"
                    
        except (PathValidationError, UnicodeError, ValueError, OSError):
            # Expected for malicious or invalid Unicode sequences
            pass
    
    @given(
        st.lists(
            st.sampled_from([
                "file.txt", "dir/file.txt", "subdir/nested.txt",
                "../evil.txt", "../../etc/passwd", "../../../root/.ssh/id_rsa"
            ]),
            min_size=1, max_size=5
        )
    )
    @settings(max_examples=30)
    def test_batch_operations_maintain_security(self, file_paths):
        """Property: Batch operations should maintain security for all paths."""
        # Test that batch operations don't allow any paths to escape
        for path in file_paths:
            try:
                # Try various operations
                self.sandboxed_fs.exists(path)
                
                # If operation succeeded for a safe path, verify it's actually safe
                if not any(unsafe in path for unsafe in ["../", "..\\"]):
                    # Safe paths should work
                    continue
                else:
                    # Unsafe paths should have been blocked
                    pytest.fail(f"Unsafe path '{path}' was not blocked")
                    
            except PathValidationError:
                # Expected for unsafe paths
                if any(unsafe in path for unsafe in ["../", "..\\"]):
                    continue  # Good - unsafe path was blocked
                else:
                    # Safe path was blocked - might be too restrictive but acceptable
                    continue
            except (FileNotFoundError, ValueError, OSError):
                # Acceptable - file doesn't exist or other filesystem error
                continue


class TestLocationSecurityStateMachine(RuleBasedStateMachine):
    """State machine for testing Location security properties over time."""
    
    def __init__(self):
        super().__init__()
        self.test_env = None
        self.location = None
        self.created_files = []
        
    @initialize()
    def setup_location(self):
        """Initialize location for testing."""
        self.test_env = SecurityTestEnvironment()
        sandbox_dir, outside_dir, sensitive_files = self.test_env.__enter__()
        
        # Clear any existing locations
        Location._locations = {}
        
        # Create location
        self.location = Location(
            name=f"security_state_test_{id(self)}",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(sandbox_dir)
            }
        )
        
        self.sandbox_dir = sandbox_dir
        self.outside_dir = outside_dir
        self.sensitive_files = sensitive_files
    
    files = Bundle('files')
    
    @rule(target=files, filename=st.text(
        alphabet=st.characters(whitelist_categories=('Ll', 'Lu', 'Nd', 'Pc')),
        min_size=1, max_size=20
    ))
    def create_safe_file(self, filename):
        """Create a safe file within the sandbox."""
        # Filter invalid filenames
        if any(char in filename for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']):
            assume(False)
        
        safe_filename = filename + ".txt"
        try:
            self.location.fs.write_text(safe_filename, f"Safe content for {filename}")
            self.created_files.append(safe_filename)
            return safe_filename
        except (PathValidationError, ValueError, OSError):
            assume(False)
    
    @rule(filename=files)
    def read_file_securely(self, filename):
        """Test that files can be read securely."""
        try:
            content = self.location.fs.read_text(filename)
            # Verify no sensitive data leaked
            SecurityTestHelpers.assert_no_sensitive_data_leak(content)
        except (FileNotFoundError, PathValidationError):
            pass  # Acceptable
    
    @rule(attack_path=st.sampled_from([
        "../sensitive.txt", "../../etc/passwd", "../../../root/.ssh/id_rsa",
        "..\\..\\Windows\\System32", "/etc/passwd", "C:\\Windows\\System32"
    ]))
    def attempt_path_traversal(self, attack_path):
        """Attempt path traversal attacks."""
        with pytest.raises(PathValidationError):
            self.location.fs.read_text(attack_path)
        
        with pytest.raises(PathValidationError):
            self.location.fs.write_text(attack_path, "malicious content")
    
    @rule(filename=files)
    def verify_file_within_sandbox(self, filename):
        """Verify that files are within sandbox boundaries."""
        if self.location.fs.exists(filename):
            # File exists - verify it's in the right place
            resolved = self.location.fs._resolve_path(filename)
            sandbox_resolved = str(self.sandbox_dir.resolve())
            assert resolved.startswith(sandbox_resolved), \
                f"File '{filename}' is outside sandbox"
    
    def teardown(self):
        """Clean up after state machine testing."""
        if self.test_env:
            self.test_env.__exit__(None, None, None)


# Hypothesis strategies for security testing
@st.composite
def malicious_paths(draw):
    """Generate potentially malicious paths for testing."""
    # Generate various path traversal patterns
    traversal_depth = draw(st.integers(min_value=1, max_value=10))
    traversal_pattern = draw(st.sampled_from(["../", "..\\"]))
    target_file = draw(st.sampled_from([
        "etc/passwd", "root/.ssh/id_rsa", "Windows/System32/config/SAM",
        "sensitive.txt", "config/database.conf"
    ]))
    
    return traversal_pattern * traversal_depth + target_file


@st.composite  
def encoded_attack_paths(draw):
    """Generate encoded attack paths."""
    base_path = draw(malicious_paths())
    encoding_type = draw(st.sampled_from(["url", "double_url", "unicode"]))
    
    if encoding_type == "url":
        from urllib.parse import quote
        return quote(base_path)
    elif encoding_type == "double_url":
        from urllib.parse import quote
        return quote(quote(base_path))
    elif encoding_type == "unicode":
        # Convert to Unicode escapes
        return ''.join(f'\\u{ord(c):04x}' if ord(c) > 127 else c for c in base_path)
    
    return base_path


class TestPropertyBasedSecurityScenarios:
    """Additional property-based security scenario tests."""
    
    @given(malicious_paths())
    @settings(max_examples=50)
    def test_generated_malicious_paths_blocked(self, malicious_path):
        """Test that generated malicious paths are blocked."""
        with SecurityTestEnvironment() as (sandbox_dir, outside_dir, sensitive_files):
            import fsspec
            base_fs = fsspec.filesystem("file")
            sandboxed_fs = PathSandboxedFileSystem(base_fs, str(sandbox_dir))
            
            with pytest.raises((PathValidationError, FileNotFoundError)):
                sandboxed_fs.read_text(malicious_path)
    
    @given(encoded_attack_paths())
    @settings(max_examples=30)
    def test_encoded_attacks_blocked(self, encoded_path):
        """Test that encoded attack paths are blocked."""
        with SecurityTestEnvironment() as (sandbox_dir, outside_dir, sensitive_files):
            import fsspec
            base_fs = fsspec.filesystem("file") 
            sandboxed_fs = PathSandboxedFileSystem(base_fs, str(sandbox_dir))
            
            try:
                sandboxed_fs.read_text(encoded_path)
                # If it succeeds, verify it's accessing safe content
                resolved = sandboxed_fs._resolve_path(encoded_path)
                assert str(sandbox_dir.resolve()) in resolved
            except (PathValidationError, UnicodeError, ValueError):
                # Expected for malicious encoded paths
                pass
    
    @given(
        st.lists(
            st.text(alphabet="abcdefghijklmnopqrstuvwxyz./\\", min_size=1, max_size=50),
            min_size=1, max_size=10
        )
    )
    @settings(max_examples=30)
    def test_path_combination_attacks(self, path_parts):
        """Test combinations of path parts for security vulnerabilities."""
        with SecurityTestEnvironment() as (sandbox_dir, outside_dir, sensitive_files):
            import fsspec
            base_fs = fsspec.filesystem("file")
            sandboxed_fs = PathSandboxedFileSystem(base_fs, str(sandbox_dir))
            
            # Combine path parts in potentially malicious ways
            combined_path = "/".join(path_parts)
            
            try:
                result = sandboxed_fs.exists(combined_path)
                # If successful, verify security boundaries
                if combined_path.count("../") > 0:
                    # Paths with traversal should generally be blocked
                    resolved = sandboxed_fs._resolve_path(combined_path)
                    assert str(sandbox_dir.resolve()) in resolved
            except (PathValidationError, ValueError, OSError):
                # Expected for invalid or malicious paths
                pass


# Run the state machine test
TestSecurityStateMachine = TestLocationSecurityStateMachine.TestCase