# Security Testing for Tellus Location Filesystem

This directory contains comprehensive security tests for the Tellus Location filesystem sandboxing functionality. These tests ensure that the `PathSandboxedFileSystem` effectively prevents path traversal attacks and maintains secure boundary enforcement.

## Quick Start

```bash
# Run all security tests
pixi run -e test pytest -m security

# Run specific security test files
pixi run -e test pytest tests/test_security_path_sandboxing.py
pixi run -e test pytest tests/test_security_property_based.py
pixi run -e test pytest tests/test_security_integration.py

# Run with verbose output and short traceback
pixi run -e test pytest -m security -v --tb=short
```

## Test Files Overview

### `test_security_path_sandboxing.py`
**Core security tests** covering fundamental attack prevention:

- **TestPathTraversalAttackPrevention**: Directory traversal attack prevention
- **TestLocationSecurityIntegration**: Location class security integration  
- **TestStorageBackendSecurity**: Security across different storage backends
- **TestSecurityRegressionPrevention**: Regression testing for security fixes
- **TestSecurityValidationFramework**: Security validation utilities
- **TestSecurityPerformanceImpact**: Performance impact assessment

### `test_security_property_based.py` 
**Property-based security tests** using Hypothesis:

- **TestPathSandboxingProperties**: Property-based path validation testing
- **TestLocationSecurityStateMachine**: State machine security testing
- **TestPropertyBasedSecurityScenarios**: Advanced property-based scenarios

### `test_security_integration.py`
**Integration security tests** for real-world scenarios:

- **TestSecurityFrameworkIntegration**: End-to-end security validation
- **TestRealWorldSecurityScenarios**: Climate/HPC workflow security testing
- **TestSecurityRegressionSuite**: Comprehensive regression testing

### `security_utils.py`
**Security testing utilities** and shared components:

- **SecurityTestVectors**: Attack payload generators
- **SecurityTestEnvironment**: Secure test environment management
- **SecurityTestHelpers**: Validation and assertion utilities

## Security Attack Vectors Tested

### Directory Traversal Attacks
```python
# Examples of attack patterns tested
attack_paths = [
    "../../../etc/passwd",           # Basic Unix traversal
    "..\\..\\Windows\\System32",     # Windows traversal  
    "../\\../\\etc/passwd",          # Mixed separators
    "dir/../../../etc/passwd",       # Nested traversal
    "%2e%2e%2f%2e%2e%2f",           # URL encoded
    "\u002e\u002e\u002f",           # Unicode encoded
]
```

### Absolute Path Injection
```python
# Absolute paths that should be sandboxed
absolute_paths = [
    "/etc/passwd",                   # Unix system files
    "C:\\Windows\\System32",         # Windows system files  
    "/root/.ssh/id_rsa",            # SSH keys
    "\\\\server\\share\\file.txt",  # UNC paths
]
```

### Encoding-Based Attacks
```python  
# Various encoding attacks
encoded_attacks = [
    quote("../../../etc/passwd"),    # URL encoding
    "../../../etc/passwd\x00.txt",  # Null byte injection
    "file<script>alert()</script>",  # XSS-style injection
]
```

## Security Test Markers

The security tests use pytest markers for organization:

```python
@pytest.mark.security         # All security-related tests
@pytest.mark.unit            # Unit-level security tests  
@pytest.mark.integration     # Integration security tests
@pytest.mark.property        # Property-based security tests
```

Run specific categories:
```bash
pixi run -e test pytest -m "security and unit"
pixi run -e test pytest -m "security and integration" 
pixi run -e test pytest -m "security and property"
```

## Expected Security Behavior

### Path Traversal Prevention
- All directory traversal attempts should raise `PathValidationError`
- No operations should succeed outside the configured sandbox
- Resolved paths must always be within the base directory

### Absolute Path Handling  
- Absolute paths are converted to relative paths within sandbox
- No access to actual system directories outside sandbox
- Files created within sandbox, never at original absolute paths

### Error Handling Security
- Error messages don't leak sensitive information
- Failed operations don't bypass security validation
- Unexpected errors maintain security boundaries

## Security Test Environment

### Test Isolation
Each test creates isolated environments:
- **Sandbox Directory**: Where safe operations should occur
- **Outside Directory**: Contains sensitive files that should be inaccessible  
- **Automatic Cleanup**: Test environments are automatically cleaned up

### Sensitive Test Data
Tests create sensitive files that should never be accessible:
```python
sensitive_files = {
    "passwd": "SENSITIVE DATA - SHOULD NOT BE ACCESSIBLE",
    "ssh_key": "-----BEGIN PRIVATE KEY-----...",
    "secrets.txt": "API_KEY=super_secret_key_12345",
}
```

## Property-Based Testing

Security tests use Hypothesis for property-based testing:

```python
@given(st.text(min_size=1, max_size=500))
def test_arbitrary_paths_cannot_escape_sandbox(self, path_input):
    """Property: No arbitrary path input should escape sandbox"""
    # Test with generated string inputs
    assert_secure_behavior(path_input)
```

### Security Properties Verified
1. **Boundary Invariant**: All resolved paths within sandbox
2. **Attack Resistance**: No input allows directory escape  
3. **Unicode Safety**: Unicode inputs don't bypass security
4. **Concurrency Safety**: Security maintained under load

## Common Test Patterns

### Testing Path Blocking
```python
def test_attack_path_blocked(self):
    attack_paths = ["../../../etc/passwd", "../../sensitive.txt"]
    
    for attack_path in attack_paths:
        with pytest.raises(PathValidationError):
            sandboxed_fs.read_text(attack_path)
            
        with pytest.raises(PathValidationError):
            sandboxed_fs.write_text(attack_path, "malicious")
```

### Testing Boundary Validation
```python
def test_boundary_enforcement(self):
    # Verify all results are within sandbox
    results = location.find_files("*", recursive=True)
    for file_path, _ in results:
        SecurityTestHelpers.verify_sandbox_boundaries(
            sandbox_dir, [file_path]
        )
```

### Testing Integration Security
```python
def test_location_security_integration(self):
    location = Location(name="test", config={"path": sandbox_dir})
    
    # All Location operations should maintain security
    with pytest.raises(PathValidationError):
        location.get("../../../etc/passwd", "downloaded.txt")
```

## Debugging Security Test Failures

### Common Issues

1. **Path Resolution Differences**: OS-specific path handling
   ```bash
   # Check path resolution on your platform
   python -c "from pathlib import Path; print(Path('/tmp').resolve())"
   ```

2. **Platform-Specific Behavior**: Windows vs Unix differences
   ```python
   # Use platform-specific test logic
   if platform.system() == "Windows":
       # Windows-specific test logic
   ```

3. **File System Permissions**: Permission-related test failures
   ```bash
   # Run tests with appropriate permissions
   pixi run -e test pytest tests/test_security_*.py --tb=long
   ```

### Verbose Debugging
```bash
# Run with maximum verbosity for debugging
pixi run -e test pytest tests/test_security_path_sandboxing.py::TestPathTraversalAttackPrevention -vv --tb=long --capture=no
```

## Contributing Security Tests

### Adding New Attack Vectors
1. Add attack patterns to `SecurityTestVectors` in `security_utils.py`
2. Create corresponding test methods in appropriate test files
3. Ensure cross-platform compatibility
4. Add performance impact assessment

### Test Development Guidelines
1. **Isolation**: Each test should be independent
2. **Cleanup**: Always clean up test environments
3. **Cross-Platform**: Consider Windows/Unix differences  
4. **Performance**: Security tests shouldn't significantly impact performance
5. **Documentation**: Document new attack vectors and test scenarios

### Security Test Review Checklist
- [ ] Test covers realistic attack scenario
- [ ] Test is isolated and doesn't affect other tests
- [ ] Test works across different platforms
- [ ] Test cleanup is implemented properly
- [ ] Attack vector is documented
- [ ] Performance impact is reasonable

## Performance Considerations

Security tests are designed to be fast and efficient:
- **Path Validation**: < 0.01s per operation
- **Test Execution**: Full security suite runs in < 10 seconds
- **Memory Usage**: Minimal memory overhead
- **Parallel Execution**: Tests can run in parallel safely

Monitor for performance regressions:
```bash
# Run with timing information
pixi run -e test pytest -m security --durations=10
```

## Security Test Maintenance

### Regular Maintenance Tasks
1. **Update Attack Vectors**: Add new attack patterns as they're discovered
2. **Platform Testing**: Test on new OS versions and platforms
3. **Dependency Updates**: Verify security with dependency updates  
4. **Performance Monitoring**: Monitor for performance regressions

### Security Test Evolution
- Keep attack vectors current with security research
- Add tests for new filesystem operations
- Improve cross-platform compatibility
- Enhance property-based test coverage

## Getting Help

### Resources
- **Documentation**: `/docs/security/SECURITY_TESTING_STRATEGY.md`
- **Code Comments**: Extensive inline documentation
- **Test Examples**: Comprehensive examples in test files

### Common Questions

**Q: Why are some tests skipped on my platform?**  
A: Some tests are platform-specific (Windows vs Unix). This is expected behavior.

**Q: How do I add a new attack vector?**  
A: Add it to `SecurityTestVectors.all_attack_payloads()` and create corresponding tests.

**Q: Tests are failing with path resolution errors?**  
A: Check for OS-specific path handling differences and use normalized path comparisons.

**Q: How do I test a new filesystem operation?**  
A: Add security tests to verify the operation maintains sandbox boundaries.

This security testing framework provides comprehensive coverage of filesystem security concerns while maintaining good performance and cross-platform compatibility.