"""
Test configuration management following Single Responsibility Principle.

This module provides configuration management for different test environments
with clear separation of concerns and dependency injection support.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Type, Union

from .interfaces import ConfigurationInterface


class TestEnvironmentType(Enum):
    """Types of test environments."""
    UNIT = "unit"
    INTEGRATION = "integration"
    END_TO_END = "e2e"
    PERFORMANCE = "performance"


@dataclass
class DatabaseConfig:
    """Database configuration for testing."""
    host: str = "localhost"
    port: int = 5432
    database: str = "tellus_test"
    username: str = "test_user"
    password: str = "test_password"
    pool_size: int = 5
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "password": self.password,
            "pool_size": self.pool_size
        }


@dataclass
class CacheConfig:
    """Cache configuration for testing."""
    type: str = "memory"  # memory, file, redis
    max_size: Optional[int] = 1024 * 1024  # 1MB default
    ttl: Optional[int] = 3600  # 1 hour default
    directory: Optional[str] = None
    redis_url: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "type": self.type,
            "max_size": self.max_size,
            "ttl": self.ttl,
            "directory": self.directory,
            "redis_url": self.redis_url
        }


@dataclass
class NetworkConfig:
    """Network configuration for testing."""
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    use_mock: bool = True
    mock_delay: float = 0.1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "use_mock": self.use_mock,
            "mock_delay": self.mock_delay
        }


@dataclass
class FilesystemConfig:
    """Filesystem configuration for testing."""
    type: str = "memory"  # memory, temp, real
    temp_dir: Optional[str] = None
    auto_cleanup: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "type": self.type,
            "temp_dir": self.temp_dir,
            "auto_cleanup": self.auto_cleanup
        }


@dataclass
class LoggingConfig:
    """Logging configuration for testing."""
    level: str = "WARNING"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    capture_output: bool = True
    log_to_file: bool = False
    log_file: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "level": self.level,
            "format": self.format,
            "capture_output": self.capture_output,
            "log_to_file": self.log_to_file,
            "log_file": self.log_file
        }


@dataclass
class TestDataConfig:
    """Test data configuration."""
    data_dir: str = "tests/data"
    sample_files_dir: str = "tests/data/samples"
    fixtures_dir: str = "tests/fixtures"
    auto_generate: bool = True
    cleanup_generated: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "data_dir": self.data_dir,
            "sample_files_dir": self.sample_files_dir,
            "fixtures_dir": self.fixtures_dir,
            "auto_generate": self.auto_generate,
            "cleanup_generated": self.cleanup_generated
        }


@dataclass
class TestConfiguration:
    """Main test configuration container."""
    environment: TestEnvironmentType = TestEnvironmentType.UNIT
    parallel_execution: bool = False
    fail_fast: bool = False
    verbose: bool = False
    
    # Component configurations
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    network: NetworkConfig = field(default_factory=NetworkConfig)
    filesystem: FilesystemConfig = field(default_factory=FilesystemConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    test_data: TestDataConfig = field(default_factory=TestDataConfig)
    
    # Custom settings
    custom_settings: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "environment": self.environment.value,
            "parallel_execution": self.parallel_execution,
            "fail_fast": self.fail_fast,
            "verbose": self.verbose,
            "database": self.database.to_dict(),
            "cache": self.cache.to_dict(),
            "network": self.network.to_dict(),
            "filesystem": self.filesystem.to_dict(),
            "logging": self.logging.to_dict(),
            "test_data": self.test_data.to_dict(),
            "custom_settings": self.custom_settings
        }


class ConfigurationManager:
    """Manager for test configuration with environment-specific overrides."""
    
    def __init__(self):
        """Initialize configuration manager."""
        self._config = TestConfiguration()
        self._environment_overrides: Dict[TestEnvironmentType, Dict[str, Any]] = {}
        self._loaded_from_file = False
    
    def get_config(self) -> TestConfiguration:
        """Get current configuration."""
        return self._config
    
    def set_environment(self, env: TestEnvironmentType) -> None:
        """Set test environment and apply overrides."""
        self._config.environment = env
        self._apply_environment_overrides(env)
    
    def load_from_file(self, config_file: Path) -> None:
        """Load configuration from file."""
        if not config_file.exists():
            return
        
        try:
            import json
            with open(config_file) as f:
                data = json.load(f)
            
            self._load_from_dict(data)
            self._loaded_from_file = True
            
        except Exception as e:
            raise ConfigurationError(f"Failed to load config from {config_file}: {e}")
    
    def save_to_file(self, config_file: Path) -> None:
        """Save configuration to file."""
        try:
            import json
            config_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(config_file, 'w') as f:
                json.dump(self._config.to_dict(), f, indent=2)
                
        except Exception as e:
            raise ConfigurationError(f"Failed to save config to {config_file}: {e}")
    
    def load_from_env(self, prefix: str = "TELLUS_TEST_") -> None:
        """Load configuration from environment variables."""
        # Database configuration
        if f"{prefix}DB_HOST" in os.environ:
            self._config.database.host = os.environ[f"{prefix}DB_HOST"]
        if f"{prefix}DB_PORT" in os.environ:
            self._config.database.port = int(os.environ[f"{prefix}DB_PORT"])
        if f"{prefix}DB_NAME" in os.environ:
            self._config.database.database = os.environ[f"{prefix}DB_NAME"]
        
        # Cache configuration
        if f"{prefix}CACHE_TYPE" in os.environ:
            self._config.cache.type = os.environ[f"{prefix}CACHE_TYPE"]
        if f"{prefix}CACHE_MAX_SIZE" in os.environ:
            self._config.cache.max_size = int(os.environ[f"{prefix}CACHE_MAX_SIZE"])
        
        # Network configuration
        if f"{prefix}NETWORK_TIMEOUT" in os.environ:
            self._config.network.timeout = int(os.environ[f"{prefix}NETWORK_TIMEOUT"])
        if f"{prefix}NETWORK_USE_MOCK" in os.environ:
            self._config.network.use_mock = os.environ[f"{prefix}NETWORK_USE_MOCK"].lower() == "true"
        
        # Filesystem configuration
        if f"{prefix}FS_TYPE" in os.environ:
            self._config.filesystem.type = os.environ[f"{prefix}FS_TYPE"]
        if f"{prefix}FS_TEMP_DIR" in os.environ:
            self._config.filesystem.temp_dir = os.environ[f"{prefix}FS_TEMP_DIR"]
        
        # General configuration
        if f"{prefix}ENVIRONMENT" in os.environ:
            env_str = os.environ[f"{prefix}ENVIRONMENT"].lower()
            for env_type in TestEnvironmentType:
                if env_type.value == env_str:
                    self._config.environment = env_type
                    break
        
        if f"{prefix}PARALLEL" in os.environ:
            self._config.parallel_execution = os.environ[f"{prefix}PARALLEL"].lower() == "true"
        
        if f"{prefix}VERBOSE" in os.environ:
            self._config.verbose = os.environ[f"{prefix}VERBOSE"].lower() == "true"
    
    def add_environment_override(self, env: TestEnvironmentType, overrides: Dict[str, Any]) -> None:
        """Add environment-specific configuration overrides."""
        if env not in self._environment_overrides:
            self._environment_overrides[env] = {}
        
        self._environment_overrides[env].update(overrides)
        
        # Apply immediately if this is the current environment
        if self._config.environment == env:
            self._apply_environment_overrides(env)
    
    def get_custom_setting(self, key: str, default: Any = None) -> Any:
        """Get custom setting value."""
        return self._config.custom_settings.get(key, default)
    
    def set_custom_setting(self, key: str, value: Any) -> None:
        """Set custom setting value."""
        self._config.custom_settings[key] = value
    
    def _load_from_dict(self, data: Dict[str, Any]) -> None:
        """Load configuration from dictionary."""
        # Load environment
        if "environment" in data:
            env_str = data["environment"]
            for env_type in TestEnvironmentType:
                if env_type.value == env_str:
                    self._config.environment = env_type
                    break
        
        # Load general settings
        self._config.parallel_execution = data.get("parallel_execution", self._config.parallel_execution)
        self._config.fail_fast = data.get("fail_fast", self._config.fail_fast)
        self._config.verbose = data.get("verbose", self._config.verbose)
        
        # Load component configurations
        if "database" in data:
            db_data = data["database"]
            self._config.database = DatabaseConfig(**db_data)
        
        if "cache" in data:
            cache_data = data["cache"]
            self._config.cache = CacheConfig(**cache_data)
        
        if "network" in data:
            network_data = data["network"]
            self._config.network = NetworkConfig(**network_data)
        
        if "filesystem" in data:
            fs_data = data["filesystem"]
            self._config.filesystem = FilesystemConfig(**fs_data)
        
        if "logging" in data:
            log_data = data["logging"]
            self._config.logging = LoggingConfig(**log_data)
        
        if "test_data" in data:
            test_data_data = data["test_data"]
            self._config.test_data = TestDataConfig(**test_data_data)
        
        # Load custom settings
        self._config.custom_settings = data.get("custom_settings", {})
    
    def _apply_environment_overrides(self, env: TestEnvironmentType) -> None:
        """Apply environment-specific overrides."""
        if env not in self._environment_overrides:
            return
        
        overrides = self._environment_overrides[env]
        
        # Apply overrides using dot notation (e.g., "cache.type", "network.timeout")
        for key, value in overrides.items():
            self._apply_override(key, value)
    
    def _apply_override(self, key: str, value: Any) -> None:
        """Apply a single configuration override."""
        parts = key.split(".")
        
        if len(parts) == 1:
            # Top-level setting
            if hasattr(self._config, key):
                setattr(self._config, key, value)
            else:
                self._config.custom_settings[key] = value
        
        elif len(parts) == 2:
            # Component setting (e.g., "cache.type")
            component, setting = parts
            
            if hasattr(self._config, component):
                component_obj = getattr(self._config, component)
                if hasattr(component_obj, setting):
                    setattr(component_obj, setting, value)


class TestConfigurationProvider:
    """Provides configuration for specific test environments."""
    
    @staticmethod
    def get_unit_test_config() -> TestConfiguration:
        """Get configuration optimized for unit tests."""
        config = TestConfiguration(
            environment=TestEnvironmentType.UNIT,
            parallel_execution=True,
            fail_fast=True
        )
        
        # Use in-memory implementations for speed
        config.cache.type = "memory"
        config.cache.max_size = 1024 * 1024  # 1MB
        config.filesystem.type = "memory"
        config.network.use_mock = True
        config.network.mock_delay = 0.0  # No delay for unit tests
        
        # Minimal logging
        config.logging.level = "ERROR"
        config.logging.capture_output = True
        config.logging.log_to_file = False
        
        return config
    
    @staticmethod
    def get_integration_test_config() -> TestConfiguration:
        """Get configuration optimized for integration tests."""
        config = TestConfiguration(
            environment=TestEnvironmentType.INTEGRATION,
            parallel_execution=False,  # Integration tests may have dependencies
            fail_fast=False
        )
        
        # Use temporary filesystem and file cache
        config.cache.type = "file"
        config.cache.max_size = 10 * 1024 * 1024  # 10MB
        config.filesystem.type = "temp"
        config.filesystem.auto_cleanup = True
        
        # Use fake network with realistic delays
        config.network.use_mock = True
        config.network.mock_delay = 0.1
        
        # More detailed logging
        config.logging.level = "INFO"
        config.logging.capture_output = True
        config.logging.log_to_file = True
        
        return config
    
    @staticmethod
    def get_e2e_test_config() -> TestConfiguration:
        """Get configuration for end-to-end tests."""
        config = TestConfiguration(
            environment=TestEnvironmentType.END_TO_END,
            parallel_execution=False,
            fail_fast=False,
            verbose=True
        )
        
        # Use real implementations
        config.cache.type = "file"
        config.cache.max_size = 100 * 1024 * 1024  # 100MB
        config.filesystem.type = "real"
        config.network.use_mock = False
        config.network.timeout = 60  # Longer timeout for real network
        
        # Comprehensive logging
        config.logging.level = "DEBUG"
        config.logging.capture_output = True
        config.logging.log_to_file = True
        
        return config
    
    @staticmethod
    def get_performance_test_config() -> TestConfiguration:
        """Get configuration for performance tests."""
        config = TestConfiguration(
            environment=TestEnvironmentType.PERFORMANCE,
            parallel_execution=True,
            fail_fast=False,
            verbose=True
        )
        
        # Optimized for performance testing
        config.cache.type = "memory"
        config.cache.max_size = 500 * 1024 * 1024  # 500MB
        config.filesystem.type = "temp"
        config.network.use_mock = True
        config.network.mock_delay = 0.0
        
        # Minimal logging for performance
        config.logging.level = "WARNING"
        config.logging.capture_output = False
        config.logging.log_to_file = False
        
        return config


class ConfigurationError(Exception):
    """Exception raised for configuration-related errors."""
    pass


class SimpleConfiguration:
    """Simple configuration implementation for basic use cases."""
    
    def __init__(self, initial_data: Optional[Dict[str, Any]] = None):
        """Initialize simple configuration."""
        self._data = initial_data or {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        keys = key.split(".")
        value = self._data
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        keys = key.split(".")
        data = self._data
        
        for k in keys[:-1]:
            if k not in data:
                data[k] = {}
            data = data[k]
        
        data[keys[-1]] = value
    
    def has(self, key: str) -> bool:
        """Check if configuration key exists."""
        return self.get(key) is not None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return self._data.copy()
    
    def update(self, config: Dict[str, Any]) -> None:
        """Update configuration with dictionary."""
        self._data.update(config)