"""
File type configuration for user-customizable content classification.

This module provides a configuration system that allows users to define
custom patterns for classifying file content types and importance levels.
"""

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Pattern

from .simulation_file import FileContentType, FileImportance

logger = logging.getLogger(__name__)


@dataclass
class FileTypeRule:
    """Rule for classifying files based on patterns."""
    
    name: str                                    # Human-readable name for the rule
    patterns: List[str]                         # List of glob or regex patterns
    content_type: FileContentType               # Content type to assign
    importance: FileImportance = FileImportance.IMPORTANT
    description: str = ""                       # Description of what this rule matches
    pattern_type: str = "glob"                  # "glob" or "regex"
    case_sensitive: bool = False                # Whether patterns are case-sensitive
    priority: int = 0                           # Higher priority rules are checked first
    
    def __post_init__(self):
        """Validate the rule configuration."""
        if not self.name or not isinstance(self.name, str):
            raise ValueError("Rule name must be a non-empty string")
        
        if not self.patterns or not isinstance(self.patterns, list):
            raise ValueError("Patterns must be a non-empty list")
        
        if self.pattern_type not in ["glob", "regex"]:
            raise ValueError("Pattern type must be 'glob' or 'regex'")
        
        # Compile regex patterns if needed to validate them
        if self.pattern_type == "regex":
            flags = 0 if self.case_sensitive else re.IGNORECASE
            for pattern in self.patterns:
                try:
                    re.compile(pattern, flags)
                except re.error as e:
                    raise ValueError(f"Invalid regex pattern '{pattern}': {e}")
    
    def matches(self, filename: str) -> bool:
        """Check if filename matches any of the patterns in this rule."""
        if self.pattern_type == "glob":
            import fnmatch
            
            test_filename = filename if self.case_sensitive else filename.lower()
            
            for pattern in self.patterns:
                test_pattern = pattern if self.case_sensitive else pattern.lower()
                if fnmatch.fnmatch(test_filename, test_pattern):
                    return True
            return False
            
        elif self.pattern_type == "regex":
            flags = 0 if self.case_sensitive else re.IGNORECASE
            
            for pattern in self.patterns:
                if re.search(pattern, filename, flags):
                    return True
            return False
        
        return False
    
    def to_dict(self) -> Dict:
        """Convert rule to dictionary for serialization."""
        return {
            "name": self.name,
            "patterns": self.patterns,
            "content_type": self.content_type.value,
            "importance": self.importance.value,
            "description": self.description,
            "pattern_type": self.pattern_type,
            "case_sensitive": self.case_sensitive,
            "priority": self.priority
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'FileTypeRule':
        """Create rule from dictionary representation."""
        return cls(
            name=data["name"],
            patterns=data["patterns"],
            content_type=FileContentType(data["content_type"]),
            importance=FileImportance(data["importance"]),
            description=data.get("description", ""),
            pattern_type=data.get("pattern_type", "glob"),
            case_sensitive=data.get("case_sensitive", False),
            priority=data.get("priority", 0)
        )


@dataclass
class FileTypeConfiguration:
    """Configuration for file type classification rules."""
    
    rules: List[FileTypeRule] = field(default_factory=list)
    default_content_type: FileContentType = FileContentType.AUXILIARY
    default_importance: FileImportance = FileImportance.IMPORTANT
    version: str = "1.0"
    
    def __post_init__(self):
        """Sort rules by priority (higher priority first)."""
        self.rules.sort(key=lambda r: r.priority, reverse=True)
    
    def add_rule(self, rule: FileTypeRule) -> None:
        """Add a new rule and resort by priority."""
        if not isinstance(rule, FileTypeRule):
            raise ValueError("Must provide a FileTypeRule instance")
        
        self.rules.append(rule)
        self.rules.sort(key=lambda r: r.priority, reverse=True)
    
    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name. Returns True if rule was found and removed."""
        for i, rule in enumerate(self.rules):
            if rule.name == name:
                del self.rules[i]
                return True
        return False
    
    def get_rule(self, name: str) -> Optional[FileTypeRule]:
        """Get a rule by name."""
        for rule in self.rules:
            if rule.name == name:
                return rule
        return None
    
    def classify_file(self, filename: str) -> tuple[FileContentType, FileImportance]:
        """
        Classify a file based on configured rules.
        
        Args:
            filename: The filename (can include path) to classify
            
        Returns:
            Tuple of (content_type, importance)
        """
        # Check rules in priority order
        for rule in self.rules:
            if rule.matches(filename):
                return rule.content_type, rule.importance
        
        # No rule matched, return defaults
        return self.default_content_type, self.default_importance
    
    def to_dict(self) -> Dict:
        """Convert configuration to dictionary for serialization."""
        return {
            "version": self.version,
            "default_content_type": self.default_content_type.value,
            "default_importance": self.default_importance.value,
            "rules": [rule.to_dict() for rule in self.rules]
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'FileTypeConfiguration':
        """Create configuration from dictionary representation."""
        config = cls(
            version=data.get("version", "1.0"),
            default_content_type=FileContentType(data.get("default_content_type", "auxiliary")),
            default_importance=FileImportance(data.get("default_importance", "important"))
        )
        
        # Add rules
        for rule_data in data.get("rules", []):
            config.rules.append(FileTypeRule.from_dict(rule_data))
        
        # Sort by priority
        config.rules.sort(key=lambda r: r.priority, reverse=True)
        
        return config
    
    def save_to_file(self, file_path: Path) -> None:
        """Save configuration to JSON file."""
        try:
            with open(file_path, 'w') as f:
                json.dump(self.to_dict(), f, indent=2)
            logger.info(f"File type configuration saved to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save file type configuration: {e}")
            raise
    
    @classmethod
    def load_from_file(cls, file_path: Path) -> 'FileTypeConfiguration':
        """Load configuration from JSON file."""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            logger.info(f"File type configuration loaded from {file_path}")
            return cls.from_dict(data)
        except FileNotFoundError:
            logger.warning(f"Configuration file not found: {file_path}")
            return cls.create_default()
        except Exception as e:
            logger.error(f"Failed to load file type configuration: {e}")
            # Fall back to default configuration
            return cls.create_default()
    
    @classmethod
    def create_default(cls) -> 'FileTypeConfiguration':
        """Create default file type configuration with sensible patterns."""
        config = cls(
            default_content_type=FileContentType.AUXILIARY,
            default_importance=FileImportance.IMPORTANT
        )
        
        # Define default rules based on common Earth System Model patterns
        default_rules = [
            FileTypeRule(
                name="NetCDF Output Data",
                patterns=["*.nc", "*.netcdf", "*output*.nc", "*results*.nc"],
                content_type=FileContentType.OUTDATA,
                importance=FileImportance.CRITICAL,
                description="NetCDF model output files",
                priority=100
            ),
            FileTypeRule(
                name="Configuration Files",
                patterns=["*.nml", "*.cfg", "*.conf", "*config*", "*namelist*"],
                content_type=FileContentType.CONFIG,
                importance=FileImportance.CRITICAL,
                description="Model configuration and namelist files",
                priority=90
            ),
            FileTypeRule(
                name="Restart Files",
                patterns=["*restart*", "*.rst", "*_r_*", "*checkpoint*"],
                content_type=FileContentType.RESTART,
                importance=FileImportance.CRITICAL,
                description="Model restart and checkpoint files",
                priority=85
            ),
            FileTypeRule(
                name="Log Files",
                patterns=["*.log", "*log*", "*.out", "*debug*", "*error*"],
                content_type=FileContentType.LOG,
                importance=FileImportance.OPTIONAL,
                description="Log and diagnostic output files",
                priority=80
            ),
            FileTypeRule(
                name="Input Data",
                patterns=["*input*", "*initial*", "*boundary*", "*forcing*"],
                content_type=FileContentType.INPUT,
                importance=FileImportance.IMPORTANT,
                description="Model input and initial condition files",
                priority=75
            ),
            FileTypeRule(
                name="Forcing Data",
                patterns=["*forcing*", "*flux*", "*sst*", "*ice*", "*atm*"],
                content_type=FileContentType.FORCING,
                importance=FileImportance.IMPORTANT,
                description="External forcing data files",
                priority=70
            ),
            FileTypeRule(
                name="Scripts",
                patterns=["*.sh", "*.py", "*.pl", "*.csh", "*.ksh", "*script*"],
                content_type=FileContentType.SCRIPTS,
                importance=FileImportance.IMPORTANT,
                description="Shell scripts and executables",
                priority=65
            ),
            FileTypeRule(
                name="Analysis Results",
                patterns=["*analysis*", "*statistics*", "*summary*", "*mean*", "*trend*"],
                content_type=FileContentType.ANALYSIS,
                importance=FileImportance.IMPORTANT,
                description="Analysis and statistical result files",
                priority=60
            ),
            FileTypeRule(
                name="Visualization Files",
                patterns=["*.png", "*.jpg", "*.jpeg", "*.gif", "*.pdf", "*.ps", "*plot*", "*fig*"],
                content_type=FileContentType.VIZ,
                importance=FileImportance.OPTIONAL,
                description="Plots, figures, and visualization files",
                priority=55
            ),
            FileTypeRule(
                name="Documentation",
                patterns=["*.txt", "*.md", "*.doc", "*.pdf", "README*", "*doc*"],
                content_type=FileContentType.AUXILIARY,
                importance=FileImportance.OPTIONAL,
                description="Documentation and auxiliary files",
                priority=50
            )
        ]
        
        for rule in default_rules:
            config.add_rule(rule)
        
        return config


def get_default_config_path() -> Path:
    """Get the default path for file type configuration."""
    # Use ~/.tellus/file_types.json as default
    config_dir = Path.home() / ".tellus"
    config_dir.mkdir(exist_ok=True)
    return config_dir / "file_types.json"


def load_file_type_config(config_path: Optional[Path] = None) -> FileTypeConfiguration:
    """Load file type configuration from file or create default."""
    if config_path is None:
        config_path = get_default_config_path()
    
    return FileTypeConfiguration.load_from_file(config_path)


def save_default_config(config_path: Optional[Path] = None) -> None:
    """Save the default configuration to file."""
    if config_path is None:
        config_path = get_default_config_path()
    
    config = FileTypeConfiguration.create_default()
    config.save_to_file(config_path)
    
    logger.info(f"Default file type configuration saved to {config_path}")