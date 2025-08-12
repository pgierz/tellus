"""
Feature flag system for progressive migration to new architecture.

This module manages feature flags that allow gradual rollout of new services
while maintaining compatibility with the legacy system.
"""

import os
from enum import Enum
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class FeatureFlag(Enum):
    """Feature flags for progressive migration."""
    USE_NEW_SIMULATION_SERVICE = "USE_NEW_SIMULATION_SERVICE"
    USE_NEW_LOCATION_SERVICE = "USE_NEW_LOCATION_SERVICE"
    USE_NEW_ARCHIVE_SERVICE = "USE_NEW_ARCHIVE_SERVICE"
    USE_NEW_WORKFLOW_SERVICE = "USE_NEW_WORKFLOW_SERVICE"
    ENABLE_DATA_MIGRATION = "ENABLE_DATA_MIGRATION"
    STRICT_VALIDATION = "STRICT_VALIDATION"


class FeatureFlagManager:
    """Manages feature flags for progressive rollout."""
    
    def __init__(self):
        self._flags: Dict[FeatureFlag, bool] = {}
        self._load_flags_from_env()
    
    def _load_flags_from_env(self):
        """Load feature flags from environment variables."""
        for flag in FeatureFlag:
            env_var = f"TELLUS_{flag.value}"
            self._flags[flag] = os.getenv(env_var, "false").lower() == "true"
            
        logger.info(f"Feature flags loaded: {dict((f.name, enabled) for f, enabled in self._flags.items() if enabled)}")
    
    def is_enabled(self, flag: FeatureFlag) -> bool:
        """Check if a feature flag is enabled."""
        return self._flags.get(flag, False)
    
    def enable_flag(self, flag: FeatureFlag):
        """Enable a feature flag."""
        self._flags[flag] = True
        logger.info(f"Feature flag enabled: {flag.value}")
    
    def disable_flag(self, flag: FeatureFlag):
        """Disable a feature flag."""
        self._flags[flag] = False
        logger.info(f"Feature flag disabled: {flag.value}")
    
    def get_enabled_flags(self) -> Dict[str, bool]:
        """Get all currently enabled flags."""
        return {flag.name: enabled for flag, enabled in self._flags.items() if enabled}


# Global instance
feature_flags = FeatureFlagManager()