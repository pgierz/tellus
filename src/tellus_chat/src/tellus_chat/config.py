import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

DEFAULT_CONFIG = {
    "provider": {
        "model_name": "llama3",
        "base_url": "http://localhost:11434",
        "temperature": 0.7,
    },
    "system_prompt": "You are a helpful assistant for working with Earth System Model data.",
    "max_history": 10,
}


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from a YAML file or use defaults.

    Args:
        config_path: Path to the configuration file. If None, looks for:
                    - TELLUS_CHAT_CONFIG environment variable
                    - ~/.config/tellus/chat_config.yaml
                    - Default configuration

    Returns:
        Dictionary containing the configuration
    """
    # Try to find config file
    if config_path is None:
        config_path = os.environ.get("TELLUS_CHAT_CONFIG")
        if config_path is None:
            default_path = Path.home() / ".config" / "tellus" / "chat_config.yaml"
            if default_path.exists():
                config_path = str(default_path)

    # Load config if path exists
    if config_path and os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}
    else:
        config = {}

    # Merge with defaults
    return {**DEFAULT_CONFIG, **config}


def save_config(config: Dict[str, Any], config_path: Optional[str] = None) -> None:
    """Save configuration to a YAML file.

    Args:
        config: Configuration dictionary to save
        config_path: Path to save the configuration file. If None, uses:
                    - TELLUS_CHAT_CONFIG environment variable
                    - ~/.config/tellus/chat_config.yaml
    """
    if config_path is None:
        config_path = os.environ.get(
            "TELLUS_CHAT_CONFIG",
            str(Path.home() / ".config" / "tellus" / "chat_config.yaml"),
        )

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(config_path), exist_ok=True)

    with open(config_path, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)


def get_default_config() -> Dict[str, Any]:
    """Get the default configuration."""
    return DEFAULT_CONFIG.copy()
