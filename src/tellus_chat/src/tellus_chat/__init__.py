"""
Tellus Chat - Natural language interface for Tellus Earth System Model data and simulations.

This package provides a chat interface for interacting with Tellus simulations using
natural language, powered by large language models.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("tellus-chat")
except PackageNotFoundError:
    # package is not installed
    __version__ = "0.1.0"

__all__ = [
    "__version__",
]
