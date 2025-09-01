from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..core.message import Message, MessageRole


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""

    @abstractmethod
    async def generate(self, messages: List[Message], **kwargs) -> Message:
        """Generate a response to a list of messages.

        Args:
            messages: List of messages in the conversation
            **kwargs: Additional provider-specific arguments

        Returns:
            The generated message
        """
        pass

    @abstractmethod
    async def stream(self, messages: List[Message], **kwargs) -> str:
        """Stream a response to a list of messages.

        Args:
            messages: List of messages in the conversation
            **kwargs: Additional provider-specific arguments

        Yields:
            Chunks of the generated response
        """
        pass

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Get the name of the model being used."""
        pass

    @classmethod
    @abstractmethod
    def from_config(cls, config: Dict[str, Any]) -> "LLMProvider":
        """Create a provider instance from a configuration dictionary."""
        pass
