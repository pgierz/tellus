import logging
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional, Type, TypeVar

from ..providers.base import LLMProvider
from .message import Message, MessageRole

T = TypeVar("T", bound="ChatInterface")

logger = logging.getLogger(__name__)


class ChatInterface:
    """Main chat interface for interacting with LLMs."""

    def __init__(
        self,
        provider: LLMProvider,
        system_prompt: Optional[str] = None,
        message_history: Optional[List[Message]] = None,
        max_history: int = 10,
    ):
        """Initialize the chat interface.

        Args:
            provider: The LLM provider to use
            system_prompt: Optional system prompt to initialize the conversation
            message_history: Optional list of previous messages
            max_history: Maximum number of messages to keep in history
        """
        self.provider = provider
        self.max_history = max_history
        self._messages: List[Message] = []

        # Add system prompt if provided
        if system_prompt:
            self._messages.append(
                Message(
                    role=MessageRole.SYSTEM,
                    content=system_prompt,
                    timestamp=datetime.utcnow(),
                )
            )

        # Add message history if provided
        if message_history:
            self._messages.extend(message_history[-self.max_history :])

    @classmethod
    def from_config(
        cls,
        config: Dict[str, Any],
        provider_class: Optional[Type[LLMProvider]] = None,
        **kwargs,
    ) -> T:
        """Create a chat interface from a configuration dictionary.

        Args:
            config: Configuration dictionary
            provider_class: Optional provider class to use (defaults to LocalLLMProvider)
            **kwargs: Additional arguments to pass to the provider

        Returns:
            A new ChatInterface instance
        """
        if provider_class is None:
            from ..providers.local_llm import LocalLLMProvider

            provider_class = LocalLLMProvider

        provider_config = config.get("provider", {})
        provider = provider_class.from_config(provider_config)

        return cls(
            provider=provider,
            system_prompt=config.get("system_prompt"),
            message_history=config.get("message_history"),
            max_history=config.get("max_history", 10),
            **kwargs,
        )

    def add_message(self, message: Message) -> None:
        """Add a message to the conversation history."""
        self._messages.append(message)

        # Trim history if needed
        if len(self._messages) > self.max_history:
            # Keep system prompt if present and not the only message
            if self._messages[0].role == MessageRole.SYSTEM and len(self._messages) > 1:
                self._messages = [self._messages[0]] + self._messages[
                    -(self.max_history - 1) :
                ]
            else:
                self._messages = self._messages[-self.max_history :]

    async def send_message(
        self, content: str, role: MessageRole = MessageRole.USER, **kwargs
    ) -> Message:
        """Send a message and get a response.

        Args:
            content: The message content
            role: The role of the message sender
            **kwargs: Additional arguments to pass to the provider

        Returns:
            The assistant's response message
        """
        # Add user message to history
        user_message = Message(role=role, content=content, timestamp=datetime.utcnow())
        self.add_message(user_message)

        # Get response from provider
        response = await self.provider.generate(self._messages, **kwargs)

        # Add assistant's response to history
        self.add_message(response)

        return response

    async def stream_response(
        self, content: str, role: MessageRole = MessageRole.USER, **kwargs
    ) -> AsyncIterator[str]:
        """Send a message and stream the response.

        Args:
            content: The message content
            role: The role of the message sender
            **kwargs: Additional arguments to pass to the provider

        Yields:
            Chunks of the assistant's response
        """
        # Add user message to history
        user_message = Message(role=role, content=content, timestamp=datetime.utcnow())
        self.add_message(user_message)

        # Create a temporary message for the assistant's response
        response = Message(
            role=MessageRole.ASSISTANT,
            content="",
            timestamp=datetime.utcnow(),
            metadata={"streaming": True},
        )

        # Stream response from provider
        full_response = ""
        async for chunk in self.provider.stream(self._messages, **kwargs):
            full_response += chunk
            yield chunk

        # Update the response message with the full content
        response.content = full_response
        response.timestamp = datetime.utcnow()
        response.metadata["streaming"] = False

        # Add the complete response to history
        self.add_message(response)

    @property
    def messages(self) -> List[Message]:
        """Get the current message history."""
        return self._messages.copy()

    def clear_history(self) -> None:
        """Clear the message history, optionally preserving the system prompt."""
        if self._messages and self._messages[0].role == MessageRole.SYSTEM:
            self._messages = [self._messages[0]]
        else:
            self._messages = []
