import logging
from typing import Any, AsyncIterator, Dict, List, Optional

from langchain_community.chat_models import ChatOllama
from langchain_community.llms import Ollama
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from ..core.message import Message, MessageRole
from .base import LLMProvider

logger = logging.getLogger(__name__)


class LocalLLMProvider(LLMProvider):
    """Local LLM provider using Ollama."""

    def __init__(
        self,
        model_name: str = "llama3",
        base_url: str = "http://localhost:11434",
        temperature: float = 0.7,
        **kwargs,
    ):
        """Initialize the local LLM provider.

        Args:
            model_name: Name of the Ollama model to use
            base_url: Base URL of the Ollama server
            temperature: Temperature for sampling
            **kwargs: Additional arguments to pass to the Ollama model
        """
        self._model_name = model_name
        self._base_url = base_url
        self._temperature = temperature
        self._model_kwargs = kwargs
        self._chat_model = self._initialize_chat_model()

    def _initialize_chat_model(self) -> BaseChatModel:
        """Initialize the chat model."""
        return ChatOllama(
            model=self._model_name,
            base_url=self._base_url,
            temperature=self._temperature,
            **self._model_kwargs,
        )

    def _convert_messages(self, messages: List[Message]) -> List:
        """Convert internal messages to LangChain messages."""
        langchain_messages = []
        for msg in messages:
            if msg.role == MessageRole.USER:
                langchain_messages.append(HumanMessage(content=msg.content))
            elif msg.role == MessageRole.ASSISTANT:
                langchain_messages.append(AIMessage(content=msg.content))
            elif msg.role == MessageRole.SYSTEM:
                langchain_messages.append(SystemMessage(content=msg.content))
        return langchain_messages

    async def generate(self, messages: List[Message], **kwargs) -> Message:
        """Generate a response to a list of messages."""
        try:
            langchain_messages = self._convert_messages(messages)
            response = await self._chat_model.ainvoke(langchain_messages, **kwargs)
            return Message(
                role=MessageRole.ASSISTANT,
                content=response.content,
                metadata={"model": self.model_name, "provider": "local_ollama"},
            )
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            raise

    async def stream(self, messages: List[Message], **kwargs) -> AsyncIterator[str]:
        """Stream a response to a list of messages."""
        try:
            langchain_messages = self._convert_messages(messages)
            async for chunk in self._chat_model.astream(langchain_messages, **kwargs):
                if hasattr(chunk, "content"):
                    yield chunk.content
        except Exception as e:
            logger.error(f"Error streaming response: {e}")
            raise

    @property
    def model_name(self) -> str:
        """Get the name of the model being used."""
        return self._model_name

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "LocalLLMProvider":
        """Create a provider instance from a configuration dictionary."""
        return cls(
            model_name=config.get("model_name", "llama3"),
            base_url=config.get("base_url", "http://localhost:11434"),
            temperature=config.get("temperature", 0.7),
            **config.get("model_kwargs", {}),
        )
