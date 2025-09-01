import asyncio
import os
from pathlib import Path
from typing import Optional

import typer

from .config import get_default_config, load_config, save_config
from .core.chat_interface import ChatInterface
from .core.chat_interface import Message as ChatMessage
from .core.message import MessageRole

app = typer.Typer(
    help="Tellus Chat - Natural language interface for Tellus simulations"
)

# Global chat interface instance
chat_interface = None


def get_chat_interface() -> ChatInterface:
    """Initialize and return the chat interface."""
    global chat_interface
    if chat_interface is None:
        config = load_config()
        chat_interface = ChatInterface.from_config(config)
    return chat_interface


@app.command()
def chat(
    message: Optional[str] = typer.Argument(None, help="Message to send to the chat"),
    stream: bool = typer.Option(True, help="Stream the response"),
    clear: bool = typer.Option(False, "--clear", help="Clear conversation history"),
):
    """Start an interactive chat session or send a single message."""
    interface = get_chat_interface()

    if clear:
        interface.clear_history()
        typer.echo("Conversation history cleared.")
        return

    if message:
        # Single message mode
        if stream:
            asyncio.run(stream_response(interface, message))
        else:
            response = asyncio.run(interface.send_message(message))
            typer.echo(f"Assistant: {response.content}")
    else:
        # Interactive mode
        typer.echo(
            "Starting interactive chat. Type 'exit' or 'quit' to end the session."
        )
        while True:
            user_input = typer.prompt("You")
            if user_input.lower() in ("exit", "quit"):
                break

            if stream:
                asyncio.run(stream_response(interface, user_input))
            else:
                response = asyncio.run(interface.send_message(user_input))
                typer.echo(f"\nAssistant: {response.content}\n")


async def stream_response(interface, message: str):
    """Stream the response from the chat interface."""
    typer.echo("Assistant: ", nl=False)
    async for chunk in interface.stream_response(message):
        typer.echo(chunk, nl=False)
    typer.echo("\n")


@app.command()
def config(
    show: bool = typer.Option(False, help="Show current configuration"),
    reset: bool = typer.Option(False, help="Reset to default configuration"),
):
    """Manage chat configuration."""
    if reset:
        config = get_default_config()
        save_config(config)
        typer.echo("Configuration reset to defaults.")
    elif show:
        config = load_config()
        typer.echo("Current configuration:")
        typer.echo(yaml.dump(config, default_flow_style=False, sort_keys=False))
    else:
        typer.echo(
            "Use --show to view current configuration or --reset to reset to defaults."
        )


@app.command()
def server(
    host: str = typer.Option("0.0.0.0", help="Host to bind the server to"),
    port: int = typer.Option(8000, help="Port to run the server on"),
    reload: bool = typer.Option(False, help="Enable auto-reload for development"),
):
    """Start the chat API server."""
    from .api.server import run_server

    run_server(host=host, port=port, reload=reload)


def main():
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
