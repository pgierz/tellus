# Tellus Chat

A natural language interface for interacting with Tellus Earth System Model data and simulations.

## Features

- **LLM-Agnostic**: Built on LangChain to support multiple LLM providers
- **Local-First**: Run with local models using Ollama by default
- **REST API**: FastAPI-based web server with streaming support
- **Command-Line Interface**: Interactive chat and configuration management
- **Extensible**: Easy to add new LLM providers and chat interfaces

## Installation

1. Install the package in development mode:

```bash
pip install -e .
```

2. Install Ollama (for local LLM support):

```bash
# On macOS
brew install ollama

# On Linux
curl -fsSL https://ollama.com/install.sh | sh

# Pull a model (e.g., llama3)
ollama pull llama3
```

## Usage

### CLI Chat

Start an interactive chat session:

```bash
tellus-chat chat
```

Or send a single message:

```bash
tellus-chat chat "Show me the temperature trends in the last simulation"
```

### Web Server

Start the FastAPI server:

```bash
tellus-chat server
```

The API will be available at `http://localhost:8000` with interactive documentation at `/docs`.

### API Endpoints

- `POST /chat`: Send a message and get a response
- `GET /conversations/{conversation_id}`: Get conversation history
- `DELETE /conversations/{conversation_id}`: Delete a conversation

## Configuration

Configuration is stored in `~/.config/tellus/chat_config.yaml`. You can:

- View current config: `tellus-chat config --show`
- Reset to defaults: `tellus-chat config --reset`

Example configuration:

```yaml
provider:
  model_name: llama3
  base_url: http://localhost:11434
  temperature: 0.7
system_prompt: "You are a helpful assistant for working with Earth System Model data."
max_history: 10
```

## Development

### Dependencies

- Python 3.8+
- [Poetry](https://python-poetry.org/) for dependency management
- [Ollama](https://ollama.ai/) for local LLM support

### Setup

1. Install dependencies:

```bash
poetry install
```

2. Run tests:

```bash
poetry run pytest
```

### Adding a New LLM Provider

1. Create a new class in `tellus_chat/providers/` that inherits from `LLMProvider`
2. Implement the required methods
3. Update the configuration loading in `ChatInterface.from_config()`

## License

[Your License Here]