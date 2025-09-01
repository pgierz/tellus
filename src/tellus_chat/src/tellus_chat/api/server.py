import os
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..config import load_config
from ..core.chat_interface import ChatInterface
from ..core.chat_interface import Message as ChatMessage
from ..core.message import MessageRole


class Message(BaseModel):
    role: MessageRole
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ChatRequest(BaseModel):
    message: str
    stream: bool = False
    conversation_id: Optional[str] = None


class ChatResponse(BaseModel):
    message: str
    conversation_id: str


# In-memory storage for conversations (replace with a database in production)
conversations: Dict[str, ChatInterface] = {}

app = FastAPI(
    title="Tellus Chat API",
    description="Natural language interface for Tellus simulations",
    version="0.1.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_chat_interface(conversation_id: Optional[str] = None) -> ChatInterface:
    """Get or create a chat interface for the conversation."""
    if not conversation_id:
        conversation_id = os.urandom(16).hex()

    if conversation_id not in conversations:
        config = load_config()
        conversations[conversation_id] = ChatInterface.from_config(config)

    return conversations[conversation_id]


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Send a message and get a response."""
    chat_interface = get_chat_interface(request.conversation_id)

    if request.stream:
        return StreamingResponse(
            stream_response(chat_interface, request.message),
            media_type="text/event-stream",
        )

    try:
        response = await chat_interface.send_message(request.message)
        return {
            "message": response.content,
            "conversation_id": request.conversation_id or "",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def stream_response(chat_interface: ChatInterface, message: str):
    """Stream the response from the chat interface."""
    try:
        async for chunk in chat_interface.stream_response(message):
            yield f"data: {chunk}\n\n"
        yield "data: [DONE]\n\n"
    except Exception as e:
        yield f"data: Error: {str(e)}\n\n"
        yield "data: [DONE]\n\n"


@app.get("/conversations/{conversation_id}")
async def get_conversation(conversation_id: str):
    """Get the conversation history."""
    if conversation_id not in conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")

    return [
        {
            "role": msg.role.value,
            "content": msg.content,
            "timestamp": msg.timestamp.isoformat(),
            "metadata": msg.metadata,
        }
        for msg in conversations[conversation_id].messages
    ]


@app.delete("/conversations/{conversation_id}")
async def delete_conversation(conversation_id: str):
    """Delete a conversation."""
    if conversation_id in conversations:
        del conversations[conversation_id]
    return {"status": "success"}


def run_server(host: str = "0.0.0.0", port: int = 8000, reload: bool = False):
    """Run the FastAPI server."""
    uvicorn.run(
        "tellus_chat.api.server:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )


if __name__ == "__main__":
    run_server()
