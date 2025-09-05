"""
Chat state management for the Tellus web UI.

This module manages the AI chat interface integration with the tellus_chat API.
"""

# Uncomment when Reflex is available
# import reflex as rx
from typing import List, Dict, Any, Optional
import json
import aiohttp
from datetime import datetime


class ChatState:
    """
    State management for AI chat interface in the web UI.
    
    This integrates with the existing tellus_chat FastAPI backend.
    When Reflex is available, this will inherit from rx.State.
    """
    
    def __init__(self):
        # Chat data
        self.messages: List[Dict[str, Any]] = []
        self.current_message: str = ""
        self.conversation_id: Optional[str] = None
        self.loading: bool = False
        self.error_message: str = ""
        
        # Chat settings
        self.chat_api_url: str = "http://localhost:8000"  # tellus_chat server
        self.stream_responses: bool = True
        
        # UI state
        self.chat_visible: bool = False
        self.chat_minimized: bool = False
    
    def add_message(self, content: str, role: str = "user", metadata: Optional[Dict[str, Any]] = None):
        """Add a message to the conversation."""
        message = {
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat(),
            "metadata": metadata or {}
        }
        self.messages.append(message)
    
    async def send_message(self):
        """Send the current message to the chat API."""
        if not self.current_message.strip():
            return
        
        # Add user message
        user_message = self.current_message
        self.add_message(user_message, "user")
        self.current_message = ""
        self.loading = True
        self.error_message = ""
        
        try:
            # Make API call to tellus_chat
            async with aiohttp.ClientSession() as session:
                payload = {
                    "message": user_message,
                    "stream": self.stream_responses,
                    "conversation_id": self.conversation_id
                }
                
                async with session.post(
                    f"{self.chat_api_url}/chat",
                    json=payload
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        
                        # Add assistant response
                        self.add_message(
                            result["message"], 
                            "assistant",
                            {"conversation_id": result["conversation_id"]}
                        )
                        
                        # Update conversation ID
                        if not self.conversation_id:
                            self.conversation_id = result["conversation_id"]
                    else:
                        error_text = await response.text()
                        self.error_message = f"Chat API error: {error_text}"
                        
        except Exception as e:
            self.error_message = f"Failed to connect to chat API: {str(e)}"
        
        finally:
            self.loading = False
    
    async def load_conversation_history(self):
        """Load existing conversation history if available."""
        if not self.conversation_id:
            return
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.chat_api_url}/conversations/{self.conversation_id}"
                ) as response:
                    if response.status == 200:
                        history = await response.json()
                        self.messages = [
                            {
                                "role": msg["role"],
                                "content": msg["content"],
                                "timestamp": msg["timestamp"],
                                "metadata": msg["metadata"]
                            }
                            for msg in history
                        ]
                    
        except Exception as e:
            self.error_message = f"Failed to load conversation: {str(e)}"
    
    def clear_conversation(self):
        """Clear the current conversation."""
        self.messages = []
        self.conversation_id = None
        self.current_message = ""
        self.error_message = ""
    
    async def delete_conversation(self):
        """Delete the current conversation on the server."""
        if not self.conversation_id:
            return
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.delete(
                    f"{self.chat_api_url}/conversations/{self.conversation_id}"
                ) as response:
                    if response.status == 200:
                        self.clear_conversation()
                    
        except Exception as e:
            self.error_message = f"Failed to delete conversation: {str(e)}"
    
    def toggle_chat(self):
        """Toggle chat visibility."""
        self.chat_visible = not self.chat_visible
    
    def minimize_chat(self):
        """Minimize the chat interface."""
        self.chat_minimized = True
    
    def restore_chat(self):
        """Restore the chat interface from minimized state."""
        self.chat_minimized = False
    
    def get_formatted_messages(self) -> List[Dict[str, Any]]:
        """Get messages formatted for display."""
        formatted = []
        for msg in self.messages:
            formatted.append({
                **msg,
                "formatted_time": self._format_timestamp(msg["timestamp"]),
                "is_user": msg["role"] == "user",
                "avatar": "👤" if msg["role"] == "user" else "🤖"
            })
        return formatted
    
    def _format_timestamp(self, timestamp: str) -> str:
        """Format timestamp for display."""
        try:
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            now = datetime.now()
            
            # If today, show time only
            if dt.date() == now.date():
                return dt.strftime("%H:%M")
            else:
                return dt.strftime("%m/%d %H:%M")
        except:
            return timestamp
    
    def get_conversation_summary(self) -> str:
        """Get a summary of the current conversation."""
        if not self.messages:
            return "No messages"
        
        user_messages = [msg for msg in self.messages if msg["role"] == "user"]
        assistant_messages = [msg for msg in self.messages if msg["role"] == "assistant"]
        
        return f"{len(user_messages)} questions, {len(assistant_messages)} responses"
    
    def suggest_questions(self) -> List[str]:
        """Get suggested questions based on current context."""
        # This could be enhanced to be context-aware based on current page
        return [
            "How many simulations are currently running?",
            "Show me the largest files in the archive",
            "What's the status of my CESM2 experiment?",
            "Help me set up a new location",
            "How do I extract files from an archive?"
        ]


# When Reflex is available, this will be a proper state class:
"""
class ChatState(rx.State):
    # All the methods above, but with proper Reflex decorators and async support
    
    @rx.var
    def formatted_messages(self) -> List[Dict[str, Any]]:
        return self.get_formatted_messages()
    
    @rx.var
    def conversation_summary(self) -> str:
        return self.get_conversation_summary()
    
    @rx.var
    def suggested_questions(self) -> List[str]:
        return self.suggest_questions()
    
    @rx.background
    async def send_message_background(self):
        # Background task version for Reflex
        await self.send_message()
"""