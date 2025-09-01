"""Custom completion implementations for Tellus CLI."""

import os
from typing import List, Optional, Set

from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.document import Document

from ...application.container import get_service_container


class LocationPathCompleter(Completer):
    """Path completer that works with Location filesystems (local, SSH, etc.)."""
    
    def __init__(self, location, only_directories: bool = True, expanduser: bool = True):
        """Initialize completer for a specific Location.
        
        Args:
            location: A Tellus LocationEntity (domain entity)
            only_directories: Only complete directories, not files
            expanduser: Expand ~ to user home directory
        """
        self.location = location
        self.only_directories = only_directories
        self.expanduser = expanduser
        self._cache: dict = {}  # Cache for remote filesystem calls
        self._fs = None  # Cached filesystem
        
    def get_completions(self, document: Document, complete_event) -> List[Completion]:
        """Get path completions for the current input."""
        text = document.text_before_cursor
        
        # Handle empty input - start from root or home
        if not text:
            return self._complete_from_path("/")
            
        # Expand user home directory
        if self.expanduser and text.startswith("~"):
            text = self._expand_user_path(text)
            
        # Extract the directory part and filename part
        if text.endswith("/"):
            directory = text
            filename_part = ""
        else:
            directory = os.path.dirname(text) or "/"
            filename_part = os.path.basename(text)
            
        return self._complete_from_path(directory, filename_part)
        
    def _expand_user_path(self, path: str) -> str:
        """Expand ~ to user home directory for the location."""
        if self.location.config.get('protocol') == "file":
            # Local filesystem - use os.path.expanduser
            return os.path.expanduser(path)
        else:
            # Remote filesystem - replace ~ with likely home path
            # This is a best guess; actual home depends on the remote system
            return path.replace("~", "/home/" + os.getenv("USER", "user"))
            
    def _complete_from_path(self, directory: str, prefix: str = "") -> List[Completion]:
        """Get completions from a specific directory."""
        try:
            # Get or create filesystem for the location
            fs = self._get_filesystem()
            
            # Cache key for this directory
            cache_key = f"{self.location.name}:{directory}"
            
            # Check cache first (valid for 30 seconds)
            import time
            current_time = time.time()
            if cache_key in self._cache:
                cached_data, cache_time = self._cache[cache_key]
                if current_time - cache_time < 30:  # 30 second cache
                    entries = cached_data
                else:
                    entries = self._list_directory(fs, directory)
                    self._cache[cache_key] = (entries, current_time)
            else:
                entries = self._list_directory(fs, directory)
                self._cache[cache_key] = (entries, current_time)
                
            # Filter entries by prefix and return completions
            completions = []
            for entry in entries:
                name = entry["name"]
                is_dir = entry["type"] == "directory"
                
                # Skip if we only want directories and this isn't one
                if self.only_directories and not is_dir:
                    continue
                    
                # Skip entries that don't match the prefix
                if prefix and not name.startswith(prefix):
                    continue
                    
                # Create completion
                display_name = name + ("/" if is_dir else "")
                completion = Completion(
                    text=name + ("/" if is_dir else ""),
                    start_position=-len(prefix),
                    display=display_name
                )
                completions.append(completion)
                
            return sorted(completions, key=lambda c: c.text.lower())
            
        except Exception as e:
            # If remote completion fails, return empty list
            # Could also fall back to local completion
            return []
            
    def _list_directory(self, fs, directory: str) -> List[dict]:
        """List directory contents using fsspec filesystem."""
        try:
            # Normalize path
            if not directory.endswith("/"):
                directory += "/"
                
            # List directory contents
            try:
                entries = fs.ls(directory, detail=True)
            except FileNotFoundError:
                # Directory doesn't exist
                return []
                
            # Convert fsspec format to our format
            result = []
            for entry in entries:
                if isinstance(entry, dict):
                    # Already detailed format
                    name = os.path.basename(entry["name"])
                    entry_type = entry.get("type", "file")
                else:
                    # Just path string
                    name = os.path.basename(entry)
                    # Try to determine if it's a directory
                    try:
                        entry_type = "directory" if fs.isdir(entry) else "file"
                    except:
                        entry_type = "file"
                        
                result.append({
                    "name": name,
                    "type": entry_type
                })
                
            return result
            
        except Exception:
            # Any error in listing - return empty
            return []
    
    def _get_filesystem(self):
        """Get or create filesystem for the location."""
        if self._fs is None:
            # Create unsandboxed filesystem for tab completion (allows browsing entire remote filesystem)
            service_container = get_service_container()
            location_service = service_container.service_factory.location_service
            self._fs = location_service._create_unsandboxed_filesystem(self.location)
        return self._fs


class SmartPathCompleter(Completer):
    """Path completer that automatically chooses local or location-based completion."""
    
    def __init__(self, location=None, **kwargs):
        """Initialize smart completer.
        
        Args:
            location: Optional Location instance. If None, uses local completion.
            **kwargs: Passed to underlying completers
        """
        self.location = location
        self.kwargs = kwargs
        
        if location and location.config.get('protocol') != "file":
            # Remote location - use LocationPathCompleter
            self._completer = LocationPathCompleter(location, **kwargs)
        else:
            # Local location or no location - use standard PathCompleter
            from prompt_toolkit.completion import PathCompleter
            self._completer = PathCompleter(**kwargs)
            
    def get_completions(self, document: Document, complete_event) -> List[Completion]:
        """Delegate to the appropriate completer."""
        return self._completer.get_completions(document, complete_event)