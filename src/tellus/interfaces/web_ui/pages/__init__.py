"""Page components for the Tellus Web UI."""

from .index import page as index_page
from .simulations import page as simulations_page  
from .locations import page as locations_page
from .files import page as files_page

__all__ = ["index_page", "simulations_page", "locations_page", "files_page"]