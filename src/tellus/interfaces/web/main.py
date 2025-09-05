"""
Main FastAPI application for Tellus Climate Data API.

This module creates and configures the FastAPI application, including all
routers, middleware, and dependency injection setup.
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ...application.container import get_service_container
from .routers import health, simulations, locations
from .version import get_version_info

# Create console for output (avoiding core.cli import)
try:
    from rich.console import Console
    console = Console()
except ImportError:
    # Fallback if rich not available
    class SimpleConsole:
        def print(self, text):
            print(text)
    console = SimpleConsole()

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    FastAPI lifespan context manager.
    
    Handles application startup and shutdown events.
    """
    # Startup
    console.print("🚀 [bold green]Starting Tellus Climate Data API[/bold green]")
    
    # Initialize service container
    container = get_service_container()
    app.state.container = container
    
    console.print("✨ [green]API ready at /docs[/green]")
    
    yield
    
    # Shutdown
    console.print("🛑 [yellow]Shutting down Tellus API[/yellow]")


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.
    
    Returns:
        Configured FastAPI application instance
    """
    # Get dynamic version information
    version_info = get_version_info()
    api_version = version_info["api_version"]
    api_path = f"/api/{api_version}"
    
    app = FastAPI(
        title="Tellus Climate Data API",
        description="""
        REST API for Tellus - the distributed data management system for Earth System Model simulations.
        
        ## Features
        
        * **Simulation Management** - Create, list, and manage climate simulations
        * **Location Management** - Configure and manage storage locations  
        * **File Discovery** - Search and discover simulation files across locations
        * **Archive Operations** - Extract, compress, and transfer simulation data
        * **Workflow Integration** - Trigger and monitor Snakemake workflows
        
        ## Architecture
        
        Built on FastAPI with Pydantic for automatic validation and serialization,
        following clean architecture principles with clear separation between
        domain logic and API concerns.
        """,
        version=version_info["tellus_version"],
        lifespan=lifespan,
        docs_url=f"{api_path}/docs",
        redoc_url=f"{api_path}/redoc",
        openapi_url=f"{api_path}/openapi.json"
    )
    
    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error(f"Unhandled exception: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "detail": "An unexpected error occurred"
            }
        )
    
    # Include routers with versioned API prefix
    app.include_router(health.router, prefix=api_path, tags=["Health"])
    app.include_router(simulations.router, prefix=f"{api_path}/simulations", tags=["Simulations"])
    app.include_router(locations.router, prefix=f"{api_path}/locations", tags=["Locations"])
    
    return app


# Create the app instance
app = create_app()