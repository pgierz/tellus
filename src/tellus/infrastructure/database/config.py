"""
Database configuration and connection management.

Provides database URL construction, connection pooling, and session management
for PostgreSQL and SQLite using SQLAlchemy 2.0 async patterns.
Falls back to local SQLite when PostgreSQL is unavailable.
"""

import os
from typing import Optional
from urllib.parse import quote_plus
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from loguru import logger

from .models import Base


class DatabaseConfig:
    """Database configuration management for PostgreSQL and SQLite."""

    def __init__(
        self,
        database_type: str = "postgresql",  # "postgresql" or "sqlite"
        host: str = "localhost",
        port: int = 5432,
        database: str = "tellus",
        username: str = "tellus",
        password: Optional[str] = None,
        ssl_mode: str = "prefer",
        pool_size: int = 5,
        max_overflow: int = 10,
        echo: bool = False,
        sqlite_path: Optional[str] = None,  # Path for SQLite database
    ):
        self.database_type = database_type
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.ssl_mode = ssl_mode
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.echo = echo
        self.sqlite_path = sqlite_path or self._default_sqlite_path()

    def _default_sqlite_path(self) -> str:
        """Get default SQLite database path in ~/.local/tellus/"""
        local_dir = Path.home() / ".local" / "tellus"
        local_dir.mkdir(parents=True, exist_ok=True)
        return str(local_dir / "tellus.db")

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create configuration from environment variables."""
        return cls(
            database_type=os.getenv("TELLUS_DB_TYPE", "postgresql"),
            host=os.getenv("TELLUS_DB_HOST", "localhost"),
            port=int(os.getenv("TELLUS_DB_PORT", "5432")),
            database=os.getenv("TELLUS_DB_NAME", "tellus"),
            username=os.getenv("TELLUS_DB_USER", "tellus"),
            password=os.getenv("TELLUS_DB_PASSWORD"),
            ssl_mode=os.getenv("TELLUS_DB_SSL_MODE", "prefer"),
            pool_size=int(os.getenv("TELLUS_DB_POOL_SIZE", "5")),
            max_overflow=int(os.getenv("TELLUS_DB_MAX_OVERFLOW", "10")),
            echo=os.getenv("TELLUS_DB_ECHO", "false").lower() == "true",
            sqlite_path=os.getenv("TELLUS_SQLITE_PATH"),
        )

    @classmethod
    def for_sqlite(cls, path: Optional[str] = None, **kwargs) -> "DatabaseConfig":
        """Create SQLite configuration."""
        config = cls(database_type="sqlite", sqlite_path=path, **kwargs)
        logger.info(f"Using SQLite database at: {config.sqlite_path}")
        return config

    @classmethod
    def from_url(cls, database_url: str, **kwargs) -> "DatabaseConfig":
        """Create configuration from database URL."""
        # Parse URL and return config
        # For now, just accept URL as-is and set other defaults
        config = cls.from_env()
        config.database_url = database_url
        for key, value in kwargs.items():
            setattr(config, key, value)
        return config

    def get_database_url(self) -> str:
        """Construct database URL for SQLAlchemy."""
        if hasattr(self, 'database_url'):
            return self.database_url

        if self.database_type == "sqlite":
            return f"sqlite+aiosqlite:///{self.sqlite_path}"

        elif self.database_type == "postgresql":
            # Handle password encoding
            if self.password:
                encoded_password = quote_plus(self.password)
                auth = f"{self.username}:{encoded_password}"
            else:
                auth = self.username

            return (
                f"postgresql+psycopg://{auth}@{self.host}:{self.port}/{self.database}"
                f"?sslmode={self.ssl_mode}"
            )

        else:
            raise ValueError(f"Unsupported database type: {self.database_type}")


class DatabaseManager:
    """
    Database connection and session management.

    Handles engine creation, session lifecycle, and connection pooling
    for async database operations. Automatically falls back from PostgreSQL
    to SQLite if PostgreSQL is unavailable.
    """

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._engine = None
        self._session_factory = None

    @property
    def engine(self):
        """Get or create the async database engine."""
        if self._engine is None:
            # Configure engine parameters based on database type
            engine_params = {
                "echo": self.config.echo,
            }

            if self.config.database_type == "postgresql":
                # PostgreSQL supports connection pooling
                engine_params.update({
                    "pool_size": self.config.pool_size,
                    "max_overflow": self.config.max_overflow,
                })
                # Use NullPool only for testing
                if os.getenv("TESTING"):
                    engine_params["poolclass"] = NullPool
            else:
                # SQLite uses NullPool by default and doesn't support pool parameters
                engine_params["poolclass"] = NullPool

            self._engine = create_async_engine(
                self.config.get_database_url(),
                **engine_params
            )
        return self._engine

    @property
    def session_factory(self):
        """Get or create the session factory."""
        if self._session_factory is None:
            self._session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )
        return self._session_factory

    async def create_tables(self):
        """Create all database tables."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def drop_tables(self):
        """Drop all database tables."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

    def get_session(self):
        """Get an async database session context manager."""
        return self.session_factory()

    async def close(self):
        """Close the database engine."""
        if self._engine:
            await self._engine.dispose()


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def _test_postgres_connection(config: DatabaseConfig) -> bool:
    """Test PostgreSQL connection synchronously using a simple connection test."""
    if config.database_type != "postgresql":
        return False

    try:
        import psycopg
        # Try a simple sync connection test
        conn_str = config.get_database_url().replace("postgresql+psycopg://", "postgresql://")
        with psycopg.connect(conn_str, connect_timeout=3) as conn:
            pass  # Just test the connection
        return True
    except Exception as e:
        logger.debug(f"PostgreSQL connection test failed: {e}")
        return False


def get_database_manager(config: Optional[DatabaseConfig] = None) -> DatabaseManager:
    """Get the global database manager instance with automatic PostgreSQL->SQLite fallback."""
    global _db_manager

    if _db_manager is None:
        if config is None:
            config = DatabaseConfig.from_env()

        # Only attempt fallback in non-testing environments
        if not os.getenv("TESTING") and config.database_type == "postgresql":
            # Test PostgreSQL connection
            if not _test_postgres_connection(config):
                logger.warning("PostgreSQL connection failed, falling back to local SQLite database")
                config = DatabaseConfig.for_sqlite(echo=config.echo)
            else:
                logger.info("Using PostgreSQL database")
        elif config.database_type == "sqlite":
            logger.info(f"Using SQLite database at: {config.sqlite_path}")

        _db_manager = DatabaseManager(config)

    return _db_manager


def set_database_manager(manager: DatabaseManager):
    """Set the global database manager instance."""
    global _db_manager
    _db_manager = manager


def reset_database_manager():
    """Reset the global database manager instance for testing."""
    global _db_manager
    _db_manager = None


def get_session():
    """Get a database session from the global manager."""
    manager = get_database_manager()
    return manager.get_session()