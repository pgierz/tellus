"""
Database configuration and connection management.

Provides database URL construction, connection pooling, and session management
for PostgreSQL using SQLAlchemy 2.0 async patterns.
"""

import os
from typing import Optional, AsyncGenerator
from urllib.parse import quote_plus

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from .models import Base


class DatabaseConfig:
    """Database configuration management."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "tellus",
        username: str = "tellus",
        password: Optional[str] = None,
        ssl_mode: str = "prefer",
        pool_size: int = 5,
        max_overflow: int = 10,
        echo: bool = False,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.ssl_mode = ssl_mode
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.echo = echo

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create configuration from environment variables."""
        return cls(
            host=os.getenv("TELLUS_DB_HOST", "localhost"),
            port=int(os.getenv("TELLUS_DB_PORT", "5432")),
            database=os.getenv("TELLUS_DB_NAME", "tellus"),
            username=os.getenv("TELLUS_DB_USER", "tellus"),
            password=os.getenv("TELLUS_DB_PASSWORD"),
            ssl_mode=os.getenv("TELLUS_DB_SSL_MODE", "prefer"),
            pool_size=int(os.getenv("TELLUS_DB_POOL_SIZE", "5")),
            max_overflow=int(os.getenv("TELLUS_DB_MAX_OVERFLOW", "10")),
            echo=os.getenv("TELLUS_DB_ECHO", "false").lower() == "true",
        )

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
        """Construct database URL for psycopg."""
        if hasattr(self, 'database_url'):
            return self.database_url

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


class DatabaseManager:
    """
    Database connection and session management.

    Handles engine creation, session lifecycle, and connection pooling
    for async database operations.
    """

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._engine = None
        self._session_factory = None

    @property
    def engine(self):
        """Get or create the async database engine."""
        if self._engine is None:
            self._engine = create_async_engine(
                self.config.get_database_url(),
                echo=self.config.echo,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                # Use NullPool for testing to avoid connection issues
                poolclass=NullPool if os.getenv("TESTING") else None,
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

    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get an async database session."""
        async with self.session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def close(self):
        """Close the database engine."""
        if self._engine:
            await self._engine.dispose()


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager(config: Optional[DatabaseConfig] = None) -> DatabaseManager:
    """Get the global database manager instance."""
    global _db_manager

    if _db_manager is None:
        if config is None:
            config = DatabaseConfig.from_env()
        _db_manager = DatabaseManager(config)

    return _db_manager


def set_database_manager(manager: DatabaseManager):
    """Set the global database manager instance."""
    global _db_manager
    _db_manager = manager


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Get a database session from the global manager."""
    manager = get_database_manager()
    async with manager.get_session() as session:
        yield session