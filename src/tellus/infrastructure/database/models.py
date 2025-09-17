"""
SQLAlchemy database models for Tellus domain entities.

These models provide the database representation of SimulationEntity and LocationEntity,
with proper relational mappings and constraints.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from sqlalchemy import Column, DateTime, ForeignKey, String, Text, Table, JSON, Boolean
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql import func

Base = declarative_base()


# Association table for simulation-location many-to-many relationship
simulation_location_association = Table(
    'simulation_location_associations',
    Base.metadata,
    Column('simulation_id', String, ForeignKey('simulations.simulation_id'), primary_key=True),
    Column('location_name', String, ForeignKey('locations.name'), primary_key=True),
    Column('created_at', DateTime(timezone=True), server_default=func.now()),
    Column('location_context', JSON, nullable=True)  # Store location-specific context
)


class SimulationModel(Base):
    """SQLAlchemy model for SimulationEntity."""

    __tablename__ = 'simulations'

    # Primary identifiers
    simulation_id: Mapped[str] = mapped_column(String, primary_key=True)
    uid: Mapped[str] = mapped_column(UUID(as_uuid=False), unique=True, default=lambda: str(uuid4()))

    # Core simulation attributes
    model_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    path: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # JSON storage for flexible attributes
    attrs: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    namelists: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    workflows: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)  # Previously snakemakes

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )

    # Relationships
    associated_locations = relationship(
        "LocationModel",
        secondary=simulation_location_association,
        back_populates="associated_simulations",
        lazy="selectin"
    )

    def __repr__(self) -> str:
        return f"<SimulationModel(simulation_id='{self.simulation_id}', model_id='{self.model_id}')>"


class LocationModel(Base):
    """SQLAlchemy model for LocationEntity."""

    __tablename__ = 'locations'

    # Primary identifier
    name: Mapped[str] = mapped_column(String, primary_key=True)

    # Core location attributes
    kinds: Mapped[List[str]] = mapped_column(JSON, nullable=False)
    protocol: Mapped[str] = mapped_column(String, nullable=False)
    path: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Configuration storage
    storage_options: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    additional_config: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    # Status and metadata
    is_remote: Mapped[bool] = mapped_column(Boolean, default=False)
    is_accessible: Mapped[bool] = mapped_column(Boolean, default=True)
    last_verified: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Path templates (stored as JSON array)
    path_templates: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )

    # Relationships
    associated_simulations = relationship(
        "SimulationModel",
        secondary=simulation_location_association,
        back_populates="associated_locations",
        lazy="selectin"
    )

    def __repr__(self) -> str:
        return f"<LocationModel(name='{self.name}', protocol='{self.protocol}')>"


class SimulationLocationContextModel(Base):
    """
    Model for storing location-specific contexts for simulations.

    This provides a more structured way to store the location context data
    that was previously embedded in the simulation's location_contexts field.
    """

    __tablename__ = 'simulation_location_contexts'

    # Composite primary key
    simulation_id: Mapped[str] = mapped_column(
        String,
        ForeignKey('simulations.simulation_id', ondelete='CASCADE'),
        primary_key=True
    )
    location_name: Mapped[str] = mapped_column(
        String,
        ForeignKey('locations.name', ondelete='CASCADE'),
        primary_key=True
    )

    # Context data specific to this simulation-location pair
    context_data: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )

    # Relationships
    simulation = relationship("SimulationModel", lazy="selectin")
    location = relationship("LocationModel", lazy="selectin")

    def __repr__(self) -> str:
        return f"<SimulationLocationContextModel(simulation_id='{self.simulation_id}', location_name='{self.location_name}')>"