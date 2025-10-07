#!/usr/bin/env python
"""
Create Tellus database schema.

This script creates all tables defined in SQLAlchemy models,
including the new workflows table.
"""

from sqlalchemy import create_engine
from src.tellus.infrastructure.database.models import Base

# Database connection
DATABASE_URL = "postgresql://tellus:tellus@localhost:5432/tellus"

print("Connecting to database...")
engine = create_engine(DATABASE_URL, echo=True)

print("\nCreating all tables from models...")
Base.metadata.create_all(engine)

print("\n✓ Database schema created successfully!")
print("\nTables created:")
for table_name in Base.metadata.tables.keys():
    print(f"  - {table_name}")

print("\nConnection details:")
print(f"  URL: {DATABASE_URL}")
print(f"  Database: tellus")
print(f"  User: tellus")
print(f"  Host: localhost:5432")
