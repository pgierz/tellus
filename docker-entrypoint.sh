#!/bin/bash
set -e

echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h "$TELLUS_DB_HOST" -U "$TELLUS_DB_USER" -d "$TELLUS_DB_NAME" > /dev/null 2>&1; do
    echo "PostgreSQL is unavailable - sleeping"
    sleep 2
done

echo "PostgreSQL is up - creating database schema..."
python -c "
from sqlalchemy import create_engine
from tellus.infrastructure.database.models import Base
import os

database_url = os.getenv('DATABASE_URL', 'postgresql://tellus:tellus@postgres:5432/tellus')
print(f'Connecting to: {database_url}')

engine = create_engine(database_url)
Base.metadata.create_all(engine)
print('✓ Database schema created successfully')
"

echo "Starting Tellus API..."
exec uvicorn tellus.interfaces.web.main:app --host 0.0.0.0 --port 1968
