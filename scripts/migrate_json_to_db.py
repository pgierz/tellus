#!/usr/bin/env python3
"""
Migration script to transfer data from JSON files to PostgreSQL database.

This script reads existing simulations.json and locations.json files and
migrates all data to the new PostgreSQL-based persistence system.
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

import click
from loguru import logger

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tellus.infrastructure.database.config import DatabaseConfig, DatabaseManager
from tellus.infrastructure.database.models import Base
from tellus.infrastructure.repositories.postgres_simulation_repository import PostgresSimulationRepository
from tellus.infrastructure.repositories.postgres_location_repository import PostgresLocationRepository
from tellus.domain.entities.simulation import SimulationEntity
from tellus.domain.entities.location import LocationEntity, LocationKind


class JSONToPostgresMigrator:
    """Handles migration from JSON files to PostgreSQL database."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.simulation_repo = None
        self.location_repo = None

    async def setup_repositories(self):
        """Initialize repositories with database session."""
        async with self.db_manager.get_session() as session:
            self.simulation_repo = PostgresSimulationRepository(session)
            self.location_repo = PostgresLocationRepository(session)

    async def migrate_all(
        self,
        simulations_file: Optional[Path] = None,
        locations_file: Optional[Path] = None,
        dry_run: bool = False
    ) -> bool:
        """
        Migrate all data from JSON files to database.

        Args:
            simulations_file: Path to simulations.json (defaults to .tellus/simulations.json)
            locations_file: Path to locations.json (defaults to .tellus/locations.json)
            dry_run: If True, only show what would be migrated without making changes

        Returns:
            True if migration succeeded, False otherwise
        """
        try:
            # Set default file paths
            if simulations_file is None:
                simulations_file = Path(".tellus/simulations.json")
            if locations_file is None:
                locations_file = Path(".tellus/locations.json")

            # Create tables if they don't exist
            if not dry_run:
                await self.db_manager.create_tables()
                logger.info("Database tables created/verified")

            # Setup repositories
            await self.setup_repositories()

            # Migrate locations first (simulations reference them)
            locations_migrated = await self.migrate_locations(locations_file, dry_run)

            # Migrate simulations
            simulations_migrated = await self.migrate_simulations(simulations_file, dry_run)

            logger.success(
                f"Migration completed: {locations_migrated} locations, {simulations_migrated} simulations"
            )
            return True

        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False

    async def migrate_simulations(self, json_file: Path, dry_run: bool = False) -> int:
        """Migrate simulations from JSON file to database."""
        if not json_file.exists():
            logger.warning(f"Simulations file not found: {json_file}")
            return 0

        try:
            with open(json_file, 'r') as f:
                data = json.load(f)

            if not data:
                logger.info("No simulations to migrate")
                return 0

            migrated_count = 0
            async with self.db_manager.get_session() as session:
                sim_repo = PostgresSimulationRepository(session)

                for sim_id, sim_data in data.items():
                    try:
                        entity = self._json_to_simulation_entity(sim_data)

                        if dry_run:
                            logger.info(f"Would migrate simulation: {entity.simulation_id}")
                        else:
                            await sim_repo.save(entity)
                            logger.info(f"Migrated simulation: {entity.simulation_id}")

                        migrated_count += 1

                    except Exception as e:
                        logger.error(f"Failed to migrate simulation {sim_id}: {e}")

                if not dry_run:
                    await session.commit()

            return migrated_count

        except Exception as e:
            logger.error(f"Failed to read simulations file {json_file}: {e}")
            return 0

    async def migrate_locations(self, json_file: Path, dry_run: bool = False) -> int:
        """Migrate locations from JSON file to database."""
        if not json_file.exists():
            logger.warning(f"Locations file not found: {json_file}")
            return 0

        try:
            with open(json_file, 'r') as f:
                data = json.load(f)

            if not data:
                logger.info("No locations to migrate")
                return 0

            migrated_count = 0
            async with self.db_manager.get_session() as session:
                loc_repo = PostgresLocationRepository(session)

                for loc_name, loc_data in data.items():
                    try:
                        entity = self._json_to_location_entity(loc_name, loc_data)

                        if dry_run:
                            logger.info(f"Would migrate location: {entity.name}")
                        else:
                            await loc_repo.save(entity)
                            logger.info(f"Migrated location: {entity.name}")

                        migrated_count += 1

                    except Exception as e:
                        logger.error(f"Failed to migrate location {loc_name}: {e}")

                if not dry_run:
                    await session.commit()

            return migrated_count

        except Exception as e:
            logger.error(f"Failed to read locations file {json_file}: {e}")
            return 0

    def _json_to_simulation_entity(self, json_data: Dict[str, Any]) -> SimulationEntity:
        """Convert JSON simulation data to SimulationEntity."""
        # Extract location contexts from the locations field
        location_contexts = json_data.get("locations", {})
        associated_locations = set(location_contexts.keys())

        return SimulationEntity(
            simulation_id=json_data["simulation_id"],
            model_id=json_data.get("model_id"),
            path=json_data.get("path"),
            attrs=json_data.get("attributes", {}),
            namelists=json_data.get("namelists", {}),
            snakemakes=json_data.get("workflows", json_data.get("snakemakes", {})),
            associated_locations=associated_locations,
            location_contexts=location_contexts,
        )

    def _json_to_location_entity(self, name: str, json_data: Dict[str, Any]) -> LocationEntity:
        """Convert JSON location data to LocationEntity."""
        # Parse kinds from string list
        kind_strings = json_data.get("kinds", ["DISK"])  # Default to DISK
        kinds = [LocationKind.from_str(k) for k in kind_strings]

        return LocationEntity(
            name=name,
            kinds=kinds,
            protocol=json_data.get("protocol", "file"),  # Default to file
            path=json_data.get("path"),
            storage_options=json_data.get("storage_options", {}),
            additional_config=json_data.get("additional_config", {}),
            is_remote=json_data.get("is_remote", False),
            is_accessible=json_data.get("is_accessible", True),
            last_verified=None,  # Will be set when first tested
            path_templates=[],  # Can be added later
        )

    async def verify_migration(self) -> bool:
        """Verify that the migration was successful by checking data in database."""
        try:
            async with self.db_manager.get_session() as session:
                sim_repo = PostgresSimulationRepository(session)
                loc_repo = PostgresLocationRepository(session)

                sim_count = await sim_repo.count()
                loc_count = await loc_repo.count()

                logger.info(f"Database verification: {sim_count} simulations, {loc_count} locations")
                return True

        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return False


@click.command()
@click.option(
    "--simulations-file", "-s",
    type=click.Path(exists=True, path_type=Path),
    help="Path to simulations.json file (default: .tellus/simulations.json)"
)
@click.option(
    "--locations-file", "-l",
    type=click.Path(exists=True, path_type=Path),
    help="Path to locations.json file (default: .tellus/locations.json)"
)
@click.option(
    "--database-url", "-d",
    help="Database URL (default: from environment variables)"
)
@click.option(
    "--dry-run", "--preview",
    is_flag=True,
    help="Show what would be migrated without making changes"
)
@click.option(
    "--verify", "-v",
    is_flag=True,
    help="Verify migration after completion"
)
def main(simulations_file, locations_file, database_url, dry_run, verify):
    """Migrate Tellus data from JSON files to PostgreSQL database."""

    async def run_migration():
        # Setup database configuration
        if database_url:
            db_config = DatabaseConfig.from_url(database_url)
        else:
            db_config = DatabaseConfig.from_env()

        db_manager = DatabaseManager(db_config)
        migrator = JSONToPostgresMigrator(db_manager)

        try:
            # Run the migration
            success = await migrator.migrate_all(
                simulations_file=simulations_file,
                locations_file=locations_file,
                dry_run=dry_run
            )

            if success and verify and not dry_run:
                await migrator.verify_migration()

            return success

        finally:
            await db_manager.close()

    # Run the async migration
    success = asyncio.run(run_migration())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()