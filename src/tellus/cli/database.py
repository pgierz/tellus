"""Database management CLI commands."""

import asyncio
import sys
from pathlib import Path

import rich_click as click
from ..core.cli import console

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))


@click.group()
def database():
    """Database management commands."""
    pass


@database.command()
@click.option(
    "--database-url", "-d",
    help="Database URL (default: from environment variables)",
    envvar="TELLUS_DB_URL"
)
@click.option(
    "--drop-tables",
    is_flag=True,
    help="Drop existing tables before creating new ones"
)
def init(database_url, drop_tables):
    """Initialize the Tellus database with required tables."""

    async def run_init():
        try:
            from ...infrastructure.database.config import DatabaseConfig, DatabaseManager

            # Setup database configuration
            if database_url:
                db_config = DatabaseConfig.from_url(database_url)
            else:
                db_config = DatabaseConfig.from_env()

            console.print(f"[blue]Connecting to database:[/blue] {db_config.get_database_url()}")

            db_manager = DatabaseManager(db_config)

            try:
                if drop_tables:
                    console.print("[yellow]Dropping existing tables...[/yellow]")
                    await db_manager.drop_tables()

                console.print("[green]Creating database tables...[/green]")
                await db_manager.create_tables()

                console.print("[green]✅ Database initialized successfully![/green]")

            finally:
                await db_manager.close()

        except Exception as e:
            console.print(f"[red]❌ Database initialization failed:[/red] {e}")
            sys.exit(1)

    asyncio.run(run_init())


@database.command()
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
    help="Database URL (default: from environment variables)",
    envvar="TELLUS_DB_URL"
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be migrated without making changes"
)
def migrate_from_json(simulations_file, locations_file, database_url, dry_run):
    """Migrate data from JSON files to PostgreSQL database."""

    # Import and run the migration script
    script_path = Path(__file__).parent.parent.parent.parent.parent / "scripts" / "migrate_json_to_db.py"

    cmd = [sys.executable, str(script_path)]

    if simulations_file:
        cmd.extend(["-s", str(simulations_file)])
    if locations_file:
        cmd.extend(["-l", str(locations_file)])
    if database_url:
        cmd.extend(["-d", database_url])
    if dry_run:
        cmd.append("--dry-run")

    import subprocess
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


@database.command()
@click.option(
    "--database-url", "-d",
    help="Database URL (default: from environment variables)",
    envvar="TELLUS_DB_URL"
)
def status(database_url):
    """Check database status and show table information."""

    async def run_status():
        try:
            from ...infrastructure.database.config import DatabaseConfig, DatabaseManager
            from ...infrastructure.repositories.postgres_simulation_repository import PostgresSimulationRepository
            from ...infrastructure.repositories.postgres_location_repository import PostgresLocationRepository

            # Setup database configuration
            if database_url:
                db_config = DatabaseConfig.from_url(database_url)
            else:
                db_config = DatabaseConfig.from_env()

            console.print(f"[blue]Database URL:[/blue] {db_config.get_database_url()}")

            db_manager = DatabaseManager(db_config)

            try:
                async with db_manager.get_session() as session:
                    sim_repo = PostgresSimulationRepository(session)
                    loc_repo = PostgresLocationRepository(session)

                    sim_count = await sim_repo.count()
                    loc_count = await loc_repo.count()

                    console.print(f"[green]✅ Database connection successful[/green]")
                    console.print(f"[blue]Simulations:[/blue] {sim_count}")
                    console.print(f"[blue]Locations:[/blue] {loc_count}")

            finally:
                await db_manager.close()

        except Exception as e:
            console.print(f"[red]❌ Database connection failed:[/red] {e}")
            sys.exit(1)

    asyncio.run(run_status())


if __name__ == "__main__":
    database()