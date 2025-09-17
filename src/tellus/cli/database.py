"""Database management CLI commands."""

import asyncio
import sys
from pathlib import Path

import rich_click as click
from ..interfaces.cli.main import console


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
            from tellus.infrastructure.database.config import get_database_manager, reset_database_manager, DatabaseConfig

            # Use fallback logic unless specific URL provided
            if database_url:
                config = DatabaseConfig.from_url(database_url)
                from tellus.infrastructure.database.config import DatabaseManager
                db_manager = DatabaseManager(config)
            else:
                # Reset any cached manager to ensure fresh fallback logic
                reset_database_manager()
                db_manager = get_database_manager()
                config = db_manager.config

            console.print(f"[blue]Connecting to database:[/blue] {config.get_database_url()}")

            if config.database_type == "sqlite":
                console.print("[green]Using local SQLite database[/green]")
            elif config.database_type == "postgresql":
                console.print("[green]Using PostgreSQL database[/green]")

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
    help="Database URL (default: from environment variables or fallback to SQLite)",
    envvar="TELLUS_DB_URL"
)
def status(database_url):
    """Show current database configuration and status."""

    async def run_status():
        try:
            from tellus.infrastructure.database.config import get_database_manager, reset_database_manager, DatabaseConfig

            # Use fallback logic unless specific URL provided
            if database_url:
                config = DatabaseConfig.from_url(database_url)
                from tellus.infrastructure.database.config import DatabaseManager
                db_manager = DatabaseManager(config)
            else:
                # Reset any cached manager to ensure fresh fallback logic
                reset_database_manager()
                db_manager = get_database_manager()
                config = db_manager.config

            # Show database configuration
            console.print("[bold]Database Status[/bold]")
            console.print("─" * 40)

            if config.database_type == "sqlite":
                console.print(f"[green]Type:[/green] SQLite (Local)")
                console.print(f"[green]Path:[/green] {config.sqlite_path}")

                # Check if file exists
                import os
                if os.path.exists(config.sqlite_path):
                    file_size = os.path.getsize(config.sqlite_path)
                    console.print(f"[green]Size:[/green] {file_size:,} bytes")
                    console.print("[green]Status:[/green] ✅ Database file exists")
                else:
                    console.print("[yellow]Status:[/yellow] ⚠️  Database file not yet created")

            elif config.database_type == "postgresql":
                console.print(f"[green]Type:[/green] PostgreSQL (Remote)")
                console.print(f"[green]Host:[/green] {config.host}:{config.port}")
                console.print(f"[green]Database:[/green] {config.database}")
                console.print(f"[green]User:[/green] {config.username}")

            console.print(f"[green]URL:[/green] {config.get_database_url()}")
            console.print()

            # Show table status if possible
            try:
                from tellus.infrastructure.repositories.postgres_simulation_repository import PostgresSimulationRepository
                from tellus.infrastructure.repositories.postgres_location_repository import PostgresLocationRepository

                async with db_manager.get_session() as session:
                    sim_repo = PostgresSimulationRepository(session)
                    loc_repo = PostgresLocationRepository(session)

                    sim_count = await sim_repo.count()
                    loc_count = await loc_repo.count()

                    console.print(f"[green]✅ Database connection successful[/green]")
                    console.print(f"[blue]Simulations:[/blue] {sim_count}")
                    console.print(f"[blue]Locations:[/blue] {loc_count}")

            except Exception as db_error:
                console.print(f"[yellow]⚠️  Database accessible but couldn't query tables: {db_error}[/yellow]")
                console.print("[dim]Run 'tellus database init' if tables don't exist yet[/dim]")

            finally:
                await db_manager.close()

        except Exception as e:
            console.print(f"[red]❌ Database status check failed:[/red] {e}")
            sys.exit(1)

    asyncio.run(run_status())


@database.command()
@click.option(
    "--output-file", "-o",
    type=click.Path(path_type=Path),
    required=True,
    help="Output file for backup (will be compressed with gzip)"
)
@click.option(
    "--database-url", "-d",
    help="Database URL (default: from environment variables)",
    envvar="TELLUS_DB_URL"
)
@click.option(
    "--include-system-tables",
    is_flag=True,
    help="Include system tables in backup"
)
def backup(output_file, database_url, include_system_tables):
    """Create a backup of the Tellus database."""
    import subprocess
    import gzip
    import tempfile

    try:
        from tellus.infrastructure.database.config import DatabaseConfig

        # Setup database configuration
        if database_url:
            db_config = DatabaseConfig.from_url(database_url)
        else:
            db_config = DatabaseConfig.from_env()

        console.print(f"[blue]Creating backup of database...[/blue]")

        # Prepare pg_dump command
        pg_dump_cmd = [
            "pg_dump",
            "--host", db_config.host,
            "--port", str(db_config.port),
            "--username", db_config.username,
            "--dbname", db_config.database,
            "--verbose",
            "--clean",  # Include DROP statements
            "--if-exists",  # Use IF EXISTS for DROP statements
        ]

        if not include_system_tables:
            # Exclude system tables and focus on tellus schema
            pg_dump_cmd.extend([
                "--exclude-table-data", "pg_*",
                "--exclude-table-data", "information_schema.*"
            ])

        # Set password via environment
        env = {"PGPASSWORD": db_config.password} if db_config.password else {}

        # Create temporary file for uncompressed backup
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as temp_file:
            console.print("[blue]Running pg_dump...[/blue]")

            # Run pg_dump
            result = subprocess.run(
                pg_dump_cmd,
                stdout=temp_file,
                stderr=subprocess.PIPE,
                env=env,
                text=False
            )

            if result.returncode != 0:
                console.print(f"[red]pg_dump failed:[/red] {result.stderr.decode()}")
                sys.exit(1)

            temp_file_path = temp_file.name

        # Compress the backup
        console.print(f"[blue]Compressing backup to {output_file}...[/blue]")

        with open(temp_file_path, 'rb') as f_in:
            with gzip.open(output_file, 'wb') as f_out:
                f_out.writelines(f_in)

        # Clean up temp file
        Path(temp_file_path).unlink()

        # Get file size
        backup_size = output_file.stat().st_size
        size_mb = backup_size / (1024 * 1024)

        console.print(f"[green]✅ Backup created successfully![/green]")
        console.print(f"[blue]File:[/blue] {output_file}")
        console.print(f"[blue]Size:[/blue] {size_mb:.1f} MB")

    except Exception as e:
        console.print(f"[red]❌ Backup failed:[/red] {e}")
        sys.exit(1)


@database.command()
@click.option(
    "--input-file", "-i",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Input backup file (gzipped SQL dump)"
)
@click.option(
    "--database-url", "-d",
    help="Database URL (default: from environment variables)",
    envvar="TELLUS_DB_URL"
)
@click.option(
    "--drop-existing",
    is_flag=True,
    help="Drop existing tables before restore (dangerous!)"
)
@click.confirmation_option(
    prompt="Are you sure you want to restore the database? This will modify existing data."
)
def restore(input_file, database_url, drop_existing):
    """Restore the Tellus database from a backup."""
    import subprocess
    import gzip
    import tempfile

    try:
        from tellus.infrastructure.database.config import DatabaseConfig

        # Setup database configuration
        if database_url:
            db_config = DatabaseConfig.from_url(database_url)
        else:
            db_config = DatabaseConfig.from_env()

        console.print(f"[yellow]⚠️  Restoring database from backup...[/yellow]")

        # Decompress backup to temporary file
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False, suffix='.sql') as temp_file:
            console.print("[blue]Decompressing backup...[/blue]")

            with gzip.open(input_file, 'rb') as f_in:
                temp_file.writelines(f_in)

            temp_file_path = temp_file.name

        # Prepare psql command
        psql_cmd = [
            "psql",
            "--host", db_config.host,
            "--port", str(db_config.port),
            "--username", db_config.username,
            "--dbname", db_config.database,
            "--file", temp_file_path,
            "--verbose"
        ]

        if drop_existing:
            console.print("[yellow]⚠️  Dropping existing tables...[/yellow]")

        # Set password via environment
        env = {"PGPASSWORD": db_config.password} if db_config.password else {}

        console.print("[blue]Running psql restore...[/blue]")

        # Run psql
        result = subprocess.run(
            psql_cmd,
            stderr=subprocess.PIPE,
            env=env,
            text=True
        )

        # Clean up temp file
        Path(temp_file_path).unlink()

        if result.returncode != 0:
            console.print(f"[red]psql restore failed:[/red] {result.stderr}")
            sys.exit(1)

        console.print(f"[green]✅ Database restored successfully![/green]")

        # Show status after restore
        console.print("\n[blue]Post-restore status:[/blue]")
        asyncio.run(_show_status(db_config))

    except Exception as e:
        console.print(f"[red]❌ Restore failed:[/red] {e}")
        sys.exit(1)


async def _show_status(db_config):
    """Helper function to show database status."""
    try:
        from tellus.infrastructure.database.config import DatabaseManager
        from tellus.infrastructure.repositories.postgres_simulation_repository import PostgresSimulationRepository
        from tellus.infrastructure.repositories.postgres_location_repository import PostgresLocationRepository

        db_manager = DatabaseManager(db_config)

        try:
            async with db_manager.get_session() as session:
                sim_repo = PostgresSimulationRepository(session)
                loc_repo = PostgresLocationRepository(session)

                sim_count = await sim_repo.count()
                loc_count = await loc_repo.count()

                console.print(f"[blue]Simulations:[/blue] {sim_count}")
                console.print(f"[blue]Locations:[/blue] {loc_count}")

        finally:
            await db_manager.close()

    except Exception as e:
        console.print(f"[dim]Could not get post-restore status: {e}[/dim]")


if __name__ == "__main__":
    database()