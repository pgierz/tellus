"""Main application entry point for Tellus."""


def main():
    """Entry point for the Tellus application script."""
    # Import the CLI orchestrator from interfaces
    from ..interfaces.cli.main import create_main_cli

    # Create and run the CLI
    cli = create_main_cli()
    cli()


if __name__ == "__main__":
    main()