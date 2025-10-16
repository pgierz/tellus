#!/usr/bin/env python3
"""
Generate a secure API key for Tellus REST API.

This script generates a cryptographically secure random API key
that can be used with the TELLUS_API_KEY environment variable.

Usage:
    python scripts/generate-api-key.py
    python scripts/generate-api-key.py --length 64
"""

import argparse
import secrets


def generate_api_key(length: int = 32) -> str:
    """
    Generate a cryptographically secure random API key.

    Args:
        length: Length of the API key in bytes (default: 32 bytes = 64 hex characters)

    Returns:
        A random API key as a hexadecimal string
    """
    return secrets.token_hex(length)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate a secure API key for Tellus REST API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate a standard API key (64 characters)
  python scripts/generate-api-key.py

  # Generate a longer API key (128 characters)
  python scripts/generate-api-key.py --length 64

  # Add to .env file
  echo "TELLUS_API_KEY=$(python scripts/generate-api-key.py)" >> .env
        """
    )

    parser.add_argument(
        "--length",
        type=int,
        default=32,
        help="Length of the API key in bytes (default: 32 bytes = 64 hex characters)"
    )

    args = parser.parse_args()

    # Validate length
    if args.length < 16:
        parser.error("Length must be at least 16 bytes (32 hex characters) for security")

    # Generate and print the key
    api_key = generate_api_key(args.length)
    print(api_key)


if __name__ == "__main__":
    main()
