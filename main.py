#!/usr/bin/env python3
"""
Main entry point for the relayer refactor project.

This script orchestrates the entire process flow, including:
- Database initialization
- Route discovery
- Fill collection
- Return processing
- Bundle processing
- Metrics calculation
- Report generation
"""

import logging
import sys

from discover_routes import discover_routes
from init_db import init_db

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """
    Main function to run the relayer monitoring process.
    """

    try:
        # Initialize database if it doesn't exist
        logger.info("Checking database")
        init_db()

        # Discover routes and update the database
        logger.info("Discovering routes")
        discover_routes()

        # TODO: Add other process steps here as they are implemented
        # - Fill collection
        # - Return processing
        # - Bundle processing
        # - Metrics calculation
        # - Report generation

        logger.info("Process completed successfully")

    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
