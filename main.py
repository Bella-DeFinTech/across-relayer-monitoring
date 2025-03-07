#!/usr/bin/env python3
"""
Main entry point for the relayer monitoring system.

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

from src.collect_fills import collect_fills
from src.discover_routes import discover_routes
from src.init_db import init_db

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

        # Collect fills and update the database
        logger.info("Collecting fills")
        collect_fills()

        # Process returns and update the database
        # logger.info("Processing returns")
        # process_returns()

        # Process bundles and update the database
        # logger.info("Processing bundles")
        # process_bundles()

        # Calculate profit metrics
        # logger.info("Calculating profit metrics")
        # calculate_profit_metrics()

        # Generate reports
        # logger.info("Generating reports")
        # generate_reports()

        logger.info("Process completed successfully")

    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
