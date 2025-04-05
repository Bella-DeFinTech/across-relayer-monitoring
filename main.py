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
from src.enrich_fills import enrich_fills
from src.init_db import init_db
from src.process_bundles import process_bundles
from src.process_repayments import process_repayments
from src.process_returns import process_returns
from src.reporting_utils import generate_reports
from src.update_token_prices import update_token_prices

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
        # # Initialize database if it doesn't exist
        # logger.info("=" * 80)
        # logger.info("Checking database")
        # init_db()

        # # Discover routes and update the database
        # logger.info("=" * 80)
        # logger.info("Discovering routes")
        # discover_routes()

        # # Collect fills and update the database
        # logger.info("=" * 80)
        # logger.info("Collecting fills")
        # collect_fills()

        # # Enrich fills with deposit timestamps and LP fees
        # logger.info("=" * 80)
        # logger.info("Enriching fills with deposit timestamps and LP fees")
        # enrich_fills()

        # # Process returns and update the database
        # logger.info("=" * 80)
        # logger.info("Processing returns")
        # process_returns()

        # # Process bundles and update the database
        # logger.info("=" * 80)
        # logger.info("Processing bundles")
        # process_bundles()

        # # Update token prices in the database
        # logger.info("=" * 80)
        # logger.info("Updating token prices")
        # update_token_prices()

        # # Process bundle repayments and update the database
        # logger.info("=" * 80)
        # logger.info("Processing bundle repayments")
        # process_repayments()


        # Update token prices in the database
        logger.info("=" * 80)
        logger.info("Updating token prices")
        update_token_prices()

        # Calculate profit metrics
        # logger.info("Calculating profit metrics")
        # calculate_profit_metrics()

        # # Generate reports
        # logger.info("=" * 80)
        # logger.info("Generating reports")
        # generate_reports()

        logger.info("Process completed successfully")

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
