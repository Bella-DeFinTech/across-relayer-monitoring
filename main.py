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

from src.calculate_daily_profits import calculate_daily_profits
from src.collect_fills import collect_fills
from src.config import setup_logging
from src.discover_routes import discover_routes
from src.enrich_fills import enrich_fills
from src.init_db import init_db
from src.process_bundles import process_bundles
from src.process_repayments import process_repayments
from src.process_returns import process_returns
from src.reporting_utils import generate_reports
from src.update_token_prices import update_token_prices

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)


def main():
    """
    Main function to run the relayer monitoring process.
    """

    try:
        init_db()  # Initialize database if it doesn't exist
        discover_routes()  # Discover routes and update the database
        # collect_fills()  # Collect fills and update the database
        # enrich_fills()  # Enrich fills with deposit timestamps and LP fees
        # process_returns()  # Process returns and update the database
        # process_bundles()  # Process bundles and update the database
        # process_repayments()  # Process bundle repayments and update the database
        # update_token_prices()  # Update token prices in the database
        # calculate_daily_profits()  # Calculate daily profits
        # generate_reports()  # Generate reports

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
