"""Script to update historical token prices in the database.

This script fetches and stores USD prices for all configured tokens from
the earliest Ethereum block to today. It skips dates where prices are
already stored to avoid duplicate API calls.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import requests

from .config import CHAINS, COINGECKO_KEY, COINGECKO_SYMBOL_MAP
from .db_utils import get_db_connection
from .web3_utils import get_block_timestamp

# Configure logging
logger = logging.getLogger(__name__)


def _get_price_from_api(coingecko_id: str, date_str: str) -> Optional[Decimal]:
    """Get historical price from CoinGecko API with retries."""
    url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/history"
    params = {"date": date_str, "localization": "false"}
    if COINGECKO_KEY:
        params["x_cg_demo_api_key"] = COINGECKO_KEY

    retries = 3
    delay = 10  # Initial delay in seconds

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params)

            # Handle rate limiting with exponential backoff
            if response.status_code == 429:
                if attempt < retries - 1:  # Don't sleep on last attempt
                    sleep_time = delay * (2**attempt)  # Exponential backoff
                    logger.info(f"Rate limit exceeded. Sleeping {sleep_time}s...")
                    time.sleep(sleep_time)
                    continue
                return None

            response.raise_for_status()
            data = response.json()

            if "market_data" not in data:
                logger.warning(f"No market data for {coingecko_id} on {date_str}")
                return None

            return Decimal(str(data["market_data"]["current_price"]["usd"]))

        except Exception as e:
            logger.error(f"Error fetching price for {coingecko_id}: {str(e)}")
            if attempt < retries - 1:  # Don't sleep on last attempt
                time.sleep(delay)
                delay *= 2  # Exponential backoff

    return None


def update_token_prices():
    """Update all token prices from earliest block to today."""

    logger.info("=" * 80)
    logger.info("Updating token prices")

    try:
        # Get start timestamp from Ethereum's start block
        eth_start_block = next(
            chain["start_block"] for chain in CHAINS if chain["chain_id"] == 1
        )
        start_timestamp = get_block_timestamp(1, eth_start_block)
        start_date = datetime.fromtimestamp(start_timestamp, tz=timezone.utc).date()
        end_date = datetime.now(timezone.utc).date()

        logger.info(f"Ethereum start block: {eth_start_block}")
        logger.info(f"Start timestamp: {start_timestamp}")
        logger.info(f"Start date: {start_date}")
        logger.info(f"End date: {end_date}")

        # Set up DB connection
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Update prices for each token, for each date
            current_date = start_date
            total_existing = 0
            total_updated = 0
            total_failed = 0

            # For tracking consecutive days with same number of skipped prices
            skip_streak_start = None
            last_skip_count = 0

            while current_date <= end_date:
                date_str = current_date.strftime("%d-%m-%Y")
                existing_count = 0

                for token_symbol, coingecko_id in COINGECKO_SYMBOL_MAP.items():
                    # Check if price exists
                    cursor.execute(
                        "SELECT 1 FROM TokenPrice WHERE token_symbol = ? AND date = ?",
                        (token_symbol, current_date),
                    )

                    if cursor.fetchone():
                        existing_count += 1
                        total_existing += 1
                        continue

                    # Get and store new price
                    if price := _get_price_from_api(coingecko_id, date_str):
                        cursor.execute(
                            """
                            INSERT INTO TokenPrice (date, token_symbol, price_usd)
                            VALUES (?, ?, ?)
                            """,
                            (current_date, token_symbol, str(price)),
                        )
                        logger.info(
                            f"Stored price for {token_symbol} on {date_str}: {price}"
                        )
                        conn.commit()
                        total_updated += 1
                    else:
                        logger.error(
                            f"Failed to get price for {token_symbol} on {date_str}"
                        )
                        total_failed += 1

                    # Sleep briefly between tokens to avoid rate limits
                    time.sleep(1)

                # Handle logging of skipped prices
                if existing_count > 0:
                    if existing_count == last_skip_count:
                        if skip_streak_start is None:
                            skip_streak_start = current_date
                    else:
                        if skip_streak_start is not None:
                            streak_days = (current_date - skip_streak_start).days
                            if streak_days > 0:
                                logger.info(
                                    f"Skipped {last_skip_count} existing prices per day for {streak_days} days ({skip_streak_start.strftime('%d-%m-%Y')} to {(current_date - timedelta(days=1)).strftime('%d-%m-%Y')})"
                                )
                        skip_streak_start = current_date
                        last_skip_count = existing_count

                current_date += timedelta(days=1)

            # Log final streak if any
            if skip_streak_start is not None:
                streak_days = (current_date - skip_streak_start).days
                if streak_days > 0:
                    logger.info(
                        f"Skipped {last_skip_count} existing prices per day for {streak_days} days ({skip_streak_start.strftime('%d-%m-%Y')} to {(current_date - timedelta(days=1)).strftime('%d-%m-%Y')})"
                    )

            # Log final summary
            logger.info("Price update summary:")
            logger.info(f"  - {total_existing} prices already existed")
            logger.info(f"  - {total_updated} prices updated")
            logger.info(f"  - {total_failed} price updates failed")
            logger.info("Successfully updated historical token prices")

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Error updating token prices: {str(e)}")
        raise
