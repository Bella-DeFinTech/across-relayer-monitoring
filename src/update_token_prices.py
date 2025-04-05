"""Script to update historical token prices in the database.

This script fetches and stores USD prices for all configured tokens from
the earliest Ethereum block to today. It skips dates where prices are
already stored to avoid duplicate API calls.
"""

import logging
import os
import time
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Optional

import requests
from dotenv import load_dotenv

from .config import COINGECKO_KEY, COINGECKO_SYMBOL_MAP, CHAINS
from .db_utils import get_db_connection
from .web3_utils import get_block_timestamp

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def _get_price_from_api(coingecko_id: str, date_str: str) -> Optional[Decimal]:
    """Get historical price from CoinGecko API with retries."""
    url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/history"
    params = {
        "date": date_str,
        "localization": "false"
    }
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
                    sleep_time = delay * (2 ** attempt)  # Exponential backoff
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
    try:
        # Get start timestamp from Ethereum's start block
        eth_start_block = next(
            chain["start_block"] 
            for chain in CHAINS 
            if chain["chain_id"] == 1
        )
        start_timestamp = get_block_timestamp(1, eth_start_block)
        start_date = date.fromtimestamp(start_timestamp)
        end_date = date.today()
        
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
            while current_date <= end_date:
                date_str = current_date.strftime("%d-%m-%Y")
                
                for token_symbol, coingecko_id in COINGECKO_SYMBOL_MAP.items():
                    # Check if price exists
                    cursor.execute(
                        "SELECT 1 FROM TokenPrice WHERE token_symbol = ? AND date = ?",
                        (token_symbol, current_date)
                    )

                    if cursor.fetchone():
                        logger.info(f"Price for {token_symbol} on {date_str} already exists")
                        continue
                        
                    # Get and store new price
                    if price := _get_price_from_api(coingecko_id, date_str):
                        cursor.execute(
                            """
                            INSERT INTO TokenPrice (date, token_symbol, price_usd)
                            VALUES (?, ?, ?)
                            """,
                            (current_date, token_symbol, str(price))
                        )
                        logger.info(f"Stored price for {token_symbol} on {date_str}: {price}")
                        conn.commit()
                    else:
                        logger.error(f"Failed to get price for {token_symbol} on {date_str}")
                    
                    # Sleep briefly between tokens to avoid rate limits
                    time.sleep(1)
                
                current_date += timedelta(days=1)
                
            logger.info("Successfully updated historical token prices")
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error updating token prices: {str(e)}")
        raise