#!/usr/bin/env python3
"""
Calculate daily profits from Fill data.

This module:
1. Aggregates Fill data by day/chain/token
2. Uses proper token decimals from Token table
3. Calculates profits in both token and USD terms
4. Stores results in DailyProfit table
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Tuple

from src.db_utils import get_db_connection

# Configure logging
logger = logging.getLogger(__name__)


def _get_date_range() -> Tuple[datetime, datetime]:
    """
    Get the date range to process:
    - Start from earliest unprocessed Fill date
    - End at latest Fill date

    Returns:
        Tuple of (start_date, end_date) as datetime objects
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Get earliest unprocessed fill date
        cursor.execute("""
            SELECT MIN(DATE(tx_timestamp, 'unixepoch'))
            FROM Fill f
            WHERE NOT EXISTS (
                SELECT 1 FROM DailyProfit dp
                WHERE dp.date = DATE(f.tx_timestamp, 'unixepoch')
                AND dp.chain_id = f.destination_chain_id
                AND dp.token_symbol = (
                    SELECT symbol 
                    FROM Token t 
                    WHERE t.token_address = f.output_token
                    AND t.chain_id = f.destination_chain_id
                )
            )
        """)
        start_date = cursor.fetchone()[0]

        # If no unprocessed fills, use earliest fill date
        if not start_date:
            cursor.execute("SELECT MIN(DATE(tx_timestamp, 'unixepoch')) FROM Fill")
            start_date = cursor.fetchone()[0]

        # Get latest fill date
        cursor.execute("SELECT MAX(DATE(tx_timestamp, 'unixepoch')) FROM Fill")
        end_date = cursor.fetchone()[0]

        return (
            datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
            datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
        )

    finally:
        conn.close()


def calculate_daily_profits() -> None:
    """
    Calculate daily profits for any unprocessed fills.

    Uses a single SQL query to:
    1. Aggregate Fill data by day/chain/token
    2. Apply proper token decimals from Token table
    3. Calculate profits in both token and USD terms
    4. Store results in DailyProfit table

    Note: Only successful fills are included in profit calculations.
    """
    logger.info("=" * 80)
    logger.info("Calculating daily profits")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Get date range to process
        start_date, end_date = _get_date_range()
        logger.info(f"Processing profits from {start_date.date()} to {end_date.date()}")

        # Process one day at a time to handle large datasets
        current_date = start_date
        while current_date <= end_date:
            next_date = current_date + timedelta(days=1)

            # Convert dates to timestamps for SQL
            start_ts = int(current_date.timestamp())
            end_ts = int(next_date.timestamp())

            # First get the number of fills we'll process
            cursor.execute(
                """
            WITH fill_amounts AS (
                SELECT 
                    DATE(f.tx_timestamp, 'unixepoch') as date,
                    f.destination_chain_id as chain_id,
                    t.symbol as token_symbol,
                    -- Success metrics
                    SUM(CASE WHEN f.is_success = 1 
                        THEN CAST(f.input_amount AS DECIMAL) / POWER(10, t.decimals) 
                        ELSE 0 END) as success_input_amount,
                    SUM(CASE WHEN f.is_success = 1 
                        THEN CAST(f.output_amount AS DECIMAL) / POWER(10, t.decimals) 
                        ELSE 0 END) as success_output_amount,
                    SUM(CASE WHEN f.is_success = 1 
                        THEN CAST(f.lp_fee AS DECIMAL) / POWER(10, t.decimals) 
                        ELSE 0 END) as success_lp_fee,
                    SUM(CASE WHEN f.is_success = 1 
                        THEN CAST(f.gas_cost AS DECIMAL) / POWER(10, 18) 
                        ELSE 0 END) as success_gas_fee_eth,
                    -- All metrics
                    SUM(CAST(f.input_amount AS DECIMAL) / POWER(10, t.decimals)) as all_input_amount,
                    SUM(CAST(f.output_amount AS DECIMAL) / POWER(10, t.decimals)) as all_output_amount,
                    SUM(CAST(f.lp_fee AS DECIMAL) / POWER(10, t.decimals)) as all_lp_fee,
                    SUM(CAST(f.gas_cost AS DECIMAL) / POWER(10, 18)) as all_gas_fee_eth,
                    COUNT(*) as total_fills,
                    SUM(CASE WHEN f.is_success = 1 THEN 1 ELSE 0 END) as successful_fills
                FROM Fill f
                JOIN Token t ON f.output_token = t.token_address 
                    AND f.destination_chain_id = t.chain_id
                WHERE f.tx_timestamp >= ? AND f.tx_timestamp < ?
                GROUP BY 
                    DATE(f.tx_timestamp, 'unixepoch'),
                    f.destination_chain_id,
                    t.symbol
            )
            SELECT COUNT(*) FROM fill_amounts""",
                (start_ts, end_ts),
            )

            # Calculate and insert daily profits
            cursor.execute(
                """
            WITH fill_amounts AS (
                SELECT 
                    DATE(f.tx_timestamp, 'unixepoch') as date,
                    f.destination_chain_id as chain_id,
                    t.symbol as token_symbol,
                    -- Success metrics
                    SUM(CASE WHEN f.is_success = 1 
                        THEN CAST(f.input_amount AS DECIMAL) / POWER(10, t.decimals) 
                        ELSE 0 END) as success_input_amount,
                    SUM(CASE WHEN f.is_success = 1 
                        THEN CAST(f.output_amount AS DECIMAL) / POWER(10, t.decimals) 
                        ELSE 0 END) as success_output_amount,
                    SUM(CASE WHEN f.is_success = 1 
                        THEN CAST(f.lp_fee AS DECIMAL) / POWER(10, t.decimals) 
                        ELSE 0 END) as success_lp_fee,
                    SUM(CASE WHEN f.is_success = 1 
                        THEN CAST(f.gas_cost AS DECIMAL) / POWER(10, 18) 
                        ELSE 0 END) as success_gas_fee_eth,
                    -- All metrics
                    SUM(CAST(f.input_amount AS DECIMAL) / POWER(10, t.decimals)) as all_input_amount,
                    SUM(CAST(f.output_amount AS DECIMAL) / POWER(10, t.decimals)) as all_output_amount,
                    SUM(CAST(f.lp_fee AS DECIMAL) / POWER(10, t.decimals)) as all_lp_fee,
                    SUM(CAST(f.gas_cost AS DECIMAL) / POWER(10, 18)) as all_gas_fee_eth,
                    COUNT(*) as total_fills,
                    SUM(CASE WHEN f.is_success = 1 THEN 1 ELSE 0 END) as successful_fills
                FROM Fill f
                JOIN Token t ON f.output_token = t.token_address 
                    AND f.destination_chain_id = t.chain_id
                WHERE f.tx_timestamp >= ? AND f.tx_timestamp < ?
                GROUP BY 
                    DATE(f.tx_timestamp, 'unixepoch'),
                    f.destination_chain_id,
                    t.symbol
            )
            
            INSERT OR REPLACE INTO DailyProfit (
                date, chain_id, token_symbol,
                success_input_amount, success_output_amount, success_lp_fee,
                success_gas_fee_eth, success_gas_fee_usd,
                all_input_amount, all_output_amount, all_lp_fee,
                all_gas_fee_eth, all_gas_fee_usd,
                total_fills, successful_fills, profit_usd
            )
            SELECT 
                f.date, f.chain_id, f.token_symbol,
                f.success_input_amount, f.success_output_amount, f.success_lp_fee,
                f.success_gas_fee_eth,
                f.success_gas_fee_eth * eth_price.price_usd as success_gas_fee_usd,
                f.all_input_amount, f.all_output_amount, f.all_lp_fee,
                f.all_gas_fee_eth,
                f.all_gas_fee_eth * eth_price.price_usd as all_gas_fee_usd,
                f.total_fills, f.successful_fills,
                (f.success_input_amount - f.success_output_amount - f.success_lp_fee) * token_price.price_usd 
                    - (f.success_gas_fee_eth * eth_price.price_usd) as profit_usd
            FROM fill_amounts f
            JOIN TokenPrice token_price 
                ON token_price.date = f.date 
                AND token_price.token_symbol = f.token_symbol
            JOIN TokenPrice eth_price 
                ON eth_price.date = f.date 
                AND eth_price.token_symbol = 'ETH'
            """,
                (start_ts, end_ts),
            )

            # Move to next day
            current_date = next_date

        conn.commit()
        logger.info("Daily profit calculation completed successfully")

    except Exception as e:
        logger.error(f"Error calculating daily profits: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    calculate_daily_profits()
