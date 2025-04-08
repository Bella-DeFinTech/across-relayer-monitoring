"""
Utilities for generating reports from the relayer monitoring database.

This module provides functions to:
1. Query BundleReturn data and generate Excel reports
2. Query DailyProfit data and generate Excel reports
3. Calculate metrics and summaries
4. Export data in various formats

To run this file directly: python3 -m src.reporting_utils
"""

import logging
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd

from .config import RETURN_DATA_FILE, DAILY_COUNT_FILE
from .db_utils import get_db_connection

# Configure logging
logger = logging.getLogger(__name__)


def format_time_elapsed(seconds: float) -> str:
    """Convert seconds to human readable time."""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    elif seconds < 86400:
        hours = seconds / 3600
        return f"{hours:.1f} hours"
    else:
        days = seconds / 86400
        return f"{days:.1f} days"


def write_bundle_returns_excel(chain_id: Optional[int] = None) -> None:
    """
    Write bundle return data to Excel file.
    Creates one sheet per chain-token combination.

    Args:
        chain_id: Optional chain ID to filter by. If None, exports data for all chains.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get unique chain-token combinations
        chain_filter = "WHERE chain_id = ?" if chain_id else ""
        cursor.execute(
            f"""
            SELECT DISTINCT chain_id, token_symbol 
            FROM BundleReturn
            {chain_filter}
            ORDER BY chain_id, token_symbol
        """,
            (chain_id,) if chain_id else (),
        )

        combinations = cursor.fetchall()
        if not combinations:
            logger.warning(
                f"No data found{' for chain ' + str(chain_id) if chain_id else ''}"
            )
            return

        # Write to Excel
        mode = "a" if Path(RETURN_DATA_FILE).exists() else "w"
        with pd.ExcelWriter(
            RETURN_DATA_FILE,
            mode=mode,
            engine="openpyxl",
            if_sheet_exists="replace" if mode == "a" else None,
        ) as writer:
            for chain_id, token_symbol in combinations:
                # Get data for this chain-token combination
                cursor.execute(
                    """
                    SELECT 
                        br.bundle_id,
                        br.return_tx_hash,
                        br.input_amount,
                        br.return_amount,
                        br.lp_fee,
                        (br.return_amount + br.lp_fee) as 'return + lp',
                        (br.input_amount - br.return_amount - br.lp_fee) as 'repayment difference',
                        br.start_block,
                        br.end_block,
                        datetime(br.start_time, 'unixepoch') as start_time,
                        datetime(br.end_time, 'unixepoch') as end_time,
                        (br.end_time - br.start_time) as time_elapsed,
                        br.fill_tx_hashes as tx_hashs,
                        br.relayer_refund_root
                    FROM BundleReturn br
                    WHERE br.chain_id = ? AND br.token_symbol = ?
                    ORDER BY br.bundle_id DESC
                """,
                    (chain_id, token_symbol),
                )

                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)

                # Format time elapsed
                df["time_elapsed"] = df["time_elapsed"].apply(format_time_elapsed)

                sheet_name = f"{chain_id}-{token_symbol}"
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                logger.info(f"Wrote {len(df)} rows to sheet {sheet_name}")

        logger.info(f"Successfully wrote bundle return data to {RETURN_DATA_FILE}")

    except Exception as e:
        logger.error(f"Error writing bundle returns to Excel: {e}")
        raise

    finally:
        if "conn" in locals():
            conn.close()


def get_daily_profits_df(cursor: sqlite3.Cursor, chain_id: int, token_symbol: str) -> pd.DataFrame:
    """
    Get daily profit data for a specific chain and token.

    Args:
        cursor: Database cursor
        chain_id: Chain ID to filter by
        token_symbol: Token symbol to filter by

    Returns:
        DataFrame with daily profit data
    """
    query = """
        WITH RECURSIVE dates(date) AS (
            SELECT MIN(date) FROM DailyProfit
            UNION ALL
            SELECT date(date, '+1 day')
            FROM dates
            WHERE date < (SELECT MAX(date) FROM DailyProfit)
        )
        SELECT 
            date(d.date) as Date,
            COALESCE(dp.profit_usd, 0) as 'Profit(USD)',
            COALESCE(dp.total_fills, 0) as 'Total Fill Orders',
            COALESCE(dp.successful_fills, 0) as 'Successful Orders',
            COALESCE(dp.input_amount, 0) as 'Total Input Amount',
            COALESCE(dp.output_amount, 0) as 'Total Output Amount',
            COALESCE(dp.lp_fee, 0) as 'Total LP Fee',
            COALESCE(dp.lp_fee * tp.price_usd, 0) as 'Total LP Fee(USD)',
            COALESCE(dp.gas_fee_eth, 0) as 'Total Gas Fee',
            COALESCE(dp.gas_fee_usd, 0) as 'Total Gas Fee(USD)',
            COALESCE(dp.gas_fee_eth, 0) as 'Total Gas Fee(ETH)',
            COALESCE(tp.price_usd, 0) as 'Token Price',
            COALESCE(eth_price.price_usd, 0) as 'ETH Price'
        FROM dates d
        LEFT JOIN DailyProfit dp ON d.date = dp.date 
            AND dp.chain_id = ? AND dp.token_symbol = ?
        LEFT JOIN TokenPrice tp ON d.date = tp.date 
            AND tp.token_symbol = ?
        LEFT JOIN TokenPrice eth_price ON d.date = eth_price.date 
            AND eth_price.token_symbol = 'ETH'
        ORDER BY d.date DESC
    """
    
    cursor.execute(query, (chain_id, token_symbol, token_symbol))
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    
    return pd.DataFrame(data, columns=columns)


def get_chain_token_pairs(cursor: sqlite3.Cursor) -> List[Dict[str, Any]]:
    """
    Get all unique chain-token pairs from DailyProfit table.

    Args:
        cursor: Database cursor

    Returns:
        List of dicts with chain_id and token_symbol
    """
    query = """
        SELECT DISTINCT chain_id, token_symbol
        FROM DailyProfit
        ORDER BY chain_id, token_symbol
    """
    
    cursor.execute(query)
    return [{"chain_id": row[0], "token_symbol": row[1]} for row in cursor.fetchall()]


def write_daily_profits_excel() -> None:
    """
    Write daily profit data to Excel file.
    Creates one sheet per chain-token combination.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all chain-token pairs
        pairs = get_chain_token_pairs(cursor)
        if not pairs:
            logger.warning("No daily profit data found")
            return

        # Prepare summary data
        summary_data = []

        # Write to Excel
        with pd.ExcelWriter(DAILY_COUNT_FILE) as writer:
            # Create a sheet for each chain-token pair
            for pair in pairs:
                chain_id = pair["chain_id"]
                token_symbol = pair["token_symbol"]
                sheet_name = f"{chain_id}_{token_symbol.lower()}"
                
                # Get data for this pair
                df = get_daily_profits_df(cursor, chain_id, token_symbol)
                if not df.empty:
                    # Calculate totals
                    total_profit = df['Profit(USD)'].sum()
                    total_lp = df['Total LP Fee(USD)'].sum()
                    total_gas = df['Total Gas Fee(USD)'].sum()
                    
                    # Add totals row
                    # totals = pd.Series({
                    #     'Date': 'Total',
                    #     'Profit(USD)': total_profit,
                    #     'Total LP Fee(USD)': total_lp,
                    #     'Total Gas Fee(USD)': total_gas
                    # })
                    # df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)
                    
                    # Write to sheet
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # Add to summary
                    summary_data.append([
                        f"{chain_id}-{token_symbol}",
                        total_profit,
                        total_lp,
                        total_gas
                    ])
            
            # Create summary sheet
            if summary_data:
                summary_df = pd.DataFrame(
                    summary_data,
                    columns=['Chain-Token', 'Total Profit(USD)', 'Total LP Fee(USD)', 'Total Gas Fee(USD)']
                )
                summary_df.to_excel(writer, sheet_name='Summary', index=False)

        logger.info(f"Daily profit data written to {DAILY_COUNT_FILE}")

    except Exception as e:
        logger.error(f"Error writing daily profits to Excel: {e}")
        raise

    finally:
        conn.close()


def get_bundle_return_summary() -> pd.DataFrame:
    """
    Get a summary of bundle returns grouped by chain and token.

    Returns:
        DataFrame with summary statistics
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                chain_id,
                token_symbol,
                COUNT(*) as bundle_count,
                SUM(input_amount) as total_input,
                SUM(return_amount) as total_return,
                SUM(lp_fee) as total_lp_fee,
                AVG(end_time - start_time) as avg_time_elapsed,
                MIN(start_block) as first_start_block,
                MIN(end_block) as first_end_block,
                MIN(start_time) as first_bundle_time,
                MAX(start_block) as last_start_block,
                MAX(end_block) as last_end_block,
                MAX(end_time) as last_bundle_time
            FROM BundleReturn
            GROUP BY chain_id, token_symbol
            ORDER BY chain_id, token_symbol
        """)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)

        # Convert timestamps to datetime
        df["first_bundle_time"] = pd.to_datetime(df["first_bundle_time"], unit="s")
        df["last_bundle_time"] = pd.to_datetime(df["last_bundle_time"], unit="s")

        # Format time elapsed
        df["avg_time_elapsed"] = df["avg_time_elapsed"].apply(format_time_elapsed)

        # Format block numbers
        df["first_blocks"] = df.apply(
            lambda x: f"{x['first_start_block']} - {x['first_end_block']}", axis=1
        )
        df["last_blocks"] = df.apply(
            lambda x: f"{x['last_start_block']} - {x['last_end_block']}", axis=1
        )

        # Drop individual block columns
        df = df.drop(
            columns=[
                "first_start_block",
                "first_end_block",
                "last_start_block",
                "last_end_block",
            ]
        )

        return df

    except Exception as e:
        logger.error(f"Error getting bundle return summary: {e}")
        raise
    finally:
        if "conn" in locals():
            conn.close()


def generate_reports() -> None:
    """Generate all reports."""
    logger.info("Generating bundle return reports")
    write_bundle_returns_excel()
    
    logger.info("Generating daily profit reports")
    write_daily_profits_excel()


if __name__ == "__main__":
    # Example usage
    generate_reports()
