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
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

from src.config import DAILY_COUNT_FILE, RETURN_DATA_FILE
from src.db_utils import get_db_connection
from src.upload_utils import upload_reports

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
    logger.info("Generating bundle return reports")
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
                    '''
                    SELECT 
                        br.bundle_id,
                        br.return_tx_hash,
                        br.input_amount,
                        br.return_amount,
                        COALESCE(SUM(CAST(f.output_amount AS DECIMAL)), 0) as output_amount,
                        br.lp_fee,
                        (br.return_amount + br.lp_fee) as 'return + lp',
                        (br.input_amount - br.return_amount - br.lp_fee) as 'repayment difference',
                        br.start_block,
                        br.end_block,
                        datetime(br.start_time, 'unixepoch') as start_time,
                        datetime(br.end_time, 'unixepoch') as end_time,
                        (br.end_time - br.start_time) as time_elapsed,
                        br.fill_tx_hashes as tx_hashs,
                        br.relayer_refund_root,
                        datetime(b.propose_timestamp, 'unixepoch') as propose_time,
                        datetime(b.settlement_timestamp, 'unixepoch') as settlement_time,
                        (b.settlement_timestamp - b.propose_timestamp) as propose_settlement_time_diff
                    FROM BundleReturn br
                    JOIN Bundle b ON b.bundle_id = br.bundle_id AND b.chain_id = br.chain_id
                    LEFT JOIN Fill f ON f.tx_hash IN (
                        SELECT value FROM json_each('["' || REPLACE(br.fill_tx_hashes, ',', '","') || '"]')
                    )
                    WHERE br.chain_id = ? AND br.token_symbol = ?
                    GROUP BY br.bundle_id, br.return_tx_hash, br.input_amount, br.return_amount, 
                             br.lp_fee, br.start_block, br.end_block, br.start_time, br.end_time,
                             br.fill_tx_hashes, br.relayer_refund_root, b.propose_timestamp, 
                             b.settlement_timestamp
                    ORDER BY br.bundle_id DESC
                    ''',
                    (chain_id, token_symbol),
                )

                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)

                # Format time elapsed
                df["time_elapsed"] = df["time_elapsed"].apply(format_time_elapsed)
                df["propose_settlement_time_diff"] = df["propose_settlement_time_diff"].apply(format_time_elapsed)

                sheet_name = f"{chain_id}-{token_symbol}"
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                # logger.info(f"Wrote {len(df)} rows to sheet {sheet_name}")

        logger.info(f"Successfully wrote bundle return data to {RETURN_DATA_FILE}")

    except Exception as e:
        logger.error(f"Error writing bundle returns to Excel: {e}")
        raise

    finally:
        if "conn" in locals():
            conn.close()


def get_daily_profits_df(
    cursor: sqlite3.Cursor, chain_id: int, token_symbol: str
) -> pd.DataFrame:
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


def get_base_capital(target_date: date, token_symbol: str) -> float:
    """
    Get the base capital allocation for a token on a specific date

    Args:
        target_date: Date to get capital for
        token_symbol: Token symbol

    Returns:
        Capital amount for the token
    """
    # Convert date to timestamp for comparison
    target_timestamp = int(
        datetime.combine(target_date, datetime.min.time()).timestamp()
    )

    # Load capital configuration
    capital_config_path = Path(__file__).parent.parent / "capital_config.yaml"
    try:
        with open(capital_config_path, "r") as f:
            config = yaml.safe_load(f)

        # Find most recent capital allocation before target_date
        for capital in reversed(config["capitals"]):
            if target_timestamp >= capital["start_date"]:
                return float(capital.get(token_symbol, 0))

        # If no matching entry found
        logger.debug(f"No capital entry found for {target_date}, token {token_symbol}")
        return 0
    except FileNotFoundError:
        logger.warning(f"Capital config file not found at {capital_config_path}")
        return 0
    except Exception as e:
        logger.error(f"Error loading capital config: {e}")
        return 0


def get_capital_with_previous_profit(
    token_symbol: str, target_date: date, current_capital: float, last_day_profit: float
) -> float:
    """
    Calculate capital including previous day's profit

    Args:
        token_symbol: Token symbol
        target_date: Date to calculate capital for
        current_capital: Current capital amount
        last_day_profit: Previous day's profit

    Returns:
        Updated capital amount
    """
    from datetime import timedelta

    # Get base capitals
    base_capital = get_base_capital(target_date, token_symbol)
    prev_day = target_date - timedelta(days=1)
    prev_day_base_capital = get_base_capital(prev_day, token_symbol)

    # Adjust for capital changes
    adjusted_capital = current_capital + base_capital - prev_day_base_capital

    # Add previous day's profit
    return adjusted_capital + last_day_profit


def calculate_apy(profit: float, capital: float) -> str:
    """
    Calculate APY and format as percentage string

    Args:
        profit: Daily profit amount
        capital: Capital amount

    Returns:
        Formatted APY string
    """
    if capital == 0:
        return "0.00%"

    # Calculate APY using the original method from calc_apy.py
    apy = profit * 365 / capital

    return f"{apy * 100:.2f}%"


def add_apy_sheet(excel_writer: pd.ExcelWriter, conn: sqlite3.Connection) -> None:
    """
    Add APY and total profit sheet to the Excel report

    Args:
        excel_writer: Open Excel writer object
        conn: Database connection
    """
    logger.info("Generating APY and total profit sheet")

    try:
        cursor = conn.cursor()

        # Get the date range from DailyProfit
        cursor.execute("SELECT MIN(date), MAX(date) FROM DailyProfit")
        min_date, max_date = cursor.fetchone()

        if not min_date or not max_date:
            logger.warning("No daily profit data found for APY calculation")
            return

        # Convert to datetime objects
        start_date = datetime.strptime(min_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(max_date, "%Y-%m-%d").date()

        # Get tokens with profit data
        cursor.execute(
            "SELECT DISTINCT token_symbol FROM DailyProfit ORDER BY token_symbol"
        )
        tokens = [row[0] for row in cursor.fetchall()]

        if not tokens:
            logger.warning("No tokens found for APY calculation")
            return

        # Initialize last day profits
        last_profits = {token: 0 for token in tokens}

        # Initialize capitals with base values from config for the start date
        capitals = {}
        for token in tokens:
            base_capital = get_base_capital(start_date, token)
            capitals[token] = base_capital
            logger.debug(
                f"Initialized capital for {token} on {start_date}: {base_capital}"
            )

        # Initialize data list for DataFrame
        data = []

        # For each date in range
        current_date = start_date
        while current_date <= end_date:
            # Initialize row data
            row_data = {"Date": current_date.strftime("%Y-%m-%d")}
            date_str = current_date.strftime("%Y-%m-%d")

            # Get token prices for this date
            cursor.execute(
                """
                SELECT token_symbol, price_usd 
                FROM TokenPrice 
                WHERE date = ? AND token_symbol IN ({})
            """.format(",".join(["?"] * len(tokens))),
                [date_str] + tokens,
            )

            token_prices = {row[0]: row[1] for row in cursor.fetchall()}

            # If we don't have prices for all tokens, skip this date
            if len(token_prices) < len(tokens):
                current_date += pd.Timedelta(days=1)
                continue

            # Get profits for this date
            cursor.execute(
                """
                SELECT token_symbol, 
                       SUM(input_amount - output_amount - lp_fee) as token_profit,
                       SUM(profit_usd) as usd_profit
                FROM DailyProfit
                WHERE date = ?
                GROUP BY token_symbol
            """,
                [date_str],
            )

            daily_profits = {
                row[0]: {"token_profit": row[1], "usd_profit": row[2]}
                for row in cursor.fetchall()
            }

            # Calculate total USD profit and capital
            total_usd_profit = 0
            total_usd_capital = 0

            # For each token
            for token in tokens:
                # Update capital with previous profit
                capitals[token] = get_capital_with_previous_profit(
                    token, current_date, capitals[token], last_profits[token]
                )

                # Get profit for this token/date
                token_profit = (
                    daily_profits.get(token, {"token_profit": 0, "usd_profit": 0})[
                        "token_profit"
                    ]
                    or 0
                )
                usd_profit = (
                    daily_profits.get(token, {"token_profit": 0, "usd_profit": 0})[
                        "usd_profit"
                    ]
                    or 0
                )

                # Calculate APY
                token_apy = calculate_apy(token_profit, capitals[token])

                # Add to total USD calculations
                token_price = token_prices.get(token, 0)
                token_usd_capital = capitals[token] * token_price

                total_usd_profit += usd_profit
                total_usd_capital += token_usd_capital

                # Add to row data
                row_data[f"{token} Capital"] = str(capitals[token])
                row_data[f"{token} Profit"] = str(token_profit)
                row_data[f"{token} APR"] = token_apy  # THIS IS AN APR CALCULATION (NO COMPOUNDING)

                # Update last day profit
                last_profits[token] = token_profit

            # Calculate total APY
            total_apy = calculate_apy(total_usd_profit, total_usd_capital) # THIS IS AN APR CALCULATION (NO COMPOUNDING)

            # Add totals to row data
            row_data["Total USD Capital"] = str(total_usd_capital)
            row_data["Total USD Profit"] = str(total_usd_profit)
            row_data["Total APR"] = total_apy # THIS IS AN APR CALCULATION (NO COMPOUNDING)

            # Add row to data
            data.append(row_data)

            # Move to next day
            current_date += pd.Timedelta(days=1)

        # Create DataFrame and write to Excel
        if data:
            df = pd.DataFrame(data)

            # Sort by date in descending order (most recent first)
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date", ascending=False)
            df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

            df.to_excel(excel_writer, sheet_name="APR", index=False)
            logger.info("APY sheet generated successfully")
        else:
            logger.warning("No data for APY calculation")

    except Exception as e:
        logger.error(f"Error generating APY sheet: {e}")
        raise


def write_daily_profits_excel() -> None:
    """
    Write daily profit data to Excel file.
    Creates one sheet per chain-token combination.
    """
    logger.info("Generating daily profit reports")

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
            # Write each chain-token pair to a separate sheet
            sheets_created = False

            for pair in pairs:
                chain_id = pair["chain_id"]
                token_symbol = pair["token_symbol"]

                df = get_daily_profits_df(cursor, chain_id, token_symbol)

                if df.empty:
                    continue

                # Add to summary data
                summary_data.append(
                    {
                        "Chain-Token": f"{chain_id}-{token_symbol}",
                        "Total Profit(USD)": df["Profit(USD)"].sum(),
                        "Total LP Fee(USD)": df["Total LP Fee(USD)"].sum(),
                        "Total Gas Fee(USD)": df["Total Gas Fee(USD)"].sum(),
                    }
                )

                # Write to sheet
                sheet_name = f"{chain_id}-{token_symbol}"
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                sheets_created = True

            # Create summary sheet
            if summary_data:
                summary_df = pd.DataFrame(
                    summary_data,
                    columns=[
                        "Chain-Token",
                        "Total Profit(USD)",
                        "Total LP Fee(USD)",
                        "Total Gas Fee(USD)",
                    ],
                )
                summary_df.to_excel(writer, sheet_name="Summary", index=False)
                sheets_created = True

            # Add APY sheet
            try:
                add_apy_sheet(writer, conn)
                sheets_created = True
            except Exception as e:
                logger.error(f"Error generating APY sheet: {e}")
                # Continue without APY sheet

            # Fallback: Create an empty sheet if no other sheets were created
            if not sheets_created:
                pd.DataFrame(columns=["No Data Available"]).to_excel(
                    writer, sheet_name="No Data", index=False
                )
                logger.warning(
                    "No data sheets were created, added fallback empty sheet"
                )

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

        cursor.execute('''
            SELECT 
                chain_id,
                token_symbol,
                COUNT(*) as bundle_count,
                SUM(input_amount) as total_input,
                SUM(return_amount) as total_return,
                SUM(lp_fee) as total_lp_fee,
                AVG(end_time - start_time) as avg_time_elapsed,
                AVG(propose_settlement_time_diff) as avg_propose_settlement_time,
                MIN(start_block) as first_start_block,
                MIN(end_block) as first_end_block,
                MIN(start_time) as first_bundle_time,
                MAX(start_block) as last_start_block,
                MAX(end_block) as last_end_block,
                MAX(end_time) as last_bundle_time
            FROM BundleReturn
            GROUP BY chain_id, token_symbol
            ORDER BY chain_id, token_symbol
        ''')
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)

        # Convert timestamps to datetime with UTC timezone
        df["first_bundle_time"] = pd.to_datetime(
            df["first_bundle_time"], unit="s", utc=True
        )
        df["last_bundle_time"] = pd.to_datetime(
            df["last_bundle_time"], unit="s", utc=True
        )

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
    """Generate all reports and optionally upload them to Google Drive."""

    logger.info("=" * 80)
    logger.info("Generating reports")

    write_bundle_returns_excel()
    write_daily_profits_excel()
    upload_reports()


if __name__ == "__main__":
    # Example usage
    generate_reports()
