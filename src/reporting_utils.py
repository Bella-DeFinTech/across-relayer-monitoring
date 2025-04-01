"""
Utilities for generating reports from the relayer monitoring database.

This module provides functions to:
1. Query BundleReturn data and generate Excel reports
2. Calculate metrics and summaries
3. Export data in various formats

To run this file directly: python3 -m src.reporting_utils
"""

import logging
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

from .config import RETURN_DATA_FILE
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


def get_bundle_returns_df(
    cursor: sqlite3.Cursor, chain_id: Optional[int] = None
) -> pd.DataFrame:
    """
    Get BundleReturn data as a DataFrame.

    Args:
        cursor: Database cursor
        chain_id: Optional chain ID to filter by

    Returns:
        DataFrame with bundle return data
    """
    query = """
        SELECT 
            br.bundle_id,
            br.chain_id as chain,
            br.token_symbol as token,
            br.return_tx_hash as tx_hash,
            br.input_amount,
            br.return_amount,
            br.lp_fee,
            br.return_amount + br.lp_fee as 'return + lp',
            br.return_amount - br.input_amount as 'repayment difference',
            br.start_block,
            br.end_block,
            datetime(br.start_time, 'unixepoch') as start_time,
            datetime(br.end_time, 'unixepoch') as end_time,
            (br.end_time - br.start_time) as time_elapsed,
            br.fill_tx_hashes as tx_hashs,
            br.relayer_refund_root
        FROM BundleReturn br
        {where_clause}
        ORDER BY br.bundle_id DESC
    """

    where_clause = "WHERE br.chain_id = ?" if chain_id else ""
    params = (chain_id,) if chain_id else ()

    cursor.execute(query.format(where_clause=where_clause), params)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()

    df = pd.DataFrame(data, columns=columns)
    df["time_elapsed"] = df["time_elapsed"].apply(format_time_elapsed)

    return df


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

        # Ensure output directory exists
        output_dir = Path(RETURN_DATA_FILE).parent
        output_dir.mkdir(parents=True, exist_ok=True)

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


def generate_reports():
    write_bundle_returns_excel()


if __name__ == "__main__":
    # Example usage
    generate_reports()

    # Get and display summary
    # summary = get_bundle_return_summary()
    # print("\nBundle Return Summary:")
    # print(summary.to_string())
