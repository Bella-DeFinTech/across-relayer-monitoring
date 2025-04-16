"""
Process bundle repayments and populate the BundleReturn table.

This module:
1. Finds unprocessed bundles from the Bundle table
2. For each chain and bundle:
   - Discovers active tokens from Route table
   - Calculates return metrics for each token
   - Updates BundleReturn table
"""

import logging
import sqlite3
from collections import defaultdict
from decimal import Decimal
from typing import Dict, List, Set, Tuple

from src.db_utils import get_db_connection
from src.web3_utils import get_block_timestamp

# Configure logging
logger = logging.getLogger(__name__)

def find_active_tokens(cursor: sqlite3.Cursor, chain_id: int) -> Set[Tuple[str, str]]:
    """
    Find active tokens for a chain by looking at Route table.
    A token is considered active if it's used in any route with this chain.

    Args:
        cursor: Database cursor
        chain_id: Chain ID to find tokens for

    Returns:
        Set of (token_address, token_symbol) tuples that are active on this chain
    """
    logger.info(f"Finding active tokens for chain {chain_id}...")

    cursor.execute(
        """
        SELECT DISTINCT t.token_address, t.symbol
        FROM Route r
        JOIN Token t ON t.token_address = r.output_token AND t.chain_id = r.destination_chain_id
        WHERE r.destination_chain_id = ?
        AND r.is_active = TRUE
    """,
        (chain_id,),
    )

    tokens = {(row[0], row[1]) for row in cursor.fetchall()}  # (address, symbol) pairs
    logger.info(f"Found {len(tokens)} active tokens for chain {chain_id}")
    return tokens


def get_bundle_fills(
    cursor: sqlite3.Cursor, bundle_id: int, chain_id: int, token_address: str
) -> Tuple[List[Dict], int, int]:
    """
    Get all fills that belong to this bundle for a specific token.
    A fill belongs to a bundle if:
    1. It's on the same chain (repayment_chain_id = chain_id)
    2. It's for the specified token (output_token = token_address)
    3. Its block_number is <= bundle's end_block
    4. Its block_number is > previous bundle's end_block

    Args:
        cursor: Database cursor
        bundle_id: Bundle ID to get fills for
        chain_id: Chain ID (repayment chain)
        token_address: Token address to filter fills

    Returns:
        Tuple of:
        - List of fill records with input_amount, lp_fee, tx_hash, tx_timestamp
        - Start block number
        - End block number
    """
    logger.debug(
        f"Getting fills for bundle {bundle_id} chain {chain_id} token {token_address}"
    )

    cursor.execute(
        """
        WITH bundle_info AS (
            -- Get this bundle's end_block and the previous bundle's end_block
            SELECT 
                b1.end_block as end_block,
                COALESCE(MAX(b2.end_block), 0) as prev_end_block
            FROM Bundle b1
            LEFT JOIN Bundle b2 ON b2.chain_id = b1.chain_id 
                AND b2.end_block < b1.end_block
            WHERE b1.bundle_id = ? AND b1.chain_id = ?
            GROUP BY b1.bundle_id, b1.chain_id
        )
        SELECT 
            f.input_amount,
            f.lp_fee,
            f.tx_hash,
            f.tx_timestamp,
            bundle_info.prev_end_block + 1 as start_block,
            bundle_info.end_block as end_block
        FROM Fill f, bundle_info
        WHERE f.repayment_chain_id = ?
        AND f.output_token = ?
        AND f.is_success = 1
        AND f.block_number <= bundle_info.end_block
        AND f.block_number > bundle_info.prev_end_block
        ORDER BY f.block_number ASC
    """,
        (bundle_id, chain_id, chain_id, token_address),
    )

    rows = cursor.fetchall()
    if not rows:
        return [], 0, 0

    # All rows have same start/end block
    fills = [
        {
            "input_amount": row[0],
            "lp_fee": row[1],
            "tx_hash": row[2],
            "tx_timestamp": row[3],
        }
        for row in rows
    ]

    start_block = rows[0][4]
    end_block = rows[0][5]

    logger.debug(f"Found {len(fills)} fills for bundle {bundle_id}")
    return fills, start_block, end_block


def get_bundle_returns(
    cursor: sqlite3.Cursor, bundle_id: int, chain_id: int, token_address: str
) -> List[Dict]:
    """
    Get all returns for a specific bundle and token.

    Args:
        cursor: Database cursor
        bundle_id: Bundle ID to get returns for
        chain_id: Chain ID
        token_address: Token address

    Returns:
        List of return records with return_amount, tx_hash
    """
    cursor.execute(
        """
        SELECT 
            r.return_amount,
            r.tx_hash
        FROM Return r
        WHERE r.return_chain_id = ?
        AND r.return_token = ?
        AND r.root_bundle_id = ?
        ORDER BY r.block_number ASC
    """,
        (chain_id, token_address, bundle_id),
    )

    returns = [dict(row) for row in cursor.fetchall()]
    logger.debug(f"Found {len(returns)} returns for bundle {bundle_id}")
    return returns


def find_unprocessed_bundles(cursor: sqlite3.Cursor) -> List[Dict]:
    """
    Find all bundles that:
    1. Don't have any entries in BundleReturn table

    Returns:
        List of dicts containing bundle info, ordered by chain_id and end_block.
        Each dict has: bundle_id, chain_id, end_block, relayer_refund_root
    """
    logger.info("Finding unprocessed bundles...")

    cursor.execute("""
        SELECT b.bundle_id, b.chain_id, b.end_block, b.relayer_refund_root
        FROM Bundle b
        WHERE NOT EXISTS (
            SELECT 1 FROM BundleReturn br 
            WHERE br.bundle_id = b.bundle_id 
            AND br.chain_id = b.chain_id
        )
        ORDER BY b.chain_id, b.end_block
    """)

    bundles = [dict(row) for row in cursor.fetchall()]
    logger.info(f"Found {len(bundles)} unprocessed bundles")

    # Group bundles by chain for logging
    chain_counts: dict[int, int] = {}
    for bundle in bundles:
        chain_counts[bundle["chain_id"]] = chain_counts.get(bundle["chain_id"], 0) + 1

    for chain_id, count in chain_counts.items():
        logger.info(f"Chain {chain_id}: {count} bundles to process")

    return bundles


def process_bundle(
    cursor: sqlite3.Cursor, bundle: Dict, active_tokens: Set[Tuple[str, str]]
) -> None:
    """
    Process a single bundle:
    1. For each active token:
        - Get all fills in this bundle for this token
        - Calculate total input amount and LP fees
        - Insert record into BundleReturn

    Args:
        cursor: Database cursor
        bundle: Bundle info dict with bundle_id, chain_id, etc
        active_tokens: Set of (token_address, token_symbol) tuples for this chain
    """
    # logger.info(f"Processing bundle {bundle['bundle_id']} on chain {bundle['chain_id']}")

    for token_address, token_symbol in active_tokens:
        # Get all fills for this bundle and token
        fills, start_block, end_block = get_bundle_fills(
            cursor, bundle["bundle_id"], bundle["chain_id"], token_address
        )

        if not fills:
            continue

        # Get returns for this bundle and token
        returns = get_bundle_returns(
            cursor, bundle["bundle_id"], bundle["chain_id"], token_address
        )

        # Calculate totals
        total_input = sum(Decimal(f["input_amount"]) for f in fills)
        total_lp_fee = sum(
            Decimal(f["lp_fee"] or 0) for f in fills
        )  # lp_fee might be NULL
        total_return = sum(Decimal(r["return_amount"]) for r in returns)

        # Get timestamps from blocks
        start_time = get_block_timestamp(bundle["chain_id"], start_block)
        end_time = get_block_timestamp(bundle["chain_id"], end_block)

        # Insert into BundleReturn
        fill_tx_hashes = ",".join(f["tx_hash"] for f in fills)
        return_tx_hash = returns[0]["tx_hash"] if returns else None

        cursor.execute(
            """
            INSERT INTO BundleReturn (
                bundle_id,
                chain_id,
                token_address,
                token_symbol,
                input_amount,
                return_amount,
                lp_fee,
                start_block,
                end_block,
                start_time,
                end_time,
                fill_tx_hashes,
                return_tx_hash,
                relayer_refund_root
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                bundle["bundle_id"],
                bundle["chain_id"],
                token_address,
                token_symbol,
                str(total_input),
                str(total_return),
                str(total_lp_fee),
                start_block,
                end_block,
                start_time,
                end_time,
                fill_tx_hashes,
                return_tx_hash,
                bundle["relayer_refund_root"],
            ),
        )

        # logger.info(
        # f"Processed bundle {bundle['bundle_id']} token {token_symbol}: "
        # f"input={total_input}, return={total_return}, lp_fee={total_lp_fee}"
        # )


def process_repayments() -> None:
    """
    Main function to process bundle repayments.

    This:
    1. Finds all unprocessed bundles
    2. Groups them by chain
    3. For each chain:
        - Gets active tokens
        - Processes each bundle
        - Updates BundleReturn table
    """

    logger.info("=" * 80)
    logger.info("Processing bundle repayments")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Find all unprocessed bundles
        unprocessed_bundles = find_unprocessed_bundles(cursor)

        if not unprocessed_bundles:
            logger.info("No unprocessed bundles found")
            return

        # Group bundles by chain
        bundles_by_chain = defaultdict(list)
        for bundle in unprocessed_bundles:
            bundles_by_chain[bundle["chain_id"]].append(bundle)

        # Process each chain
        for chain_id, bundles in bundles_by_chain.items():
            logger.info(f"Processing {len(bundles)} bundles for chain {chain_id}")

            # Get active tokens for this chain
            active_tokens = find_active_tokens(cursor, chain_id)

            if not active_tokens:
                logger.warning(f"No active tokens found for chain {chain_id}")
                continue

            # Process each bundle
            for bundle in bundles:
                process_bundle(cursor, bundle, active_tokens)
                conn.commit()

            logger.info(f"Finished processing chain {chain_id}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    process_repayments()
