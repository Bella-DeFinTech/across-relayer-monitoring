#!/usr/bin/env python3
"""
Enrich Fill records with deposit timestamps and LP fees.

This module:
1. Retrieves Fill records missing deposit timestamps
2. Finds corresponding deposit events on origin chains
3. Gets LP fees from the Across API using deposit parameters
4. Updates Fill records with the enriched data
"""

import asyncio
import logging
import sqlite3
from typing import Dict, List, Optional, Tuple, cast

import aiohttp

from .config import CHAINS, LOGGING_CONFIG, get_db_path
from .web3_utils import get_spokepool_contracts

# Configure logging
logging.basicConfig(
    level=logging.getLevelName(LOGGING_CONFIG["level"]), format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)


def get_deposit_start_block(chain_id: int) -> int:
    """
    Get the block to start searching for deposit events from.

    Args:
        chain_id: Chain ID to get start block for

    Returns:
        Latest deposit block number from Fill table if exists,
        otherwise chain's start_block - 1000000 for first run

    Raises:
        TypeError: If chain_id or start_block in configuration has invalid type
    """
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    try:
        # Try to get the latest deposit block for this chain
        cursor.execute(
            """
            SELECT MAX(deposit_block_number) 
            FROM Fill 
            WHERE origin_chain_id = ?
            """,
            (chain_id,),
        )
        result = cursor.fetchone()

        if result and result[0] is not None:
            logger.info(f"Using latest deposit block {result[0]} for chain {chain_id}")
            return result[0] - 1000000  # buffer in case any misses in prev run.

        # If no deposits found, use chain's start_block - 1M blocks
        chain = next((c for c in CHAINS if c["chain_id"] == chain_id), None)
        if not chain:
            raise ValueError(f"No configuration found for chain {chain_id}")

        if "start_block" not in chain:
            raise ValueError(f"No start_block configured for chain {chain_id}")

        try:
            start_block = cast(int, chain["start_block"]) - 1000000
            logger.info(
                f"No deposits found for chain {chain_id}, using start_block - 1M: {start_block}"
            )
            return start_block
        except (TypeError, ValueError) as e:
            raise TypeError(f"Invalid start_block type for chain {chain_id}: {str(e)}")

    except Exception as e:
        logger.error(
            f"Error getting deposit start block for chain {chain_id}: {str(e)}"
        )
        raise
    finally:
        conn.close()


def get_unenriched_fills() -> List[Dict]:
    """
    Retrieve all Fill records that need deposit timestamp and LP fee enrichment.

    Returns:
        List of Fill records missing deposit timestamp or LP fee information
    """
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            tx_hash,
            deposit_id,
            origin_chain_id,
            destination_chain_id,
            input_token,
            output_token,
            input_amount
        FROM Fill 
        WHERE is_success = 1 
          AND (deposit_timestamp IS NULL OR lp_fee IS NULL)
    """)

    columns = [col[0] for col in cursor.description]
    fills = [dict(zip(columns, row)) for row in cursor.fetchall()]

    conn.close()
    return fills


def get_deposit_events(deposit_ids: List[str]) -> Dict[str, Dict]:
    """
    Find FundsDeposited events matching the given deposit IDs across all chains.

    Args:
        deposit_ids: List of deposit IDs to search for

    Returns:
        Dictionary mapping deposit IDs to their corresponding events

    Raises:
        TypeError: If chain_id in configuration has invalid type
    """
    # Get contracts for all chains
    contracts = get_spokepool_contracts()
    if not contracts:
        logger.error("No contracts initialized")
        return {}

    # Convert string IDs to integers for contract filtering
    int_deposit_ids = [int(deposit_id) for deposit_id in deposit_ids]

    all_events = []

    # Query each chain for deposit events
    for chain in CHAINS:
        try:
            try:
                chain_id = cast(int, chain.get("chain_id"))
            except (TypeError, ValueError) as e:
                logger.error(
                    f"Invalid chain_id in configuration: {chain.get('chain_id')}"
                )
                raise TypeError(f"Invalid chain_id type in configuration: {str(e)}")

            chain_name = chain["name"]

            # Get appropriate start block for this chain
            start_block = get_deposit_start_block(chain_id)

            if chain_id not in contracts:
                logger.warning(f"No contract configured for chain {chain_name}")
                continue

            contract = contracts[chain_id]

            logger.info(
                f"Searching for deposit events on {chain_name} from block {start_block}"
            )

            try:
                event_filter = contract.events.FundsDeposited.create_filter(
                    from_block=start_block,
                    argument_filters={"depositId": int_deposit_ids},
                )
                events = event_filter.get_all_entries()
                all_events.extend(events)
                logger.info(f"Found {len(events)} events on {chain_name}")

            except Exception as e:
                logger.error(f"Error getting events from {chain_name}: {str(e)}")
                continue

        except Exception as e:
            logger.error(
                f"Error processing chain {chain.get('name', 'unknown')}: {str(e)}"
            )

    # Convert to dictionary format mapping deposit IDs to events
    events_by_deposit_id = {}
    for event in all_events:
        deposit_id = str(event["args"]["depositId"])
        events_by_deposit_id[deposit_id] = event

    return events_by_deposit_id


async def get_lp_fee(
    input_token: str,
    output_token: str,
    origin_chain_id: int,
    destination_chain_id: int,
    amount: str,
    deposit_timestamp: int,
    session: aiohttp.ClientSession,
    max_retries: int = 3,
    initial_delay: float = 1.0,
) -> Optional[str]:
    """
    Retrieve LP fee from the Across API for a given transfer.
    Implements exponential backoff retry logic for failed requests.

    Args:
        input_token: Token address on origin chain
        output_token: Token address on destination chain
        origin_chain_id: Chain ID where deposit originated
        destination_chain_id: Chain ID where fill happened
        amount: Amount being transferred (in smallest unit)
        deposit_timestamp: Unix timestamp when deposit was made
        session: Aiohttp client session for making requests
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds before first retry (default: 1.0)

    Returns:
        LP fee as a string in the smallest unit, or None if all retries fail
    """
    url = f"https://app.across.to/api/suggested-fees?inputToken={input_token}&outputToken={output_token}&originChainId={origin_chain_id}&destinationChainId={destination_chain_id}&amount={amount}&timestamp={deposit_timestamp}"

    # Old code without exponential backoff
    # try:
    #     async with session.get(url) as response:
    #         if response.status == 200:
    #             data = await response.json()
    #             return str(data["lpFee"]["total"])
    #         else:
    #             logger.error(f"API request failed with status {response.status}")
    #             return None
    # except Exception as e:
    #     logger.error(f"Error retrieving LP fee from API: {str(e)}")
    #     return None

    for attempt in range(max_retries + 1):  # +1 for initial attempt
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return str(data["lpFee"]["total"])

                # If not last attempt, prepare for retry
                if attempt < max_retries:
                    delay = initial_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        f"API request failed with status {response.status}. "
                        f"Retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"API request failed with status {response.status} "
                        f"after {max_retries} retries"
                    )
                    return None

        except Exception as e:
            if attempt < max_retries:
                delay = initial_delay * (2**attempt)
                logger.warning(
                    f"Error retrieving LP fee: {str(e)}. "
                    f"Retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"Error retrieving LP fee after {max_retries} retries: {str(e)}"
                )
                return None

    return None  # Should never reach here, but keeps mypy happy


def update_fill_with_enrichment(
    tx_hash: str, deposit_timestamp: int, deposit_block_number: int, lp_fee: str
) -> bool:
    """
    Update a Fill record with deposit timestamp, block number and LP fee.

    Args:
        tx_hash: Transaction hash of the fill
        deposit_timestamp: Unix timestamp of the deposit event
        deposit_block_number: Block number where deposit occurred
        lp_fee: Calculated LP fee

    Returns:
        Boolean indicating success
    """
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            UPDATE Fill 
            SET deposit_timestamp = ?, 
                deposit_block_number = ?,
                lp_fee = ? 
            WHERE tx_hash = ?
            """,
            (deposit_timestamp, deposit_block_number, lp_fee, tx_hash),
        )
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Error updating fill {tx_hash}: {str(e)}")
        conn.rollback()
        return False
    finally:
        conn.close()


async def process_fill_batch(
    fills: List[Dict], deposit_events: Dict[str, Dict]
) -> Tuple[int, int]:
    """
    Process a batch of fills asynchronously.

    Args:
        fills: List of fills to process
        deposit_events: Dictionary of deposit events

    Returns:
        Tuple of (processed_count, failed_count)
    """
    processed = 0
    failed = 0
    missing_deposits: List[str] = []
    api_failures: List[str] = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for fill in fills:
            deposit_id = fill["deposit_id"]
            if deposit_id not in deposit_events:
                failed += 1
                missing_deposits.append(deposit_id)
                continue

            event = deposit_events[deposit_id]
            deposit_timestamp = event["args"]["quoteTimestamp"]
            deposit_block_number = event["blockNumber"]

            # Create task for getting LP fee
            task = asyncio.create_task(
                get_lp_fee(
                    fill["input_token"],
                    fill["output_token"],
                    fill["origin_chain_id"],
                    fill["destination_chain_id"],
                    fill["input_amount"],
                    deposit_timestamp,
                    session,
                )
            )
            tasks.append(
                (
                    fill["tx_hash"],
                    deposit_timestamp,
                    deposit_block_number,
                    task,
                    deposit_id,
                )
            )

        # Wait for all LP fee requests to complete
        for tx_hash, deposit_timestamp, deposit_block_number, task, deposit_id in tasks:
            try:
                lp_fee = await task
                if lp_fee is not None:
                    update_fill_with_enrichment(
                        tx_hash, deposit_timestamp, deposit_block_number, lp_fee
                    )
                    processed += 1
                else:
                    failed += 1
                    api_failures.append(deposit_id)
            except Exception as e:
                logger.error(f"Error processing fill {tx_hash}: {str(e)}")
                failed += 1
                api_failures.append(deposit_id)

    # Log failure statistics
    if missing_deposits:
        logger.info(f"Missing deposit events: {len(missing_deposits)} fills")
        logger.info(f"  Deposit IDs: {', '.join(str(id) for id in missing_deposits)}")

    if api_failures:
        logger.info(f"LP fee API failures: {len(api_failures)} fills")
        logger.info(f"  Deposit IDs: {', '.join(str(id) for id in api_failures)}")

    return processed, failed


async def enrich_fills_async():
    """
    Main async function to enrich Fill records with deposit timestamps and LP fees.
    """
    # Get fills that need enrichment
    fills = get_unenriched_fills()
    if not fills:
        logger.info("No fills pending enrichment")
        return

    logger.info(f"Found {len(fills)} fills needing enrichment")

    # Get all the deposit IDs we need to look up
    deposit_ids = [fill["deposit_id"] for fill in fills]

    # Find deposit events for those IDs
    deposit_events = get_deposit_events(deposit_ids)
    logger.info(f"Found {len(deposit_events)} matching deposit events")

    # Process fills in batches
    processed, failed = await process_fill_batch(fills, deposit_events)

    logger.info(f"Enrichment complete: {processed} processed, {failed} failed")


def enrich_fills():
    """
    Main synchronous entry point to enrich Fill records.
    """
    asyncio.run(enrich_fills_async())


if __name__ == "__main__":
    enrich_fills()
