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
import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from web3 import Web3

from config import CHAINS, LOGGING_CONFIG, get_db_path

# Configure logging
logging.basicConfig(
    level=logging.getLevelName(LOGGING_CONFIG["level"]), format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)

# Load contract ABIs
ABI_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "abi")
SPOKE_ABI_PATH = os.path.join(ABI_DIR, "spoke_abi.json")

try:
    with open(SPOKE_ABI_PATH, "r") as file:
        SPOKE_POOL_ABI = json.load(file)
except FileNotFoundError:
    logger.error(f"Could not find Spoke Pool ABI file at {SPOKE_ABI_PATH}")
    SPOKE_POOL_ABI = []

# Store contract instances
contracts: Dict[int, Dict[str, Any]] = {}


def initialize_contracts() -> Dict[int, Dict[str, Any]]:
    """
    Initialize Web3 contract instances for each chain.

    Returns:
        Dictionary mapping chain IDs to dictionaries containing web3 instances
        and contract instances.
    """
    initialized_contracts: Dict[int, Dict[str, Any]] = {}

    if not SPOKE_POOL_ABI:
        logger.error("Cannot initialize contracts without Spoke Pool ABI")
        return initialized_contracts

    for chain in CHAINS:
        try:
            chain_id = int(chain["chain_id"])
            w3 = Web3(Web3.HTTPProvider(str(chain["rpc_url"])))
            spoke_pool_address = chain.get("spoke_pool_address")

            if not spoke_pool_address:
                logger.warning(f"No spoke pool address for chain {chain['name']}")
                continue

            # Create contract instance
            spoke_contract = w3.eth.contract(
                address=Web3.to_checksum_address(spoke_pool_address), abi=SPOKE_POOL_ABI
            )

            # Store all necessary objects for this chain
            initialized_contracts[chain_id] = {
                "web3": w3,
                "spoke_contract": spoke_contract,
                "chain_info": chain,
            }

            logger.debug(f"Initialized contract for chain {chain['name']}")

        except Exception as e:
            logger.error(
                f"Error initializing contract for {chain.get('name', 'unknown')}: {str(e)}"
            )

    return initialized_contracts


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
          AND (tx_timestamp IS NULL OR lp_fee IS NULL)
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
    """
    # Initialize contracts for all chains
    chain_contracts = initialize_contracts()
    if not chain_contracts:
        logger.error("No contracts initialized")
        return {}

    # Convert string IDs to integers for contract filtering
    int_deposit_ids = [int(deposit_id) for deposit_id in deposit_ids]

    all_events = []

    # Query each chain for deposit events
    for chain in CHAINS:
        try:
            chain_id = int(chain["chain_id"])
            chain_name = chain["name"]
            start_block = chain["start_block"]  #! Why is this needed?
            # start_block = chain["start_block"] - 1000000 #! Why is this needed?

            if chain_id not in chain_contracts:
                logger.warning(f"No contract configured for chain {chain_name}")
                continue

            contract = chain_contracts[chain_id]["spoke_contract"]

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
) -> Optional[str]:
    """
    Retrieve LP fee from the Across API for a given transfer.

    Args:
        input_token: Token address on origin chain
        output_token: Token address on destination chain
        origin_chain_id: Chain ID where deposit originated
        destination_chain_id: Chain ID where fill happened
        amount: Amount being transferred (in smallest unit)
        deposit_timestamp: Unix timestamp when deposit was made
        session: Aiohttp client session for making requests

    Returns:
        LP fee as a string in the smallest unit, or None if error
    """
    url = f"https://app.across.to/api/suggested-fees?inputToken={input_token}&outputToken={output_token}&originChainId={origin_chain_id}&destinationChainId={destination_chain_id}&amount={amount}&timestamp={deposit_timestamp}"

    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return str(data["lpFee"]["total"])
            else:
                logger.error(f"API request failed with status {response.status}")
                return None
    except Exception as e:
        logger.error(f"Error retrieving LP fee from API: {str(e)}")
        return None


def update_fill_with_enrichment(
    tx_hash: str, deposit_timestamp: int, lp_fee: str
) -> bool:
    """
    Update a Fill record with deposit timestamp and LP fee.

    Args:
        tx_hash: Transaction hash of the fill
        deposit_timestamp: Unix timestamp of the deposit event
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
            SET deposit_timestamp = ?, lp_fee = ? 
            WHERE tx_hash = ?
            """,
            (deposit_timestamp, lp_fee, tx_hash),
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
            deposit_timestamp = event["args"][
                "quoteTimestamp"
            ]  # This is guaranteed to exist

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
            tasks.append((fill["tx_hash"], deposit_timestamp, task, deposit_id))

        # Wait for all LP fee requests to complete
        for tx_hash, deposit_timestamp, task, deposit_id in tasks:
            try:
                lp_fee = await task
                if lp_fee is not None:
                    update_fill_with_enrichment(tx_hash, deposit_timestamp, lp_fee)
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
