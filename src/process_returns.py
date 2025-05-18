"""
Process return events from spoke pool contracts across all chains.
"""

import logging
from typing import cast
from web3.exceptions import ContractLogicError
from requests.exceptions import RequestException
import json

from web3.contract import Contract

from src.config import CHAINS, RELAYER_ADDRESS
from src.db_utils import get_db_connection
from src.web3_utils import get_block_timestamp, get_spokepool_contracts

# Configure logging
logger = logging.getLogger(__name__)


def get_start_block(chain_id: int) -> int:
    """Get the block to start processing returns from."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT MAX(block_number) 
            FROM Return 
            WHERE return_chain_id = ?
        """,
            (chain_id,),
        )

        result = cursor.fetchone()
        last_block = result[0] if result else None

        if last_block is not None:
            return last_block + 1

        # If no returns processed yet, use chain's configured start block
        chain = next(c for c in CHAINS if c["chain_id"] == chain_id)
        return cast(int, chain["start_block"])

    finally:
        conn.close()


def get_executed_relayer_refund_root_events(contract: Contract, start_block: int, chain_id: int) -> list:
    """
    Fetch ExecutedRelayerRefundRoot events using pagination to handle large block ranges.
    
    Args:
        contract (Contract): Web3 contract instance
        start_block (int): Starting block number
        chain_id (int): Chain ID for logging purposes
        
    Returns:
        list: List of ExecutedRelayerRefundRoot events
        
    Raises:
        ContractLogicError: If there's an error fetching events from the contract
        RequestException: If there's a network request error
    """
    current_block = contract.w3.eth.block_number
    all_events = []
    current_start = start_block
    
    while current_start < current_block:
        current_end = min(current_start + 499, current_block)
        logger.info(f"Fetching blocks {current_start} to {current_end} for chain {chain_id}")
        
        chunk_events = contract.events.ExecutedRelayerRefundRoot.get_logs(
            from_block=current_start,
            to_block=current_end
        )
        all_events.extend(chunk_events)
        current_start = current_end + 1
        
    return all_events


def process_chain_returns(chain_id: int, contract: Contract, start_block: int) -> int:
    """
    Process return events for a specific blockchain chain.

    Args:
        chain_id (int): ID of the blockchain chain to process returns for
        contract (Contract): Web3 contract instance for the chain
        start_block (int): Block number to start processing returns from

    Returns:
        int: Number of return events processed and saved

    Note:
        Looks for ExecutedRelayerRefundRoot events and stores them in the Return table.
        Only processes events where the refund address matches RELAYER_ADDRESS.
    """
    logger.info(f"Processing returns from block {start_block} for chain {chain_id}")

    try:
        # Get ExecutedRelayerRefundRoot events
        try:
            events = get_executed_relayer_refund_root_events(contract, start_block, chain_id)
        except (ContractLogicError, RequestException) as e:
            # Extract the full error response if available
            error_msg = str(e)
            try:
                if hasattr(e, 'response') and e.response is not None:
                    error_details = e.response.json()
                    error_msg = f"Full error: {json.dumps(error_details, indent=2)}"
            except Exception:
                pass
            logger.error(f"Error getting logs for chain {chain_id}: {error_msg}")
            return 0

        if not events:
            logger.info(f"No new return events found for chain {chain_id}")
            return 0

        matching_events = sum(
            1 for event in events if RELAYER_ADDRESS in event["args"]["refundAddresses"]
        )
        logger.info(
            f"Found {len(events)} return events for chain {chain_id} ({matching_events} matching our relayer address)"
        )

        # Process events
        conn = get_db_connection()
        cursor = conn.cursor()
        returns_saved = 0

        try:
            for event in events:
                refund_addresses = event["args"]["refundAddresses"]
                if RELAYER_ADDRESS in refund_addresses:
                    timestamp = get_block_timestamp(chain_id, event["blockNumber"])
                    indices = [
                        i
                        for i, addr in enumerate(refund_addresses)
                        if addr == RELAYER_ADDRESS
                    ]
                    for index in indices:
                        try:
                            cursor.execute(
                                """
                                INSERT INTO Return (
                                    tx_hash,
                                    return_chain_id,
                                    return_token,
                                    return_amount,
                                    root_bundle_id,
                                    leaf_id,
                                    refund_address,
                                    is_deferred,
                                    caller,
                                    block_number,
                                    tx_timestamp
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  
                            """,
                                (
                                    event["transactionHash"].hex(),  #! tx_hash
                                    chain_id,  #! return_chain_id
                                    event["args"]["l2TokenAddress"],  #! return_token
                                    str(
                                        event["args"]["refundAmounts"][index]
                                    ),  #! return_amount
                                    event["args"]["rootBundleId"],  #! root_bundle_id
                                    event["args"]["leafId"],  #! leaf_id
                                    event["args"]["refundAddresses"][
                                        index
                                    ],  #! refund_address
                                    1
                                    if event["args"]["deferredRefunds"]
                                    else 0,  #! is_deferred
                                    event["args"]["caller"],  #! caller
                                    event["blockNumber"],  #! block_number
                                    timestamp,  #! tx_timestamp
                                ),
                            )
                            returns_saved += 1
                        except Exception as e:
                            logger.error(
                                f"Error inserting return event for chain {chain_id}: {e}"
                            )
            conn.commit()
            logger.info(
                f"Processed returns up to block {events[-1]['blockNumber']} for chain {chain_id}"
            )
            return returns_saved

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error processing returns for chain {chain_id}: {e}")
        return 0


def process_returns():
    """Process returns across all chains."""

    logger.info("=" * 80)
    logger.info("Processing returns")

    # Get contracts using web3_utils
    contracts = get_spokepool_contracts()
    if not contracts:
        logger.error("No contracts initialized. Cannot process returns.")
        return

    # Process each chain
    total_returns = 0
    for chain in CHAINS:
        try:
            chain_id = cast(int, chain["chain_id"])
            if chain_id == 1: # we only have returns for WBTC on eth_mainnet
                start_block = get_start_block(chain_id)
                total_returns += process_chain_returns(
                    chain_id, contracts[chain_id], start_block
                )
        except Exception as e:
            logger.error(f"Failed to process chain {chain_id}: {e}")

    logger.info(f"Total returns saved: {total_returns}")


if __name__ == "__main__":
    process_returns()
