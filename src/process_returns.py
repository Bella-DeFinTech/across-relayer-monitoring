"""
Process return events from spoke pool contracts across all chains.
"""

import json
import logging
import os
from typing import cast

from web3 import Web3

from .config import CHAINS, LOGGING_CONFIG, RELAYER_ADDRESS
from .db_utils import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.getLevelName(LOGGING_CONFIG["level"]), format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)


def initialize_spoke_contracts():
    """Initialize spoke pool contracts for each chain."""
    contracts = {}

    # Load spoke pool ABI
    abi_path = os.path.join(os.path.dirname(__file__), "abi", "spoke_abi.json")
    try:
        with open(abi_path, "r") as file:
            spoke_pool_abi = json.load(file)
    except FileNotFoundError:
        logger.error(f"Could not find Spoke Pool ABI at {abi_path}")
        return {}

    # Initialize contract for each chain
    for chain in CHAINS:
        try:
            w3 = Web3(Web3.HTTPProvider(chain["rpc_url"]))
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(chain["spoke_pool_address"]),
                abi=spoke_pool_abi,
            )
            contracts[chain["chain_id"]] = {"contract": contract, "web3": w3}
            logger.debug(f"Initialized spoke pool contract for {chain['name']}")
        except Exception as e:
            logger.error(f"Failed to initialize contract for {chain['name']}: {e}")

    return contracts


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


def process_chain_returns(chain_id: int, contract_data: dict, start_block: int):
    """
    Process return events for a specific blockchain chain.

    Args:
        chain_id (int): ID of the blockchain chain to process returns for
        contract_data (dict): Dictionary containing the Web3 contract and provider
        start_block (int): Block number to start processing returns from

    Note:
        Looks for ExecutedRelayerRefundRoot events and stores them in the Return table.
        Only processes events where the refund address matches RELAYER_ADDRESS.
    """
    logger.info(f"Processing returns from block {start_block} for chain {chain_id}")

    try:
        contract = contract_data["contract"]
        w3 = contract_data["web3"]

        # Get ExecutedRelayerRefundRoot events
        events = contract.events.ExecutedRelayerRefundRoot.get_logs(
            from_block=start_block
        )

        if not events:
            logger.info(f"No new return events found for chain {chain_id}")
            return 0

        logger.info(f"Found {len(events)} return events for chain {chain_id}")

        # Process events
        conn = get_db_connection()
        cursor = conn.cursor()
        returns_saved = 0

        try:
            for event in events:
                refund_addresses = event["args"]["refundAddresses"]
                if RELAYER_ADDRESS in refund_addresses:
                    block = w3.eth.get_block(event["blockNumber"])
                    timestamp = block["timestamp"]
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
    logger.info("Starting return processing")

    # Initialize contracts
    contracts = initialize_spoke_contracts()
    if not contracts:
        logger.error("No contracts initialized. Cannot process returns.")
        return

    # Process each chain
    total_returns = 0
    for chain in CHAINS:
        chain_id = chain["chain_id"]
        if chain_id not in contracts:
            logger.warning(f"Skipping chain {chain_id} - no contract available")
            continue

        try:
            start_block = get_start_block(chain_id)
            total_returns += process_chain_returns(
                chain_id, contracts[chain_id], start_block
            )
        except Exception as e:
            logger.error(f"Failed to process chain {chain_id}: {e}")

    logger.info(f"Total returns saved: {total_returns}")


if __name__ == "__main__":
    process_returns()
