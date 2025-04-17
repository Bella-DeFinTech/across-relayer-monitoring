"""
Process bundle events from the Across Protocol hub and spoke contracts.

This module handles the processing of bundle events by:
1. Tracking the last processed bundle ID for each chain
2. Fetching ProposeRootBundle events from the hub contract on Ethereum
3. Matching these with RelayedRootBundle events from spoke contracts
4. Storing the bundle information in the database with block numbers and timestamps
"""

import logging
import time
from typing import Dict, List, cast

from src.config import CHAINS
from src.db_utils import get_db_connection
from src.web3_utils import get_hub_contract, get_spokepool_contracts

# Configure logging
logger = logging.getLogger(__name__)


def get_last_processed_bundle(chain_id: int) -> int:
    """
    Retrieves the most recently processed bundle ID for a specific chain.

    This function:
    1. Queries the Bundle table for the highest bundle_id for the given chain
    2. If no bundles exist, returns the chain's configured start_block
    3. Uses this information to determine where to resume bundle processing

    Args:
        chain_id: The numeric ID of the chain to query (e.g. 1 for Ethereum)

    Returns:
        int: The highest processed bundle ID if bundles exist,
             or the chain's start_block if no bundles exist,
             or 0 if no configuration exists
    """
    conn = get_db_connection()
    if not conn:
        return 0

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MAX(bundle_id) FROM Bundle WHERE chain_id = ?", (chain_id,)
        )
        result = cursor.fetchone()

        if result and result[0] is not None:
            return cast(int, result[0])

        # If no bundles exist, get start_block from chain config
        chain = next((c for c in CHAINS if c["chain_id"] == chain_id), None)
        if chain and "start_block" in chain:
            return cast(int, chain["start_block"])

        logger.error(f"No start_block configured for chain {chain_id}")
        return 0

    except Exception as e:
        logger.error(f"Error getting last bundle for chain {chain_id}: {str(e)}")
        return 0
    finally:
        conn.close()


def get_last_bundle_end_block(bundle_id: int, chain_id: int) -> int:
    """
    Retrieves the end block number associated with a specific bundle ID for a chain.

    This function is used to:
    1. Find the block range for processing new bundles
    2. Determine where to resume processing after interruptions
    3. Track bundle evaluation block numbers across chains

    If no bundle exists (end_block = 0), returns the chain's configured start_block
    as the starting point for processing.

    Args:
        bundle_id: The bundle ID to look up
        chain_id: The numeric ID of the chain to get the end block for

    Returns:
        int: The end block number for the bundle if found,
             or the chain's configured start_block if no bundle exists

    Note:
        The end block represents the block number used for bundle evaluation
        on the specified chain, as determined by the hub contract
    """

    logger.info(
        f"Getting last bundle end block for bundle {bundle_id} on chain {chain_id}"
    )

    conn = get_db_connection()
    if not conn:
        return 0

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT end_block FROM Bundle WHERE bundle_id = ? AND chain_id = ?",
            (bundle_id, chain_id),
        )
        result = cursor.fetchone()

        # Check if we got a result row and if end_block is not None
        if result is not None and result[0] is not None:
            return cast(int, result[0])

        # If no bundle exists, get start_block from chain config
        chain = next((c for c in CHAINS if c["chain_id"] == chain_id), None)
        if chain and "start_block" in chain:
            logger.info(
                f"No existing bundles found for chain {chain_id}, using start_block from config"
            )
            return cast(int, chain["start_block"])

        logger.error(f"No start_block configured for chain {chain_id}")
        return 0

    except Exception as e:
        logger.error(
            f"Error getting block for bundle {bundle_id} on chain {chain_id}: {str(e)}"
        )
        return 0
    finally:
        conn.close()


def get_spoke_bundle_events(
    chain_id: int, relayer_roots: List[str], start_block: int
) -> List[Dict]:
    """
    Fetches RelayedRootBundle events from a spoke contract that match specific relayer roots.

    This function:
    1. Gets the appropriate spoke contract for the chain
    2. Creates an event filter starting from the specified block
    3. Filters events to only those matching the provided relayer roots
    4. Returns all matching events with their full data

    The events contain:
    - rootBundleId: The ID of the bundle
    - relayerRefundRoot: The root hash for relayer refunds
    - blockNumber: The block where the event was emitted

    Args:
        chain_id: The numeric ID of the chain to get events from
        relayer_roots: List of relayer refund root hashes to filter by
        start_block: The block number to start searching from

    Returns:
        List[Dict]: List of matching event data dictionaries,
                   or empty list if no events found or error occurs
    """
    contracts = get_spokepool_contracts()
    if chain_id not in contracts:
        logger.error(f"No spoke contract for chain {chain_id}")
        return []

    try:
        contract = contracts[chain_id]
        events = contract.events.RelayedRootBundle.create_filter(
            from_block=start_block,
            argument_filters={"relayerRefundRoot": relayer_roots},
        ).get_all_entries()

        return events
    except Exception as e:
        logger.error(f"Error getting spoke events for chain {chain_id}: {str(e)}")
        return []


def process_chain_bundles(chain_id: int, hub_contract) -> None:
    """
    Find the last bundle processed for a chain, and process all new bundles from there.
    Processes and stores bundle information for a specific chain by matching hub and spoke events.



    This function performs the following steps:
    1. Gets the last processed bundle and corresponding block numbers from DB
    2. Fetches new ProposeRootBundle events from the hub contract
    3. Extracts relayer roots from hub events
    4. Fetches matching RelayedRootBundle events from the spoke contract
    5. For each matching pair of events:
       - Extracts bundle ID and block numbers
       - Stores bundle information in the database

    The process ensures:
    - Only new bundles are processed (using last_bundle_id)
    - Block numbers are properly tracked across chains
    - Bundle data is consistently stored with timestamps

    Args:
        chain_id: The numeric ID of the chain to process bundles for
        hub_contract: Web3 contract instance for the Ethereum hub
        spoke_contract: Web3 contract instance for the chain's spoke pool

    Note:
        This function commits database transactions for successful bundle processing
        and rolls back on errors to maintain data consistency
    """

    logger.info(f"* Processing bundles for chain {chain_id}")

    chain = next((c for c in CHAINS if c["chain_id"] == chain_id), None)
    if not chain:
        logger.error(f"No configuration found for chain {chain_id}")
        return

    # Get the last processed bundle id
    last_bundle_id = get_last_processed_bundle(chain_id)
    # Get the end_block for the last processed bundle.
    last_bundle_end_block = get_last_bundle_end_block(last_bundle_id, chain_id) + 1
    last_eth_bundle_end_block = get_last_bundle_end_block(
        last_bundle_id, 1
    )  # Ethereum chain ID is 1

    logger.info(
        f"(Last bundle endblock: {last_bundle_end_block}, last bundle eth endblock: {last_eth_bundle_end_block})"
    )
    try:
        # From eth hub contract, get all ProposeRootBundle events from the last processed bundle endblock
        # Todo: Pagination
        propose_events = hub_contract.events.ProposeRootBundle.create_filter(
            from_block=last_eth_bundle_end_block
        ).get_all_entries()

        if not propose_events:
            logger.info("No new hub events found")
            return

        # Extract relayer roots
        relayer_roots = [event["args"]["relayerRefundRoot"] for event in propose_events]

        # From spoke contract, get all RelayedRootBundle events that match the relayer roots
        bundle_events = get_spoke_bundle_events(
            chain_id, relayer_roots, last_bundle_end_block
        )

        if not bundle_events:
            logger.info(f"No matching spoke events found for chain {chain_id}")
            return

        # Process matching events
        conn = get_db_connection()
        if not conn:
            return

        try:
            cursor = conn.cursor()
            bundles_processed = 0
            latest_bundle_id = last_bundle_id

            # Iterate through each ProposeRootBundle event from the Ethereum hub contract
            for propose_event in propose_events:
                # Find the matching RelayedRootBundle event from the spoke chain by comparing relayer refund roots
                # The relayer refund root uniquely identifies a bundle across chains
                matching_spoke_event = next(
                    (
                        e
                        for e in bundle_events
                        if e["args"]["relayerRefundRoot"]
                        == propose_event["args"]["relayerRefundRoot"]
                    ),
                    None,
                )

                # source: https://github.com/UMAprotocol/UMIPs/blob/master/UMIPs/umip-179.md
                # A Root Bundle Proposal shall consist of the following:
                # 1. relayerRefundRoot:
                # Merkle Root of RelayerRefundLeaf objects of the proposal.
                # 2. bundleEvaluationBlockNumbers
                # The ordered array of block numbers signifying the end block of the proposal for each respective chainId.

                if matching_spoke_event:
                    # Extract the bundle ID from the spoke event
                    bundle_id = matching_spoke_event["args"]["rootBundleId"]

                    #  bundleEvaluationBlockNumbers

                    bundle_eval_block_numbers = propose_event["args"][
                        "bundleEvaluationBlockNumbers"
                    ]

                    # print(len(bundle_eval_block_numbers))  # ALWAYS 19

                    # Get the block number for this specific chain using its index in the array
                    # The index is configured per chain in the CHAINS config
                    bundle_block_index = chain["bundle_block_index"]
                    bundle_end_block = bundle_eval_block_numbers[bundle_block_index]

                    if bundle_end_block >= last_bundle_end_block:
                        # Insert bundle record
                        cursor.execute(
                            """
                            INSERT INTO Bundle (
                                bundle_id,
                                chain_id,
                                relayer_refund_root,
                                end_block,
                                processed_timestamp
                            ) VALUES (?, ?, ?, ?, ?)
                        """,
                            (
                                bundle_id,
                                chain_id,
                                propose_event["args"]["relayerRefundRoot"].hex(),
                                bundle_end_block,
                                int(time.time()),
                            ),
                        )
                        bundles_processed += 1
                        latest_bundle_id = max(latest_bundle_id, bundle_id)

            conn.commit()
            logger.info(
                f"Processed {bundles_processed} bundles for chain {chain_id}. "
                f"Latest bundle ID: {latest_bundle_id}"
            )

        except Exception as e:
            logger.error(f"Error processing bundles for chain {chain_id}: {str(e)}")
            conn.rollback()
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error getting events for chain {chain_id}: {str(e)}")


def process_bundles() -> None:
    """
    Main entry point for processing bundles across all configured chains.

    This function:
    1. Initializes hub and spoke contract connections
    2. Iterates through all configured chains
    3. Processes bundles for each chain that has a valid spoke contract
    4. Logs the overall processing status and any errors

    The process ensures:
    - All chains are processed independently
    - Contract connections are properly established
    - Processing errors in one chain don't affect others
    """
    logger.info("=" * 80)
    logger.info("Processing bundles")

    logger.info("Starting bundle processing")

    # Get contract instances
    hub_contract = get_hub_contract()
    if not hub_contract:
        logger.error("Could not initialize hub contract")
        return

    spoke_contracts = get_spokepool_contracts()
    if not spoke_contracts:
        logger.error("Could not initialize spoke contracts")
        return

    # Process each chain
    for chain in CHAINS:
        chain_id = cast(int, chain["chain_id"])
        if chain_id in spoke_contracts:
            process_chain_bundles(chain_id, hub_contract)
        else:
            logger.warning(f"No spoke contract for chain {chain_id}")

    logger.info("Bundle processing complete")


if __name__ == "__main__":
    process_bundles()
