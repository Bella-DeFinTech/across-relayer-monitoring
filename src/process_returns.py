"""
Process return events from spoke pool contracts across all chains.
"""

import logging
from web3 import Web3
import json
import os

from .config import CHAINS, LOGGING_CONFIG
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
                abi=spoke_pool_abi
            )
            contracts[chain["chain_id"]] = {
                "contract": contract,
                "web3": w3
            }
            logger.debug(f"Initialized spoke pool contract for {chain['name']}")
        except Exception as e:
            logger.error(f"Failed to initialize contract for {chain['name']}: {e}")
            
    return contracts

def get_start_block(chain_id: int) -> int:
    """Get the block to start processing returns from."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT MAX(block_number) 
            FROM Return 
            WHERE return_chain_id = ?
        """, (chain_id,))
        
        last_block = cursor.fetchone()[0]
        
        if last_block is not None:
            return last_block + 1
            
        # If no returns processed yet, use chain's configured start block
        chain = next(c for c in CHAINS if c["chain_id"] == chain_id)
        return chain["start_block"]
        
    finally:
        conn.close()

def process_chain_returns(chain_id: int, contract_data: dict, start_block: int):
    """Process returns for a specific chain."""
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
            return
            
        logger.info(f"Found {len(events)} return events for chain {chain_id}")
        
        # Process events
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            for event in events:
                args = event["args"]
                block = w3.eth.get_block(event["blockNumber"])
                
                # Process each refund in the event
                for leaf_id, (refund_address, amount) in enumerate(
                    zip(args["refundAddresses"], args["refundAmounts"])
                ):
                    try:
                        cursor.execute("""
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
                        """, (
                            event["transactionHash"].hex(),
                            chain_id,
                            args["l2TokenAddress"],
                            str(amount),
                            args["rootBundleId"],
                            leaf_id,
                            refund_address,
                            False,  # is_deferred - we'll need to determine this properly
                            event.get("transaction", {}).get("from", ""),  # caller
                            event["blockNumber"],
                            block["timestamp"]
                        ))
                    except Exception as e:
                        logger.error(f"Error inserting return: {e}")
                        continue
                        
            conn.commit()
            logger.info(f"Processed returns up to block {events[-1]['blockNumber']} for chain {chain_id}")
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error processing returns for chain {chain_id}: {e}")

def process_returns():
    """Process returns across all chains."""
    logger.info("Starting return processing")
    
    # Initialize contracts
    contracts = initialize_spoke_contracts()
    if not contracts:
        logger.error("No contracts initialized. Cannot process returns.")
        return
        
    # Process each chain
    for chain in CHAINS:
        chain_id = chain["chain_id"]
        if chain_id not in contracts:
            logger.warning(f"Skipping chain {chain_id} - no contract available")
            continue
            
        try:
            start_block = get_start_block(chain_id)
            process_chain_returns(chain_id, contracts[chain_id], start_block)
        except Exception as e:
            logger.error(f"Failed to process chain {chain_id}: {e}")

if __name__ == "__main__":
    process_returns()