#!/usr/bin/env python3
"""
Enrich Fill records with deposit timestamps and LP fees.

This module:
1. Retrieves Fill records missing deposit timestamps
2. Finds corresponding deposit events on origin chains
3. Gets LP fees from the Across API using deposit parameters
4. Updates Fill records with the enriched data
"""

import logging
import os
import sqlite3
import json
import requests
import time
from typing import Dict, List, Any, Optional, Tuple
from pprint import pprint
from web3 import Web3

from config import CHAINS, LOGGING_CONFIG, get_db_path

# Configure logging
logging.basicConfig(
    level=logging.getLevelName(LOGGING_CONFIG["level"]),
    format=LOGGING_CONFIG["format"]
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


def initialize_contracts() -> Dict[int, Dict[str, Any]]:
    """
    Initialize Web3 contract instances for each chain.
    
    Returns:
        Dictionary mapping chain IDs to dictionaries containing web3 instances 
        and contract instances.
    """
    contracts = {}
    
    if not SPOKE_POOL_ABI:
        logger.error("Cannot initialize contracts without Spoke Pool ABI")
        return contracts

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
                address=Web3.to_checksum_address(spoke_pool_address),
                abi=SPOKE_POOL_ABI
            )
            
            # Store all necessary objects for this chain
            contracts[chain_id] = {
                "web3": w3,
                "spoke_contract": spoke_contract,
                "chain_info": chain
            }
            
            logger.debug(f"Initialized contract for chain {chain['name']}")
            
        except Exception as e:
            logger.error(f"Error initializing contract for {chain.get('name', 'unknown')}: {str(e)}")
    
    return contracts


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
            start_block = chain["start_block"] - 1000000 #! Why is this needed?
            # start_block = chain["start_block"] - 1000000 #! Why is this needed?
            
            if chain_id not in chain_contracts:
                logger.warning(f"No contract configured for chain {chain_name}")
                continue
                
            contract = chain_contracts[chain_id]["spoke_contract"]
            
            logger.info(f"Searching for deposit events on {chain_name} from block {start_block}")
            
            try:
                event_filter = contract.events.FundsDeposited.create_filter(
                    from_block=start_block,
                    argument_filters={'depositId': int_deposit_ids}
                )
                events = event_filter.get_all_entries()
                all_events.extend(events)
                logger.info(f"Found {len(events)} events on {chain_name}")
                
            except Exception as e:
                logger.error(f"Error getting events from {chain_name}: {str(e)}")
                continue
                
        except Exception as e:
            logger.error(f"Error processing chain {chain.get('name', 'unknown')}: {str(e)}")
            
    # Convert to dictionary format mapping deposit IDs to events
    events_by_deposit_id = {}
    for event in all_events:
        deposit_id = str(event["args"]["depositId"])
        events_by_deposit_id[deposit_id] = event
        
    return events_by_deposit_id


def get_lp_fee(
    input_token: str,
    output_token: str, 
    origin_chain_id: int,
    destination_chain_id: int,
    amount: str,
    deposit_timestamp: int
) -> str:
    """
    Retrieve LP fee from the Across API for a given transfer.
    
    Args:
        input_token: Token address on origin chain
        output_token: Token address on destination chain
        origin_chain_id: Chain ID where deposit originated
        destination_chain_id: Chain ID where fill happened
        amount: Amount being transferred (in smallest unit)
        deposit_timestamp: Unix timestamp when deposit was made
        
    Returns:
        LP fee as a string in the smallest unit
    """
    # Format URL exactly as in the original implementation
    url = f"https://app.across.to/api/suggested-fees?inputToken={input_token}&outputToken={output_token}&originChainId={origin_chain_id}&destinationChainId={destination_chain_id}&amount={amount}&timestamp={deposit_timestamp}"
    
    # logger.info(f"Requesting LP fee from: {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for non-200 responses
        
        # Log full response for debugging
        data = response.json()
        # logger.info(f"API response: {json.dumps(data, indent=2)}")
        
        # Check response structure and extract LP fee
        if 'lpFee' in data and 'total' in data['lpFee']:
            lp_fee = data['lpFee']['total']
            # logger.info(f"Got LP fee: {lp_fee}")
            return str(lp_fee)
        else:
            logger.warning(f"Unexpected API response structure. Keys in response: {list(data.keys())}")
            if 'lpFee' in data:
                logger.warning(f"lpFee keys: {list(data['lpFee'].keys()) if isinstance(data['lpFee'], dict) else 'not a dict'}")
            return "0"  # Default to zero on unexpected response
        
    except Exception as e:
        logger.error(f"Error retrieving LP fee from API: {str(e)}")
        return "0"  # Default to zero on API error


def update_fill_with_enrichment(
    tx_hash: str,
    deposit_timestamp: int,
    lp_fee: str
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
        # Check if deposit_timestamp column exists
        cursor.execute("PRAGMA table_info(Fill)")
        columns = [col[1] for col in cursor.fetchall()]
        deposit_column_exists = "deposit_timestamp" in columns
        
        if deposit_column_exists:
            cursor.execute(
                """
                UPDATE Fill 
                SET deposit_timestamp = ?, lp_fee = ? 
                WHERE tx_hash = ?
                """,
                (deposit_timestamp, lp_fee, tx_hash)
            )
        else:
            # Just update the lp_fee if deposit_timestamp doesn't exist
            cursor.execute(
                """
                UPDATE Fill 
                SET lp_fee = ? 
                WHERE tx_hash = ?
                """,
                (lp_fee, tx_hash)
            )
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Error updating fill {tx_hash}: {str(e)}")
        conn.rollback()
        return False
    finally:
        conn.close()


def enrich_fills():
    """
    Main function to enrich Fill records with deposit timestamps and LP fees.
    
    Returns:
        Tuple of (processed_fills, failed_fills)
    """
    # Get fills that need enrichment
    fills = get_unenriched_fills()
    if not fills:
        logger.info("No fills pending enrichment")
    
    logger.info(f"Found {len(fills)} fills needing enrichment")
    
    # Get all the deposit IDs we need to look up
    deposit_ids = [fill['deposit_id'] for fill in fills]

    # Find deposit events for those IDs
    deposit_events = get_deposit_events(deposit_ids)
    logger.info(f"Found {len(deposit_events)} matching deposit events")

    # Process and update each fill
    processed = 0
    failed = 0
    
    for fill in fills:
        deposit_id = fill['deposit_id']
        if deposit_id not in deposit_events:
            logger.warning(f"No deposit event found for deposit ID {deposit_id}")
            failed += 1
            continue
            
        event = deposit_events[deposit_id]
        
        # Extract deposit timestamp (quoteTimestamp)
        deposit_timestamp = event['args'].get('quoteTimestamp')
        if not deposit_timestamp:
            logger.warning(f"No quoteTimestamp found in event for deposit ID {deposit_id}")
            failed += 1
            continue
            
        # Get LP fee from Across API
        lp_fee = get_lp_fee(
            fill['input_token'],
            fill['output_token'],
            fill['origin_chain_id'],
            fill['destination_chain_id'],
            fill['input_amount'],
            deposit_timestamp
        )
        
        # Update the fill record
        success = update_fill_with_enrichment(
            fill['tx_hash'],
            deposit_timestamp,
            lp_fee
        )
        
        if success:
            processed += 1
        else:
            failed += 1
    
    logger.info(f"Enrichment complete: {processed} processed, {failed} failed")
    logger.info(f"Enriched {processed} fills, failed on {failed} fills")



if __name__ == "__main__":
    enrich_fills()


