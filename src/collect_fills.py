"""
Collect and store fill transactions from supported chains.

This module monitors blockchain transactions for fills and stores them in the database.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, cast

import requests
from web3 import Web3

from .config import CHAINS, FILL_RELAY_METHOD_ID, LOGGING_CONFIG, RELAYER_ADDRESS
from .db_utils import get_db_connection

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
contracts = {}


def initialize_contracts():
    """Initialize Web3 contract instances for each chain."""
    if not SPOKE_POOL_ABI:
        logger.error("Cannot initialize contracts without Spoke Pool ABI")
        return

    for chain in CHAINS:
        try:
            w3 = Web3(Web3.HTTPProvider(str(chain["rpc_url"])))
            spoke_pool_address = chain.get("spoke_pool_address")

            if not spoke_pool_address:
                logger.warning(f"No spoke pool address configured for {chain['name']}")
                continue

            checksum_address = Web3.to_checksum_address(spoke_pool_address)
            contract = w3.eth.contract(address=checksum_address, abi=SPOKE_POOL_ABI)
            contracts[chain["chain_id"]] = contract
            logger.debug(f"Initialized contract for {chain['name']}")

        except Exception as e:
            logger.error(f"Error initializing contract for {chain['name']}: {str(e)}")


def get_last_processed_block(chain_id: int) -> int:
    """
    Get the last processed block for a chain from the Fill table.

    Args:
        chain_id: Chain ID to query

    Returns:
        Last processed block number or chain start_block if no fills exist
    """
    conn = get_db_connection()
    if not conn:
        return 0

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MAX(block_number) as last_block FROM Fill WHERE destination_chain_id = ?",
            (chain_id,),
        )
        result: Optional[Dict[str, Any]] = cursor.fetchone()

        if result and result["last_block"] is not None:
            # SQLite returns integers for INTEGER columns
            return cast(int, result["last_block"])

        # If no fills exist, get start_block from chain config
        chain = next((c for c in CHAINS if c["chain_id"] == chain_id), None)
        if chain and "start_block" in chain:
            return cast(int, chain["start_block"])
        return 0

    except Exception as e:
        logger.error(
            f"Error getting last processed block for chain {chain_id}: {str(e)}"
        )
        return 0
    finally:
        conn.close()


def get_fill_transactions(chain: Dict, start_block: int) -> List[Dict]:
    """
    Get fill transactions from the blockchain explorer API.

    Args:
        chain: Chain configuration dictionary
        start_block: Block number to start from

    Returns:
        List of fill transactions
    """
    logger.info(f"Fetching fills for {chain['name']} from block {start_block}")

    url = (
        f"{chain['explorer_api_url']}"
        f"?module=account"
        f"&action=txlist"
        f"&address={RELAYER_ADDRESS}"
        f"&startblock={start_block + 1}"
        f"&endblock=999999999"
        f"&sort=asc"
        f"&apikey={chain['api_key']}"
    )

    try:
        response = requests.get(url)
        data = response.json()

        if data["status"] != "1":
            logger.error(
                f"Error fetching data for {chain['name']}: {data.get('message', 'Unknown error')}"
            )
            return []

        # Filter for successful fillRelay transactions
        fills = [
            tx
            for tx in data.get("result", [])
            if tx.get("methodId") == FILL_RELAY_METHOD_ID and tx.get("isError") == "0"
        ]

        logger.info(f"Found {len(fills)} fills for {chain['name']}")
        return fills

    except Exception as e:
        logger.error(f"Error fetching fills for {chain['name']}: {str(e)}")
        return []


def process_and_store_fill(tx: Dict, chain: Dict):
    """
    Process a fill transaction and store it in the database.

    Args:
        tx: Transaction data from the blockchain explorer
        chain: Chain configuration dictionary
    """
    try:
        # Get contract instance for decoding
        contract = contracts.get(chain["chain_id"])
        if not contract:
            logger.error(f"No contract instance for {chain['name']}")
            return

        # Decode transaction input
        decoded_input = contract.decode_function_input(tx["input"])
        relay_data = decoded_input[1]["relayData"]

        # Get route_id
        conn = get_db_connection()
        if not conn:
            return

        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT route_id 
                FROM Route 
                WHERE origin_chain_id = ? 
                AND destination_chain_id = ? 
                AND input_token = ? 
                AND output_token = ?
            """,
                (
                    int(relay_data["originChainId"]),
                    chain["chain_id"],
                    Web3.to_checksum_address(relay_data["inputToken"].hex()[-40:]),
                    Web3.to_checksum_address(relay_data["outputToken"].hex()[-40:]),
                ),
            )
            result = cursor.fetchone()

            if not result:
                logger.error(f"No route found for fill {tx['hash']}")
                return

            route_id = result["route_id"]

            # Insert fill into database
            cursor.execute(
                """
                INSERT INTO Fill (
                    tx_hash,
                    is_success,
                    route_id,
                    depositor,
                    recipient,
                    exclusive_relayer,
                    input_token,
                    output_token,
                    input_amount,
                    output_amount,
                    origin_chain_id,
                    destination_chain_id,
                    deposit_id,
                    fill_deadline,
                    exclusivity_deadline,
                    message,
                    repayment_chain_id,
                    repayment_address,
                    gas_cost,
                    gas_price,
                    block_number,
                    tx_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    tx["hash"],
                    True,  # is_success (we filtered for successful txs)
                    route_id,
                    Web3.to_checksum_address(relay_data["depositor"].hex()[-40:]),
                    Web3.to_checksum_address(relay_data["recipient"].hex()[-40:]),
                    Web3.to_checksum_address(
                        relay_data["exclusiveRelayer"].hex()[-40:]
                    ),
                    Web3.to_checksum_address(relay_data["inputToken"].hex()[-40:]),
                    Web3.to_checksum_address(relay_data["outputToken"].hex()[-40:]),
                    str(relay_data["inputAmount"]),
                    str(relay_data["outputAmount"]),
                    int(relay_data["originChainId"]),
                    chain["chain_id"],
                    relay_data["depositId"],
                    relay_data.get("fillDeadline"),
                    relay_data.get("exclusivityDeadline"),
                    relay_data.get("message", ""),
                    decoded_input[1].get("repaymentChainId"),
                    Web3.to_checksum_address(
                        decoded_input[1]["repaymentAddress"].hex()[-40:]
                    )
                    if decoded_input[1].get("repaymentAddress")
                    else None,
                    str(int(tx["gasUsed"]) * int(tx["gasPrice"])),
                    tx["gasPrice"],
                    int(tx["blockNumber"]),
                    int(tx["timeStamp"]),
                ),
            )
            conn.commit()
            # logger.info(f"Stored fill {tx['hash']} for {chain['name']}")

        except Exception as e:
            logger.error(f"Error storing fill {tx['hash']}: {str(e)}")
            conn.rollback()
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error processing fill {tx.get('hash', 'unknown')}: {str(e)}")


def collect_fills():
    """
    Main function to collect fills from all chains.
    """
    logger.info("Starting fill collection")
    initialize_contracts()

    if not contracts:
        logger.error("No contracts initialized. Cannot proceed with fill collection.")
        return

    for chain in CHAINS:
        try:
            last_block = get_last_processed_block(chain["chain_id"])
            fills = get_fill_transactions(chain, last_block)

            for fill in fills:
                process_and_store_fill(fill, chain)

        except Exception as e:
            logger.error(f"Error processing chain {chain['name']}: {str(e)}")

    logger.info("Fill collection completed")


if __name__ == "__main__":
    collect_fills()
