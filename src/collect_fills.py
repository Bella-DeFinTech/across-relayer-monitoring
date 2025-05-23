"""
Collect and store fill transactions from supported chains.

This module monitors blockchain transactions for fills and stores them in the database.
"""

import logging
from typing import Any, Dict, List, Optional, cast

import requests
from web3 import Web3

from src.config import CHAINS, FILL_RELAY_METHOD_ID, RELAYER_ADDRESS
from src.db_utils import get_db_connection
from src.web3_utils import get_spokepool_contracts

# Configure logging
logger = logging.getLogger(__name__)


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
            if data.get("message") == "No transactions found":
                logger.info(f"No fill transactions found for {chain['name']}")
            else:
                logger.error(
                    f"Error fetching data for {chain['name']}: {data.get('message', 'Unknown error')}"
                )
            return []

        # Filter for fillRelay transactions (both successful and failed)
        fills = [
            tx
            for tx in data.get("result", [])
            if tx.get("methodId") == FILL_RELAY_METHOD_ID
        ]

        successful = len([tx for tx in fills if tx.get("isError") == "0"])
        failed = len([tx for tx in fills if tx.get("isError") == "1"])
        logger.info(
            f"Found {len(fills)} fills for {chain['name']} "
            f"({successful} successful, {failed} failed)"
        )
        return fills

    except Exception as e:
        logger.error(f"Error fetching fills for {chain['name']}: {str(e)}")
        return []


def process_and_store_fill(tx: Dict, chain: Dict, contracts: Dict[int, Any]):
    """
    Process a fill transaction and store it in the database.

    Args:
        tx: Transaction data from the blockchain explorer
        chain: Chain configuration dictionary
        contracts: Dictionary mapping chain IDs to contract instances
    """
    try:
        # Get contract instance for decoding
        contract = contracts.get(chain["chain_id"])
        if not contract:
            logger.error(f"No contract instance for {chain['name']}")
            return

        try:
            # Decode transaction input - may fail for failed transactions
            decoded_input = contract.decode_function_input(tx["input"])
            relay_data = decoded_input[1]["relayData"]
        except Exception as e:
            logger.error(f"Failed to decode input for tx {tx['hash']}: {str(e)}")
            # For failed txs where we can't decode input, we can't proceed
            return

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
                    tx.get("isError") == "0",  # is_success based on transaction status
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
            status = "successful" if tx.get("isError") == "0" else "failed"
            logger.debug(f"Stored {status} fill {tx['hash']} for {chain['name']}")

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
    logger.info("=" * 80)
    logger.info("Collecting fills")
    contracts = get_spokepool_contracts()

    if not contracts:
        logger.error("No contracts initialized. Cannot proceed with fill collection.")
        return

    for chain in CHAINS:
        try:
            last_block = get_last_processed_block(chain["chain_id"])
            fills = get_fill_transactions(chain, last_block)

            for fill in fills:
                process_and_store_fill(fill, chain, contracts)

        except Exception as e:
            logger.error(f"Error processing chain {chain['name']}: {str(e)}")

    logger.info("Fill collection completed")


if __name__ == "__main__":
    collect_fills()
