"""
This script identifies tokens used in Across Protocol's fillRelay transactions
by analyzing the relayer's blockchain transactions.
"""

import json
import os
from typing import Any, Dict, List

import requests
from web3 import Web3

from .config import (
    CHAINS,
    FILL_RELAY_METHOD_ID,
    RELAYER_ADDRESS,
    chain_id_to_name,
)
from .db_utils import get_db_connection, insert_route, insert_token

# Define paths to ABI files
ABI_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "abi")
ERC20_ABI_PATH = os.path.join(ABI_DIR, "erc20_abi.json")
SPOKE_ABI_PATH = os.path.join(ABI_DIR, "spoke_abi.json")

# Load the ABIs
try:
    with open(ERC20_ABI_PATH, "r") as file:
        ERC20_ABI = json.load(file)
except FileNotFoundError:
    print(f"Error: Could not find ERC20 ABI file at {ERC20_ABI_PATH}")
    ERC20_ABI = []

try:
    with open(SPOKE_ABI_PATH, "r") as file:
        SPOKE_POOL_ABI = json.load(file)
except FileNotFoundError:
    print(f"Error: Could not find Spoke Pool ABI file at {SPOKE_ABI_PATH}")
    SPOKE_POOL_ABI = []

# Store contract instances
contracts = {}


def initialize_contracts():
    """Initialize Web3 contract instances for each chain."""
    if not SPOKE_POOL_ABI:
        print("Warning: Cannot initialize contracts without Spoke Pool ABI")
        return

    for chain in CHAINS:
        # Convert chain_id to string first, then to int for safety
        if "chain_id" not in chain or chain["chain_id"] is None:
            print(f"Error: Missing chain_id in chain configuration: {chain}")
            continue

        chain_id_str = str(chain["chain_id"])
        try:
            chain_id = int(chain_id_str)
        except ValueError:
            print(f"Error: Invalid chain_id in chain configuration: {chain_id_str}")
            continue

        try:
            # Ensure RPC URL is treated as string
            rpc_url = str(chain.get("rpc_url", ""))
            if not rpc_url:
                print(f"Warning: Missing or empty RPC URL for chain {chain_id}")
                continue

            w3 = Web3(Web3.HTTPProvider(rpc_url))
            spoke_pool_address = chain.get("spoke_pool_address")

            if not spoke_pool_address:
                print(f"Warning: No spoke pool address configured for {chain['name']}")
                continue

            # Convert spoke pool address to checksum address
            checksum_address = Web3.to_checksum_address(spoke_pool_address)
            contract = w3.eth.contract(address=checksum_address, abi=SPOKE_POOL_ABI)
            contracts[chain_id] = contract

        except Exception as e:
            print(f"Error initializing contract for {chain['name']}: {str(e)}")


def get_token_info(token_address: str, chain_id: int) -> Dict[str, Any]:
    """
    Get token information using Web3 calls.

    Args:
        token_address (str): Token contract address
        chain_id (int): ID of the chain where the token is deployed

    Returns:
        dict: Token information including name, symbol and decimals
    """
    try:
        chain = next((c for c in CHAINS if c["chain_id"] == chain_id), None)
        if chain and chain.get("rpc_url") and ERC20_ABI:
            w3 = Web3(Web3.HTTPProvider(str(chain["rpc_url"])))
            checksum_address = Web3.to_checksum_address(token_address)
            token_contract = w3.eth.contract(address=checksum_address, abi=ERC20_ABI)

            return {
                "address": token_address,
                "name": token_contract.functions.name().call(),
                "symbol": token_contract.functions.symbol().call(),
                "decimals": token_contract.functions.decimals().call(),
            }
    except Exception as e:
        print(
            f"Error getting token info for {token_address} on chain {chain_id}: {str(e)}"
        )

    return {"address": token_address, "name": None, "symbol": None, "decimals": None}


def get_fill_routes() -> List[Dict[str, Any]]:
    """
    Get all unique fill routes from relayer transactions.

    Returns:
        list: List of dictionaries containing route information:
            - origin_chain_id: ID of the origin chain
            - destination_chain_id: ID of the destination chain
            - input_token: Token address on origin chain
            - output_token: Token address on destination chain
            - token_name: Token name if available
            - token_decimals: Token decimals if available
    """
    initialize_contracts()

    if not contracts:
        print("Error: No contracts initialized. Cannot proceed with route analysis.")
        return []

    routes = []
    unique_routes = set()  # Track unique routes

    for destination_chain in CHAINS:
        # Ensure chain_id is properly handled as integer
        if "chain_id" not in destination_chain or destination_chain["chain_id"] is None:
            print(
                f"Error: Missing chain_id in chain configuration: {destination_chain}"
            )
            continue

        # Convert chain_id to string first, then to int for safety
        chain_id_str = str(destination_chain["chain_id"])
        try:
            destination_chain_id = int(chain_id_str)
        except ValueError:
            print(f"Error: Invalid chain_id in chain configuration: {chain_id_str}")
            continue

        if destination_chain_id not in contracts:
            continue

        print(f"Scanning {destination_chain['name']} for fill routes...")

        # Get transactions from block explorer API
        url = f"{destination_chain['explorer_api_url']}?module=account&action=txlist&address={RELAYER_ADDRESS}&startblock=0&endblock=999999999&sort=desc&apikey={destination_chain['api_key']}"

        try:
            response = requests.get(url)
            data = response.json()

            if data["status"] != "1":
                print(
                    f"Error fetching data for {destination_chain['name']}: {data['message']}"
                )
                continue

            # Filter for fillRelay transactions
            for tx in data.get("result", []):
                if (
                    tx.get("methodId") == FILL_RELAY_METHOD_ID
                    and tx.get("isError") == "0"
                ):
                    try:
                        # Decode transaction input
                        decoded_input = contracts[
                            destination_chain_id
                        ].decode_function_input(tx["input"])
                        relay_data = decoded_input[1]["relayData"]

                        # Extract token addresses and chain IDs
                        input_token = Web3.to_checksum_address(
                            relay_data["inputToken"].hex()[-40:]
                        )
                        output_token = Web3.to_checksum_address(
                            relay_data["outputToken"].hex()[-40:]
                        )
                        origin_chain_id = int(relay_data["originChainId"])

                        # Create unique route identifier
                        route_key = f"{origin_chain_id}:{destination_chain_id}:{input_token}:{output_token}"

                        if route_key not in unique_routes:
                            unique_routes.add(route_key)
                            # print(f"Found unique route: {route_key}")

                            # Get token information
                            input_token_info = get_token_info(
                                input_token, origin_chain_id
                            )
                            output_token_info = get_token_info(
                                output_token, destination_chain_id
                            )

                            routes.append(
                                {
                                    "origin_chain_id": origin_chain_id,
                                    "origin_chain_name": chain_id_to_name(
                                        origin_chain_id
                                    ),
                                    "destination_chain_id": destination_chain_id,
                                    "destination_chain_name": chain_id_to_name(
                                        destination_chain_id
                                    ),
                                    "input_token": input_token,
                                    "input_token_symbol": input_token_info.get(
                                        "symbol"
                                    ),
                                    "input_token_name": input_token_info.get("name"),
                                    "input_token_decimals": input_token_info.get(
                                        "decimals"
                                    ),
                                    "output_token": output_token,
                                    "output_token_symbol": output_token_info.get(
                                        "symbol"
                                    ),
                                    "output_token_name": output_token_info.get("name"),
                                    "output_token_decimals": input_token_info.get(
                                        "decimals"
                                    )
                                    or output_token_info.get("decimals"),
                                }
                            )

                    except Exception as e:
                        print(
                            f"Error processing transaction {tx.get('hash', 'unknown')}: {str(e)}"
                        )

        except Exception as e:
            print(f"Error scanning {destination_chain['name']}: {str(e)}")

    return routes


def insert_routes_into_db(routes: List[Dict[str, Any]]) -> None:
    """Insert routes into the database."""
    conn = get_db_connection()
    if not conn:
        print("Warning: Could not establish database connection")
        return

    try:
        new_routes = 0
        existing_routes = 0

        for route in routes:
            result = insert_route(
                route["origin_chain_id"],
                route["destination_chain_id"],
                route["input_token"],
                route["output_token"],
                route.get("output_token_symbol", ""),
            )
            if result:
                new_routes += 1
            else:
                existing_routes += 1

        print("Route insertion summary:")
        print(f"  - {new_routes} new routes inserted")
        print(f"  - {existing_routes} existing routes skipped")
        print(f"  - {new_routes + existing_routes} total unique routes processed")

    except Exception as e:
        print(f"Error during route insertion: {e}")
    finally:
        conn.close()


def insert_token_info_into_db(routes: List[Dict[str, Any]]) -> None:
    """Insert token information into the database."""
    conn = get_db_connection()
    if not conn:
        print("Warning: Could not establish database connection")
        return

    try:
        print("\nStarting token insertion process...")
        # Collect unique tokens by address
        unique_tokens = {}
        for route in routes:
            # Add input token
            unique_tokens[route["input_token"]] = {
                "address": route["input_token"],
                "chain_id": route["origin_chain_id"],
                "symbol": route.get("input_token_symbol"),
                "decimals": route.get("input_token_decimals"),
            }

            # Add output token
            unique_tokens[route["output_token"]] = {
                "address": route["output_token"],
                "chain_id": route["destination_chain_id"],
                "symbol": route.get("output_token_symbol"),
                "decimals": route.get("output_token_decimals"),
            }

        new_tokens = 0
        existing_tokens = 0

        # Insert unique tokens into database
        for token in unique_tokens.values():
            result = insert_token(
                token["address"], token["chain_id"], token["symbol"], token["decimals"]
            )
            if result:
                new_tokens += 1
            else:
                existing_tokens += 1

        print("\nToken insertion summary:")
        print(f"  - {new_tokens} new tokens inserted")
        print(f"  - {existing_tokens} existing tokens skipped")
        print(f"  - {new_tokens + existing_tokens} total unique tokens processed")

    except Exception as e:
        print(f"Error during token insertion: {e}")
    finally:
        conn.close()


def discover_routes():
    """
    Discover routes and insert them into the database.
    """
    routes = get_fill_routes()
    insert_routes_into_db(routes)
    insert_token_info_into_db(routes)


if __name__ == "__main__":
    discover_routes()
