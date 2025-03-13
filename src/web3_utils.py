"""
Web3 utility functions for blockchain interactions.
"""

import json
import logging
import os
from typing import Any, Dict, Optional, cast

from web3 import Web3
from web3.contract import Contract

from .config import CHAINS, HUB_ADDRESS, get_chains

logger = logging.getLogger(__name__)


def get_hub_contract() -> Optional[Contract]:
    """
    Get Web3 contract instance for the Across Hub contract on Ethereum.

    Returns:
        Optional[Contract]: Web3 contract instance for the hub, or None if initialization fails
    """
    # Load Hub ABI
    hub_abi_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "abi", "hub_abi.json"
    )
    try:
        with open(hub_abi_path, "r") as file:
            hub_abi = json.load(file)
    except FileNotFoundError:
        logger.error(f"Could not find Hub ABI file at {hub_abi_path}")
        return None

    eth_chain = next((c for c in CHAINS if c["chain_id"] == 1), None)
    if not eth_chain:
        logger.error("Ethereum chain configuration not found")
        return None

    try:
        rpc_url = cast(str, eth_chain["rpc_url"])
        w3 = Web3(Web3.HTTPProvider(rpc_url))

        hub_address = cast(str, HUB_ADDRESS)
        if not hub_address:
            logger.error("Hub contract address not configured")
            return None

        contract = w3.eth.contract(
            address=Web3.to_checksum_address(hub_address), abi=hub_abi
        )
        return contract
    except Exception as e:
        logger.error(f"Error initializing hub contract: {str(e)}")
        return None


def get_spokepool_contracts() -> Dict[int, Contract]:
    """
    Get Web3 contract instances for spoke pools on each chain.

    Returns:
        Dict[int, Contract]: Dictionary mapping chain IDs to contract instances

    Raises:
        TypeError: If chain configuration has invalid types for required fields
    """
    # Load Spoke Pool ABI
    spoke_abi_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "abi", "spoke_abi.json"
    )
    try:
        with open(spoke_abi_path, "r") as file:
            spoke_pool_abi = json.load(file)
    except FileNotFoundError:
        logger.error(f"Could not find Spoke Pool ABI file at {spoke_abi_path}")
        return {}

    contracts: Dict[int, Contract] = {}

    for chain in CHAINS:
        try:
            # Validate chain configuration
            chain_id = chain.get("chain_id")
            rpc_url = chain.get("rpc_url")
            spoke_pool_address = chain.get("spoke_pool_address")

            if not all([chain_id, rpc_url, spoke_pool_address]):
                logger.warning(
                    f"Missing required configuration for chain {chain.get('name', 'unknown')}"
                )
                continue

            # Initialize Web3 and contract
            try:
                rpc_url_str = cast(str, rpc_url)
                spoke_pool_address_str = cast(str, spoke_pool_address)
                chain_id_int = cast(int, chain_id)
            except (TypeError, ValueError) as e:
                logger.error(f"Invalid type in chain configuration: {str(e)}")
                continue

            w3 = Web3(Web3.HTTPProvider(rpc_url_str))
            checksum_address = Web3.to_checksum_address(spoke_pool_address_str)
            contract = w3.eth.contract(address=checksum_address, abi=spoke_pool_abi)
            contracts[chain_id_int] = contract
            logger.debug(f"Got contract for {chain['name']}")

        except Exception as e:
            logger.error(
                f"Error getting contract for {chain.get('name', 'unknown')}: {str(e)}"
            )

    return contracts


def get_erc20_token_info(token_address: str, chain_id: int) -> Dict[str, Any]:
    """
    Get ERC20 token information using Web3 calls.

    Args:
        token_address (str): Token contract address
        chain_id (int): ID of the chain where the token is deployed

    Returns:
        dict: Token information including name, symbol and decimals
    """
    # Load ERC20 ABI
    erc20_abi_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "abi", "erc20_abi.json"
    )
    try:
        with open(erc20_abi_path, "r") as file:
            erc20_abi = json.load(file)
    except FileNotFoundError:
        logger.error(f"Could not find ERC20 ABI file at {erc20_abi_path}")
        return {
            "address": token_address,
            "name": None,
            "symbol": None,
            "decimals": None,
        }

    try:
        chain = get_chains(chain_id)
        if chain and chain.get("rpc_url"):
            try:
                rpc_url_str = cast(str, chain["rpc_url"])
            except (TypeError, ValueError) as e:
                logger.error(f"Invalid RPC URL type for chain {chain_id}: {str(e)}")
                return {
                    "address": token_address,
                    "name": None,
                    "symbol": None,
                    "decimals": None,
                }

            w3 = Web3(Web3.HTTPProvider(rpc_url_str))
            checksum_address = Web3.to_checksum_address(token_address)
            token_contract = w3.eth.contract(address=checksum_address, abi=erc20_abi)

            return {
                "address": token_address,
                "name": token_contract.functions.name().call(),
                "symbol": token_contract.functions.symbol().call(),
                "decimals": token_contract.functions.decimals().call(),
            }
    except Exception as e:
        logger.error(
            f"Error getting token info for {token_address} on chain {chain_id}: {str(e)}"
        )

    return {"address": token_address, "name": None, "symbol": None, "decimals": None}


def get_block_timestamp(chain_id: int, block_number: int) -> int:
    """
    Get block timestamp for a given block number on a specific chain.

    Args:
        chain_id (int): ID of the blockchain chain
        block_number (int): Block number to get timestamp for

    Returns:
        int: Block timestamp

    Raises:
        ValueError: If chain configuration is missing or invalid
    """
    try:
        chain = get_chains(chain_id)
        if not chain or not chain.get("rpc_url"):
            raise ValueError(f"Missing or invalid configuration for chain {chain_id}")

        try:
            rpc_url_str = cast(str, chain["rpc_url"])
        except (TypeError, ValueError) as e:
            logger.error(f"Invalid RPC URL type for chain {chain_id}: {str(e)}")
            raise ValueError(f"Invalid RPC URL configuration for chain {chain_id}")

        w3 = Web3(Web3.HTTPProvider(rpc_url_str))
        block = w3.eth.get_block(block_number)
        return block["timestamp"]

    except Exception as e:
        logger.error(
            f"Error getting block timestamp for block {block_number} on chain {chain_id}: {str(e)}"
        )
        raise
