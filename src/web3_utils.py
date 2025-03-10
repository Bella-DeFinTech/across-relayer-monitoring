"""
Web3 utility functions for blockchain interactions.
"""

import json
import logging
import os
from typing import Dict, Optional, Any

from web3 import Web3
from web3.contract import Contract

from .config import CHAINS, get_chains

logger = logging.getLogger(__name__)

def get_spokepool_contracts() -> Dict[int, Contract]:
    """
    Get Web3 contract instances for spoke pools on each chain.

    Returns:
        Dict[int, Contract]: Dictionary mapping chain IDs to contract instances
    """
    # Load Spoke Pool ABI
    spoke_abi_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 
        "abi",
        "spoke_abi.json"
    )
    try:
        with open(spoke_abi_path, "r") as file:
            spoke_pool_abi = json.load(file)
    except FileNotFoundError:
        logger.error(f"Could not find Spoke Pool ABI file at {spoke_abi_path}")
        return {}

    contracts = {}

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
            w3 = Web3(Web3.HTTPProvider(str(rpc_url)))
            checksum_address = Web3.to_checksum_address(spoke_pool_address)
            contract = w3.eth.contract(address=checksum_address, abi=spoke_pool_abi)
            contracts[chain_id] = contract
            logger.debug(f"Got contract for {chain['name']}")

        except Exception as e:
            logger.error(f"Error getting contract for {chain.get('name', 'unknown')}: {str(e)}")

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
        os.path.dirname(os.path.abspath(__file__)), 
        "abi",
        "erc20_abi.json"
    )
    try:
        with open(erc20_abi_path, "r") as file:
            erc20_abi = json.load(file)
    except FileNotFoundError:
        logger.error(f"Could not find ERC20 ABI file at {erc20_abi_path}")
        return {"address": token_address, "name": None, "symbol": None, "decimals": None}

    try:
        chain = get_chains(chain_id)
        if chain and chain.get("rpc_url"):
            w3 = Web3(Web3.HTTPProvider(str(chain["rpc_url"])))
            checksum_address = Web3.to_checksum_address(token_address)
            token_contract = w3.eth.contract(address=checksum_address, abi=erc20_abi)

            return {
                "address": token_address,
                "name": token_contract.functions.name().call(),
                "symbol": token_contract.functions.symbol().call(),
                "decimals": token_contract.functions.decimals().call(),
            }
    except Exception as e:
        logger.error(f"Error getting token info for {token_address} on chain {chain_id}: {str(e)}")

    return {"address": token_address, "name": None, "symbol": None, "decimals": None}
