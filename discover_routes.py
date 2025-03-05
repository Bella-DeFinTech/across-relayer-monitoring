"""
This script identifies tokens used in Across Protocol's fillRelay transactions
by analyzing the relayer's blockchain transactions.
"""

import os
import json
import requests
from web3 import Web3
from typing import List, Dict, Any
from pprint import pprint
from config import (
    CHAINS, 
    chain_id_to_name, 
    RELAYER_ADDRESS, 
    FILL_RELAY_METHOD_ID
)
from db_utils import insert_route, insert_token

# Define paths to ABI files
ABI_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'abi')
ERC20_ABI_PATH = os.path.join(ABI_DIR, 'erc20_abi.json')
SPOKE_ABI_PATH = os.path.join(ABI_DIR, 'spoke_abi.json')

# Load the ABIs
try:
    with open(ERC20_ABI_PATH, 'r') as file:
        ERC20_ABI = json.load(file)
except FileNotFoundError:
    print(f"Error: Could not find ERC20 ABI file at {ERC20_ABI_PATH}")
    ERC20_ABI = []

try:
    with open(SPOKE_ABI_PATH, 'r') as file:
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
        chain_id = chain['chain_id']
        try:
            w3 = Web3(Web3.HTTPProvider(chain['rpc_url']))
            spoke_pool_address = chain.get('spoke_pool_address')
            
            if not spoke_pool_address:
                print(f"Warning: No spoke pool address configured for {chain['name']}")
                continue
                
            # Convert spoke pool address to checksum address
            checksum_address = Web3.to_checksum_address(spoke_pool_address)
            contract = w3.eth.contract(
                address=checksum_address,
                abi=SPOKE_POOL_ABI
            )
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
        chain = next((c for c in CHAINS if c['chain_id'] == chain_id), None)
        if chain and chain['rpc_url'] and ERC20_ABI:
            w3 = Web3(Web3.HTTPProvider(chain['rpc_url']))
            token_contract = w3.eth.contract(address=token_address, abi=ERC20_ABI)
            
            return {
                'address': token_address,
                'name': token_contract.functions.name().call(),
                'symbol': token_contract.functions.symbol().call(),
                'decimals': token_contract.functions.decimals().call()
            }
    except Exception as e:
        print(f"Error getting token info for {token_address} on chain {chain_id}: {str(e)}")
    
    return {
        'address': token_address,
        'name': None,
        'symbol': None,
        'decimals': None
    }

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
        destination_chain_id = destination_chain['chain_id']
        if destination_chain_id not in contracts:
            continue
            
        print(f"Scanning {destination_chain['name']} for fill routes...")
        
        # Get transactions from block explorer API
        url = f"{destination_chain['explorer_api_url']}?module=account&action=txlist&address={RELAYER_ADDRESS}&startblock=0&endblock=999999999&sort=desc&apikey={destination_chain['api_key']}"
        
        try:
            response = requests.get(url)
            data = response.json()
            
            if data['status'] != '1':
                print(f"Error fetching data for {destination_chain['name']}: {data['message']}")
                continue
            
            # Filter for fillRelay transactions
            for tx in data.get('result', []):
                if tx.get('methodId') == FILL_RELAY_METHOD_ID and tx.get('isError') == '0':
                    try:
                        # Decode transaction input
                        decoded_input = contracts[destination_chain_id].decode_function_input(tx['input'])
                        relay_data = decoded_input[1]['relayData']
                        
                        # Extract token addresses and chain IDs
                        input_token = Web3.to_checksum_address(relay_data['inputToken'].hex()[-40:])
                        output_token = Web3.to_checksum_address(relay_data['outputToken'].hex()[-40:])
                        origin_chain_id = relay_data['originChainId']
                        
                        # Create unique route identifier
                        route_key = f"{origin_chain_id}:{destination_chain_id}:{input_token}:{output_token}"
                        
                        if route_key not in unique_routes:
                            unique_routes.add(route_key)
                            # print(f"Found unique route: {route_key}")
                            
                            # Get token information
                            input_token_info = get_token_info(input_token, origin_chain_id)
                            output_token_info = get_token_info(output_token, destination_chain_id)
                            
                            routes.append({
                                'origin_chain_id': origin_chain_id,
                                'origin_chain_name': chain_id_to_name(origin_chain_id),
                                'destination_chain_id': destination_chain_id,
                                'destination_chain_name': chain_id_to_name(destination_chain_id),
                                'input_token': input_token,
                                'input_token_symbol': input_token_info.get('symbol'),
                                'input_token_name': input_token_info.get('name'),
                                'input_token_decimals': input_token_info.get('decimals'),
                                'output_token': output_token,
                                'output_token_symbol': output_token_info.get('symbol'),
                                'output_token_name': output_token_info.get('name'),
                                'output_token_decimals': input_token_info.get('decimals') or output_token_info.get('decimals')
                            })
                            
                    except Exception as e:
                        print(f"Error processing transaction {tx.get('hash', 'unknown')}: {str(e)}")
                        
        except Exception as e:
            print(f"Error scanning {destination_chain['name']}: {str(e)}")
    
    return routes

def insert_routes_into_db(routes: List[Dict[str, Any]]):
    """
    Insert routes into the database. 
    Do not insert duplicate routes.
    Print the number of routes inserted and skipped.
    """
    new_routes_count = 0
    existing_routes_count = 0
    failed_routes_count = 0
    
    for route in routes:
        route_id = insert_route(
            origin_chain_id=str(route['origin_chain_id']),  # Convert to string as per schema
            destination_chain_id=str(route['destination_chain_id']),
            input_token=route['input_token'],
            output_token=route['output_token'],
            token_symbol=route['output_token_symbol']
        )
        if route_id and route_id > 0:  # Positive ID means new route was inserted
            new_routes_count += 1
        elif route_id is None:  # None means route already existed
            existing_routes_count += 1
        else:  # Zero means error occurred
            failed_routes_count += 1
            
    print(f"Route insertion summary:")
    print(f"  - {new_routes_count} new routes inserted")
    print(f"  - {existing_routes_count} existing routes skipped")
    if failed_routes_count > 0:
        print(f"  - {failed_routes_count} routes failed to process")
    print(f"  - {new_routes_count + existing_routes_count} total unique routes processed")

def insert_token_info_into_db(routes: List[Dict[str, Any]]):
    """
    Insert token info into the database.
    Do not insert duplicate tokens.
    Print the number of tokens inserted and skipped.
    """
    # Keep track of processed tokens to avoid duplicates
    processed_tokens = set()
    new_tokens_count = 0
    existing_tokens_count = 0
    failed_tokens_count = 0
    
    for route in routes:
        # Process input token
        input_token_key = (route['input_token'], str(route['origin_chain_id']))
        if input_token_key not in processed_tokens:
            result = insert_token(
                token_address=route['input_token'],
                chain_id=str(route['origin_chain_id']),
                symbol=route['input_token_symbol'],
                decimals=route['input_token_decimals']
            )
            if result and result > 0:  # Positive ID means new token was inserted
                new_tokens_count += 1
            elif result is None:  # None means token already existed
                existing_tokens_count += 1
            else:  # Zero means error occurred
                failed_tokens_count += 1
            processed_tokens.add(input_token_key)
            
        # Process output token
        output_token_key = (route['output_token'], str(route['destination_chain_id']))
        if output_token_key not in processed_tokens:
            result = insert_token(
                token_address=route['output_token'],
                chain_id=str(route['destination_chain_id']),
                symbol=route['output_token_symbol'],
                decimals=route['output_token_decimals']
            )
            if result and result > 0:  # Positive ID means new token was inserted
                new_tokens_count += 1
            elif result is None:  # None means token already existed
                existing_tokens_count += 1
            else:  # Zero means error occurred
                failed_tokens_count += 1
            processed_tokens.add(output_token_key)
            
    print(f"Token insertion summary:")
    print(f"  - {new_tokens_count} new tokens inserted")
    print(f"  - {existing_tokens_count} existing tokens skipped")
    if failed_tokens_count > 0:
        print(f"  - {failed_tokens_count} tokens failed to process")
    print(f"  - {len(processed_tokens)} total unique tokens processed")

def discover_routes():
    """
    Discover routes and insert them into the database.
    """
    routes = get_fill_routes()
    insert_routes_into_db(routes)
    insert_token_info_into_db(routes)

if __name__ == '__main__':
    discover_routes()