"""
Configuration module for the relayer monitoring application.

This module loads and provides access to all configuration settings,
including environment variables, database connections, and chain information.
"""

import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Database configuration
DB_FILE = os.getenv("DB_FILE", "relayer_monitoring.db")

# Report configuration
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)  # Ensure reports directory exists
RETURN_DATA_FILE = os.getenv("RETURN_DATA_FILE", str(REPORTS_DIR / "return_data.xlsx"))
DAILY_COUNT_FILE = os.getenv(
    "DAILY_COUNT_FILE", str(REPORTS_DIR / "daily_profits.xlsx")
)

# Relayer configuration
RELAYER_ADDRESS = os.getenv("RELAYER_ADDRESS")
FILL_RELAY_METHOD_ID = os.getenv("FILL_RELAY_METHOD_ID")

# Mic Keys
COINGECKO_KEY = os.getenv("COINGECKO_KEY")
GOOGLE_DRIVE_KEY = os.getenv("GOOGLE_DRIVE_KEY")
KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

# Token configuration
COINGECKO_SYMBOL_MAP = {
    "ETH": "ethereum",
    "USDC": "usd-coin",
    "DAI": "dai",
    "USDT": "tether",
    "WETH": "weth",
    "WBTC": "wrapped-bitcoin",
}

# Hub Contract Address
HUB_ADDRESS = os.getenv("HUB_ADDRESS")
# Chain configurations  (3/1/25 blocks for now)
CHAINS = [
    {
        "chain_id": 1,
        "name": "Ethereum",
        "explorer_api_url": "https://api.etherscan.io/api",
        "rpc_url": os.getenv("ETH_RPC"),
        "api_key": os.getenv("ETH_SCAN_KEY"),
        "spoke_pool_address": os.getenv("ETH_SPOKE_POOL_ADDRESS"),
        "start_block": 21950406,  # The block of the first fill on this chain you want to start monitoring from
        "bundle_block_index": 0,  # Index in bundleEvaluationBlockNumbers array
    },
    {
        "chain_id": 10,
        "name": "Optimism",
        "explorer_api_url": "https://api-optimistic.etherscan.io/api",
        "rpc_url": os.getenv("OP_RPC"),
        "api_key": os.getenv("OP_SCAN_KEY"),
        "spoke_pool_address": os.getenv("OP_SPOKE_POOL_ADDRESS"),
        "start_block": 132848581,  # The block of the first fill on this chain you want to start monitoring from
        "bundle_block_index": 1,  # Index in bundleEvaluationBlockNumbers array
    },
    {
        "chain_id": 42161,
        "name": "Arbitrum",
        "explorer_api_url": "https://api.arbiscan.io/api",
        "rpc_url": os.getenv("ARB_RPC"),
        "api_key": os.getenv("ARB_SCAN_KEY"),
        "spoke_pool_address": os.getenv("ARB_SPOKE_POOL_ADDRESS"),
        "start_block": 311048309,  # The block of the first fill on this chain you want to start monitoring from
        "bundle_block_index": 4,  # Index in bundleEvaluationBlockNumbers array
    },
    {
        "chain_id": 8453,
        "name": "Base",
        "explorer_api_url": "https://api.basescan.org/api",
        "rpc_url": os.getenv("BASE_RPC"),
        "api_key": os.getenv("BASE_SCAN_KEY"),
        "spoke_pool_address": os.getenv("BASE_SPOKE_POOL_ADDRESS"),
        "start_block": 27011711,  # The block of the first fill on this chain you want to start monitoring from
        "bundle_block_index": 6,  # Index in bundleEvaluationBlockNumbers array
    },
]


# Logging configuration
def setup_logging():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)  # Ensure logs directory exists
    log_file = str(logs_dir / f"{timestamp}_relayer_monitoring.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),  # Write logs to file
            logging.StreamHandler(),  # Print logs to console
        ],
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Starting relayer monitoring at {timestamp}")
    logger.info(f"Log file: {log_file}")

def get_chains(chain_id):
    """
    Get chain configuration by chain ID.

    Args:
        chain_id (int): The chain ID to look up

    Returns:
        dict: Chain configuration or None if not found
    """
    return next((chain for chain in CHAINS if chain["chain_id"] == chain_id), None)


def chain_id_to_name(chain_id):
    """
    Get chain key (short name) for a chain ID.

    Args:
        chain_id (int): The chain ID to look up

    Returns:
        str: Chain key (short name) or None if not found
    """
    chain_keys = {chain["chain_id"]: chain["name"] for chain in CHAINS}
    return chain_keys.get(chain_id)


def get_db_path():
    """
    Get the full database path.

    Returns:
        str: Full path to the database file
    """
    return DB_FILE
