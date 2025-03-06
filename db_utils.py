"""
Database utility functions for the relayer monitoring application.

This module provides functions for common database operations, including:
- Connecting to the database
- Executing queries
- Handling route information
"""

import logging
import sqlite3
from datetime import datetime

from config import LOGGING_CONFIG, get_db_path

# Configure logging
logging.basicConfig(
    level=logging.getLevelName(LOGGING_CONFIG["level"]), format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Create and return a connection to the SQLite database.

    Returns:
        sqlite3.Connection: Database connection object
    """
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    return conn


def execute_query(query, params=(), fetchall=False, commit=False):
    """
    Execute a SQL query with error handling.

    Args:
        query (str): SQL query to execute
        params (tuple): Parameters for the query
        fetchall (bool): Whether to fetch all results
        commit (bool): Whether to commit the transaction

    Returns:
        list or cursor: Query results or cursor object
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query, params)

        if commit:
            conn.commit()

        if fetchall:
            return cursor.fetchall()
        return cursor
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        if commit:
            conn.rollback()
        raise
    finally:
        conn.close()


def route_exists(origin_chain_id, destination_chain_id, input_token, output_token):
    """
    Check if a route already exists in the database.

    Args:
        origin_chain_id (str): Origin chain ID
        destination_chain_id (str): Destination chain ID
        input_token (str): Input token address
        output_token (str): Output token address

    Returns:
        int or None: Route ID if exists, None otherwise
    """
    query = """
    SELECT route_id FROM Route
    WHERE origin_chain_id = ?
    AND destination_chain_id = ?
    AND input_token = ?
    AND output_token = ?
    """

    try:
        results = execute_query(
            query,
            (origin_chain_id, destination_chain_id, input_token, output_token),
            fetchall=True,
        )
        if results and len(results) > 0:
            return results[0]["route_id"]
        return None
    except sqlite3.Error as e:
        logger.error(f"Error checking if route exists: {e}")
        return None


def insert_route(
    origin_chain_id, destination_chain_id, input_token, output_token, token_symbol
):
    """
    Insert a new route into the database.

    Args:
        origin_chain_id (str): Origin chain ID
        destination_chain_id (str): Destination chain ID
        input_token (str): Input token address
        output_token (str): Output token address
        token_symbol (str): Token symbol

    Returns:
        int or None: Route ID if newly inserted, None if already existed, 0 on error
    """
    # First check if route already exists
    existing_route_id = route_exists(
        origin_chain_id, destination_chain_id, input_token, output_token
    )
    if existing_route_id:
        logger.debug(f"Route already exists with ID {existing_route_id}")
        return None

    # Ensure token exists in Token table for origin chain
    insert_token(input_token, origin_chain_id, token_symbol, 6)  # Default decimals

    # Ensure token exists in Token table for destination chain
    insert_token(
        output_token, destination_chain_id, token_symbol, 6
    )  # Default decimals

    # Insert new route
    query = """
    INSERT INTO Route (
        origin_chain_id,
        destination_chain_id,
        input_token,
        output_token,
        token_symbol,
        discovery_timestamp,
        is_active
    ) VALUES (?, ?, ?, ?, ?, ?, 1)
    """

    current_timestamp = int(datetime.now().timestamp())

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            query,
            (
                origin_chain_id,
                destination_chain_id,
                input_token,
                output_token,
                token_symbol,
                current_timestamp,
            ),
        )
        route_id = cursor.lastrowid
        conn.commit()
        conn.close()

        logger.info(
            f"Inserted new route with ID {route_id}: {origin_chain_id} -> {destination_chain_id} for token {token_symbol}"
        )
        return route_id
    except sqlite3.Error as e:
        logger.error(f"Error inserting route: {e}")
        return 0


def insert_token(token_address, chain_id, symbol, decimals):
    """
    Insert a token into the database.

    Args:
        token_address (str): Token address
        chain_id (str): Chain ID
        symbol (str): Token symbol
        decimals (int): Token decimals

    Returns:
        int or None: Token ID if newly inserted, None if already existed, 0 on error
    """
    query = """
    SELECT ROWID FROM Token 
    WHERE token_address = ? AND chain_id = ?
    """

    try:
        results = execute_query(query, (token_address, chain_id), fetchall=True)
        if not results:
            insert_query = """
            INSERT INTO Token (token_address, chain_id, symbol, decimals)
            VALUES (?, ?, ?, ?)
            """
            cursor = execute_query(
                insert_query, (token_address, chain_id, symbol, decimals), commit=True
            )
            token_id = cursor.lastrowid
            logger.info(
                f"Inserted token {symbol} ({token_address}) on chain {chain_id}"
            )
            return token_id
        else:
            # Return None to indicate token already existed
            return None
    except sqlite3.Error as e:
        logger.error(f"Error inserting token: {e}")
        return 0


def get_all_routes():
    """
    Get all routes from the database.

    Returns:
        list: List of route dictionaries
    """
    query = "SELECT * FROM Route WHERE is_active = 1"

    try:
        return execute_query(query, fetchall=True)
    except sqlite3.Error as e:
        logger.error(f"Error getting routes: {e}")
        return []


def get_token_info(token_address, chain_id, default_info=None):
    """
    Get token information from the database.

    Args:
        token_address (str): Token address
        chain_id (str): Chain ID
        default_info (dict): Default token info if not found

    Returns:
        dict: Token information {symbol, decimals} or default if not found
    """
    query = """
    SELECT symbol, decimals FROM Token
    WHERE token_address = ? AND chain_id = ?
    """

    try:
        results = execute_query(query, (token_address, chain_id), fetchall=True)
        if results and len(results) > 0:
            return {"symbol": results[0]["symbol"], "decimals": results[0]["decimals"]}
        return default_info
    except sqlite3.Error as e:
        logger.error(f"Error getting token info: {e}")
        return default_info


def get_latest_block_for_chain(chain_id):
    """
    Get the latest processed block for a chain.

    Args:
        chain_id (str): Chain ID

    Returns:
        int: Latest block number or 0 if not found
    """
    query = """
    SELECT MAX(block_number) as latest_block FROM Fill
    WHERE origin_chain_id = ? OR destination_chain_id = ?
    """

    try:
        results = execute_query(query, (chain_id, chain_id), fetchall=True)
        if results and len(results) > 0 and results[0]["latest_block"] is not None:
            return int(results[0]["latest_block"])
        return 0
    except sqlite3.Error as e:
        logger.error(f"Error getting latest block for chain {chain_id}: {e}")
        return 0
