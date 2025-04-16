#!/usr/bin/env python3
"""
Initialize the database for the relayer refactor project.

This script follows simple logic:
- If database file exists, check if it has tables
- If database file doesn't exist or is empty, create it and initialize tables
"""

import logging
import os
import sqlite3
import sys

from config import CHAINS, get_db_path

# Configure logging
logger = logging.getLogger(__name__)


def init_db():
    """
    Simple database initialization logic:
    - If database file exists, check if it has tables
    - If database file doesn't exist or is empty, create it and initialize tables
    """
    logger.info("=" * 80)
    logger.info("Checking database")

    # Get database file path from config
    db_file = get_db_path()
    if not db_file:
        logger.error("Database file path not configured")
        sys.exit(1)

    # Check if database file exists and has tables
    file_exists = os.path.exists(db_file)
    tables_exist = False

    if file_exists:
        # Check if database has tables
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        conn.close()

        tables_exist = len(tables) > 0

    # If database exists and has tables, do nothing
    if file_exists and tables_exist:
        logger.info(
            f"Database file {db_file} already exists and has tables, nothing to do"
        )
        return

    if file_exists:
        logger.info(f"Database file {db_file} exists but is empty, initializing tables")
    else:
        logger.info(f"Creating new database file: {db_file}")

    # Connect to database (this will create the file if it doesn't exist)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        logger.info("Initializing database tables")

        # Create tables based on the new schema
        cursor.executescript("""
        CREATE TABLE IF NOT EXISTS Chain (
            chain_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Token (
            token_address TEXT NOT NULL,
            chain_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            decimals INTEGER NOT NULL,
            PRIMARY KEY (token_address, chain_id),
            FOREIGN KEY (chain_id) REFERENCES Chain(chain_id)
        );

        CREATE TABLE IF NOT EXISTS Route (
            route_id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_chain_id INTEGER NOT NULL,
            destination_chain_id INTEGER NOT NULL,
            input_token TEXT NOT NULL,
            output_token TEXT NOT NULL,
            token_symbol TEXT NOT NULL,
            discovery_timestamp INTEGER NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            FOREIGN KEY (origin_chain_id) REFERENCES Chain(chain_id),
            FOREIGN KEY (destination_chain_id) REFERENCES Chain(chain_id),
            FOREIGN KEY (input_token, origin_chain_id) REFERENCES Token(token_address, chain_id),
            FOREIGN KEY (output_token, destination_chain_id) REFERENCES Token(token_address, chain_id),
            UNIQUE(origin_chain_id, destination_chain_id, input_token, output_token)
        );

        CREATE TABLE IF NOT EXISTS Fill (
            tx_hash TEXT PRIMARY KEY,
            is_success BOOLEAN DEFAULT TRUE,
            route_id INTEGER NOT NULL,
            depositor TEXT NOT NULL,
            recipient TEXT NOT NULL,
            exclusive_relayer TEXT NOT NULL,
            input_token TEXT NOT NULL,
            output_token TEXT NOT NULL,
            input_amount TEXT NOT NULL,
            output_amount TEXT NOT NULL,
            origin_chain_id INTEGER NOT NULL,
            destination_chain_id INTEGER NOT NULL,
            deposit_id TEXT NOT NULL,
            fill_deadline INTEGER,
            exclusivity_deadline INTEGER,
            message TEXT,
            repayment_chain_id INTEGER,
            repayment_address TEXT,
            gas_cost TEXT,
            gas_price TEXT,
            block_number INTEGER NOT NULL,
            tx_timestamp INTEGER NOT NULL,
            deposit_block_number INTEGER,
            deposit_timestamp INTEGER,
            lp_fee TEXT,
            bundle_id TEXT,
            is_return BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (route_id) REFERENCES Route(route_id),
            FOREIGN KEY (repayment_chain_id) REFERENCES Chain(chain_id)
        );

        CREATE TABLE IF NOT EXISTS Return (
            tx_hash TEXT NOT NULL,
            return_chain_id INTEGER NOT NULL,
            return_token TEXT NOT NULL,
            return_amount TEXT NOT NULL,
            root_bundle_id INTEGER NOT NULL,
            leaf_id INTEGER NOT NULL,
            refund_address TEXT NOT NULL,
            is_deferred BOOLEAN NOT NULL,
            caller TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            tx_timestamp INTEGER NOT NULL,
            PRIMARY KEY (tx_hash, return_token, refund_address),
            FOREIGN KEY (return_chain_id) REFERENCES Chain(chain_id),
            FOREIGN KEY (return_token, return_chain_id) REFERENCES Token(token_address, chain_id)
        );

        CREATE TABLE IF NOT EXISTS Bundle (
            bundle_id INTEGER NOT NULL,
            chain_id INTEGER NOT NULL,
            relayer_refund_root TEXT NOT NULL,
            end_block INTEGER NOT NULL,
            processed_timestamp INTEGER,
            PRIMARY KEY (bundle_id, chain_id),
            FOREIGN KEY (chain_id) REFERENCES Chain(chain_id)
        );

        CREATE TABLE IF NOT EXISTS BundleReturn (
            bundle_id INTEGER NOT NULL,
            chain_id INTEGER NOT NULL,
            token_address TEXT NOT NULL,
            token_symbol TEXT NOT NULL,
            input_amount DECIMAL(36,18) NOT NULL DEFAULT 0,
            return_amount DECIMAL(36,18) NOT NULL DEFAULT 0,
            lp_fee DECIMAL(36,18) NOT NULL DEFAULT 0,
            start_block INTEGER NOT NULL,
            end_block INTEGER NOT NULL,
            start_time INTEGER NOT NULL,
            end_time INTEGER NOT NULL,
            fill_tx_hashes TEXT,  -- Comma-separated list of fill transaction hashes
            return_tx_hash TEXT,  -- Single transaction hash for the return
            relayer_refund_root TEXT,
            created_at INTEGER NOT NULL DEFAULT (unixepoch()),
            PRIMARY KEY (bundle_id, chain_id, token_address),
            FOREIGN KEY (chain_id) REFERENCES Chain(chain_id),
            FOREIGN KEY (token_address, chain_id) REFERENCES Token(token_address, chain_id)
        );

        -- New tables for profit tracking
        CREATE TABLE IF NOT EXISTS TokenPrice (
            date DATE,
            token_symbol TEXT,
            price_usd DECIMAL,
            PRIMARY KEY (date, token_symbol)
        );

        CREATE TABLE IF NOT EXISTS DailyProfit (
            date DATE,
            chain_id INTEGER,
            token_symbol TEXT,
            input_amount DECIMAL,
            output_amount DECIMAL,
            lp_fee DECIMAL,
            gas_fee_eth DECIMAL,
            gas_fee_usd DECIMAL,
            total_fills INTEGER,
            successful_fills INTEGER,
            profit_usd DECIMAL,
            PRIMARY KEY (date, chain_id, token_symbol),
            FOREIGN KEY (chain_id) REFERENCES Chain(chain_id)
        );
        """)

        # Insert chain data
        insert_chain_sql = """
        INSERT OR IGNORE INTO Chain
            (chain_id, name)
        VALUES
            (?, ?)
        """

        # Insert each chain from config
        for chain in CHAINS:
            # Skip chains with missing required data
            if not all([chain.get("chain_id"), chain.get("name")]):
                logger.warning(
                    f"Skipping chain {chain.get('name', 'Unknown')} due to missing data"
                )
                continue

            chain_data = (
                int(chain["chain_id"]),  # Store as integer
                chain["name"],
            )

            cursor.execute(insert_chain_sql, chain_data)
            logger.info(
                f"Inserted chain data for {chain['name']} (Chain ID: {chain['chain_id']})"
            )

        conn.commit()
        logger.info("Database tables created successfully")

    except sqlite3.Error as e:
        logger.error(f"Error creating database tables: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
