#!/usr/bin/env python3
"""
Test for init_db.py

Tests database initialization behavior:
1. When database exists with tables - should not modify
2. When database doesn't exist - should create with correct schema
3. When database exists but is empty - should create tables
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

# Add the parent directory to sys.path to import init_db
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Mock the config module
mock_chains = [
    {
        "chain_id": 1,
        "name": "Ethereum",
    },
    {
        "chain_id": 42161,
        "name": "Arbitrum",
    },
]


class TestInitDb(unittest.TestCase):
    """Test case for init_db.py."""

    def setUp(self):
        """Set up test environment before each test."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = os.path.join(self.temp_dir.name, "test.db")

        # Save original environment
        self.original_env = os.environ.copy()

        # Set up mock for config
        self.config_patcher = mock.patch("src.init_db.CHAINS", mock_chains)
        self.config_patcher.start()

        # Mock get_db_path to return our test path
        self.db_path_patcher = mock.patch(
            "src.init_db.get_db_path", return_value=self.test_db_path
        )
        self.db_path_patcher.start()

    def tearDown(self):
        """Clean up after each test."""
        # Restore original environment
        os.environ.clear()
        os.environ.update(self.original_env)

        # Stop patchers
        self.config_patcher.stop()
        self.db_path_patcher.stop()

        # Remove temporary directory and all files in it
        self.temp_dir.cleanup()

    def test_db_file_already_exists_with_tables(self):
        """Test that init_db doesn't modify an existing database with tables."""
        # Create a database file with the expected schema
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()

        # Create Chain table with test data
        cursor.execute("""
            CREATE TABLE Chain (
                chain_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        cursor.execute("INSERT INTO Chain (chain_id, name) VALUES (1, 'Test Chain')")

        # Create Token table
        cursor.execute("""
            CREATE TABLE Token (
                token_address TEXT NOT NULL,
                chain_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                decimals INTEGER NOT NULL,
                PRIMARY KEY (token_address, chain_id),
                FOREIGN KEY (chain_id) REFERENCES Chain(chain_id)
            )
        """)

        # Create Route table
        cursor.execute("""
            CREATE TABLE Route (
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
            )
        """)

        # Create Fill table
        cursor.execute("""
            CREATE TABLE Fill (
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
                deposit_timestamp INTEGER,
                lp_fee TEXT,
                bundle_id TEXT,
                is_return BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (route_id) REFERENCES Route(route_id),
                FOREIGN KEY (repayment_chain_id) REFERENCES Chain(chain_id)
            )
        """)

        # Create Return table
        cursor.execute("""
            CREATE TABLE Return (
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
            )
        """)

        # Create Bundle table
        cursor.execute("""
            CREATE TABLE Bundle (
                bundle_id INTEGER NOT NULL,
                chain_id INTEGER NOT NULL,
                relayer_refund_root TEXT NOT NULL,
                end_block INTEGER NOT NULL,
                processed_timestamp INTEGER,
                PRIMARY KEY (bundle_id, chain_id),
                FOREIGN KEY (chain_id) REFERENCES Chain(chain_id)
            )
        """)

        conn.commit()
        conn.close()

        # Get file modification time
        original_mtime = os.path.getmtime(self.test_db_path)

        # Run init_db
        from src.init_db import init_db

        init_db()

        # Check that file modification time hasn't changed
        self.assertEqual(original_mtime, os.path.getmtime(self.test_db_path))

        # Verify the test data is still intact
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM Chain WHERE chain_id=1")
        result = cursor.fetchone()
        conn.close()

        self.assertEqual(result[0], "Test Chain")

    def test_creates_correct_tables(self):
        """Test that init_db creates the correct tables when database doesn't exist."""
        # Run init_db
        from src.init_db import init_db

        init_db()

        # Connect to database and check created tables
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()

        # Verify expected tables were created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        expected_tables = {
            "Chain",
            "Token",
            "Route",
            "Fill",
            "Return",
            "Bundle",
            "BundleReturn",
            "sqlite_sequence",
        }
        self.assertEqual(expected_tables, tables)

        # Verify Chain table schema
        cursor.execute("PRAGMA table_info(Chain)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        self.assertEqual(columns["chain_id"], "INTEGER")
        self.assertEqual(columns["name"], "TEXT")

        # Verify Token table schema
        cursor.execute("PRAGMA table_info(Token)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        self.assertEqual(columns["token_address"], "TEXT")
        self.assertEqual(columns["chain_id"], "INTEGER")
        self.assertEqual(columns["symbol"], "TEXT")
        self.assertEqual(columns["decimals"], "INTEGER")

        # Verify Route table schema
        cursor.execute("PRAGMA table_info(Route)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        self.assertEqual(columns["route_id"], "INTEGER")
        self.assertEqual(columns["origin_chain_id"], "INTEGER")
        self.assertEqual(columns["destination_chain_id"], "INTEGER")
        self.assertEqual(columns["input_token"], "TEXT")
        self.assertEqual(columns["output_token"], "TEXT")
        self.assertEqual(columns["token_symbol"], "TEXT")
        self.assertEqual(columns["discovery_timestamp"], "INTEGER")
        self.assertEqual(columns["is_active"], "BOOLEAN")

        # Verify Fill table schema
        cursor.execute("PRAGMA table_info(Fill)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        self.assertEqual(columns["tx_hash"], "TEXT")
        self.assertEqual(columns["route_id"], "INTEGER")
        self.assertEqual(columns["input_token"], "TEXT")
        self.assertEqual(columns["output_token"], "TEXT")
        self.assertEqual(columns["input_amount"], "TEXT")
        self.assertEqual(columns["output_amount"], "TEXT")
        self.assertEqual(columns["origin_chain_id"], "INTEGER")
        self.assertEqual(columns["destination_chain_id"], "INTEGER")
        self.assertEqual(columns["block_number"], "INTEGER")
        self.assertEqual(columns["tx_timestamp"], "INTEGER")
        self.assertEqual(columns["deposit_timestamp"], "INTEGER")

        # Verify Return table schema
        cursor.execute("PRAGMA table_info(Return)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        self.assertEqual(columns["tx_hash"], "TEXT")
        self.assertEqual(columns["return_chain_id"], "INTEGER")
        self.assertEqual(columns["return_token"], "TEXT")
        self.assertEqual(columns["return_amount"], "TEXT")
        self.assertEqual(columns["root_bundle_id"], "INTEGER")
        self.assertEqual(columns["leaf_id"], "INTEGER")
        self.assertEqual(columns["refund_address"], "TEXT")
        self.assertEqual(columns["is_deferred"], "BOOLEAN")
        self.assertEqual(columns["caller"], "TEXT")
        self.assertEqual(columns["block_number"], "INTEGER")
        self.assertEqual(columns["tx_timestamp"], "INTEGER")

        # Verify Bundle table schema
        cursor.execute("PRAGMA table_info(Bundle)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        self.assertEqual(columns["bundle_id"], "INTEGER")
        self.assertEqual(columns["chain_id"], "INTEGER")
        self.assertEqual(columns["relayer_refund_root"], "TEXT")
        self.assertEqual(columns["end_block"], "INTEGER")
        self.assertEqual(columns["processed_timestamp"], "INTEGER")

        # Verify Chain data was inserted
        cursor.execute("SELECT chain_id, name FROM Chain ORDER BY chain_id")
        chains = cursor.fetchall()
        self.assertEqual(len(chains), 2)
        self.assertEqual(chains[0], (1, "Ethereum"))
        self.assertEqual(chains[1], (42161, "Arbitrum"))

        conn.close()

    def test_creates_tables_in_empty_db(self):
        """Test that init_db creates tables when database exists but is empty."""
        # Create an empty database file
        conn = sqlite3.connect(self.test_db_path)
        conn.close()

        # Run init_db
        from src.init_db import init_db

        init_db()

        # Verify tables were created
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        expected_tables = {
            "Chain",
            "Token",
            "Route",
            "Fill",
            "Return",
            "Bundle",
            "BundleReturn",
            "sqlite_sequence",
        }
        self.assertEqual(expected_tables, tables)
        conn.close()


if __name__ == "__main__":
    unittest.main()
