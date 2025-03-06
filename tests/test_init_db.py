#!/usr/bin/env python3
"""
Simple test for init_db.py

Tests that init_db doesn't modify an existing database file.
And test that it properly creates the tables from the schema.
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

    def test_db_file_already_exists(self):
        """Test that init_db doesn't modify an existing database file."""
        # Create a simple database file with a test table
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)")
        cursor.execute("INSERT INTO test_table (value) VALUES ('original_data')")
        conn.commit()
        conn.close()

        # Get file modification time
        original_mtime = os.path.getmtime(self.test_db_path)

        # Run init_db
        from src.init_db import init_db

        init_db()

        # Check that file modification time hasn't changed
        self.assertEqual(original_mtime, os.path.getmtime(self.test_db_path))

        # Connect to database and check the test data is still there
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM test_table WHERE id=1")
        result = cursor.fetchone()
        conn.close()

        # Verify original data is intact
        self.assertEqual(result[0], "original_data")

        # Also check that init_db didn't create the tables from the schema
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()

        # Verify that only our test table exists
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0], "test_table")

    def test_creates_correct_tables(self):
        """Test that init_db creates the correct tables when database doesn't exist."""
        # Run init_db
        from src.init_db import init_db

        init_db()

        # Connect to database and check created tables
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()

        # Verify expected tables were created
        expected_tables = ["Chain", "Token", "Route", "Fill"]
        for table in expected_tables:
            self.assertIn(table, tables)

        # Verify table structure
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()

        # Check Chain table schema
        cursor.execute("PRAGMA table_info(Chain)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        self.assertEqual(columns["chain_id"], "TEXT")
        self.assertEqual(columns["name"], "TEXT")

        # Check Token table schema
        cursor.execute("PRAGMA table_info(Token)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        self.assertEqual(columns["token_address"], "TEXT")
        self.assertEqual(columns["chain_id"], "TEXT")
        self.assertEqual(columns["symbol"], "TEXT")
        self.assertEqual(columns["decimals"], "INTEGER")

        # Check Fill table primary key
        cursor.execute("PRAGMA table_info(Fill)")
        for row in cursor.fetchall():
            if row[1] == "tx_hash":
                self.assertEqual(
                    row[5], 1
                )  # Check if it's a primary key (pk column = 1)

        conn.close()


if __name__ == "__main__":
    unittest.main()
