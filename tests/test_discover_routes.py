"""
Unit tests for the route discovery module.
"""

import json
import unittest
from unittest.mock import MagicMock, patch

from web3 import Web3

# Import from parent directory
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import check_and_insert_token, route_exists, insert_route


class TestDiscoverRoutes(unittest.TestCase):
    """Test cases for the route discovery module."""

    @patch("discover_routes.get_spoke_pool")
    @patch("discover_routes.requests.get")
    def test_scan_chain_for_routes(self, mock_get, mock_get_spoke_pool):
        """Test scanning a chain for routes."""
        # Import here to avoid circular import
        from discover_routes import discover_routes
        
        # Mock get_spoke_pool
        mock_contract = MagicMock()
        mock_get_spoke_pool.return_value = mock_contract
        
        # Mock contract.decode_function_input
        mock_contract.decode_function_input.return_value = (
            None,
            {
                "relayData": {
                    "inputToken": bytes.fromhex("000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
                    "outputToken": bytes.fromhex("000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831"),
                    "originChainId": 1,
                }
            }
        )
        
        # Mock requests.get
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "1",
            "result": [
                {
                    "hash": "0x123",
                    "methodId": "0xc6c3da79",  # FILL_RELAY_METHOD_ID
                    "isError": "0",
                    "input": "0x..."
                }
            ]
        }
        mock_get.return_value = mock_response
        
        # Call discover_routes with mocked discovery part
        with patch("discover_routes.execute_query") as mock_execute_query:
            # Mock check_and_insert_token
            with patch("discover_routes.check_and_insert_token") as mock_check_token:
                mock_check_token.return_value = True
                
                # Mock get_token_metadata
                with patch("discover_routes.get_token_metadata") as mock_get_metadata:
                    mock_get_metadata.return_value = ("USDC", 6)
                    
                    # Run the discovery process
                    result = discover_routes()
                    
                    # Verify the result
                    self.assertGreaterEqual(result, 0)

    @patch("db_utils.execute_query")
    def test_check_and_insert_token(self, mock_execute_query):
        """Test checking if token exists and inserting it."""
        # Test when token doesn't exist
        mock_execute_query.return_value = []  # No token exists
        result = check_and_insert_token("0x123", "1", "TEST", 18)
        self.assertTrue(result)
        mock_execute_query.assert_called()
        
        # Test when token exists
        mock_execute_query.reset_mock()
        mock_execute_query.return_value = [{"token_address": "0x123"}]  # Token exists
        result = check_and_insert_token("0x123", "1", "TEST", 18)
        self.assertTrue(result)
        # Should query but not insert
        mock_execute_query.assert_called_once()

    @patch("db_utils.execute_query")
    def test_route_exists(self, mock_execute_query):
        """Test checking if route exists."""
        # Test when route exists
        mock_execute_query.return_value = [{"route_id": 1}]
        self.assertEqual(route_exists("1", "10", "0x123", "0x456"), 1)
        
        # Test when route doesn't exist
        mock_execute_query.reset_mock()
        mock_execute_query.return_value = []
        self.assertIsNone(route_exists("1", "10", "0x789", "0xabc"))

    @patch("db_utils.route_exists")
    @patch("db_utils.execute_query")
    def test_insert_route(self, mock_execute_query, mock_route_exists):
        """Test inserting a route."""
        # Test when route already exists
        mock_route_exists.return_value = 5
        self.assertEqual(insert_route("1", "10", "0x123", "0x456", "TEST"), -5)
        
        # Test when route doesn't exist
        mock_route_exists.reset_mock()
        mock_route_exists.return_value = None
        
        # Setup for successful insert
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 10
        mock_conn.cursor.return_value = mock_cursor
        
        with patch("db_utils.get_db_connection", return_value=mock_conn):
            with patch("db_utils.check_and_insert_token") as mock_check_token:
                mock_check_token.return_value = True
                self.assertEqual(insert_route("1", "10", "0x789", "0xabc", "NEW"), 10)


if __name__ == "__main__":
    unittest.main()
