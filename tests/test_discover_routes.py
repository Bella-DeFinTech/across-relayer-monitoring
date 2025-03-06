"""Unit tests for the discover_routes module.

Test cases:
1. Contract initialization works correctly
2. Token information can be retrieved
3. Routes can be discovered
4. Database operations work as expected
5. Duplicate entries are handled properly
6. Error conditions are handled gracefully

"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from discover_routes import (
    get_fill_routes,
    get_token_info,
    initialize_contracts,
    insert_routes_into_db,
    insert_token_info_into_db,
)


# Test data fixtures
@pytest.fixture
def mock_chain_data():
    return {
        "chain_id": 1,
        "name": "Ethereum",
        "rpc_url": "https://eth.example.com",
        "explorer_api_url": "https://api.etherscan.io/api",
        "api_key": "test_key",
        "spoke_pool_address": "0x1234567890123456789012345678901234567890",
    }


@pytest.fixture
def mock_token_data():
    return {
        "address": "0x2222222222222222222222222222222222222222",
        "name": "Test Token",
        "symbol": "TEST",
        "decimals": 18,
    }


@pytest.fixture
def mock_route_data():
    return {
        "origin_chain_id": 1,
        "origin_chain_name": "Ethereum",
        "destination_chain_id": 2,
        "destination_chain_name": "Optimism",
        "input_token": "0x3333333333333333333333333333333333333333",
        "input_token_symbol": "INPUT",
        "input_token_name": "Input Token",
        "input_token_decimals": 18,
        "output_token": "0x4444444444444444444444444444444444444444",
        "output_token_symbol": "OUTPUT",
        "output_token_name": "Output Token",
        "output_token_decimals": 18,
    }


@pytest.fixture
def mock_transaction_data():
    return {
        "hash": "0x1234",
        "methodId": "0x12345678",
        "input": "0x...",  # Mock transaction input data
        "isError": "0",
    }


@pytest.fixture
def test_db():
    """Create a temporary in-memory SQLite database for testing."""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Create the necessary tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Token (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            chain_id INTEGER NOT NULL,
            symbol TEXT,
            decimals INTEGER,
            UNIQUE(address, chain_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Route (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_chain_id INTEGER NOT NULL,
            destination_chain_id INTEGER NOT NULL,
            input_token TEXT NOT NULL,
            output_token TEXT NOT NULL,
            token_symbol TEXT,
            UNIQUE(origin_chain_id, destination_chain_id, input_token, output_token)
        )
    """)

    conn.commit()
    yield conn
    conn.close()


# Test contract initialization
def test_initialize_contracts(mock_chain_data):
    with (
        patch("discover_routes.CHAINS", [mock_chain_data]),
        patch("discover_routes.Web3") as mock_web3,
        patch("discover_routes.SPOKE_POOL_ABI", [{"some": "abi"}]),
        patch("discover_routes.contracts", {}),
    ):
        # Create mock objects
        mock_eth = MagicMock()
        mock_web3.return_value.eth = mock_eth
        mock_web3.HTTPProvider.return_value = MagicMock()
        mock_web3.to_checksum_address.return_value = mock_chain_data[
            "spoke_pool_address"
        ]

        initialize_contracts()

        mock_web3.HTTPProvider.assert_called_once_with(mock_chain_data["rpc_url"])
        mock_eth.contract.assert_called_once_with(
            address=mock_chain_data["spoke_pool_address"], abi=[{"some": "abi"}]
        )


# Test token info retrieval
def test_get_token_info(mock_chain_data, mock_token_data):
    with (
        patch("discover_routes.CHAINS", [mock_chain_data]),
        patch("discover_routes.Web3") as mock_web3,
        patch("discover_routes.ERC20_ABI", [{"some": "abi"}]),
    ):
        # Create mock objects for contract functions
        mock_name_func = MagicMock()
        mock_name_func.call.return_value = mock_token_data["name"]

        mock_symbol_func = MagicMock()
        mock_symbol_func.call.return_value = mock_token_data["symbol"]

        mock_decimals_func = MagicMock()
        mock_decimals_func.call.return_value = mock_token_data["decimals"]

        # Create mock contract with functions
        mock_functions = MagicMock()
        mock_functions.name.return_value = mock_name_func
        mock_functions.symbol.return_value = mock_symbol_func
        mock_functions.decimals.return_value = mock_decimals_func

        mock_contract = MagicMock()
        mock_contract.functions = mock_functions

        # Set up Web3 mock
        mock_eth = MagicMock()
        mock_eth.contract.return_value = mock_contract
        mock_web3.return_value.eth = mock_eth

        mock_web3.HTTPProvider.return_value = MagicMock()
        mock_web3.to_checksum_address.return_value = mock_token_data["address"]

        result = get_token_info(mock_token_data["address"], mock_chain_data["chain_id"])

        assert result["name"] == mock_token_data["name"]
        assert result["symbol"] == mock_token_data["symbol"]
        assert result["decimals"] == mock_token_data["decimals"]


# Test route discovery
def test_get_fill_routes(mock_chain_data, mock_transaction_data):
    with (
        patch("discover_routes.CHAINS", [mock_chain_data]),
        patch("discover_routes.requests.get") as mock_get,
        patch("discover_routes.contracts") as mock_contracts,
    ):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "1",
            "result": [mock_transaction_data],
        }
        mock_get.return_value = mock_response

        mock_contract = MagicMock()
        mock_contracts.__getitem__.return_value = mock_contract

        # Mock the decode_function_input to return expected relay data
        mock_contract.decode_function_input.return_value = (
            None,
            {
                "relayData": {
                    "inputToken": b"0" * 40,
                    "outputToken": b"0" * 40,
                    "originChainId": 1,
                }
            },
        )

        routes = get_fill_routes()
        assert isinstance(routes, list)


# Database integration tests
def test_insert_routes_into_db_with_real_db(test_db, mock_route_data):
    """Test route insertion with a real SQLite database."""
    routes = [mock_route_data]
    insert_routes_into_db(routes, conn=test_db)

    cursor = test_db.cursor()
    cursor.execute("SELECT * FROM Route")
    rows = cursor.fetchall()

    assert len(rows) == 1
    route = rows[0]
    assert str(route[1]) == str(mock_route_data["origin_chain_id"])
    assert str(route[2]) == str(mock_route_data["destination_chain_id"])
    assert route[3] == mock_route_data["input_token"]
    assert route[4] == mock_route_data["output_token"]


def test_insert_token_info_into_db_with_real_db(test_db, mock_route_data):
    """Test token info insertion with a real SQLite database."""
    routes = [mock_route_data]
    insert_token_info_into_db(routes, conn=test_db)

    cursor = test_db.cursor()
    cursor.execute("SELECT * FROM Token")
    rows = cursor.fetchall()

    assert len(rows) == 2  # Should have both input and output tokens

    # Check input token
    input_token = next(row for row in rows if row[1] == mock_route_data["input_token"])
    assert str(input_token[2]) == str(mock_route_data["origin_chain_id"])
    assert input_token[3] == mock_route_data["input_token_symbol"]
    assert input_token[4] == mock_route_data["input_token_decimals"]

    # Check output token
    output_token = next(
        row for row in rows if row[1] == mock_route_data["output_token"]
    )
    assert str(output_token[2]) == str(mock_route_data["destination_chain_id"])
    assert output_token[3] == mock_route_data["output_token_symbol"]
    assert output_token[4] == mock_route_data["output_token_decimals"]


def test_duplicate_route_insertion(test_db, mock_route_data):
    """Test that duplicate routes are handled properly."""
    routes = [mock_route_data, mock_route_data]  # Try to insert the same route twice
    insert_routes_into_db(routes, conn=test_db)

    cursor = test_db.cursor()
    cursor.execute("SELECT COUNT(*) FROM Route")
    count = cursor.fetchone()[0]
    assert count == 1  # Should only have one route despite trying to insert twice


def test_duplicate_token_insertion(test_db, mock_route_data):
    """Test that duplicate tokens are handled properly."""
    routes = [mock_route_data, mock_route_data]  # Try to insert the same tokens twice
    insert_token_info_into_db(routes, conn=test_db)

    cursor = test_db.cursor()
    cursor.execute("SELECT COUNT(*) FROM Token")
    count = cursor.fetchone()[0]
    assert (
        count == 2
    )  # Should only have two tokens (input and output) despite trying to insert twice


# Test error handling
def test_get_token_info_error_handling():
    with (
        patch("discover_routes.CHAINS", []),
        patch("discover_routes.Web3") as mock_web3,
    ):
        mock_web3.HTTPProvider.side_effect = Exception("Connection error")

        result = get_token_info("0x1234", 1)
        assert result["name"] is None
        assert result["symbol"] is None
        assert result["decimals"] is None


def test_initialize_contracts_error_handling():
    mock_chain = {
        "chain_id": 1,
        "name": "Test Chain",  # Added name to fix KeyError
        "rpc_url": "invalid_url",
    }

    with (
        patch("discover_routes.CHAINS", [mock_chain]),
        patch("discover_routes.SPOKE_POOL_ABI", [{"some": "abi"}]),
        patch("discover_routes.Web3") as mock_web3,
    ):
        mock_web3.HTTPProvider.side_effect = Exception("Connection error")

        # Should not raise an exception
        initialize_contracts()


def test_database_connection_error(mock_route_data):
    """Test handling of database connection errors."""
    with patch(
        "discover_routes.get_db_connection",
        side_effect=sqlite3.Error("Connection error"),
    ):
        routes = [mock_route_data]

        # Should not raise an exception
        insert_routes_into_db(routes)
        insert_token_info_into_db(routes)
