# relayer_refactor

## Tests
```bash 
# Run all tests
pytest -v

# Run specific test
python -m pytest tests/test_init_db.py -v
python -m pytest tests/test_discover_routes.py -v
```


## Refactor Goals 
1. Ensure that the database is the source of truth for all data. 
2. Use Excel as a view of the database, not a data source
3. Automatically determine relayed routes from scanner data rather than hardcoding routes. 
4. Remove all hardcoded values. 
5. Find a way to calculate current capital by factoring in bundles / repayments in transit. 

## Plan 
1. Figure out DB Schema 
2. Figure out folder structure.
3. Implement DB population:
  - Routes 
  - Bundles for those routes 
  - Enrich with LP Fee 
  - Price / Capital / APY Calculation.
4. Implement Excel Views. 

## New DB Schema 
### Core Tables 

```sql
   CREATE TABLE Route (
       route_id INTEGER PRIMARY KEY AUTOINCREMENT,
       origin_chain_id INTEGER NOT NULL,              -- Chain ID where funds originate
       destination_chain_id INTEGER NOT NULL,         -- Chain ID where funds are sent
       token_address TEXT NOT NULL,                   -- Token contract address
       token_symbol TEXT NOT NULL,                    -- Token symbol (USDC, WETH, etc.)
       token_decimals INTEGER NOT NULL,               -- Token decimal places
       discovery_timestamp INTEGER NOT NULL,          -- When this route was first discovered
       is_active BOOLEAN DEFAULT 1                    -- Whether this route is still active
   )

   CREATE TABLE Fill (
       fill_id INTEGER PRIMARY KEY AUTOINCREMENT,
       tx_hash TEXT UNIQUE NOT NULL,                  -- Transaction hash
       route_id INTEGER NOT NULL,                     -- Reference to Route
       block_number INTEGER NOT NULL,                 -- Block where tx was confirmed
       timestamp INTEGER NOT NULL,                    -- Transaction timestamp
       input_amount TEXT NOT NULL,                    -- Amount received from user (in wei/smallest unit)
       output_amount TEXT NOT NULL,                   -- Amount sent to user (in wei/smallest unit)
       gas_cost TEXT NOT NULL,                        -- Gas spent on transaction (in wei)
       is_success BOOLEAN NOT NULL,                   -- Transaction success status
       bundle_id INTEGER,                             -- Bundle this fill belongs to (if any)
       deposit_id TEXT,                               -- Original deposit ID reference
       lp_fee TEXT,                                   -- LP fee if applicable
       FOREIGN KEY (route_id) REFERENCES Route(route_id)
   )

   CREATE TABLE Return (
       return_id INTEGER PRIMARY KEY AUTOINCREMENT,
       tx_hash TEXT UNIQUE NOT NULL,                  -- Return transaction hash
       repayment_chain_id INTEGER NOT NULL,           -- Chain where return was received
       token_address TEXT NOT NULL,                   -- Token address
       amount TEXT NOT NULL,                          -- Amount returned (in wei/smallest unit)
       timestamp INTEGER NOT NULL,                    -- Transaction timestamp
       block_number INTEGER NOT NULL,                 -- Block where tx was confirmed
       bundle_id INTEGER NOT NULL                     -- Bundle ID this return is for
   )
  
   CREATE TABLE Bundle (
       bundle_id INTEGER PRIMARY KEY,                 -- Bundle ID from protocol
       refund_root TEXT UNIQUE NOT NULL,              -- Refund root hash
       created_at INTEGER NOT NULL,                   -- When bundle was created
       processed_at INTEGER,                          -- When bundle was processed by our system
       status TEXT NOT NULL DEFAULT 'PENDING'         -- Status: PENDING, PROCESSED, RECONCILED
   )

    CREATE TABLE Chain (
       chain_id INTEGER PRIMARY KEY,                  -- Chain ID (e.g., 1 for Ethereum)
       name TEXT NOT NULL,                            -- Chain name (e.g., Ethereum)
       explorer_api_url TEXT NOT NULL,                -- Block explorer API URL
       rpc_url TEXT NOT NULL,                         -- RPC endpoint
       api_key TEXT,                                  -- API key for explorer
       last_processed_block INTEGER DEFAULT 0,        -- Last block processed by our system
       spoke_pool_address TEXT NOT NULL               -- Across protocol spoke pool address
   )

```

### Analytics Tables 
```sql 
   CREATE TABLE DailyMetric (
       metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
       date TEXT NOT NULL,                            -- Date in YYYY-MM-DD format
       route_id INTEGER NOT NULL,                     -- Reference to Route
       fill_count INTEGER NOT NULL DEFAULT 0,         -- Number of fills
       successful_fill_count INTEGER NOT NULL DEFAULT 0,  -- Number of successful fills
       input_amount TEXT NOT NULL DEFAULT '0',        -- Total input amount (in token units)
       output_amount TEXT NOT NULL DEFAULT '0',       -- Total output amount (in token units)
       lp_fee TEXT NOT NULL DEFAULT '0',              -- Total LP fees
       gas_cost TEXT NOT NULL DEFAULT '0',            -- Total gas cost (in ETH)
       profit TEXT NOT NULL DEFAULT '0',              -- Calculated profit
       token_price TEXT,                              -- Token price in USD
       eth_price TEXT,                                -- ETH price in USD
       FOREIGN KEY (route_id) REFERENCES Route(route_id),
       UNIQUE(date, route_id)
   )

   CREATE TABLE Capital (
       capital_id INTEGER PRIMARY KEY AUTOINCREMENT,
       effective_date TEXT NOT NULL,                  -- Date from which capital allocation is effective
       token_address TEXT NOT NULL,                   -- Token address
       chain_id INTEGER NOT NULL,                     -- Chain ID
       amount TEXT NOT NULL,                          -- Amount of capital allocated
       UNIQUE(effective_date, token_address, chain_id)
   )

   CREATE TABLE ProcessingState (
       key TEXT PRIMARY KEY,                          -- State identifier
       value TEXT NOT NULL,                           -- State value
       updated_at INTEGER NOT NULL                    -- When state was last updated
   )
```


## Program Flow 
1. init_db.py
Creates database tables if they don't exist
Initializes reference data (chains, etc.)
Sets up initial processing state

2. route_discovery.py
Discovers routes by analyzing blockchain transactions
Updates the Routes table with new routes
Main function: discover_and_update_routes()

3. fill_collector.py
Collects fill transactions from each chain
Parses transaction data and stores in Fill table
Updates last processed block for each chain
Main function: collect_fills_for_all_chains()

4. return_processor.py
Processes return events from spoke contract
Stores return data in Return table
Links returns to bundles
Main function: process_returns_for_all_chains()

5. bundle_processor.py
Groups fills into bundles based on timestamp and chain
Updates bundle status when returns are processed
Reconciles input amounts vs return amounts
Logs alerts for discrepancies
Main function: process_bundles()

6. metrics_calculator.py
Calculates daily metrics for each route
Aggregates fill data into daily summaries
Stores results in DailyMetric table
Main function: calculate_daily_metrics()

7. profit_analyzer.py
Calculates profit metrics and APY
Uses capital allocation data
Main function: calculate_profit_and_apy()

8. report_generator.py
Generates Excel reports from database data
Creates views for different stakeholders
Main function: generate_reports()

9. alert_manager.py
Monitors for anomalies in the data
Sends alerts for significant discrepancies
Main function: check_and_send_alerts()

10. main.py
Orchestrates the entire process flow
Logs execution progress and errors
Can be run with specific modules only
Handles dependencies between modules

## Program Flow Steps
Route Discovery
Scans blockchain transactions for fillRelay method calls
Extracts token and chain information
Updates Routes table with new routes

Fill Collection
For each chain, queries explorer API for relayer transactions
Filters for fillRelay method calls
Decodes transaction data using Web3
Stores fill details in Fill table
Updates last processed block in ProcessingState table

Return Processing
Queries spoke contracts for ExecutedRelayerRefundRoot events
Extracts return amounts and token information
Stores return data in Return table

Bundle Processing
Groups fills by timestamp windows and chains
Assigns bundle IDs based on relayer refund roots
Links returns to corresponding bundles
Calculates expected vs actual return amounts
Flags discrepancies for alerts

Metrics Calculation
Aggregates fill and return data by day
Calculates token prices using external APIs
Computes daily profit metrics for each route
Stores results in DailyMetric table

Profit Analysis
Retrieves capital allocation data
Calculates profit relative to capital
Computes APY metrics

Report Generation
Queries database for metrics
Generates Excel reports:
Daily metrics by route
Bundle performance
Returns and reconciliation
Alerts and anomalies

Alert Management
Checks for significant discrepancies
Sends alerts to designated channels

## Route Discovery 

```bash
ETHEREUM (1) DESTINATION ROUTES:
-------------------------------
Route 1:
  Token: WBTC
  Origin: Optimism (10)

Route 2:
  Token: WBTC
  Origin: Arbitrum (42161)

Route 3:
  Token: USDC
  Origin: Base (8453)

Route 4:
  Token: USDC
  Origin: Arbitrum (42161)

Route 5:
  Token: USDT
  Origin: Optimism (10)

BASE (8453) DESTINATION ROUTES:
-----------------------------
Route 6:
  Token: USDC
  Origin: Ethereum (1)

Route 7:
  Token: USDC
  Origin: Arbitrum (42161)

ARBITRUM (42161) DESTINATION ROUTES:
----------------------------------
Route 8:
  Token: USDC
  Origin: Ethereum (1)

Route 9:
  Token: USDC
  Origin: Optimism (10)

Route 10:
  Token: USDC
  Origin: Base (8453)
```