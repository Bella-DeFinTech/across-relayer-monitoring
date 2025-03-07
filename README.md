# relayer_refactor

Across Spokepool Contract: https://github.com/across-protocol/contracts/blob/master/contracts/SpokePool.sol

Across Breaking Changed: https://docs.across.to/introduction/migration-guides/migration-guide-for-non-evm-and-prefills/breaking-changes-for-relayers

## return and bundle

Bundle Table Updates
The Bundle table is updated by the update_bundle(chain, start_block) function in tool.py. This is called in the main flow in calc_return.py for each supported chain (base, op, arb, eth) with specific starting block numbers.
The process works as follows:
The function connects to the Ethereum hub contract and retrieves ProposeRootBundle events.
It then looks for corresponding RelayedRootBundle events on the specified chain using the get_event_bundle_id() function.
When it finds matching events based on the relayerRefundRoot hash, it:
Extracts the bundle ID
Gets block numbers for all chains from the event data
Inserts a new record into the Bundle table with:
chain
bundle_id
refund_root (the hash)
End block numbers for each chain (base_end_block, op_end_block, arb_end_block, eth_end_block)
After processing, it updates a variable to track the most recent bundle ID processed for that chain.
The Bundle table serves as a record of root bundles proposed and relayed across chains, tracking the block ranges for each bundle.
Return Table Updates
The Return table is updated by the insert_return_data(chain, contract, cursor, web3, block) function in get_chain_fills.py. This function is called during the fill collection process for each chain.
The process works as follows:
The function retrieves ExecutedRelayerRefundRoot events from the chain's contract starting from a specified block.
For each event, it checks if the relayer's wallet address is in the list of refund addresses.
When it finds matching refund addresses:
It extracts transaction data including hash, token address, amount, timestamp, and bundle ID
Inserts a new record into the Return table with:
tx_hash
output_token (the token address)
output_amount
aim_chain (the chain where the return happened)
block number
time_stamp
bundle_id (from the event's rootBundleId)
The function handles potential duplicate entries with a try-except block for IntegrityError.
The Return table records refunds issued to the relayer for relaying transactions, tied to specific bundles.
Additional Updates
The system also updates LP fees and deposit times for Fill records using the update_deposit_time_and_lp_fee() function, which:
Sets LP fees to 0 for fills where repayment chain equals origin chain
For other fills with missing deposit times:
Retrieves deposit event data for the corresponding deposit IDs
Calculates LP fees based on token amounts and timestamps
Updates the Fill records with deposit timestamps and calculated LP fees
This process is part of the overall data collection and reconciliation flow seen in calc_return.py, where first fills are collected, then deposit times and LP fees updated, then bundles updated, and finally returns calculated.




## Tests
```bash 
# Run all tests
tox -e lint # check linting
tox -e format # check formatting
tox -e coverage # measure coverage

# pytest
pytest tests # run tests

# ruff 
ruff format --check # check formatting
ruff format # fix formatting 
ruff check # check linting
ruff check --fix # fix linting
ruff check --fix --unsafe-fixes # fix linting with unsafe fixes


```

# RUOSHAN UPDATE REQUESTS

`token price table`
- eliminate state / variable table. 

`bundle returns .xlsx`
- include output amount.
- for a bundles start and end time/block:
  - have to subtract the succuseful TX gas cost, 
  - Also compute unsuccestful TX gas cost.  

So the actual profit = return amount - output amount - gas cost. (lp fee deduction is already indlude in the retun amount so not needed here.)
return amount is in token, gas cost is in ETH. 
Need to convert everything together at some point. But need to have all three fields.
- output amount
- gas cost of successful tx (bundle)
- gas cost of unsuccessful tx (bundle)
`Only query tx of the (fillRelay) method`
- neet to include historical and current forms of fillRelay data.

`relayer_daily_profit.xlsx`
right now everything is being refunded on the destination chain. 


- from now on, refund strategy will always be on destination chain. from the time the program was restarted 3/2/25
- So we can calculate the overhead using the official fee calculation with API. So always include that in the overrhead calculation. For historical data. 
- 




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

`ACTUAL NEW DB SCHEMA`
## ACTUAL NEW DB SCHEMA
```sql
CREATE TABLE Chain (
    chain_id TEXT PRIMARY KEY,              -- e.g., 'eth', 'base', 'arb', 'op'
    name TEXT NOT NULL                      -- Human-readable name
);

CREATE TABLE Token (
    token_address TEXT NOT NULL,
    chain_id TEXT NOT NULL,
    symbol TEXT NOT NULL,                   -- Token symbol (USDC, WETH, etc.)
    decimals INTEGER NOT NULL,              -- Number of decimals for the token
    PRIMARY KEY (token_address, chain_id),
    FOREIGN KEY (chain_id) REFERENCES Chain(chain_id)
);

CREATE TABLE Route (
    route_id INTEGER PRIMARY KEY AUTOINCREMENT,
    origin_chain_id TEXT NOT NULL,
    destination_chain_id TEXT NOT NULL,
    input_token TEXT NOT NULL,            -- Token contract address (output token address)
    output_token TEXT NOT NULL,            -- Token contract address (input token address)
    input_token_symbol TEXT NOT NULL,                    -- Token symbol (USDC, WETH, etc.)
    output_token_symbol TEXT NOT NULL,                    -- Token symbol (USDC, WETH, etc.)
    input_token_decimals INTEGER NOT NULL,               -- Token decimal places
    output_token_decimals INTEGER NOT NULL,               -- Token decimal places
    discovery_timestamp INTEGER NOT NULL,   -- When this route was first discovered
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (origin_chain_id) REFERENCES Chain(chain_id),
    FOREIGN KEY (destination_chain_id) REFERENCES Chain(chain_id),
    FOREIGN KEY (token_address, origin_chain_id) REFERENCES Token(token_address, chain_id),
    UNIQUE(origin_chain_id, destination_chain_id, token_address)
);
```

```bash
# FILLRELAY FUNCTION
Function: fillRelay((bytes32,bytes32,bytes32,bytes32,bytes32,uint256,uint256,uint256,uint256,uint32,uint32,bytes), uint256, bytes32)
#	Name	Type	Data
0	relayData.depositor	bytes32	0x000000000000000000000000236570e2749f744cc21612e9666d076b43a3e273
0	relayData.recipient	bytes32	0x000000000000000000000000236570e2749f744cc21612e9666d076b43a3e273
0	relayData.exclusiveRelayer	bytes32	0x0000000000000000000000000000000000000000000000000000000000000000
0	relayData.inputToken	bytes32	0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
0	relayData.outputToken	bytes32	0x000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831
0	relayData.inputAmount	uint256	38714658
0	relayData.outputAmount	uint256	38706642
0	relayData.originChainId	uint256	1
0	relayData.depositId	uint256	2352238
0	relayData.fillDeadline	uint32	1741149864
0	relayData.exclusivityDeadline	uint32	0
0	relayData.message	bytes	0x
2	repaymentChainId	uint256	42161
3	repaymentAddress	bytes32	0x00000000000000000000000084a36d2c3d2078c560ff7b62815138a16671b549
More Details:
Click to show less
```
```sql
CREATE TABLE FILL (
    tx_hash TEXT PRIMARY KEY,                       -- Transaction hash
    is_success BOOLEAN DEFAULT TRUE,                -- Transaction success status
    route_id INTEGER NOT NULL,                      -- Reference to Route
    depositor TEXT NOT NULL,                        -- User who made the deposit
    recipient TEXT NOT NULL,                        -- User who receives the funds
    exclusive_relayer TEXT NOT NULL,               -- Address of the exclusive relayer 
    input_token TEXT NOT NULL,                      -- Token address on origin chain
    output_token TEXT NOT NULL,                     -- Token address on destination chain
    input_amount TEXT NOT NULL,                     -- Amount received on origin chain (in smallest unit)
    output_amount TEXT NOT NULL,                    -- Amount sent to user on destination chain (in smallest unit)
    origin_chain_id TEXT NOT NULL,                  -- Chain where funds originated
    destination_chain_id TEXT NOT NULL,             -- Chain where funds are sent
    deposit_id TEXT NOT NULL,                       -- Unique deposit ID from protocol
    fill_deadline INTEGER,                          -- Deadline for filling the relay
    exclusivity_deadline INTEGER,                   -- Deadline for exclusive relay
    message TEXT,                                   -- Any message included with the relay
    repayment_chain_id TEXT,                        -- Chain where funds are repaid
    repayment_address TEXT,                         -- Address where funds are repaid
    gas_cost TEXT,                                  -- Gas spent on transaction (in wei)
    gas_price TEXT,                                 -- Gas price used for transaction
    block_number INTEGER NOT NULL,                  -- Block where tx was confirmed
    tx_timestamp INTEGER NOT NULL,                     -- Transaction timestamp
    lp_fee TEXT,                                    -- LP fee charged by protocol
    bundle_id TEXT,                                 -- Bundle ID this fill belongs to (NOT USED)
    is_return BOOLEAN DEFAULT FALSE,                -- Whether this fill is a return (NOT USED)
    FOREIGN KEY (route_id) REFERENCES Route(route_id),
    FOREIGN KEY (repayment_chain_id) REFERENCES Chain(chain_id)
);

CREATE TABLE Fill (
    tx_hash TEXT PRIMARY KEY,
    route_id INTEGER NOT NULL,
    input_token TEXT NOT NULL,
    output_token TEXT NOT NULL,
    input_amount TEXT NOT NULL,             -- Amount received on origin chain
    output_amount TEXT NOT NULL,            -- Amount sent to user on destination chain
    is_success BOOLEAN DEFAULT TRUE,
    gas TEXT,
    deposit_id TEXT,
    timestamp INTEGER NOT NULL,
    block INTEGER NOT NULL,
    bundle_id TEXT,
    lp_fee TEXT,
    repayment_chain_id TEXT,
    FOREIGN KEY (route_id) REFERENCES Route(route_id),
    FOREIGN KEY (repayment_chain_id) REFERENCES Chain(chain_id)
);




```



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


## Order of operations 
- first step, calculate from program restart date (2025)
- later, let's see if we can compute from the beginning.


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