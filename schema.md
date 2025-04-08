# New Schema 

```sql
CREATE TABLE Chain (
    chain_id INTEGER PRIMARY KEY,              -- e.g., 1 (ETH), 10 (OP), 42161 (ARB), 8453 (BASE)
    name TEXT NOT NULL                      -- Human-readable name
);

CREATE TABLE Token (
    token_address TEXT NOT NULL,
    chain_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,                   -- Token symbol (USDC, WETH, etc.)
    decimals INTEGER NOT NULL,              -- Number of decimals for the token
    PRIMARY KEY (token_address, chain_id),
    FOREIGN KEY (chain_id) REFERENCES Chain(chain_id)
);

CREATE TABLE Route (
    route_id INTEGER PRIMARY KEY AUTOINCREMENT,
    origin_chain_id INTEGER NOT NULL,
    destination_chain_id INTEGER NOT NULL,
    input_token TEXT NOT NULL,              -- Token contract address on origin chain
    output_token TEXT NOT NULL,             -- Token contract address on destination chain
    token_symbol TEXT NOT NULL,             -- Token symbol on destination chain
    discovery_timestamp INTEGER NOT NULL,   -- When this route was first discovered
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (origin_chain_id) REFERENCES Chain(chain_id),
    FOREIGN KEY (destination_chain_id) REFERENCES Chain(chain_id),
    FOREIGN KEY (input_token, origin_chain_id) REFERENCES Token(token_address, chain_id),
    FOREIGN KEY (output_token, destination_chain_id) REFERENCES Token(token_address, chain_id),
    UNIQUE(origin_chain_id, destination_chain_id, input_token, output_token)
);

CREATE TABLE Fill (
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
    origin_chain_id INTEGER NOT NULL,                  -- Chain where funds originated
    destination_chain_id INTEGER NOT NULL,             -- Chain where funds are sent
    deposit_id TEXT NOT NULL,                       -- Unique deposit ID from protocol
    fill_deadline INTEGER,                          -- Deadline for filling the relay
    exclusivity_deadline INTEGER,                   -- Deadline for exclusive relay
    message TEXT,                                   -- Any message included with the relay
    repayment_chain_id INTEGER,                        -- Chain where funds are repaid
    repayment_address TEXT,                         -- Address where funds are repaid
    gas_cost TEXT,                                  -- Gas spent on transaction (in wei)
    gas_price TEXT,                                 -- Gas price used for transaction
    block_number INTEGER NOT NULL,                  -- Block where tx was confirmed
    tx_timestamp INTEGER NOT NULL,                     -- Transaction timestamp
    deposit_block_number INTEGER,                   -- Block number where deposit event occurred
    deposit_timestamp INTEGER,                      -- Timestamp from deposit event    
    lp_fee TEXT,                                    -- LP fee charged by protocol
    bundle_id TEXT,                                 -- Bundle ID this fill belongs to (NOT USED)
    is_return BOOLEAN DEFAULT FALSE,                -- Whether this fill is a return (NOT USED)
    FOREIGN KEY (route_id) REFERENCES Route(route_id),
    FOREIGN KEY (repayment_chain_id) REFERENCES Chain(chain_id)
);

CREATE TABLE Return (
    tx_hash TEXT NOT NULL,                      -- Transaction hash of the refund event
    return_chain_id INTEGER NOT NULL,                  -- Chain where return occurred (from chainId)
    return_token TEXT NOT NULL,                -- Token address being returned (from l2TokenAddress)
    return_amount TEXT NOT NULL,                       -- Amount returned (from refundAmounts[i])
    root_bundle_id INTEGER NOT NULL,            -- Bundle ID from event (from rootBundleId)
    leaf_id INTEGER NOT NULL,                   -- Leaf ID from event
    refund_address TEXT NOT NULL,               -- Address receiving refund (from refundAddresses[i])
    is_deferred BOOLEAN NOT NULL,              -- Whether refund was deferred
    caller TEXT NOT NULL,                       -- Address that called the refund
    block_number INTEGER NOT NULL,              -- Block where return occurred
    tx_timestamp INTEGER NOT NULL,              -- Transaction timestamp
    PRIMARY KEY (tx_hash, return_token, refund_address),
    FOREIGN KEY (return_chain_id) REFERENCES Chain(chain_id),
    FOREIGN KEY (return_token, return_chain_id) REFERENCES Token(token_address, chain_id)
);


CREATE TABLE Bundle (
    bundle_id INTEGER NOT NULL,             -- Bundle ID from event
    chain_id INTEGER NOT NULL,              -- Chain where this bundle applies
    relayer_refund_root TEXT NOT NULL,              -- Relayer refund root hash
    end_block INTEGER NOT NULL,             -- Ending block for this bundle on this chain
    processed_timestamp INTEGER,            -- When this bundle was processed
    PRIMARY KEY (bundle_id, chain_id),
    FOREIGN KEY (chain_id) REFERENCES Chain(chain_id)
);

CREATE TABLE BundleReturn (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bundle_id INTEGER NOT NULL,
    chain_id INTEGER NOT NULL,
    token_address TEXT NOT NULL,
    token_symbol TEXT NOT NULL,
    input_amount DECIMAL(36,18) NOT NULL DEFAULT 0,
    return_amount DECIMAL(36,18) NOT NULL DEFAULT 0,
    lp_fee DECIMAL(36,18) NOT NULL DEFAULT 0,
    start_time INTEGER NOT NULL,
    end_time INTEGER NOT NULL,
    fill_tx_hashes TEXT,  -- Comma-separated list of fill transaction hashes
    return_tx_hash TEXT,  -- Single transaction hash for the return
    relayer_refund_root TEXT,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (bundle_id, chain_id) REFERENCES Bundle(bundle_id, chain_id),
    UNIQUE(bundle_id, chain_id, token_address)
);


CREATE TABLE TokenPrice (
    date DATE,
    token_symbol TEXT,
    price_usd DECIMAL,
    PRIMARY KEY (date, token_symbol)
)

CREATE TABLE DailyProfit (
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
)

## Return Design 

Old Table: Return
-------------
tx_hash: TEXT       PRIMARY KEY
output_token: TEXT       PRIMARY KEY
output_amount: TEXT   
aim_chain: TEXT       PRIMARY KEY
block: INTEGER  
time_stamp: TEXT   
bundle_id: TEXT   

    event ExecutedRelayerRefundRoot(
        uint256 amountToReturn,
        uint256 indexed chainId,
        uint256[] refundAmounts,
        uint32 indexed root BundleId,
        uint32 indexed leafId,
        address l2TokenAddress,
        address[] refundAddresses,
        bool deferredRefunds,
        address caller
    );


## Bundles Design 

Old Table: Bundle
-------------
bundle_id: TEXT       PRIMARY KEY
refund_root: TEXT   
chain: TEXT       PRIMARY KEY
base_end_block: INTEGER  
op_end_block: INTEGER  
arb_end_block: INTEGER  
eth_end_block: INTEGER  

    event RelayedRootBundle(
        uint32 indexed rootBundleId, # bundle id
        bytes32 indexed relayerRefundRoot,
        bytes32 indexed slowRelayRoot
    );


## Fill Design 

event FundsDeposited(
        bytes32 inputToken,
        bytes32 outputToken,
        uint256 inputAmount,
        uint256 outputAmount,
        uint256 indexed destinationChainId,
        uint256 indexed depositId,
        uint32 quoteTimestamp,
        uint32 fillDeadline,
        uint32 exclusivityDeadline,
        bytes32 indexed depositor,
        bytes32 recipient,
        bytes32 exclusiveRelayer,
        bytes message
    );


event FilledRelay(
        bytes32 inputToken,
        bytes32 outputToken,
        uint256 inputAmount,
        uint256 outputAmount,
        uint256 repaymentChainId,
        uint256 indexed originChainId,
        uint256 indexed depositId,
        uint32 fillDeadline,
        uint32 exclusivityDeadline,
        bytes32 exclusiveRelayer,
        bytes32 indexed relayer,
        bytes32 depositor,
        bytes32 recipient,
        bytes32 messageHash,
        V3RelayExecutionEventInfo relayExecutionInfo
);


To compute LP fee, we need to get the deposit timestamp (quoteTimestamp) from the FundsDeposited event. 
We can find the correct FundsDeposited event by matching on the depositId. 
