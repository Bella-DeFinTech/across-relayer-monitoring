# Across Relayer Monitoring

Tool for monitoring Across Relayer Bot relays, returns, and profit. 

## Setup
```bash
# install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# set up and populate .env 
cp .env.example .env

# run code 
python3 main.py
```

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

## Calc Daily 
calc_daily.py Output (per chain_token sheet):
Date (YYYYMMDD format)
Total Input Amount
Total Output Amount
Total LP Fee
Total Gas Fee (in ETH)
Total Gas Fee (in USD)
Total Orders
Successful Orders
Token Price
ETH Price
Profit (in USD)
LP Fee (in USD)
Gas Fee (in USD)