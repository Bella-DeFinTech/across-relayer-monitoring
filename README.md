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

# fill out capital config
cp capital_config.example.yaml capital_config.yaml

# ensure GCP service_account.json is in root directory 
touch service_account.json

# run code 
python3 main.py # directly 
/home/ubuntu/across-relayer-monitoring/cron_relayer_monitoring.sh # via cron job

## cron sample 
0 10 * * * /home/ubuntu/across-relayer-monitoring/cron_relayer_monitoring.sh

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
