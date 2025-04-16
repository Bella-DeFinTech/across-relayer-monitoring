#!/bin/bash

# Go to the project directory
cd /home/ubuntu/across-relayer-monitoring # change to your project directory

# Activate the virtual environment
source .venv/bin/activate

# Run the Python script
python main.py

# Deactivate the virtual environment when done
deactivate