#!/bin/bash

# Exit on error
set -e

# Configuration
VENV_NAME="beam-mcp-venv"

# Activate virtual environment
if [ ! -d "$VENV_NAME" ]; then
    echo "Virtual environment not found. Please run setup_dev_env.sh first."
    exit 1
fi

# Activate virtual environment
source $VENV_NAME/bin/activate

# Set environment variables
export DEFAULT_RUNNER=direct
export DEBUG=true
export ENVIRONMENT=development

# Create temp directory if it doesn't exist
mkdir -p /tmp/beam-test

# Run the server with test configuration
python main.py --config config/test_config.yaml --debug --reload 