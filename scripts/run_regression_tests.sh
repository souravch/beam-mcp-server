#!/bin/bash
# Script to run regression tests for the Apache Beam MCP Server
# This script will:
# 1. Make sure no previous server is running
# 2. Run the regression tests
# 3. Display the test results

# Set up environment
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TEST_PATH="$PROJECT_ROOT/tests/api_regression_tests.py"
PORT=8083

# Color constants
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=====================================================${NC}"
echo -e "${YELLOW}Apache Beam MCP Server - API Regression Test Runner${NC}"
echo -e "${YELLOW}=====================================================${NC}"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is required but not installed.${NC}"
    exit 1
fi

# Check if the test file exists
if [ ! -f "$TEST_PATH" ]; then
    echo -e "${RED}Error: Test file not found at $TEST_PATH${NC}"
    exit 1
fi

# Stop any running MCP server on port 8083
echo "Stopping any running MCP server on port $PORT..."
pkill -f "python main.py.*--port $PORT" || true
sleep 2

# Prepare virtual environment if needed
if [ -d "$PROJECT_ROOT/beam-mcp-venv" ]; then
    echo "Activating virtual environment..."
    source "$PROJECT_ROOT/beam-mcp-venv/bin/activate"
fi

# Run the tests
echo -e "${YELLOW}Running regression tests...${NC}"
cd "$PROJECT_ROOT" && python -m unittest "$TEST_PATH"
TEST_RESULT=$?

# Print results
echo -e "${YELLOW}=====================================================${NC}"
if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}✅ All regression tests passed!${NC}"
else
    echo -e "${RED}❌ Some tests failed. Please check the output above for details.${NC}"
fi
echo -e "${YELLOW}=====================================================${NC}"

# Make script executable
chmod +x "$0"

exit $TEST_RESULT 