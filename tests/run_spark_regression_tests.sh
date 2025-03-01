#!/bin/bash
# Run Spark API regression tests for Apache Beam MCP Server
#
# This script runs the comprehensive test suite for all Spark API endpoints
# and can be used as part of a CI/CD pipeline for regression testing.
#
# Usage:
#   ./tests/run_spark_regression_tests.sh

set -e

# Go to project root
cd "$(dirname "$0")/.."

echo "Running Spark API regression tests..."

# Make sure any previous test output is cleaned up
rm -rf output/test_output

# Make sure any previous server is stopped
pkill -f "python -m src.server.app" || true

# Run the regression tests
python -m pytest tests/test_spark_api_regression.py -v

# Print summary
echo "Spark API regression tests completed." 