#!/bin/bash

# Wrapper script for running end-to-end tests
# This script:
# 1. Checks for required dependencies
# 2. Starts a local Flink cluster if needed
# 3. Runs the end-to-end tests

set -e  # Exit on any error

echo "========================================================"
echo "Apache Beam MCP Server - End-to-End Test Runner"
echo "========================================================"

# Default values
CONFIG_PATH="config/flink_config.yaml"
SERVER_PORT=8082
SKIP_FLINK_CHECK=false

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --config) CONFIG_PATH="$2"; shift ;;
        --port) SERVER_PORT="$2"; shift ;;
        --skip-flink-check) SKIP_FLINK_CHECK=true ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not installed."
    exit 1
fi

# Check for Flink installation if not skipped
if [ "$SKIP_FLINK_CHECK" = false ]; then
    echo "Checking Flink installation..."
    if ! command -v flink &> /dev/null; then
        echo "Warning: Flink CLI not found in PATH. Attempting to find Flink in common locations..."
        
        # Common Flink installation paths
        POSSIBLE_FLINK_HOMES=(
            "$HOME/flink"
            "/usr/local/flink"
            "/opt/flink"
            "$HOME/Downloads/flink"
        )
        
        FLINK_HOME=""
        for path in "${POSSIBLE_FLINK_HOMES[@]}"; do
            if [ -d "$path" ] && [ -f "$path/bin/start-cluster.sh" ]; then
                FLINK_HOME="$path"
                echo "Found Flink installation at $FLINK_HOME"
                break
            fi
        done
        
        if [ -z "$FLINK_HOME" ]; then
            echo "Warning: Flink not found. Flink runner tests may fail."
            echo "To install Flink, follow the instructions at https://flink.apache.org/downloads/"
        else
            # Add Flink to PATH
            export PATH="$FLINK_HOME/bin:$PATH"
        fi
    else
        FLINK_HOME=$(dirname $(dirname $(which flink)))
        echo "Found Flink installation at $FLINK_HOME"
    fi

    # Check if a Flink cluster is running
    check_flink_cluster() {
        if [ -n "$FLINK_HOME" ]; then
            # Check if Flink's JobManager is running on port 8081
            if curl -s http://localhost:8081 > /dev/null; then
                return 0  # Cluster is running
            fi
        fi
        return 1  # Cluster is not running
    }

    # Start Flink cluster if needed
    if check_flink_cluster; then
        echo "Flink cluster is already running"
    else
        echo "Starting Flink cluster..."
        if [ -n "$FLINK_HOME" ]; then
            "$FLINK_HOME/bin/start-cluster.sh"
            echo "Waiting for Flink cluster to start..."
            sleep 5  # Give Flink time to start
            
            if check_flink_cluster; then
                echo "Flink cluster started successfully"
            else
                echo "Warning: Failed to start Flink cluster. Flink runner tests may fail."
            fi
        else
            echo "Warning: Flink not found. Skipping Flink cluster startup."
        fi
    fi
fi

# Check for Spark installation
echo "Checking Spark installation..."
if ! command -v spark-submit &> /dev/null; then
    echo "Warning: Spark not found in PATH. Spark runner tests may fail."
    echo "To install Spark, follow the instructions at https://spark.apache.org/downloads/"
else
    SPARK_HOME=$(dirname $(dirname $(which spark-submit)))
    echo "Found Spark installation at $SPARK_HOME"
fi

# Run the end-to-end tests
echo "========================================================"
echo "Running end-to-end tests..."
echo "========================================================"
python3 scripts/e2e_test.py --config "$CONFIG_PATH" --port "$SERVER_PORT"

TEST_RESULT=$?

echo "========================================================"
echo "End-to-end tests completed"
echo "========================================================"

if [ "$SKIP_FLINK_CHECK" = false ] && [ -n "$FLINK_HOME" ]; then
    echo "Note: Flink cluster is still running. To stop it, run:"
    echo "  $FLINK_HOME/bin/stop-cluster.sh"
fi

if [ $TEST_RESULT -eq 0 ]; then
    echo "✅ All tests passed!"
    exit 0
else
    echo "❌ Some tests failed. Please check the logs for details."
    exit 1
fi 