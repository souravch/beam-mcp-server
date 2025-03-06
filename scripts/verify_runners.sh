#!/bin/bash

# Script to verify that all supported runners are working correctly
# This script will:
# 1. Start a local Flink cluster if it's not already running
# 2. Run the test_runners.py script to test Direct, Flink, and Spark runners
# 3. Provide a summary of the results

echo "========================================================"
echo "Apache Beam MCP Server - Runner Verification Script"
echo "========================================================"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not installed."
    exit 1
fi

# Configuration
MCP_SERVER_HOST="localhost"
MCP_SERVER_PORT=8888
CONFIG_PATH="config/all_runners.yaml"

# Check if Flink is installed
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

# Check if Spark is installed
echo "Checking Spark installation..."
if ! command -v spark-submit &> /dev/null; then
    echo "Warning: Spark not found in PATH. Spark runner tests may fail."
    echo "To install Spark, follow the instructions at https://spark.apache.org/downloads/"
else
    SPARK_HOME=$(dirname $(dirname $(which spark-submit)))
    echo "Found Spark installation at $SPARK_HOME"
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

# Start MCP server in the background
echo "Starting MCP server in the background..."
CONFIG_PATH="$CONFIG_PATH" python3 main.py --debug --port $MCP_SERVER_PORT &
MCP_SERVER_PID=$!

# Wait for the server to start
echo "Waiting for MCP server to start..."
sleep 5

# Check if the server is running
if ! curl -s $MCP_SERVER_URL/api/v1/health > /dev/null; then
    echo "Error: MCP server failed to start"
    if [ -n "$MCP_SERVER_PID" ]; then
        kill $MCP_SERVER_PID
    fi
    exit 1
fi

echo "MCP server started successfully at $MCP_SERVER_URL"

# Run the runner tests
echo "========================================================"
echo "Running runner tests..."
echo "========================================================"
python3 scripts/test_runners.py --server $MCP_SERVER_URL --config $CONFIG_PATH

TEST_RESULT=$?

# Cleanup
echo "========================================================"
echo "Cleaning up..."
echo "========================================================"

# Stop MCP server
if [ -n "$MCP_SERVER_PID" ]; then
    echo "Stopping MCP server..."
    kill $MCP_SERVER_PID
    wait $MCP_SERVER_PID 2>/dev/null
fi

echo "========================================================"
echo "Runner verification completed"
echo "========================================================"

if [ $TEST_RESULT -eq 0 ]; then
    echo "✅ All tests passed!"
else
    echo "❌ Some tests failed. Please check the logs for details."
fi

# Don't stop Flink cluster automatically - user may want to keep using it
echo "Note: Flink cluster is still running. To stop it, run:"
echo "  $FLINK_HOME/bin/stop-cluster.sh"

exit $TEST_RESULT 