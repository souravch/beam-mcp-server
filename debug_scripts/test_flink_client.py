#!/usr/bin/env python3
"""
Simple test script to diagnose Flink client issues.
This standalone script directly tests the FlinkClient without the full server.
"""

import asyncio
import yaml
import logging
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_flink_client")

# Add the project directory to the path so we can import modules
sys.path.insert(0, os.path.abspath("."))

async def test_flink_client():
    """Test the FlinkClient's get_runner_info method directly."""
    try:
        # First, import the necessary modules
        print("Importing modules...")
        
        # Import the FlinkClient
        from src.server.core.runners.flink_client import FlinkClient
        print("Successfully imported FlinkClient")
        
        # Import the models
        from src.server.models.runner import Runner, RunnerType, RunnerStatus, RunnerCapability
        print("Successfully imported runner models")
        
        # Load the configuration file
        print("Loading configuration file...")
        with open("config/flink_config.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        # Extract the Flink configuration
        flink_config = config.get("runners", {}).get("flink", {})
        print(f"Flink config: {flink_config}")
        
        # Create the Flink client
        print("Creating Flink client...")
        client = FlinkClient(flink_config)
        print(f"Flink client created: {client}")
        
        # Test the _ensure_session method
        print("Testing _ensure_session...")
        await client._ensure_session()
        print("Session created successfully")
        
        # Test direct Flink connection
        print("Testing direct connection to Flink...")
        try:
            import aiohttp
            async with client.session.get(f"{client.jobmanager_url}/overview", timeout=3.0) as response:
                if response.status == 200:
                    cluster_info = await response.json()
                    print(f"Connected to Flink cluster. Version: {cluster_info.get('flink-version', 'unknown')}")
                else:
                    print(f"Failed to connect to Flink: HTTP {response.status}")
        except Exception as e:
            print(f"Error connecting to Flink directly: {str(e)}")
        
        # Test the get_runner_info method
        print("Testing get_runner_info...")
        try:
            runner_info = await client.get_runner_info()
            print(f"Successfully got runner info: {runner_info}")
            print(f"Runner type: {runner_info.runner_type}")
            print(f"Runner status: {runner_info.status}")
            print(f"Runner capabilities: {runner_info.capabilities}")
        except Exception as e:
            print(f"Error getting runner info: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
        
        # Clean up
        if client.session:
            await client.session.close()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    # Run the test
    asyncio.run(test_flink_client()) 