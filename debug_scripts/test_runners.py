#!/usr/bin/env python

import asyncio
import yaml
import os
import logging
import sys

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

# Add the project directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

async def test_list_runners():
    """Test the list_runners method directly."""
    print("Starting test_list_runners")
    
    try:
        # Load the configuration
        config_path = "config/flink_config.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        
        print(f"Loaded config from {config_path}")
        print(f"Runners in config: {list(config['runners'].keys())}")
        print(f"Flink enabled: {config['runners'].get('flink', {}).get('enabled', False)}")
        
        # Import the client manager
        from src.server.core.client_manager import BeamClientManager
        
        # Create a client manager instance
        client_manager = BeamClientManager(config)
        
        # Initialize the client manager
        print("Initializing client manager")
        await client_manager.initialize()
        
        # List runners
        print("Calling list_runners")
        runner_list = await client_manager.list_runners()
        
        # Print the result
        print(f"Runner list contains {len(runner_list.runners)} runners")
        for i, runner in enumerate(runner_list.runners):
            print(f"Runner {i+1}: {runner.name} (type={runner.runner_type})")
        
        print("Test completed successfully")
        
    except Exception as e:
        print(f"Error in test: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(test_list_runners()) 