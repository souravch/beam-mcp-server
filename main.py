#!/usr/bin/env python3
"""
Main entry point for the Apache Beam MCP Server.

This module provides a command-line interface to start the server
that can be used to manage data pipelines on various runners.
"""

import os
import argparse
import logging
import uvicorn
from src.server.app import create_app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description='Apache Beam MCP Server')
    parser.add_argument('-c', '--config', help='Path to config file')
    parser.add_argument('-p', '--port', type=int, default=8080, help='Port to listen on')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload for development')
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_args()
    
    # Set environment variables
    if args.config:
        os.environ['CONFIG_PATH'] = args.config
    
    # Get settings from environment or arguments
    port = int(os.environ.get('PORT', args.port))
    debug = os.environ.get('DEBUG', '').lower() == 'true' or args.debug
    reload = os.environ.get('RELOAD', '').lower() == 'true' or args.reload
    
    logger.info(f"Starting Apache Beam MCP Server on port {port}, debug={debug}")
    
    # Run the app with uvicorn
    uvicorn.run(
        "src.server.app:create_app",
        host=args.host,
        port=port,
        log_level="debug" if debug else "info",
        reload=reload,
        factory=True
    )

if __name__ == '__main__':
    main() 