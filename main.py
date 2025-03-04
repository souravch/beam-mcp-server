#!/usr/bin/env python3
"""
Main entry point for the Apache Beam MCP Server.

This module provides a command-line interface to start the server
that implements the Model Context Protocol (MCP) standard for
managing Apache Beam pipelines across different runners.
"""

import os
import argparse
import logging
import uvicorn
import sys
from src.server.app import create_app
from src.server.config import Settings

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
    parser.add_argument('-p', '--port', type=int, default=8888, help='Port to listen on')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload for development')
    
    # MCP-specific arguments
    mcp_group = parser.add_argument_group('MCP Options')
    mcp_group.add_argument('--mcp-version', choices=['1.0'], default='1.0', 
                         help='MCP protocol version to use')
    mcp_group.add_argument('--mcp-stdio', action='store_true',
                         help='Enable MCP stdio transport (for command-line tools)')
    mcp_group.add_argument('--mcp-sse', action='store_true',
                         help='Enable MCP Server-Sent Events transport')
    mcp_group.add_argument('--mcp-websocket', action='store_true',
                         help='Enable MCP WebSocket transport')
    mcp_group.add_argument('--mcp-log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                         default='INFO', help='MCP log level')
    mcp_group.add_argument('--default-runner', choices=['direct', 'dataflow', 'spark', 'flink'],
                         help='Default runner to use for jobs')
    
    # Base URL argument
    parser.add_argument('--base-url', help='Base URL for API endpoints and resources')
    
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_args()
    
    # Set environment variables
    if args.config:
        os.environ['CONFIG_PATH'] = args.config
    
    # Set MCP-specific environment variables
    if args.mcp_version:
        os.environ['BEAM_MCP_MCP_VERSION'] = args.mcp_version
    if args.mcp_log_level:
        os.environ['BEAM_MCP_MCP_LOG_LEVEL'] = args.mcp_log_level
    if args.default_runner:
        os.environ['BEAM_MCP_DEFAULT_RUNNER'] = args.default_runner
    if args.base_url:
        os.environ['BEAM_MCP_BASE_URL'] = args.base_url
    
    # Get settings from environment or arguments
    port = int(os.environ.get('PORT', args.port))
    debug = os.environ.get('DEBUG', '').lower() == 'true' or args.debug
    reload = os.environ.get('RELOAD', '').lower() == 'true' or args.reload
    
    # Check if using a command-line MCP transport
    if args.mcp_stdio:
        logger.info("Starting Apache Beam MCP Server with stdio transport")
        from mcp.server import MCPServer
        from mcp.server.stdio import serve_stdio
        
        # Create settings
        settings = Settings()
        
        # Create MCP server
        server = MCPServer(
            name=settings.mcp_server_name,
            instructions="Apache Beam MCP Server for managing data pipelines"
        )
        
        # Serve MCP over stdio
        serve_stdio(server)
        return
    
    # Standard HTTP server mode
    logger.info(f"Starting Apache Beam MCP Server on port {port}, debug={debug}")
    logger.info(f"MCP protocol version: {args.mcp_version}")
    
    if args.base_url:
        logger.info(f"Using base URL: {args.base_url}")
    
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