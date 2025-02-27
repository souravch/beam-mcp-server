"""
Runner clients for Apache Beam MCP server.

This package contains client implementations for different runners:
- Direct (local) runner
- Flink runner  
- Spark runner
"""

import logging

logger = logging.getLogger(__name__)

# Base client is required
from .base_client import BaseRunnerClient

# Direct client is required
from .direct_client import DirectClient

# Conditionally import other runner clients
# Flink client
try:
    from .flink_client import FlinkClient
    logger.info("Successfully imported FlinkClient")
    FLINK_AVAILABLE = True
except ImportError as e:
    logger.warning(f"FlinkClient import failed: {str(e)}")
    FLINK_AVAILABLE = False

# Spark client
try:
    from .spark_client import SparkClient
    logger.info("Successfully imported SparkClient")
    SPARK_AVAILABLE = True
except ImportError as e:
    logger.warning(f"SparkClient import failed: {str(e)}")
    SPARK_AVAILABLE = False

__all__ = [
    'BaseRunnerClient',
    'DirectClient',
]

if FLINK_AVAILABLE:
    __all__.append('FlinkClient')

if SPARK_AVAILABLE:
    __all__.append('SparkClient') 