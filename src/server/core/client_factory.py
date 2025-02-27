"""
Client factory for Apache Beam MCP server.

This module provides a factory for creating runner clients.
"""

import logging
from typing import Dict, Any, Optional
import importlib

from ..models import RunnerType
from .runners.direct_client import DirectClient

# Set up logger
logger = logging.getLogger(__name__)

# Check which runner clients are available
RUNNER_AVAILABILITY = {
    RunnerType.DIRECT: True  # Direct runner is always available
}

# Try to import runner clients
try:
    from ..services.dataflow_client import DataflowClient
    RUNNER_AVAILABILITY[RunnerType.DATAFLOW] = True
    logger.info("Dataflow runner client module is available")
except ImportError:
    RUNNER_AVAILABILITY[RunnerType.DATAFLOW] = False
    logger.warning("Dataflow runner client module is not available")

try:
    from .runners.flink_client import FlinkClient
    RUNNER_AVAILABILITY[RunnerType.FLINK] = True
    logger.info("Flink runner client module is available")
except ImportError:
    RUNNER_AVAILABILITY[RunnerType.FLINK] = False
    logger.warning("Flink runner client module is not available")

# Spark is imported dynamically when needed, don't import here

class ClientFactory:
    """Client factory for runner clients."""
    
    @staticmethod
    def create_client(runner_type: RunnerType, config: Dict[str, Any]):
        """
        Create a runner client.
        
        Args:
            runner_type (RunnerType): Type of runner
            config (Dict[str, Any]): Runner configuration
            
        Returns:
            BaseRunnerClient: Runner client
            
        Raises:
            ValueError: If client creation fails
        """
        logger.info(f"Creating client for runner type: {runner_type}")
        logger.debug(f"Runner config: {config}")
        
        try:
            if runner_type == RunnerType.DIRECT:
                logger.info("Creating Direct runner client")
                return DirectClient(config)
            elif runner_type == RunnerType.DATAFLOW:
                logger.info("Creating Dataflow runner client")
                # Check if the Dataflow client is available
                if not RUNNER_AVAILABILITY.get(RunnerType.DATAFLOW, False):
                    raise ImportError("Dataflow runner client module is not available")
                from ..services.dataflow_client import DataflowClient
                return DataflowClient(config)
            elif runner_type == RunnerType.SPARK:
                logger.info("Creating Spark runner client")
                try:
                    # Import SparkClient only when needed
                    from .runners.spark_client import SparkClient
                    RUNNER_AVAILABILITY[RunnerType.SPARK] = True
                    return SparkClient(config)
                except ImportError:
                    RUNNER_AVAILABILITY[RunnerType.SPARK] = False
                    raise ImportError("Spark runner client module is not available")
            elif runner_type == RunnerType.FLINK:
                logger.info("Creating Flink runner client")
                # Check if the Flink client is available
                if not RUNNER_AVAILABILITY.get(RunnerType.FLINK, False):
                    raise ImportError("Flink runner client module is not available")
                from .runners.flink_client import FlinkClient
                return FlinkClient(config)
            else:
                raise ValueError(f"Unsupported runner type: {runner_type}")
        except Exception as e:
            logger.error(f"Failed to create client for runner {runner_type}: {str(e)}")
            raise ValueError(f"Failed to create client for runner {runner_type}: {str(e)}") 