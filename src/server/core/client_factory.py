"""
Client factory for Apache Beam MCP server.

This module provides a factory for creating runner clients.
"""

import logging
from typing import Dict, Any, Optional

from ..models import RunnerType
from ..services.dataflow_client import DataflowClient
from .runners.direct_client import DirectClient
from .runners.flink_client import FlinkClient

logger = logging.getLogger(__name__)

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
                return DataflowClient(config)
            elif runner_type == RunnerType.SPARK:
                logger.info("Creating Spark runner client")
                # Import SparkClient only when needed
                from .runners.spark_client import SparkClient
                return SparkClient(config)
            elif runner_type == RunnerType.FLINK:
                logger.info("Creating Flink runner client")
                return FlinkClient(config)
            else:
                raise ValueError(f"Unsupported runner type: {runner_type}")
        except Exception as e:
            logger.error(f"Failed to create client for runner {runner_type}: {str(e)}")
            raise ValueError(f"Failed to create client for runner {runner_type}: {str(e)}") 