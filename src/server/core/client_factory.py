"""
Client factory for Apache Beam MCP server.

This module provides a factory for creating runner clients.
"""

import logging
from typing import Dict, Any, Optional

from ..models import RunnerType
from .runners.direct_client import DirectClient
from .runners.dataflow_client import DataflowClient
from .runners.spark_client import SparkClient
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
        try:
            if runner_type == RunnerType.DIRECT:
                return DirectClient(config)
            elif runner_type == RunnerType.DATAFLOW:
                return DataflowClient(config)
            elif runner_type == RunnerType.SPARK:
                return SparkClient(config)
            elif runner_type == RunnerType.FLINK:
                return FlinkClient(config)
            else:
                raise ValueError(f"Unsupported runner type: {runner_type}")
        except Exception as e:
            logger.error(f"Failed to create client for runner {runner_type}: {str(e)}")
            raise ValueError(f"Failed to create client for runner {runner_type}: {str(e)}") 