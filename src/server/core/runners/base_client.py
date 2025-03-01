"""
Base runner client for Apache Beam MCP server.

This module provides the base class that all runner clients must implement.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from ...models.job import JobParameters, Job, JobStatus
from ...models.savepoint import SavepointRequest, Savepoint
from ...models.metrics import JobMetrics
from ...models.runner import Runner, RunnerType, RunnerStatus

logger = logging.getLogger(__name__)

class BaseRunnerClient:
    """Base class for all runner clients."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the base runner client.
        
        Args:
            config (Dict[str, Any]): Runner configuration
        """
        self.config = config
    
    async def create_job(self, params: JobParameters) -> Job:
        """
        Create a new job.
        
        Args:
            params (JobParameters): Job parameters
            
        Returns:
            Job: Created job
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError()
    
    async def get_job(self, job_id: str) -> Optional[Job]:
        """
        Get job details.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            Optional[Job]: Job details if found
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError()
    
    async def list_jobs(self) -> List[Job]:
        """
        List all jobs.
        
        Returns:
            List[Job]: List of jobs
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError()
    
    async def list_job_ids(self) -> List[str]:
        """
        List all job IDs.
        
        Returns:
            List[str]: List of job IDs
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError()
    
    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running job.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            bool: True if cancelled successfully
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError()
    
    async def get_metrics(self, job_id: str) -> Optional[JobMetrics]:
        """
        Get job metrics.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            Optional[JobMetrics]: Job metrics if available
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError()
    
    async def create_savepoint(self, job_id: str, params: SavepointRequest) -> Savepoint:
        """
        Create a savepoint for a job.
        
        Args:
            job_id (str): Job ID
            params (SavepointRequest): Savepoint parameters
            
        Returns:
            Savepoint: Created savepoint
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError()
    
    async def get_runner_info(self) -> Runner:
        """
        Get information about this runner.
        
        Returns:
            Runner: Runner information
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError()
    
    def _validate_config(self) -> None:
        """
        Validate runner configuration.
        
        Raises:
            ValueError: If configuration is invalid
        """
        if not self.config:
            raise ValueError("Runner configuration is required")
        
        if not self.config.get('enabled', False):
            raise ValueError("Runner is not enabled") 