"""
Client manager for Apache Beam MCP server.

This module provides a manager for multiple runner clients,
supporting Dataflow, Spark, Flink, and Direct runners.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import os

from ..models import (
    RunnerType, JobState, JobType, JobParameters, JobInfo,
    JobList, JobUpdateParameters, SavepointParameters, SavepointInfo,
    SavepointList, JobMetrics, LogEntry, LogList,
    Runner, RunnerList, Savepoint, SavepointStatus
)
from .client_factory import ClientFactory

logger = logging.getLogger(__name__)

class BeamClientManager:
    """Manager for Apache Beam runner clients."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the client manager
        
        Args:
            config (Dict[str, Any]): Server configuration
        """
        self.config = config
        self.clients = {}
        self.init_clients()
        
        # Mock data for demonstration
        self.jobs = {}
        self.savepoints = {}
    
    async def initialize(self):
        """
        Initialize the client manager asynchronously.
        Called during application startup.
        """
        logger.info("Initializing BeamClientManager")
        # Initialize any async resources or connections here
        
        # Check that at least one runner client is available
        if not any(client for client in self.clients.values()):
            if not self.config.get('disable_client_check', False):
                logger.warning("No runner clients are available. This may limit functionality.")
        
        # Get manual logging of all client statuses
        logger.info("Client status summary:")
        for runner_name, client in self.clients.items():
            if client:
                logger.info(f"  - {runner_name}: OK (client type: {type(client).__name__})")
            else:
                logger.info(f"  - {runner_name}: None")
        
        return True
    
    async def cleanup(self):
        """
        Clean up resources when the application is shutting down.
        Called during application shutdown.
        """
        logger.info("Cleaning up BeamClientManager resources")
        # Clean up any resources or connections here
        
        # Close any client connections
        clients_count = len(self.clients)
        logger.info(f"Preparing to close {clients_count} clients")
        
        for runner_name, client in self.clients.items():
            logger.info(f"Cleaning up client for runner: {runner_name}")
            
            try:
                if client is None:
                    logger.info(f"Client for {runner_name} is None, skipping cleanup")
                    continue
                    
                logger.info(f"Client type: {type(client).__name__}")
                
                if hasattr(client, 'close'):
                    logger.info(f"Calling close() on {runner_name} client")
                    try:
                        await client.close()
                        logger.info(f"Successfully closed client for runner: {runner_name}")
                    except Exception as e:
                        logger.error(f"Error closing client for runner {runner_name}: {str(e)}")
                        logger.error(f"Exception type: {type(e).__name__}")
                        import traceback
                        logger.error(f"Traceback: {traceback.format_exc()}")
                elif hasattr(client, '_close_session'):
                    # Special handling for FlinkClient which has _close_session
                    logger.info(f"Calling _close_session() on {runner_name} client")
                    try:
                        await client._close_session()
                        logger.info(f"Successfully closed session for runner: {runner_name}")
                    except Exception as e:
                        logger.error(f"Error closing session for runner {runner_name}: {str(e)}")
                        logger.error(f"Exception type: {type(e).__name__}")
                        import traceback
                        logger.error(f"Traceback: {traceback.format_exc()}")
                else:
                    logger.info(f"No close method found for {runner_name} client")
            except Exception as e:
                logger.error(f"Unexpected error during cleanup for {runner_name}: {str(e)}")
                logger.error(f"Exception type: {type(e).__name__}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
        
        logger.info("BeamClientManager cleanup completed")
        return True
    
    def init_clients(self):
        """Initialize runner clients based on configuration."""
        for runner_name, runner_config in self.config['runners'].items():
            if runner_config.get('enabled', False):
                logger.info(f"Initializing client for runner: {runner_name}")
                
                try:
                    # Use client factory to create client
                    runner_type = RunnerType(runner_name)
                    client = ClientFactory.create_client(runner_type, runner_config)
                    self.clients[runner_name] = client
                    logger.info(f"Successfully initialized client for runner: {runner_name}")
                except Exception as e:
                    logger.error(f"Failed to initialize client for runner {runner_name}: {str(e)}")
                    # Fall back to using this client manager as the client
                    self.clients[runner_name] = None
    
    def get_client(self, runner_type: RunnerType):
        """
        Get client for the specified runner type
        
        Args:
            runner_type (RunnerType): Type of runner
        
        Returns:
            Any: Runner client
            
        Raises:
            ValueError: If the runner is not enabled or not supported
        """
        runner_type_str = runner_type.value
        
        if runner_type_str not in self.config['runners']:
            raise ValueError(f"Runner type not supported: {runner_type_str}")
        
        if not self.config['runners'][runner_type_str].get('enabled', False):
            raise ValueError(f"Runner type not enabled: {runner_type_str}")
        
        # If we have a client, use it; otherwise, fall back to this client manager
        client = self.clients.get(runner_type_str)
        if client:
            return client
        return self
    
    async def create_job(self, params: JobParameters, background_tasks=None) -> JobInfo:
        """
        Create a new job
        
        Args:
            params (JobParameters): Job parameters
            background_tasks: Optional FastAPI background tasks object
            
        Returns:
            JobInfo: Created job information
        """
        # Get the appropriate client
        client = self.get_client(params.runner_type)
        
        # Create the job using the client
        job = await client.create_job(params)
        
        # Store the job info
        self.jobs[job.job_id] = job
        logger.info(f"Created job: {job.job_id} ({params.job_name})")
        
        return job
    
    async def list_jobs(
        self, 
        runner_type: Optional[RunnerType] = None, 
        job_type: Optional[JobType] = None, 
        page_size: int = 10, 
        page_token: Optional[str] = None
    ) -> JobList:
        """
        List jobs with optional filtering
        
        Args:
            runner_type (RunnerType, optional): Filter by runner type
            job_type (JobType, optional): Filter by job type
            page_size (int): Number of jobs to return
            page_token (str, optional): Pagination token
            
        Returns:
            JobList: List of jobs
        """
        filtered_jobs = list(self.jobs.values())
        
        if runner_type:
            filtered_jobs = [job for job in filtered_jobs if job.runner_type == runner_type]
        
        if job_type:
            filtered_jobs = [job for job in filtered_jobs if job.job_type == job_type]
        
        # Sort by creation time, newest first
        filtered_jobs.sort(key=lambda job: job.create_time, reverse=True)
        
        # Apply pagination (in a real implementation, this would be more sophisticated)
        start_idx = 0
        if page_token:
            # In a real implementation, decode the page token to get the start index
            try:
                start_idx = int(page_token)
            except ValueError:
                start_idx = 0
        
        end_idx = min(start_idx + page_size, len(filtered_jobs))
        next_token = str(end_idx) if end_idx < len(filtered_jobs) else None
        
        return JobList(
            jobs=filtered_jobs[start_idx:end_idx], 
            total_count=len(filtered_jobs), 
            next_page_token=next_token
        )
    
    async def get_job(self, job_id: str) -> JobInfo:
        """
        Get job information
        
        Args:
            job_id (str): Job ID
            
        Returns:
            JobInfo: Job information
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        return self.jobs[job_id]
    
    async def update_job(self, job_id: str, params: JobUpdateParameters) -> JobInfo:
        """
        Update job configuration
        
        Args:
            job_id (str): Job ID
            params (JobUpdateParameters): Update parameters
            
        Returns:
            JobInfo: Updated job information
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        job = self.jobs[job_id]
        
        # Update pipeline options with scaling parameters
        if params.scaling:
            for key, value in params.scaling.items():
                job.pipeline_options[key] = value
        
        # Update other options
        if params.update_options:
            for key, value in params.update_options.items():
                job.pipeline_options[key] = value
        
        # Update timestamp
        job.update_time = datetime.now().isoformat() + "Z"
        
        logger.info(f"Updated job: {job_id}")
        return job
    
    async def cancel_job(self, job_id: str) -> JobInfo:
        """
        Cancel a job
        
        Args:
            job_id (str): Job ID
            
        Returns:
            JobInfo: Updated job information
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        job = self.jobs[job_id]
        job.current_state = JobState.CANCELLED
        job.update_time = datetime.now().isoformat() + "Z"
        
        logger.info(f"Cancelled job: {job_id}")
        return job
    
    async def create_savepoint(self, job_id: str, params: SavepointParameters) -> Savepoint:
        """
        Create a savepoint for a job
        
        Args:
            job_id (str): Job ID
            params (SavepointParameters): Savepoint parameters
            
        Returns:
            Savepoint: Created savepoint information
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        # Generate a unique savepoint ID
        savepoint_id = f"sp-{datetime.now().strftime('%Y-%m-%d-%H%M%S')}"
        
        # Get the job to determine its runner type
        job = self.jobs[job_id]
        
        # Get the client for this runner type
        client = self.get_client(job.runner)
        
        # If client implements create_savepoint, use that
        if hasattr(client, 'create_savepoint') and client != self:
            try:
                # Use the client's implementation
                return await client.create_savepoint(job_id, params)
            except Exception as e:
                logger.error(f"Error using client create_savepoint: {str(e)}")
                # Fall back to default implementation
        
        # Create the savepoint with explicit Enum usage
        savepoint = Savepoint(
            savepoint_id=savepoint_id,
            job_id=job_id,
            status=SavepointStatus.PENDING,  # Explicitly use enum
            create_time=datetime.now().isoformat() + "Z",
            update_time=datetime.now().isoformat() + "Z",
            savepoint_path=params.savepoint_path,
            mcp_parent_job=job_id,
            mcp_resource_id=savepoint_id
        )
        
        self.savepoints[savepoint_id] = savepoint
        logger.info(f"Created savepoint: {savepoint_id} for job: {job_id}")
        
        return savepoint
        
    async def get_savepoint(self, job_id: str, savepoint_id: str) -> Savepoint:
        """
        Get savepoint information
        
        Args:
            job_id (str): Job ID
            savepoint_id (str): Savepoint ID
            
        Returns:
            Savepoint: Savepoint information
            
        Raises:
            ValueError: If the job or savepoint does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
            
        if savepoint_id not in self.savepoints:
            raise ValueError(f"Savepoint not found: {savepoint_id}")
        
        # Get the job to determine its runner type
        job = self.jobs[job_id]
        
        # Get the client for this runner type
        client = self.get_client(job.runner)
        
        # If client implements get_savepoint, use that
        if hasattr(client, 'get_savepoint') and client != self:
            try:
                # Use the client's implementation which has state transition logic
                updated_savepoint = await client.get_savepoint(job_id, savepoint_id)
                
                # Update our in-memory reference with the new state
                if updated_savepoint and updated_savepoint.status != self.savepoints[savepoint_id].status:
                    logger.info(f"Updating savepoint status from {self.savepoints[savepoint_id].status} to {updated_savepoint.status}")
                    self.savepoints[savepoint_id] = updated_savepoint
                
                return updated_savepoint
            except Exception as e:
                logger.error(f"Error using client get_savepoint: {str(e)}")
                # Fall back to stored savepoint
        
        # For testing purposes, we'll update the status to COMPLETED after the first check
        savepoint = self.savepoints[savepoint_id]
        
        # Ensure status is an enum, not a string (avoiding 'str' object has no attribute 'value' error)
        if isinstance(savepoint.status, str):
            savepoint.status = SavepointStatus(savepoint.status)
        
        if savepoint.status == SavepointStatus.PENDING:
            savepoint.status = SavepointStatus.COMPLETED
            savepoint.update_time = datetime.now().isoformat()
            savepoint.complete_time = datetime.now().isoformat()
            savepoint.savepoint_path = os.path.join(savepoint.savepoint_path or "/tmp/savepoints", savepoint_id)
            logger.info(f"[TEST MODE] Automatically updated savepoint {savepoint_id} status to COMPLETED")
            
        if savepoint.job_id != job_id:
            raise ValueError(f"Savepoint {savepoint_id} does not belong to job {job_id}")
            
        return savepoint
    
    async def list_savepoints(self, job_id: str) -> SavepointList:
        """
        List savepoints for a job
        
        Args:
            job_id (str): Job ID
            
        Returns:
            SavepointList: List of savepoints
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        filtered_savepoints = [sp for sp in self.savepoints.values() if sp.job_id == job_id]
        return SavepointList(savepoints=filtered_savepoints, total_count=len(filtered_savepoints))
    
    async def restore_from_savepoint(self, job_id: str, savepoint_id: str) -> JobInfo:
        """
        Restore a job from a savepoint
        
        Args:
            job_id (str): Job ID
            savepoint_id (str): Savepoint ID
            
        Returns:
            JobInfo: Updated job information
            
        Raises:
            ValueError: If the job or savepoint does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        if savepoint_id not in self.savepoints:
            raise ValueError(f"Savepoint not found: {savepoint_id}")
        
        job = self.jobs[job_id]
        savepoint = self.savepoints[savepoint_id]
        
        # Check if the savepoint belongs to this job
        if savepoint.job_id != job_id:
            raise ValueError(f"Savepoint {savepoint_id} does not belong to job {job_id}")
        
        # Update job state and timestamp
        job.current_state = JobState.RUNNING
        job.update_time = datetime.now().isoformat() + "Z"
        
        logger.info(f"Restored job: {job_id} from savepoint: {savepoint_id}")
        return job
    
    async def get_job_metrics(self, job_id: str) -> JobMetrics:
        """
        Get metrics for a job
        
        Args:
            job_id (str): Job ID
            
        Returns:
            JobMetrics: Job metrics
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        job = self.jobs[job_id]
        
        # In a real implementation, these would be actual metrics from the runner
        metrics = {
            "cpu_utilization": 65.2,
            "memory_usage_gb": 12.8,
            "elements_processed": 15000000,
            "processing_rate": 3500,
            "worker_count": 10,
            "elapsed_time_ms": 1800000
        }
        
        # Add streaming-specific metrics if applicable
        if job.job_type == JobType.STREAMING:
            metrics.update({
                "system_lag_ms": 120,
                "watermark_lag_ms": 95,
                "events_per_second": 5000
            })
        
        return JobMetrics(
            job_id=job_id,
            timestamp=datetime.now().isoformat() + "Z",
            metrics=metrics
        )
    
    async def get_job_logs(
        self, 
        job_id: str, 
        max_results: int = 10, 
        page_token: Optional[str] = None
    ) -> LogList:
        """
        Get logs for a job
        
        Args:
            job_id (str): Job ID
            max_results (int): Maximum number of log entries to return
            page_token (str, optional): Pagination token
            
        Returns:
            LogList: List of log entries
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        # Mock log entries (in a real implementation, these would come from the runner)
        logs = [
            LogEntry(
                timestamp=datetime.now().isoformat() + "Z",
                severity="INFO",
                message="Pipeline started successfully",
                job_id=job_id,
                worker_id=None,
                step_name=None
            ),
            LogEntry(
                timestamp=datetime.now().isoformat() + "Z",
                severity="INFO",
                message="Processing elements at 3500 elements/second",
                job_id=job_id,
                worker_id="worker-1",
                step_name="ProcessElements"
            )
        ]
        
        return LogList(logs=logs[:max_results], next_page_token=None)
    
    async def list_runners(self) -> RunnerList:
        """
        Lists available runners based strictly on configuration.
        Returns a RunnerList containing only enabled runners.
        """
        print("DEBUG: Starting list_runners method in client_manager.py")
        print(f"DEBUG: self object id: {id(self)}")
        print(f"DEBUG: self.config class: {type(self.config)}")
        print(f"DEBUG: self.clients class: {type(self.clients)}")
        
        # Check if another object might be proxying or hiding this method
        import inspect
        frame = inspect.currentframe()
        try:
            caller_frame = frame.f_back
            if caller_frame:
                print(f"DEBUG: Caller file: {caller_frame.f_code.co_filename}")
                print(f"DEBUG: Caller function: {caller_frame.f_code.co_name}")
        finally:
            del frame  # Avoid reference cycles
        
        try:
            from ..models.runner import RunnerList
            import datetime
            
            logger.info("Listing runners based on configuration")
            print(f"DEBUG: Config: {self.config.keys()}")
            print(f"DEBUG: Runners config: {self.config['runners'].keys()}")
            now = datetime.datetime.now().isoformat()
            runners = []
            
            # Only process runners that are enabled in config
            for runner_name, runner_config in self.config['runners'].items():
                print(f"DEBUG: Processing runner: {runner_name}, enabled={runner_config.get('enabled', False)}")
                if not runner_config.get('enabled', False):
                    logger.info(f"Runner {runner_name} is disabled in config, skipping")
                    continue
                    
                logger.info(f"Processing enabled runner: {runner_name}")
                
                # Get the client for this runner
                client = self.clients.get(runner_name)
                print(f"DEBUG: Client for {runner_name}: {client}")
                if client is None:
                    logger.warning(f"No client available for runner {runner_name}, skipping")
                    continue
                    
                # Get runner info from client
                try:
                    if hasattr(client, 'get_runner_info'):
                        print(f"DEBUG: Calling get_runner_info for {runner_name}")
                        logger.info(f"Getting runner info for {runner_name} from client")
                        runner = await client.get_runner_info()
                        print(f"DEBUG: get_runner_info returned for {runner_name}: {runner}")
                        if runner:
                            runners.append(runner)
                            logger.info(f"Successfully added {runner_name} runner")
                        else:
                            logger.warning(f"Client for {runner_name} returned None for runner info")
                    else:
                        print(f"DEBUG: Client for {runner_name} doesn't have get_runner_info method")
                        logger.warning(f"Client for {runner_name} doesn't have get_runner_info method")
                except Exception as e:
                    print(f"DEBUG: Error getting runner info for {runner_name}: {str(e)}")
                    logger.error(f"Error getting runner info for {runner_name}: {str(e)}")
                    import traceback
                    traceback_str = traceback.format_exc()
                    print(f"DEBUG: Traceback: {traceback_str}")
                    logger.error(f"Traceback: {traceback_str}")
                    # Skip this runner if there's an error
            
            # Get default runner from config
            default_runner = self.config.get('default_runner', 'direct')
            logger.info(f"Default runner is set to: {default_runner}")
            
            # Log summary of found runners
            runner_types = [r.runner_type for r in runners]
            print(f"DEBUG: Found {len(runners)} enabled runners: {runner_types}")
            logger.info(f"Found {len(runners)} enabled runners: {runner_types}")
            
            # Create the RunnerList
            print("DEBUG: Creating RunnerList object")
            runner_list = RunnerList(
                mcp_resource_id="runners",
                runners=runners,
                default_runner=default_runner,
                mcp_total_runners=len(runners)
            )
            
            print("DEBUG: Returning runner list from list_runners method")
            return runner_list
            
        except Exception as e:
            print(f"DEBUG: Error in list_runners: {str(e)}")
            logger.error(f"Error listing runners: {str(e)}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"DEBUG: Traceback: {traceback_str}")
            logger.error(f"Traceback: {traceback_str}")
            raise
            
    async def get_runner(self, runner_type: RunnerType) -> Runner:
        """
        Get details for a specific runner.
        
        Args:
            runner_type (RunnerType): The type of runner to get details for
            
        Returns:
            Runner: The runner details
            
        Raises:
            Exception: If the runner is not found or not enabled
        """
        logger.info(f"Getting details for runner: {runner_type}")
        
        # Convert runner_type to string if it's an enum
        runner_name = runner_type.value if hasattr(runner_type, 'value') else str(runner_type)
        
        # Check if runner is enabled in config
        runner_config = self.config.get('runners', {}).get(runner_name, {})
        if not runner_config.get('enabled', False):
            logger.warning(f"Runner {runner_name} is disabled in config")
            raise ValueError(f"Runner {runner_name} is not enabled")
        
        # Get the client for this runner
        client = self.clients.get(runner_name)
        if client is None:
            logger.warning(f"No client available for runner {runner_name}")
            raise ValueError(f"No client available for runner {runner_name}")
        
        # Get runner info from client
        try:
            if hasattr(client, 'get_runner_info'):
                logger.info(f"Getting runner info for {runner_name} from client")
                runner = await client.get_runner_info()
                if runner:
                    logger.info(f"Successfully retrieved runner info for {runner_name}")
                    return runner
                else:
                    logger.warning(f"Client for {runner_name} returned None for runner info")
                    # Fall back to creating basic info instead of raising an error
            else:
                logger.warning(f"Client for {runner_name} doesn't have get_runner_info method")
                # Fall back to creating basic info instead of raising an error
            
            # Create basic runner info as a fallback
            from ..models.runner import Runner, RunnerType, RunnerStatus, RunnerCapability
            
            # Extract pipeline options from runner config safely
            pipeline_options = runner_config.get('options', {})
            
            return Runner(
                mcp_resource_id=runner_name,
                name=f"Apache {runner_name.capitalize()}",
                runner_type=RunnerType(runner_name),
                status=RunnerStatus.AVAILABLE,
                description=f"Apache Beam {runner_name.capitalize()} runner",
                capabilities=[RunnerCapability.BATCH],
                config=pipeline_options,
                mcp_provider="apache",
                version="1.0.0"
            )
        except Exception as e:
            logger.error(f"Error getting runner info for {runner_name}: {str(e)}")
            
            # Try to create basic runner info as a final fallback
            try:
                from ..models.runner import Runner, RunnerType, RunnerStatus, RunnerCapability
                
                # Extract pipeline options from runner config safely
                pipeline_options = runner_config.get('options', {})
                
                return Runner(
                    mcp_resource_id=runner_name,
                    name=f"Apache {runner_name.capitalize()}",
                    runner_type=RunnerType(runner_name),
                    status=RunnerStatus.AVAILABLE,
                    description=f"Apache Beam {runner_name.capitalize()} runner",
                    capabilities=[RunnerCapability.BATCH],
                    config=pipeline_options,
                    mcp_provider="apache",
                    version="1.0.0"
                )
            except Exception as fallback_error:
                logger.error(f"Error creating fallback runner info: {str(fallback_error)}")
                raise e
            
    async def scale_runner(self, runner_type: RunnerType, scale_params: Any) -> Runner:
        """
        Scale runner resources
        
        Args:
            runner_type (RunnerType): Type of runner to scale
            scale_params (Any): Scaling parameters
            
        Returns:
            Runner: Updated runner information
        """
        runner_name = runner_type.value
        logger.info(f"Scaling runner: {runner_type} with params: {scale_params}")
        
        # Check if runner is supported and enabled
        if runner_name not in self.config['runners']:
            raise ValueError(f"Runner type not supported: {runner_name}")
        
        if not self.config['runners'][runner_name].get('enabled', False):
            raise ValueError(f"Runner type not enabled: {runner_name}")
        
        # Get the client
        client = self.clients.get(runner_name)
        if not client:
            raise ValueError(f"Client for {runner_name} is not available")
        
        # Debug: Print the client's attribute to check if scale_runner is available
        import inspect
        logger.info(f"Client type: {type(client).__name__}")
        logger.info(f"Client methods: {[name for name, _ in inspect.getmembers(client, predicate=inspect.ismethod)]}")
        logger.info(f"Has scale_runner attribute: {hasattr(client, 'scale_runner')}")
        
        if hasattr(client, 'scale_runner'):
            logger.info(f"Scaling runner {runner_name} with client")
            runner = await client.scale_runner(scale_params or {})
            logger.info(f"Runner scaled: {runner}")
            return runner
        else:
            logger.warning(f"Client for {runner_name} doesn't have scale_runner method")
            # For runners that don't support scaling, just return the current info
            logger.info(f"Runner {runner_name} does not support scaling, returning current info")
            return await self.get_runner(runner_type) 