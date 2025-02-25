"""
Example of using the Dataflow MCP server with Gemini Flash 2.0 for pipeline management.
"""

import asyncio
import json
from typing import Dict, List, Optional
from datetime import datetime
import google.generativeai as genai
from mcp import MCPClient, Context, MCPError

class PipelineAgent:
    """Agent for managing Dataflow pipelines using LLM guidance."""
    
    def __init__(self, mcp_url: str, gemini_api_key: str):
        """Initialize the agent."""
        self.mcp_client = MCPClient(base_url=mcp_url)
        self.context = None
        
        # Initialize Gemini
        genai.configure(api_key=gemini_api_key)
        self.model = genai.GenerativeModel('gemini-pro')
        self.chat = self.model.start_chat(history=[])
        
        # Store tool manifest
        self.manifest = None
        self.tools = {}
    
    async def initialize(self) -> None:
        """Initialize the agent with MCP manifest."""
        # Get MCP manifest
        self.manifest = await self.mcp_client.get_manifest()
        
        # Create session context
        self.context = await self.mcp_client.create_context(
            session_id=f"llm-session-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
            user_id="llm-agent"
        )
        
        # Process manifest for tools
        for tool in self.manifest["tools"]:
            self.tools[tool["name"]] = tool
        
        # Initialize LLM with tool knowledge
        system_prompt = self._create_system_prompt()
        self.chat = self.model.start_chat(history=[
            {"role": "system", "content": system_prompt}
        ])
    
    def _create_system_prompt(self) -> str:
        """Create system prompt with tool information."""
        tools_desc = []
        for tool in self.manifest["tools"]:
            params = json.dumps(tool.get("parameters", {}), indent=2)
            tools_desc.append(f"""
Tool: {tool['name']}
Description: {tool['description']}
Parameters: {params}
            """)
        
        return f"""You are an AI assistant that helps manage Apache Beam data pipelines.
You have access to the following tools through the Dataflow MCP server:

{''.join(tools_desc)}

When making decisions:
1. Always validate parameters before operations
2. Monitor pipeline health through metrics
3. Take corrective actions when needed
4. Maintain proper error handling
5. Consider cost and performance implications

Current session ID: {self.context.session_id}
"""
    
    async def process_user_request(self, request: str) -> str:
        """Process a user request using LLM guidance."""
        # Get LLM's analysis and plan
        response = await self.chat.send_message(f"""
User Request: {request}

Please analyze this request and provide:
1. Required steps to fulfill it
2. Tools needed from the manifest
3. Parameters to validate
4. Success criteria
5. Monitoring requirements

Provide your analysis in a structured format.
        """)
        
        plan = response.text
        
        # Execute the plan
        try:
            result = await self._execute_plan(plan)
            return f"Successfully executed plan:\n{result}"
        except Exception as e:
            error_analysis = await self.chat.send_message(f"""
Error occurred during execution: {str(e)}

Please analyze the error and provide:
1. Root cause analysis
2. Recommended recovery steps
3. Prevention measures

Error context:
{self.context.to_dict()}
            """)
            
            return f"Error executing plan: {str(e)}\nAnalysis: {error_analysis.text}"
    
    async def _execute_plan(self, plan: str) -> str:
        """Execute a plan from LLM."""
        # First, validate the plan with LLM
        validation = await self.chat.send_message(f"""
Please validate this execution plan:
{plan}

Check for:
1. Parameter completeness
2. Resource dependencies
3. Potential risks
4. Performance implications

Respond with either:
- VALID: <execution steps in JSON>
- INVALID: <reason>
        """)
        
        if validation.text.startswith("INVALID"):
            raise ValueError(f"Invalid plan: {validation.text}")
        
        # Extract steps from validation
        steps = json.loads(validation.text.replace("VALID:", "").strip())
        
        results = []
        for step in steps:
            tool_name = step["tool"]
            params = step["parameters"]
            
            # Validate parameters against tool schema
            if not self._validate_parameters(tool_name, params):
                raise ValueError(f"Invalid parameters for tool {tool_name}")
            
            # Execute tool
            result = await self.mcp_client.invoke_tool(
                tool_name=tool_name,
                parameters=params,
                context=self.context
            )
            
            results.append({
                "step": step,
                "result": result.dict()
            })
            
            # If monitoring is required, start monitoring loop
            if step.get("monitor", False):
                await self._monitor_execution(step, result)
        
        return json.dumps(results, indent=2)
    
    def _validate_parameters(self, tool_name: str, params: Dict) -> bool:
        """Validate parameters against tool schema."""
        tool = self.tools.get(tool_name)
        if not tool:
            return False
        
        # Get parameter schema
        schema = tool.get("parameters", {})
        
        # Validate required parameters
        for param, spec in schema.items():
            if spec.get("required", False) and param not in params:
                return False
        
        return True
    
    async def _monitor_execution(self, step: Dict, result: Dict) -> None:
        """Monitor execution and take corrective actions."""
        job_id = result.get("job_id")
        if not job_id:
            return
        
        while True:
            # Get metrics
            metrics = await self.mcp_client.invoke_tool(
                tool_name="get_metrics",
                parameters={"job_id": job_id},
                context=self.context
            )
            
            # Analyze metrics with LLM
            analysis = await self.chat.send_message(f"""
Please analyze these metrics and recommend actions:
{json.dumps(metrics.dict(), indent=2)}

Consider:
1. Performance issues
2. Resource utilization
3. Error rates
4. Backpressure
5. Cost efficiency

Respond with either:
- OK: <reason>
- ACTION: <action_json>
            """)
            
            if analysis.text.startswith("ACTION:"):
                action = json.loads(analysis.text.replace("ACTION:", "").strip())
                await self._take_corrective_action(job_id, action)
            
            # Wait before next check
            await asyncio.sleep(60)
    
    async def _take_corrective_action(self, job_id: str, action: Dict) -> None:
        """Take corrective action based on LLM recommendation."""
        action_type = action["type"]
        
        if action_type == "scale":
            await self.mcp_client.invoke_tool(
                tool_name="update_job",
                parameters={
                    "job_id": job_id,
                    "scaling": action["parameters"]
                },
                context=self.context
            )
        elif action_type == "savepoint":
            await self.mcp_client.invoke_tool(
                tool_name="create_savepoint",
                parameters={
                    "job_id": job_id,
                    "savepoint_path": action["parameters"]["path"]
                },
                context=self.context
            )

async def main():
    """Example usage of the Pipeline Agent."""
    # Initialize agent
    agent = PipelineAgent(
        mcp_url="http://localhost:8000",
        gemini_api_key="your-api-key"
    )
    await agent.initialize()
    
    # Process a user request
    result = await agent.process_user_request("""
Please create a streaming pipeline that:
1. Reads from Pub/Sub topic 'events'
2. Processes events in 10-second windows
3. Aggregates by user_id
4. Writes results to BigQuery
5. Monitors for backpressure and scales as needed
    """)
    
    print(result)

if __name__ == "__main__":
    asyncio.run(main()) 