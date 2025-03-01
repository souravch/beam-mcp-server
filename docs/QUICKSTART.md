# Developer Quickstart Guide

This guide helps you set up the Apache Beam MCP Server for development and shows how to run your first job.

## Setup

### Prerequisites

- Python 3.9+
- Apache Flink 1.17+ (optional, for Flink runner)
- Apache Spark 3.3+ (optional, for Spark runner)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/beam-mcp-server.git
   cd beam-mcp-server
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv beam-mcp-venv
   source beam-mcp-venv/bin/activate  # On Windows: beam-mcp-venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Running the Server

Start the server with the Direct runner (no external dependencies):

```bash
python main.py --debug --port 8082
```

Or with Flink runner (if you have Flink installed):

```bash
CONFIG_PATH=config/flink_config.yaml python main.py --debug --port 8082
```

## Testing Your Setup

### Health Check

```bash
curl http://localhost:8082/api/v1/health/health
```

Response should be:
```json
{"status":"healthy","timestamp":"2025-03-01T12:34:56.789Z","version":"1.0.0"}
```

### List Available Runners

```bash
curl http://localhost:8082/api/v1/runners
```

### Run an Example Job

1. **Create a test input file**
   ```bash
   echo "This is a test file for Apache Beam WordCount example" > /tmp/input.txt
   ```

2. **Submit a WordCount job**
   ```bash
   curl -X POST http://localhost:8082/api/v1/jobs \
     -H "Content-Type: application/json" \
     -d '{
       "job_name": "test-wordcount",
       "runner_type": "direct",
       "job_type": "BATCH",
       "code_path": "examples/pipelines/wordcount.py",
       "pipeline_options": {
         "input_file": "/tmp/input.txt",
         "output_path": "/tmp/output"
       }
     }'
   ```

3. **Check job status**
   ```bash
   # Replace JOB_ID with the job_id from the previous response
   curl http://localhost:8082/api/v1/jobs/JOB_ID
   ```

## Development Workflow

### Running Tests

Run the regression tests:

```bash
./scripts/run_regression_tests.sh
```

### Adding a New Runner

1. Create a new client implementation in `src/server/core/runners/`
2. Register it in `src/server/core/client_factory.py`
3. Add configuration options in `config/example_config.yaml`

### Making Changes

1. Make your changes
2. Run tests to ensure nothing breaks
3. Create a pull request

## Directory Structure

```
beam-mcp-server/
├── config/              # Configuration files
├── docs/                # Documentation
├── examples/            # Example code and pipelines
├── scripts/             # Helper scripts
├── src/                 # Source code
│   └── server/          # Server implementation
│       ├── api/         # API endpoints
│       ├── core/        # Core server logic
│       │   └── runners/ # Runner implementations
│       ├── models/      # Data models
│       └── services/    # Services
└── tests/               # Tests
```

## Next Steps

- See [DESIGN.md](DESIGN.md) for architecture details
- Check [mcp-compliance.md](mcp-compliance.md) for MCP protocol information
- Explore [llm_integration.md](llm_integration.md) for LLM integration
- Run the examples in `examples/` directory 