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

## Starting the Server

Run the server with the following command:

```bash
# Run with default configuration (Direct runner)
python main.py --debug --port 8888

# Or with Flink configuration
CONFIG_PATH=config/flink_config.yaml python main.py --debug --port 8888
```

The server will start and listen on port 8888. You can confirm it's running by checking the health endpoint:

```bash
curl http://localhost:8888/api/v1/health/health
```

You should see a response indicating the server is healthy.

## Using the API

### Listing Available Runners

To list available runners:

```bash
curl http://localhost:8888/api/v1/runners
```

### Creating a Job

To create a new job:

```bash
curl -X POST http://localhost:8888/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline": "wordcount.py",
    "runner": "direct",
    "options": {
      "input": "gs://dataflow-samples/shakespeare/kinglear.txt",
      "output": "/tmp/wordcount-output"
    }
  }'
```

This will return a job ID that you can use to track the job's progress.

### Monitoring a Job

To get the status of a job:

```bash
curl http://localhost:8888/api/v1/jobs/JOB_ID
```

Replace `JOB_ID` with the ID returned when you created the job.

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