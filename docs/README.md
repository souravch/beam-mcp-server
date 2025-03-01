# Apache Beam MCP Server Documentation

Welcome to the Apache Beam MCP Server documentation! This project provides a standardized server that implements the Model Context Protocol (MCP) for managing Apache Beam pipelines across different runners.

## Quick Navigation

- **[System Design](DESIGN.md)** - Architecture, components, and implementation details
- **[MCP Compliance](mcp-compliance.md)** - How this server implements the Model Context Protocol
- **[LLM Integration](llm_integration.md)** - Guide for integrating with LLMs for AI-controlled pipelines

## Getting Started

To get started with the Apache Beam MCP Server:

1. **Installation**
   ```bash
   git clone https://github.com/yourusername/beam-mcp-server.git
   cd beam-mcp-server
   pip install -r requirements.txt
   ```

2. **Start the server**
   ```bash
   python main.py --port 8082 --config config/flink_config.yaml
   ```

3. **Verify it's running**
   ```bash
   curl http://localhost:8082/api/v1/health/health
   ```

## Key Features

- **Multi-Runner Support**: Works with Flink, Spark, Dataflow, and Direct runners
- **MCP Compliant**: Fully implements the Model Context Protocol for AI/LLM integration
- **Unified Management**: Consistent interface for managing pipelines across different runners
- **Extensible**: Easy to add support for new runners or features

## Contributing

We welcome contributions! Please see our [Contributing Guide](../CONTRIBUTING.md) for details.

## Testing

Run the regression tests to verify everything is working:
```bash
./scripts/run_regression_tests.sh
```

## License

This project is licensed under the Apache License 2.0. 