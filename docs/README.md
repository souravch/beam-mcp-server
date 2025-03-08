# Apache Beam MCP Server Documentation

Welcome to the Apache Beam MCP Server documentation! This project provides a standardized server that implements the Model Context Protocol (MCP) for managing Apache Beam pipelines across different runners.

## Available Documentation

Here you'll find all the documentation for the Apache Beam MCP Server:

- **[Quickstart Guide](QUICKSTART.md)** - Get started quickly with the Beam MCP Server
- **[System Design](DESIGN.md)** - Architecture and implementation details
- **[MCP Protocol Compliance](mcp_protocol_compliance.md)** - How this server implements the Model Context Protocol
- **[User Guide & LLM Integration](mcp/user_guide_llm_integration.md)** - Comprehensive guide for using the server and LLM integration
- **[Kubernetes Deployment](kubernetes_deployment.md)** - How to deploy on Kubernetes
- **[Cloud Optimization](cloud_optimization.md)** - Best practices for cloud deployments
- **[Troubleshooting](TROUBLESHOOTING.md)** - Solutions to common problems

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
   python main.py --port 8888 --config config/flink_config.yaml
   ```

3. **Verify it's running**
   ```bash
   curl http://localhost:8888/api/v1/health/health
   ```

## Key Features

- **Multi-Runner Support**: Works with Flink, Spark, Dataflow, and Direct runners
- **MCP Compliant**: Fully implements the Model Context Protocol for AI/LLM integration
- **Unified Management**: Consistent interface for managing pipelines across different runners
- **Extensible**: Easy to add support for new runners or features
- **Standardized API**: RESTful API with consistent response formats and error handling

## Contributing

We welcome contributions! Please see our [Contributing Guide](../CONTRIBUTING.md) for details.

## Testing

Run the regression tests to verify everything is working:
```bash
./scripts/run_regression_tests.sh
```

## License

This project is licensed under the Apache License 2.0. 