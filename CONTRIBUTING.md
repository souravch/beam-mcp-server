# Contributing to Apache Beam MCP Server

Thank you for your interest in contributing to the Apache Beam MCP Server! This document provides guidelines and instructions for contributing.

## Getting Started

### Prerequisites

- Python 3.9+
- Git
- Docker (optional, for containerized development)
- Apache Flink 1.17+ (optional, for Flink runner testing)
- Apache Spark 3.3+ (optional, for Spark runner testing)

### Development Setup

1. **Fork the Repository**
   
   Fork the repository on GitHub, then clone your fork:
   
   ```bash
   git clone https://github.com/YOUR_USERNAME/beam-mcp-server.git
   cd beam-mcp-server
   ```

2. **Set Up Development Environment**
   
   ```bash
   # Create a virtual environment
   python -m venv beam-mcp-venv
   source beam-mcp-venv/bin/activate  # On Windows: beam-mcp-venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Install development dependencies
   pip install -r requirements-dev.txt
   ```

3. **Create a Branch**
   
   Create a branch for your feature or bugfix:
   
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Workflow

### Code Style

We follow PEP 8 coding standards and use Black for code formatting:

```bash
# Format code with Black
black src/ tests/ examples/

# Check code style with flake8
flake8 src/ tests/ examples/
```

### Running Tests

```bash
# Run all tests
python -m pytest

# Run specific tests
python -m pytest tests/test_runners.py

# Run the regression tests
./scripts/run_regression_tests.sh
```

### Running the Server

```bash
# Simplified local development run
python main.py --debug --port 8888

# With explicit config file path
CONFIG_PATH=config/flink_config.yaml python main.py --debug --port 8888
```

## Making Contributions

### Types of Contributions

We welcome various types of contributions:

- **Bug fixes**: If you find a bug, please create an issue first, then submit a PR with the fix
- **Feature additions**: New features that extend the functionality
- **Documentation improvements**: Enhancements to documentation and examples
- **Runner implementations**: Adding support for new Apache Beam runners

### Contribution Process

1. **Find or Create an Issue**
   
   Before working on a contribution, check if there's an existing issue. If not, create one.

2. **Implement Your Change**
   
   Make your changes, following the code style guidelines.

3. **Add Tests**
   
   Add appropriate tests for your changes. We aim for high test coverage.

4. **Update Documentation**
   
   Update relevant documentation to reflect your changes.

5. **Submit a Pull Request**
   
   Push your changes to your fork and submit a pull request to the main repository.
   
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Code Review**
   
   Your PR will be reviewed by maintainers. Address any feedback provided.

### Commit Messages

Follow these guidelines for commit messages:

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- First line should be 50 characters or less
- Reference issues and pull requests after the first line

Example:
```
Add Spark runner implementation

This adds support for the Apache Spark runner with the following features:
- Job submission
- Status monitoring
- Metrics collection

Fixes #123
```

## Runner Implementation Guidelines

When adding support for a new runner:

1. Create a new client implementation in `src/server/core/runners/`
2. Implement the `BaseRunnerClient` interface
3. Register the runner in `src/server/core/client_factory.py`
4. Add configuration options in `config/example_config.yaml`
5. Add tests for the new runner
6. Update documentation to include the new runner

## Documentation Guidelines

- Keep documentation concise and focused
- Use examples where possible
- Follow Markdown best practices
- Include code snippets for API usage

## Getting Help

If you have questions or need help, you can:

- Open an issue on GitHub
- Reach out to the maintainers
- Join the Apache Beam community discussions

Thank you for contributing to the Apache Beam MCP Server! 