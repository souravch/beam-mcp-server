# Apache Beam MCP Server Tests

This directory contains tests for the Apache Beam MCP Server.

## Test Types

- Unit tests: Test individual components in isolation
- Integration tests: Test interactions between components
- End-to-end tests: Test complete system workflows
- API Regression tests: Verify all API endpoints function correctly

## Local Environment Requirements

Before running the regression tests, ensure your local environment is properly set up.

### General Requirements

- Python 3.9 or higher
- beam-mcp-venv virtual environment with all dependencies installed
- Sufficient disk space (at least 1GB free) for temporary files

### Flink Runner Tests Requirements

To run the Flink regression tests, you need:

1. **Local Flink Installation**: 
   - Flink 1.16.0 or newer installed locally
   - FLINK_HOME environment variable set pointing to your Flink installation

2. **Running Flink Cluster**:
   - A local Flink cluster running (or use the standalone mode)
   - JobManager REST API accessible at http://localhost:8081
   - Ensure the Flink cluster has at least 2 task slots available

3. **Java Requirements**:
   - Java 11 or higher installed
   - JAVA_HOME environment variable properly set
   - The example `WordCount.jar` file should be available in your Flink installation's examples directory

### Spark Runner Tests Requirements

To run the Spark regression tests, you need:

1. **Local Spark Installation**:
   - Spark 3.2.0 or newer installed locally 
   - SPARK_HOME environment variable set pointing to your Spark installation

2. **PySpark**:
   - PySpark package installed in your virtual environment
   - Compatible with your local Spark installation

3. **Spark Configuration**:
   - Default master URL set to "local[*]" for local testing
   - Sufficient executor memory (at least 1g)
   - Python environment properly configured for PySpark

## Recent API Fixes

### API Response Structure Fixes

Recent updates have fixed issues with the API response structure to ensure consistency across all endpoints:

1. **list_runners endpoint**: Now returns a properly structured response with these fields:
   - `success`: Boolean indicating if the call was successful
   - `data`: Object containing:
     - `runners`: Array of runner objects
     - `default_runner`: String name of the default runner
     - `total_count`: Number of runners returned

2. **get_runner endpoint**: Fixed to properly handle fallback scenarios when runner clients don't implement the `get_runner_info` method. The endpoint now:
   - Creates a basic runner info object when the method is missing
   - Properly formats runner names as "Apache [RunnerName]" to match test expectations
   - Returns appropriate error messages when failures occur

These fixes ensure that the API endpoints match the expected format in the regression tests.

## Running Tests

### Running All Tests

To run all tests:

```bash
python -m pytest
```

### Running Unit Tests

```bash
python -m pytest tests/unit/
```

### Running Integration Tests

```bash
python -m pytest tests/integration/
```

### Running End-to-End Tests

```bash
python -m pytest tests/e2e/
```

## Runner-Specific Tests

### Direct Runner Tests

To test the Direct Runner:

```bash
python -m pytest tests/test_direct_runner.py
```

### Flink Runner Tests

To test the Flink Runner:

```bash
python -m pytest tests/test_flink_local.py
```

### Spark Runner Tests

To test the Spark Runner:

```bash
python -m pytest tests/test_spark_local.py
```

## API Regression Tests

These tests verify that all API endpoints are functioning correctly and can be used for regression testing before releases or after major changes.

### Running Flink API Regression Tests

```bash
./tests/run_flink_regression_tests.sh
```

Or

```bash
python -m pytest tests/api_regression_tests.py -v
```

### Running Spark API Regression Tests

These tests verify that all Spark-related API endpoints are functioning correctly. They include:

1. Health and manifest endpoint testing
2. Runner discovery and management
3. Job creation, monitoring, and management using PySpark directly
4. Both local mode and cluster mode testing (if a Spark cluster is available)

To run the Spark API regression tests:

```bash
./tests/run_spark_regression_tests.sh
```

Or

```bash
python -m pytest tests/test_spark_api_regression.py -v
```

## Configuration

The tests use configuration files in the `config/` directory.

- `config/flink_config.yaml`: Configuration for Flink tests
- `config/spark_config.yaml`: Configuration for Spark tests
- `config/direct_config.yaml`: Configuration for Direct Runner tests

## CI/CD Integration

For CI/CD pipelines, use the regression test scripts:

```bash
# Run Flink regression tests
./tests/run_flink_regression_tests.sh

# Run Spark regression tests
./tests/run_spark_regression_tests.sh
``` 