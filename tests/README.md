# Apache Beam MCP Server Tests

This directory contains tests for the Apache Beam MCP Server.

## Test Structure

- **Unit Tests**: Tests for individual components (`unit/`)
- **Integration Tests**: Tests for component interactions (`integration/`)
- **End-to-End Tests**: Full system tests (`e2e/`)
- **Runner Tests**: Tests for specific runners:
  - `test_direct_runner.py`: Tests for the Direct runner
  - `test_flink_local.py`: Tests for the Flink runner
  - `test_spark_local.py`: Tests for the Spark runner
  - `test_runners.py`: Tests for various runner configurations

## API Regression Tests

The `api_regression_tests.py` file contains regression tests for the MCP server's API endpoints. These tests verify that all API endpoints continue to function correctly after code changes.

### API Tests Coverage

- **Health API**: Tests that the server is healthy
- **Manifest API**: Tests that the server's manifest is correctly defined
- **Runners API**: Tests listing runners and getting runner details
- **Jobs API**:
  - Creating jobs
  - Getting job details
  - Checking job status 
  - Cancelling jobs
- **Metrics API**: Tests retrieving job metrics

### Running the Regression Tests

Use the provided script to run the regression tests:

```bash
./scripts/run_regression_tests.sh
```

This script will:
1. Stop any running MCP server on port 8083
2. Run the regression tests
3. Display the test results

Alternatively, you can run the tests directly with Python's unittest framework:

```bash
python -m unittest tests/api_regression_tests.py
```

Or use pytest for more detailed reporting:

```bash
python -m pytest tests/api_regression_tests.py -v
```

## Adding New Tests

When adding new features to the MCP server, you should:

1. Create unit tests for the new functionality
2. Update the API regression tests if the feature adds or modifies API endpoints
3. Verify that existing tests still pass

This ensures that new features don't break existing functionality and that the API remains stable. 