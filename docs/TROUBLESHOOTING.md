# Troubleshooting Guide

This guide covers common issues you might encounter when working with the Apache Beam MCP Server and how to resolve them.

## Table of Contents
- [API Response Structure Issues](#api-response-structure-issues)
- [Runner Configuration Issues](#runner-configuration-issues)
- [Flink Runner Issues](#flink-runner-issues)
- [Spark Runner Issues](#spark-runner-issues)
- [Running Regression Tests](#running-regression-tests)

## API Response Structure Issues

### Issue: Tests failing with assertions about response structure

**Symptoms:**
- Test failures indicating that response structures don't match expected formats
- Errors like `'runners'` key not found in response
- Assertions failing on runner names or structure

**Solution:**
1. Ensure API response structure is consistent with the test expectations:
   ```python
   # Example proper structure for list_runners
   {
     "success": true,
     "data": {
       "runners": [...],
       "default_runner": "flink",
       "total_count": 4
     },
     "message": "Successfully retrieved 4 runners"
   }
   ```

2. Check that runner names are formatted correctly:
   - Runner names should follow the format `"Apache Flink"`, not `"Flink Runner"`
   - If you're creating custom runners, follow the naming convention in the tests

### Issue: KeyError: 'runners' when starting the server

**Symptoms:**
- Server fails to start with `KeyError: 'runners'`
- Error occurs in the client manager initialization

**Solution:**
1. Ensure your configuration file has a valid `runners` section:
   ```yaml
   runners:
     flink:
       enabled: true
       jobmanager_url: http://localhost:8081
       # other configuration...
   ```

2. Verify you're using the correct configuration file path:
   ```bash
   python -m src.server.app --config config/minimal_config.yaml
   ```

## Runner Configuration Issues

### Issue: Runner client initialization failures

**Symptoms:**
- Errors like `KeyError: 'pipeline_options'` or `'options'`
- Runners appear disabled or unavailable

**Solution:**
1. Make sure each runner in your config has all required fields:
   ```yaml
   runners:
     flink:
       enabled: true
       jobmanager_url: http://localhost:8081
       options:  # This is essential
         parallelism.default: 4
         taskmanager.numberOfTaskSlots: 4
   ```

2. If you're encountering errors in specific runner clients, use the fallback mechanism:
   ```python
   # Example fallback pattern
   try:
       # Try to get runner-specific info
   except Exception as e:
       # Create a basic runner info object instead
       return Runner(
           name=f"Apache {runner_name.capitalize()}",
           # other fields...
       )
   ```

## Flink Runner Issues

### Issue: Cannot connect to Flink cluster

**Symptoms:**
- Error messages like "Failed to connect to Flink JobManager"
- Jobs stay in CREATED state but never run

**Solution:**
1. Verify your Flink cluster is running:
   ```bash
   # Start Flink in standalone mode if needed
   $FLINK_HOME/bin/start-cluster.sh
   ```

2. Check the Flink JobManager UI at http://localhost:8081

3. Ensure your configuration has the correct JobManager URL:
   ```yaml
   runners:
     flink:
       enabled: true
       jobmanager_url: http://localhost:8081
   ```

### Issue: Flink job submissions failing

**Symptoms:**
- Error messages during job submission
- Jobs enter FAILED state immediately

**Solution:**
1. Verify the JAR file exists and is accessible:
   ```yaml
   pipeline_options:
     jar_path: "/correct/path/to/WordCount.jar"
   ```

2. Check Java is correctly installed and JAVA_HOME is set

3. Ensure your Flink cluster has enough task slots available

## Spark Runner Issues

### Issue: PySpark errors or pickle serialization issues

**Symptoms:**
- Errors relating to serialization or pickle
- `TypeError: no default __reduce__ due to non-trivial __cinit__`

**Solution:**
1. Ensure compatible PySpark version:
   ```bash
   pip install pyspark==3.3.0
   ```

2. Avoid serializing objects that contain non-serializable components like gRPC channels

3. If running in a cluster, make sure Python environments match between driver and executors

### Issue: Spark jobs stuck in RUNNING state

**Symptoms:**
- Jobs never complete and remain in RUNNING state
- Test timeouts waiting for job completion

**Solution:**
1. Extend job monitoring timeout:
   ```python
   # Increase timeout for Spark jobs
   max_checks = 20  # Increase from 10
   check_interval = 2  # seconds between checks
   ```

2. Check Spark logs for errors:
   ```bash
   cat /tmp/spark-logs/*
   ```

3. For local testing, use `local[*]` master to ensure enough execution resources

## Running Regression Tests

### Issue: Regression tests failing even with correct configuration

**Symptoms:**
- API regression tests fail even though the API seems to be working
- Inconsistent test failures

**Solution:**

1. Run a clean server instance:
   ```bash
   # Kill any existing server processes
   pkill -f 'python -m src.server.app'
   
   # Start a fresh server
   python -m src.server.app --port 8083 --config config/minimal_config.yaml
   ```

2. Check API endpoints manually before running tests:
   ```bash
   # Verify runners endpoint
   curl -s http://localhost:8083/api/v1/runners | jq
   ```

3. Run individual tests to isolate issues:
   ```bash
   python -m pytest tests/api_regression_tests.py::MCPServerAPITests::test_03_list_runners -v
   ```

4. Ensure your local environment meets all requirements described in [tests/README.md](../tests/README.md)

### Issue: Test data path issues

**Symptoms:**
- Tests fail with file not found errors
- Output directories cannot be created

**Solution:**
1. Ensure test input and output directories exist and are writable:
   ```bash
   # Create test input file
   echo "Sample test data" > /tmp/mcp_regression_test_input.txt
   
   # Ensure the output directory is writable
   touch /tmp/mcp_regression_test_output.txt
   ```

2. Update test file paths if needed in the test configuration 