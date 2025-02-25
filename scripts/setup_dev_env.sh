#!/bin/bash

# Exit on error
set -e

# Configuration
VENV_NAME="beam-mcp-venv"
PYTHON_VERSION="3.12"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Setting up development environment for Beam MCP Server...${NC}"

# Check if Python 3.12 is installed
if ! command -v python3.12 &> /dev/null; then
    echo -e "${RED}Python 3.12 is required but not found.${NC}"
    echo -e "${YELLOW}On macOS, you can install it using:${NC}"
    echo "brew install python@3.12"
    echo -e "${YELLOW}On Ubuntu/Debian, you can install it using:${NC}"
    echo "sudo apt-get install python3.12 python3.12-venv"
    exit 1
fi

# Remove existing virtualenv if it's causing issues
if [ "$1" == "--clean" ]; then
    echo -e "${YELLOW}Removing existing virtual environment...${NC}"
    rm -rf $VENV_NAME
fi

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_NAME" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3.12 -m venv $VENV_NAME
else
    echo -e "${YELLOW}Virtual environment already exists.${NC}"
fi

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source $VENV_NAME/bin/activate

# Upgrade pip, setuptools, and wheel first
echo -e "${YELLOW}Upgrading pip, setuptools, and wheel...${NC}"
python -m pip install --upgrade pip setuptools wheel

# Install dependencies in order to handle dependencies properly
echo -e "${YELLOW}Installing dependencies in stages...${NC}"

# Stage 1: Core dependencies
echo -e "${YELLOW}Installing core dependencies...${NC}"
pip install -v \
    "fastapi>=0.104.0" \
    "uvicorn[standard]>=0.24.0" \
    "pydantic>=2.5.0" \
    "pydantic-settings>=2.0.0" \
    "mcp>=1.2.1" \
    "python-dotenv>=1.0.0" \
    "pyyaml>=6.0.1"

# Stage 2: Apache Beam and its dependencies
echo -e "${YELLOW}Installing Apache Beam and GCP dependencies...${NC}"
pip install -v \
    "apache-beam[gcp]>=2.50.0" \
    "google-api-python-client>=2.108.0" \
    "google-auth>=2.23.4" \
    "google-cloud-logging>=3.8.0"

# Stage 3: Testing dependencies
echo -e "${YELLOW}Installing testing dependencies...${NC}"
pip install -v \
    "pytest>=7.4.3" \
    "pytest-asyncio>=0.23.2" \
    "pytest-cov>=4.1.0" \
    "httpx>=0.25.1"

# Stage 4: Development tools
echo -e "${YELLOW}Installing development tools...${NC}"
pip install -v \
    "black>=23.11.0" \
    "isort>=5.12.0" \
    "flake8>=6.1.0" \
    "mypy>=1.7.0"

# Stage 5: Utilities and remaining dependencies
echo -e "${YELLOW}Installing utilities and remaining dependencies...${NC}"
pip install -v \
    "starlette>=0.27.0" \
    "requests>=2.31.0" \
    "python-multipart>=0.0.6" \
    "marshmallow>=3.20.1" \
    "kubernetes>=28.1.0" \
    "gitpython>=3.1.40" \
    "opentelemetry-api>=1.21.0" \
    "opentelemetry-sdk>=1.21.0" \
    "opentelemetry-exporter-otlp>=1.21.0" \
    "prometheus-client>=0.19.0"

# Install the package in development mode
echo -e "${YELLOW}Installing beam-mcp-server in development mode...${NC}"
pip install -e .

# Create necessary directories
echo -e "${YELLOW}Creating necessary directories...${NC}"
mkdir -p /tmp/beam-test
mkdir -p examples/pipelines

# Set up pre-commit hooks (optional)
if [ -f ".pre-commit-config.yaml" ]; then
    echo -e "${YELLOW}Setting up pre-commit hooks...${NC}"
    pip install pre-commit
    pre-commit install
fi

echo -e "${GREEN}Setup complete!${NC}"
echo -e "${GREEN}To activate the virtual environment, run:${NC}"
echo -e "${YELLOW}source $VENV_NAME/bin/activate${NC}"
echo -e "${YELLOW}If you encounter any issues, try running:${NC}"
echo -e "${YELLOW}./scripts/setup_dev_env.sh --clean${NC}" 