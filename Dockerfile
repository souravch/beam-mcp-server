FROM python:3.9-slim

WORKDIR /app

# Install necessary system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create directories for config and data
RUN mkdir -p /app/config /app/data

# Expose port
EXPOSE 8080

# Set environment variables
ENV PORT=8080
ENV CONFIG_PATH=/app/config/beam_mcp_config.yaml
ENV PYTHONPATH=/app

# Run the application
CMD ["python", "main.py"] 