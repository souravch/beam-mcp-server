# Kubernetes Deployment Guide

This guide provides detailed instructions for deploying the Apache Beam MCP Server on Kubernetes.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Deployment Options](#deployment-options)
- [Basic Deployment](#basic-deployment)
- [Production Deployment](#production-deployment)
- [Configuration](#configuration)
- [Scaling](#scaling)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Prerequisites

Before deploying the Apache Beam MCP Server on Kubernetes, ensure you have:

1. A Kubernetes cluster (v1.19+)
2. `kubectl` CLI configured to access your cluster
3. `kustomize` CLI (v4.0.0+) or Kubernetes with built-in kustomize support
4. Container registry access (if building custom images)

## Deployment Options

The Apache Beam MCP Server can be deployed to Kubernetes in several ways:

1. **Basic Deployment**: Simple deployment for development or testing
2. **Production Deployment**: Highly available, scalable deployment for production use
3. **Cloud-Specific Deployment**: Optimized for specific cloud providers (GKE, EKS, AKS)

## Basic Deployment

For a quick deployment to test the Apache Beam MCP Server:

```bash
# Create a namespace
kubectl create namespace beam-mcp

# Apply the basic deployment
kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/configmap.yaml
kubectl apply -f kubernetes/deployment.yaml
kubectl apply -f kubernetes/service.yaml

# Check deployment status
kubectl -n beam-mcp get pods
kubectl -n beam-mcp get services
```

To access the server:

```bash
# Port forward the service
kubectl -n beam-mcp port-forward svc/beam-mcp-server 8888:8888
```

The server will be accessible at http://localhost:8888.

## Production Deployment

For a production-grade deployment, use Kustomize to apply all resources:

```bash
# Clone the repository if you haven't already
git clone https://github.com/yourusername/beam-mcp-server.git
cd beam-mcp-server

# Customize the deployment (edit kubernetes/kustomization.yaml as needed)
# Update the configmap with your specific settings

# Apply the complete deployment
kubectl apply -k kubernetes/

# Verify all resources
kubectl -n beam-mcp get all
```

### High Availability Setup

For high availability, the deployment includes:

1. Multiple replicas (2 by default)
2. Pod disruption budget
3. Horizontal pod autoscaling
4. Readiness and liveness probes
5. Resource requests and limits

## Configuration

### ConfigMap

The `configmap.yaml` file contains the configuration for the Apache Beam MCP Server. Key settings include:

- `GCP_PROJECT_ID`: Your Google Cloud project ID (for Dataflow)
- `GCP_REGION`: Your Google Cloud region
- `GCP_BUCKET`: Your Google Cloud Storage bucket
- `DEFAULT_RUNNER`: Default Apache Beam runner (direct, dataflow, flink, spark)
- `ENVIRONMENT`: Deployment environment (development, staging, production)

Edit the ConfigMap before applying:

```bash
# Edit the ConfigMap
kubectl -n beam-mcp edit configmap beam-mcp-config
```

### Secrets

For sensitive information, use Kubernetes Secrets:

```bash
# Create a secret for GCP credentials
kubectl -n beam-mcp create secret generic gcp-credentials \
  --from-file=service-account.json=/path/to/your/service-account-key.json
```

## Scaling

### Horizontal Pod Autoscaler

The deployment includes a Horizontal Pod Autoscaler (HPA) that scales based on CPU and memory utilization:

```bash
# Check HPA status
kubectl -n beam-mcp get hpa
```

To modify the HPA settings:

```bash
kubectl -n beam-mcp edit hpa beam-mcp-server-hpa
```

### Manual Scaling

To manually scale the deployment:

```bash
kubectl -n beam-mcp scale deployment beam-mcp-server --replicas=5
```

## Monitoring

### Prometheus and Grafana

The deployment includes configurations for Prometheus and Grafana:

1. **Prometheus**: Scrapes metrics from the Apache Beam MCP Server
2. **Grafana**: Provides dashboards for monitoring

To deploy the monitoring stack:

```bash
# Apply Prometheus and Grafana resources
kubectl apply -f monitoring/prometheus/
kubectl apply -f monitoring/grafana/

# Access Grafana
kubectl -n beam-mcp port-forward svc/grafana 3000:3000
```

Access Grafana at http://localhost:3000 (default credentials: admin/admin).

### Logging

Logs are sent to stdout/stderr and can be viewed with:

```bash
# View logs for a specific pod
kubectl -n beam-mcp logs -f deployment/beam-mcp-server

# View logs for all pods with the app=beam-mcp-server label
kubectl -n beam-mcp logs -f -l app=beam-mcp-server
```

For production, consider using a log aggregation solution like Elasticsearch, Fluentd, and Kibana (EFK) or Cloud Logging.

## Troubleshooting

### Common Issues

1. **Pods not starting**:
   ```bash
   kubectl -n beam-mcp describe pod <pod-name>
   ```

2. **Service not accessible**:
   ```bash
   kubectl -n beam-mcp get endpoints beam-mcp-server
   ```

3. **Configuration issues**:
   ```bash
   kubectl -n beam-mcp logs <pod-name> | grep ERROR
   ```

### Health Checks

The deployment includes readiness and liveness probes that check the `/health` endpoint. To manually check:

```bash
# Port forward to a pod
kubectl -n beam-mcp port-forward <pod-name> 8888:8888

# Check health endpoint
curl http://localhost:8888/health
```

### Restarting the Deployment

If needed, restart the deployment:

```bash
kubectl -n beam-mcp rollout restart deployment beam-mcp-server
```

### Checking Events

View Kubernetes events for troubleshooting:

```bash
kubectl -n beam-mcp get events --sort-by='.lastTimestamp'
```

## Advanced Topics

### Ingress Configuration

For external access, deploy an Ingress resource:

```bash
kubectl apply -f kubernetes/ingress.yaml
```

### TLS Configuration

To enable TLS, create a TLS secret and update the Ingress:

```bash
# Create a TLS secret
kubectl -n beam-mcp create secret tls beam-mcp-tls \
  --cert=/path/to/tls.crt \
  --key=/path/to/tls.key
```

### Network Policies

To restrict network traffic, apply network policies:

```bash
kubectl apply -f kubernetes/network-policy.yaml
```

### Custom Resource Definitions (CRDs)

If using custom resources for Apache Beam jobs, apply the CRDs:

```bash
kubectl apply -f kubernetes/crds/
```

### Helm Chart (Alternative)

For Helm users, a Helm chart is available:

```bash
# Add the repository
helm repo add beam-mcp https://yourusername.github.io/beam-mcp-charts

# Install the chart
helm install beam-mcp beam-mcp/beam-mcp-server \
  --namespace beam-mcp \
  --create-namespace \
  --values values.yaml
``` 