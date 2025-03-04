# Beam MCP Server Helm Chart

This Helm chart deploys the Apache Beam MCP Server on Kubernetes.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+
- PV provisioner support in the underlying infrastructure (if persistence is enabled)

## Getting Started

### Add the Helm Repository

```bash
helm repo add beam-mcp https://yourusername.github.io/beam-mcp-charts
helm repo update
```

### Install the Chart

To install the chart with the release name `beam-mcp`:

```bash
helm install beam-mcp beam-mcp/beam-mcp-server \
  --namespace beam-mcp \
  --create-namespace
```

The command deploys the Beam MCP Server on the Kubernetes cluster in the default configuration. The [Parameters](#parameters) section lists the parameters that can be configured during installation.

### Uninstall the Chart

To uninstall/delete the `beam-mcp` deployment:

```bash
helm uninstall beam-mcp -n beam-mcp
```

## Parameters

### Global Parameters

| Name                      | Description                                     | Value           |
| ------------------------- | ----------------------------------------------- | --------------- |
| `replicaCount`            | Number of replicas                              | `2`             |
| `image.repository`        | Image repository                                | `ghcr.io/yourusername/beam-mcp-server` |
| `image.pullPolicy`        | Image pull policy                               | `IfNotPresent`  |
| `image.tag`               | Image tag (overrides appVersion)                | `""`            |
| `imagePullSecrets`        | Image pull secrets                              | `[]`            |
| `nameOverride`            | String to partially override fullname template  | `""`            |
| `fullnameOverride`        | String to fully override fullname template      | `""`            |

### Service Account Parameters

| Name                                 | Description                                                      | Value   |
| ------------------------------------ | ---------------------------------------------------------------- | ------- |
| `serviceAccount.create`              | Enable creation of ServiceAccount                                | `true`  |
| `serviceAccount.annotations`         | Additional ServiceAccount annotations                            | `{}`    |
| `serviceAccount.name`                | Name of ServiceAccount to use                                    | `""`    |

### Pod Parameters

| Name                     | Description                                      | Value           |
| ------------------------ | ------------------------------------------------ | --------------- |
| `podAnnotations`         | Pod annotations                                  | `{}`            |
| `podSecurityContext`     | Pod security context                             | `{}`            |
| `securityContext`        | Container security context                       | `{}`            |

### Service Parameters

| Name                     | Description                                      | Value           |
| ------------------------ | ------------------------------------------------ | --------------- |
| `service.type`           | Service type                                     | `ClusterIP`     |
| `service.port`           | Service port                                     | `8888`          |

### Ingress Parameters

| Name                     | Description                                      | Value           |
| ------------------------ | ------------------------------------------------ | --------------- |
| `ingress.enabled`        | Enable ingress                                   | `false`         |
| `ingress.className`      | Ingress class name                               | `nginx`         |
| `ingress.annotations`    | Ingress annotations                              | `{}`            |
| `ingress.hosts`          | Ingress hosts                                    | `[]`            |
| `ingress.tls`            | Ingress TLS configuration                        | `[]`            |

### Resource Parameters

| Name                     | Description                                      | Value           |
| ------------------------ | ------------------------------------------------ | --------------- |
| `resources.limits.cpu`   | CPU limits                                       | `1000m`         |
| `resources.limits.memory`| Memory limits                                    | `1Gi`           |
| `resources.requests.cpu` | CPU requests                                     | `500m`          |
| `resources.requests.memory` | Memory requests                               | `512Mi`         |

### Autoscaling Parameters

| Name                                           | Description                                      | Value   |
| ---------------------------------------------- | ------------------------------------------------ | ------- |
| `autoscaling.enabled`                          | Enable autoscaling                               | `true`  |
| `autoscaling.minReplicas`                      | Minimum number of replicas                       | `2`     |
| `autoscaling.maxReplicas`                      | Maximum number of replicas                       | `10`    |
| `autoscaling.targetCPUUtilizationPercentage`   | Target CPU utilization percentage                | `70`    |
| `autoscaling.targetMemoryUtilizationPercentage`| Target memory utilization percentage             | `80`    |

### Beam MCP Server Configuration

| Name                     | Description                                      | Value           |
| ------------------------ | ------------------------------------------------ | --------------- |
| `beamMcp.config`         | Beam MCP Server configuration                    | See `values.yaml` |
| `beamMcp.env`            | Environment variables                            | See `values.yaml` |

### Prometheus and Grafana Parameters

| Name                     | Description                                      | Value           |
| ------------------------ | ------------------------------------------------ | --------------- |
| `prometheus.enabled`     | Enable Prometheus                                | `true`          |
| `grafana.enabled`        | Enable Grafana                                   | `true`          |

## Configuration

### Environment Variables

The following table lists the configurable environment variables of the Beam MCP Server chart and their default values.

| Parameter                 | Description                                      | Default                                                 |
|---------------------------|--------------------------------------------------|----------------------------------------------------------|
| `PORT`                    | Port to listen on                                | `8888`                                                   |
| `CONFIG_PATH`             | Path to config file                              | `/app/config/beam_mcp_config.yaml`                       |
| `GCP_PROJECT_ID`          | Google Cloud project ID                          | `your-gcp-project`                                       |
| `GCP_REGION`              | Google Cloud region                              | `us-central1`                                            |
| `GCP_BUCKET`              | Google Cloud Storage bucket                      | `your-gcp-bucket`                                        |
| `DEFAULT_RUNNER`          | Default Apache Beam runner                       | `dataflow`                                               |
| `ENVIRONMENT`             | Deployment environment                           | `production`                                             |

### Using External Runners

The Beam MCP Server can be configured to use external runners like Flink, Spark, or Dataflow. The configuration for these runners is specified in the `beamMcp.config` section of the `values.yaml` file.

#### Flink

To use an external Flink cluster, update the following values:

```yaml
beamMcp:
  config:
    flink:
      job_manager_url: "http://your-flink-jobmanager:8081"
      parallelism: 4
```

#### Spark

To use an external Spark cluster, update the following values:

```yaml
beamMcp:
  config:
    spark:
      master_url: "spark://your-spark-master:7077"
      deploy_mode: "cluster"
```

#### Dataflow

To use Google Cloud Dataflow, update the following values:

```yaml
beamMcp:
  config:
    dataflow:
      project_id: "your-gcp-project"
      region: "us-central1"
      temp_location: "gs://your-gcp-bucket/temp"
      staging_location: "gs://your-gcp-bucket/staging"
```

## Persistence

The Beam MCP Server itself does not require persistent storage, but the Prometheus and Grafana components do. By default, persistence is enabled for these components.

## Metrics and Monitoring

The Beam MCP Server exposes metrics in Prometheus format at the `/metrics` endpoint. The Prometheus and Grafana components are included in this chart to collect and visualize these metrics.

## Security

The Beam MCP Server is deployed with a non-root user and with a read-only root filesystem. The container runs with user ID 1000 and group ID 1000.

## Troubleshooting

### Pods not starting

If the pods are not starting, check the pod events:

```bash
kubectl -n beam-mcp describe pod -l app.kubernetes.io/name=beam-mcp-server
```

### Service not accessible

If the service is not accessible, check the endpoints:

```bash
kubectl -n beam-mcp get endpoints beam-mcp-server
```

### Configuration issues

If there are configuration issues, check the pod logs:

```bash
kubectl -n beam-mcp logs -l app.kubernetes.io/name=beam-mcp-server
``` 