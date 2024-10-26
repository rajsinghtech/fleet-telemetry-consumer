# Fleet Telemetry Consumer

![Fleet Telemetry Consumer](https://your-image-url.com/logo.png)

## Overview

Fleet Telemetry Consumer is a robust application designed to consume vehicle telemetry data from Tesla's [Fleet Telemetry project](https://github.com/teslamotors/fleet-telemetry). It processes the data using Protobuf and exports metrics to Prometheus, enabling real-time monitoring and analysis. The application is containerized with Docker and optimized for deployment in Kubernetes environments using Kustomize.

## Features

- **Kafka Consumer**: Efficiently consumes messages from a Kafka topic (`tesla_V` by default) containing vehicle telemetry data from Tesla’s Fleet Telemetry project.
- **Protobuf Processing**: Deserializes Protobuf data defined in Tesla's [vehicle_data.proto](https://github.com/teslamotors/fleet-telemetry/blob/main/protos/vehicle_data.proto) to process telemetry information such as location, door status, and various sensor values.
- **Prometheus Metrics**: Exposes vehicle telemetry data as Prometheus metrics, facilitating seamless real-time monitoring.
- **Docker & Kubernetes**: Fully Dockerized for easy containerization and equipped with `Kustomize` support for streamlined Kubernetes deployments.
- **Dashboard Integration**: Includes Grafana dashboards for visualizing telemetry data, enabling comprehensive insights into fleet operations.

## Prerequisites

Before setting up the Fleet Telemetry Consumer, ensure you have the following installed and configured:

- **Docker**: To build and run the application containers.
- **Docker Compose**: For orchestrating multi-container Docker applications.
- **Kubernetes**: For deploying the application in a cluster environment.
- **Kustomize**: To manage Kubernetes configurations.
- **Kafka**: A running Kafka cluster with the topic `tesla_V`.
- **Prometheus**: To scrape and store metrics exposed by the application.
- **Grafana**: For visualizing the metrics collected by Prometheus.

## Tesla's Fleet Telemetry Data

The application consumes telemetry data structured according to Tesla's [Protobuf definition](https://github.com/teslamotors/fleet-telemetry/blob/main/protos/vehicle_data.proto). The Protobuf message `Payload` includes fields for various vehicle data such as:

- `LocationValue`: GPS coordinates (latitude, longitude)
- `DoorValue`: Open/closed status of the vehicle’s doors
- `DoubleValue`, `FloatValue`, `IntValue`, etc.: Different sensor values
- `TimeValue`: Timestamp data related to telemetry events

The application extracts and processes these fields, converting them into Prometheus metrics for monitoring and analysis.

## Configuration

The application requires a configuration file in YAML format. This file contains settings for Kafka consumer configuration and AWS integration. By default, the app looks for `examples/config.yaml`.

### Example `config.yaml`:

```yaml
bootstrap.servers: "localhost:9092"
group.id: "fleet-telemetry-consumer"
auto.offset.reset: "earliest"
aws:
  access_key_id: "YOUR_AWS_ACCESS_KEY_ID"
  secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY"
  bucket:
    name: "ceph-tesla"
    host: "rook-ceph-rgw-my-store.rook-ceph.svc.cluster.local"
    port: 80
    protocol: "http"
    region: "us-east-1"
  enabled: true
```

## Build and Run

### Using Makefile

The project includes a `Makefile` to simplify build and run commands.

#### Build the Docker Image

```bash
make build
```

#### Run the Application with Docker Compose

```bash
make run
```

### Docker Instructions

#### Build Docker Image Manually

```bash
docker buildx build --platform linux/amd64 --load -t fleet-telemetry-consumer .
```

#### Run with Docker Compose

```bash
docker compose up --build
```

### Kubernetes Deployment

Deploy the application in a Kubernetes environment using Kustomize.

#### Setup Kustomization

Ensure your `kustomization.yaml` includes the necessary resources:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - ./fleet-telemetry-consumer
  - ./dashboards
```

#### Apply the Configuration

```bash
kubectl apply -k kustomization/
```

This command will deploy the Fleet Telemetry Consumer into your Kubernetes cluster along with the configured Grafana dashboards.

## Prometheus Metrics

The consumer exposes metrics at `/metrics` on port `2112`. Prometheus can scrape this endpoint to monitor various vehicle telemetry data, including:

- **Vehicle Location**: GPS coordinates (latitude, longitude)
- **Door States**: Open or closed status of each door
- **Time and Speed Metrics**: Timestamps and speed-related data
- **Sensor Data**: Boolean and numerical telemetry values

### Sample Prometheus Query

```promql
vehicle_data{field="Latitude"}
```

## Grafana Dashboards

The application includes pre-configured Grafana dashboards for visualizing telemetry data.

- **Vehicle Locations**: Displays real-time locations of vehicles on a geomap.
- **Odometer**: Shows odometer readings per vehicle.
- **Battery Level**: Monitors battery levels across the fleet.
- **Temperature Readings**: Tracks inside and outside temperatures.
- **Vehicle Speed**: Visualizes speed metrics.
- **Gear Status**: Displays current gear states.
- **Battery Metrics**: Provides detailed battery performance insights.

### Importing Dashboards

Ensure that the dashboards are included in the Kubernetes deployment by verifying the `kustomization/dashboards/vehicle-data.json` file is correctly referenced.

## Examples

### Example Data

Sample telemetry data can be found in `examples/data.json`. This data follows the structure defined by Tesla's Protobuf schema and is used for testing and development purposes.

### Commands

Common commands for managing the application:

```bash
# Ensure dependencies are up to date
go mod tidy

# Run the application with a specific configuration
go run main.go -config examples/config.yaml

# Set environment variables for AWS integration
export AWS_ACCESS_KEY_ID=your_access_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_access_key
export AWS_BUCKET_HOST=rook-ceph-rgw-my-store.rook-ceph.svc.cluster.local
export AWS_BUCKET_NAME=ceph-tesla
export AWS_BUCKET_PORT=80
export AWS_BUCKET_PROTOCOL=http
export AWS_BUCKET_REGION=us-east-1
export AWS_ENABLED=true
```

## Development

### Dependencies

The project manages dependencies using Go Modules. Ensure all dependencies are installed by running:

```bash
go mod download
```

### Dockerfile

The `Dockerfile` is multi-staged to optimize build times and reduce the final image size.

```dockerfile
# syntax=docker/dockerfile:1

FROM golang:1.22.5-bullseye AS build

# Install build dependencies and librdkafka dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    libssl-dev \
    libsasl2-dev \
    libzstd-dev \
    pkg-config \
    liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

# Build and install librdkafka from source
ENV LIBRDKAFKA_VERSION=1.9.2
RUN wget https://github.com/edenhill/librdkafka/archive/refs/tags/v${LIBRDKAFKA_VERSION}.tar.gz && \
    tar -xzf v${LIBRDKAFKA_VERSION}.tar.gz && \
    cd librdkafka-${LIBRDKAFKA_VERSION} && \
    ./configure --prefix=/usr && \
    make && make install && \
    ldconfig

WORKDIR /go/src/fleet-telemetry-consumer

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build with dynamic linking
RUN go build -tags dynamic -o /go/bin/fleet-telemetry-consumer

# Use a minimal base image
FROM debian:bullseye-slim

WORKDIR /

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl1.1 \
    libsasl2-2 \
    libzstd1 \
    liblz4-1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /go/bin/fleet-telemetry-consumer /fleet-telemetry-consumer

ENTRYPOINT ["/fleet-telemetry-consumer"]
```

## Contribution

Contributions are welcome! If you'd like to improve Fleet Telemetry Consumer, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Commit your changes with clear and descriptive messages.
4. Push your branch to your forked repository.
5. Open a pull request detailing your changes.

Please ensure your code adheres to the project's coding standards and passes all tests.

## License

This project is licensed under the [MIT License](LICENSE).

---

*For any questions or support, please open an issue on the [GitHub repository](https://github.com/rajsinghtech/fleet-telemetry-consumer/issues).*