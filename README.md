# Fleet Telemetry Consumer

This application consumes vehicle telemetry data from Tesla's [Fleet Telemetry project](https://github.com/teslamotors/fleet-telemetry), processes it using Protobuf, and exports metrics to Prometheus. It is intended to be deployed in a Kubernetes environment and can be monitored using Prometheus.

## Features

- **Kafka Consumer**: Consumes messages from a Kafka topic (`tesla_V` by default) containing vehicle telemetry data from Tesla’s Fleet Telemetry project.
- **Protobuf Processing**: Deserializes the Protobuf data defined in Tesla's [vehicle_data.proto](https://github.com/teslamotors/fleet-telemetry/blob/main/protos/vehicle_data.proto) and processes telemetry information such as location, door status, and sensor values.
- **Prometheus Metrics**: Exposes vehicle telemetry data as Prometheus metrics, enabling real-time monitoring.
- **Docker & Kubernetes**: Dockerized and ready for Kubernetes deployment with `Kustomize` support.

## Prerequisites

- **Docker**: Ensure Docker is installed to build and run the application.
- **Kafka**: A running Kafka cluster with the topic `tesla_V`.
- **Prometheus**: To scrape metrics exposed by the app.
- **Kubernetes**: For deployment using `Kustomize`.

## Tesla's Fleet Telemetry Data

The application consumes telemetry data structured according to Tesla's [Protobuf definition](https://github.com/teslamotors/fleet-telemetry/blob/main/protos/vehicle_data.proto). The Protobuf message `Payload` includes fields for various vehicle data such as:

- `LocationValue`: GPS coordinates (latitude, longitude)
- `DoorValue`: The open/closed status of the vehicle’s doors
- `DoubleValue`, `FloatValue`, `IntValue`, etc.: Different sensor values
- `TimeValue`: Timestamp data related to telemetry events

The application extracts and processes these fields, converting them into Prometheus metrics.

## Configuration

The application requires a configuration file in JSON format. This file contains settings for Kafka consumer configuration. By default, the app looks for `config.json`.

### Example `config.json`:
```json
{
  "bootstrap.servers": "localhost:9092",
  "group.id": "fleet-telemetry-consumer",
  "auto.offset.reset": "earliest"
}
```

## Build and Run

### Build Docker Image

To build the Docker image for the consumer:

```bash
docker buildx build --platform linux/amd64 --load -t fleet-telemetry-consumer .
```

### Run with Docker Compose

To start the application using Docker Compose:

```bash
docker compose up --build
```

### Kubernetes Deployment

To deploy the application in a Kubernetes environment using Kustomize, use the following `kustomization.yaml` configuration:

Apply the configuration with:

```bash
kubectl apply -k kustomization
```

This will deploy the fleet telemetry consumer into the Kubernetes cluster.

## Prometheus Metrics

The consumer exposes metrics at `/metrics` on port `2112`. Prometheus can scrape this endpoint to monitor the application. The metrics include various vehicle telemetry data such as:

- Vehicle location
- Door states
- Time and speed metrics
- Boolean and numerical telemetry data

### Sample Prometheus Query:

```promql
vehicle_data{field="Latitude"}
```

## Contributions

Feel free to open issues or submit pull requests if you'd like to contribute!