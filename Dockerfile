FROM golang:1.22.5-bullseye as build
WORKDIR /go/src/fleet-telemetry-consumer
COPY . .
RUN go build -o /go/bin/fleet-telemetry-consumer

FROM gcr.io/distroless/cc-debian11
WORKDIR /
COPY --from=build /go/bin/fleet-telemetry-consumer /
CMD ["/fleet-telemetry-consumer", "-config", "/etc/fleet-telemetry-consumer/config.json"]