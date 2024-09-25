package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "os"
    "log"
    "net/http"
    "github.com/confluentinc/confluent-kafka-go/kafka"
    "github.com/teslamotors/fleet-telemetry/protos"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "google.golang.org/protobuf/proto"
)

func main() {
    // Define a command-line flag for the configuration file path
    configFilePath := flag.String("config", "config.json", "Path to the JSON configuration file")
    flag.Parse()

    // Read the configuration file
    configFile, err := os.ReadFile(*configFilePath)
    if err != nil {
        log.Fatalf("Failed to read configuration file: %s", err)
    }

    // Parse the JSON configuration file
    var configMap map[string]interface{}
    if err := json.Unmarshal(configFile, &configMap); err != nil {
        log.Fatalf("Failed to parse configuration file: %s", err)
    }

    // Convert the map to kafka.ConfigMap
    config := kafka.ConfigMap{}
    for key, value := range configMap {
        config[key] = value
    }

    // Create the consumer
    consumer, err := kafka.NewConsumer(&config)
    if err != nil {
        log.Fatalf("Failed to create consumer: %s", err)
    }
    defer consumer.Close()

    // Get topics
    metadata, err := consumer.GetMetadata(nil, true, 10000)
    if err != nil {
        log.Fatalf("Failed to get metadata: %s", err)
    }

    fmt.Println("Available topics:")
    for topic := range metadata.Topics {
        fmt.Println(topic)
    }

    // Subscribe to the topic
    err = consumer.Subscribe("tesla_V", nil)
    if err != nil {
        log.Fatalf("Failed to subscribe to topic: %s", err)
    }

    // Initialize Prometheus metrics
    vehicleDataGauge := prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "vehicle_data",
            Help: "Vehicle data metrics",
        },
        []string{"field", "vin"},
    )

    // Register the metric
    prometheus.MustRegister(vehicleDataGauge)

    // Start HTTP server for Prometheus metrics
    go func() {
        http.Handle("/metrics", promhttp.Handler())
        log.Fatal(http.ListenAndServe(":2112", nil))
    }()

    // Consume messages
    fmt.Println("Waiting for messages...")

    for {
        msg, err := consumer.ReadMessage(-1)
        if err != nil {
            // Handle errors
            log.Printf("Error while consuming: %v\n", err)
            continue
        }

        // Deserialize the Protobuf message
        vehicleData := &protos.Payload{}
        if err := proto.Unmarshal(msg.Value, vehicleData); err != nil {
            log.Printf("Failed to unmarshal Protobuf message: %v\n", err)
            continue
        }

        // Process each Datum in the Payload
        for _, datum := range vehicleData.Data {
            fieldName := datum.Key.String() // Get the field name from the enum
            value := datum.Value

            var numericValue float64

            switch v := value.Value.(type) {
            case *protos.Value_DoubleValue:
                numericValue = v.DoubleValue
            case *protos.Value_FloatValue:
                numericValue = float64(v.FloatValue)
            case *protos.Value_IntValue:
                numericValue = float64(v.IntValue)
            case *protos.Value_LongValue:
                numericValue = float64(v.LongValue)
            case *protos.Value_BooleanValue:
                if v.BooleanValue {
                    numericValue = 1.0
                } else {
                    numericValue = 0.0
                }
            case *protos.Value_ChargingValue:
                numericValue = float64(v.ChargingValue.Number())
            case *protos.Value_ShiftStateValue:
                numericValue = float64(v.ShiftStateValue.Number())
            case *protos.Value_LaneAssistLevelValue:
                numericValue = float64(v.LaneAssistLevelValue.Number())
            case *protos.Value_ScheduledChargingModeValue:
                numericValue = float64(v.ScheduledChargingModeValue.Number())
            case *protos.Value_SentryModeStateValue:
                numericValue = float64(v.SentryModeStateValue.Number())
            case *protos.Value_SpeedAssistLevelValue:
                numericValue = float64(v.SpeedAssistLevelValue.Number())
            case *protos.Value_BmsStateValue:
                numericValue = float64(v.BmsStateValue.Number())
            case *protos.Value_BuckleStatusValue:
                numericValue = float64(v.BuckleStatusValue.Number())
            case *protos.Value_CarTypeValue:
                numericValue = float64(v.CarTypeValue.Number())
            case *protos.Value_ChargePortValue:
                numericValue = float64(v.ChargePortValue.Number())
            case *protos.Value_ChargePortLatchValue:
                numericValue = float64(v.ChargePortLatchValue.Number())
            case *protos.Value_CruiseStateValue:
                numericValue = float64(v.CruiseStateValue.Number())
            case *protos.Value_DriveInverterStateValue:
                numericValue = float64(v.DriveInverterStateValue.Number())
            case *protos.Value_HvilStatusValue:
                numericValue = float64(v.HvilStatusValue.Number())
            case *protos.Value_WindowStateValue:
                numericValue = float64(v.WindowStateValue.Number())
            case *protos.Value_SeatFoldPositionValue:
                numericValue = float64(v.SeatFoldPositionValue.Number())
            case *protos.Value_TractorAirStatusValue:
                numericValue = float64(v.TractorAirStatusValue.Number())
            case *protos.Value_FollowDistanceValue:
                numericValue = float64(v.FollowDistanceValue.Number())
            case *protos.Value_ForwardCollisionSensitivityValue:
                numericValue = float64(v.ForwardCollisionSensitivityValue.Number())
            case *protos.Value_GuestModeMobileAccessValue:
                numericValue = float64(v.GuestModeMobileAccessValue.Number())
            case *protos.Value_TrailerAirStatusValue:
                numericValue = float64(v.TrailerAirStatusValue.Number())
            case *protos.Value_DetailedChargeStateValue:
                numericValue = float64(v.DetailedChargeStateValue.Number())
            case *protos.Value_LocationValue:
                // Handle LocationValue separately
                lat := v.LocationValue.Latitude
                lon := v.LocationValue.Longitude
                vehicleDataGauge.WithLabelValues(fieldName+"_latitude", vehicleData.Vin).Set(lat)
                vehicleDataGauge.WithLabelValues(fieldName+"_longitude", vehicleData.Vin).Set(lon)
                continue // Skip the rest since we've handled this case
            default:
                // Skip non-numeric or unsupported types
                continue
            }

            // Update the metric
            vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(numericValue)
        }
    }
}