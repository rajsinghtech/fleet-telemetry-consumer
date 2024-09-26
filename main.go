package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net/http"
    "os"
    "strconv"

    "github.com/confluentinc/confluent-kafka-go/kafka"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/teslamotors/fleet-telemetry/protos"
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

        // Print the VIN
        fmt.Printf("VIN: %s\n", vehicleData.Vin)
        // Process each Datum in the Payload
        for _, datum := range vehicleData.Data {
            fieldName := datum.Key.String() // Get the field name from the enum
            value := datum.Value
            fmt.Printf("Field Name: %s, Value: %v\n", fieldName, value)
            switch v := value.Value.(type) {
            case *protos.Value_DoubleValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(v.DoubleValue)
            case *protos.Value_FloatValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.FloatValue))
            case *protos.Value_IntValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.IntValue))
            case *protos.Value_LongValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.LongValue))
            case *protos.Value_BooleanValue:
                var numericValue float64
                if v.BooleanValue {
                    numericValue = 1.0
                } else {
                    numericValue = 0.0
                }
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(numericValue)
            case *protos.Value_StringValue:
                // Try to parse the string value as a float64
                floatVal, err := strconv.ParseFloat(v.StringValue, 64)
                if err == nil {
                    vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(floatVal)
                } else {
                    // Handle non-numeric string values
                    vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(0) // Placeholder value
                    log.Printf("Received non-numeric string value for field %s: %s", fieldName, v.StringValue)
                }
            case *protos.Value_Invalid:
                // Handle invalid value
                log.Printf("Received invalid value for field %s", fieldName)
            case *protos.Value_LocationValue:
                // Handle LocationValue separately
                vehicleDataGauge.WithLabelValues("Latitude", vehicleData.Vin).Set(v.LocationValue.Latitude)
                vehicleDataGauge.WithLabelValues("Longitude", vehicleData.Vin).Set(v.LocationValue.Longitude)
            case *protos.Value_DoorValue:
                // Handle Doors by setting individual door states
                doors := v.DoorValue
                doorFields := map[string]bool{
                    "DriverFrontDoor":    doors.DriverFront,
                    "PassengerFrontDoor": doors.PassengerFront,
                    "DriverRearDoor":     doors.DriverRear,
                    "PassengerRearDoor":  doors.PassengerRear,
                    "TrunkFront":         doors.TrunkFront,
                    "TrunkRear":          doors.TrunkRear,
                }
                for doorName, state := range doorFields {
                    var numericValue float64
                    if state {
                        numericValue = 1.0
                    } else {
                        numericValue = 0.0
                    }
                    vehicleDataGauge.WithLabelValues(doorName, vehicleData.Vin).Set(numericValue)
                }
            case *protos.Value_TimeValue:
                // Handle TimeValue by converting to seconds since midnight
                timeValue := v.TimeValue
                totalSeconds := float64(timeValue.Hour*3600 + timeValue.Minute*60 + timeValue.Second)
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(totalSeconds)
            // Handle all enum types
            case *protos.Value_ChargingValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.ChargingValue.Number()))
            case *protos.Value_ShiftStateValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.ShiftStateValue.Number()))
            case *protos.Value_LaneAssistLevelValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.LaneAssistLevelValue.Number()))
            case *protos.Value_ScheduledChargingModeValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.ScheduledChargingModeValue.Number()))
            case *protos.Value_SentryModeStateValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.SentryModeStateValue.Number()))
            case *protos.Value_SpeedAssistLevelValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.SpeedAssistLevelValue.Number()))
            case *protos.Value_BmsStateValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.BmsStateValue.Number()))
            case *protos.Value_BuckleStatusValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.BuckleStatusValue.Number()))
            case *protos.Value_CarTypeValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.CarTypeValue.Number()))
            case *protos.Value_ChargePortValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.ChargePortValue.Number()))
            case *protos.Value_ChargePortLatchValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.ChargePortLatchValue.Number()))
            case *protos.Value_CruiseStateValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.CruiseStateValue.Number()))
            case *protos.Value_DriveInverterStateValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.DriveInverterStateValue.Number()))
            case *protos.Value_HvilStatusValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.HvilStatusValue.Number()))
            case *protos.Value_WindowStateValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.WindowStateValue.Number()))
            case *protos.Value_SeatFoldPositionValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.SeatFoldPositionValue.Number()))
            case *protos.Value_TractorAirStatusValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.TractorAirStatusValue.Number()))
            case *protos.Value_TrailerAirStatusValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.TrailerAirStatusValue.Number()))
            case *protos.Value_FollowDistanceValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.FollowDistanceValue.Number()))
            case *protos.Value_ForwardCollisionSensitivityValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.ForwardCollisionSensitivityValue.Number()))
            case *protos.Value_GuestModeMobileAccessValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.GuestModeMobileAccessValue.Number()))
            case *protos.Value_DetailedChargeStateValue:
                vehicleDataGauge.WithLabelValues(fieldName, vehicleData.Vin).Set(float64(v.DetailedChargeStateValue.Number()))
            default:
                // Log unhandled types
                log.Printf("Unhandled value type for field %s", fieldName)
                continue
            }
        }
    }
}