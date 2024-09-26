package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "math"
    "net/http"
    "os"
    "strconv"

    "github.com/confluentinc/confluent-kafka-go/kafka"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/teslamotors/fleet-telemetry/protos"
    "google.golang.org/protobuf/encoding/protojson"
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

        // Marshal vehicleData to JSON
        vehicleDataJSON, err := protojson.Marshal(vehicleData)
        if err != nil {
            log.Printf("Failed to marshal vehicleData to JSON: %v\n", err)
            continue
        }
        fmt.Println(string(vehicleDataJSON))

        // Process each Datum in the Payload
        for _, datum := range vehicleData.Data {
            fieldName := datum.Key.String() // Get the field name from the enum
            value := datum.Value
            processValue(fieldName, value, vehicleDataGauge, vehicleData.Vin)
        }
    }
}

func processValue(fieldName string, value *protos.Value, vehicleDataGauge *prometheus.GaugeVec, vin string) {
    // Process each value type
    switch v := value.Value.(type) {
    case *protos.Value_DoubleValue:
        vehicleDataGauge.WithLabelValues(fieldName, vin).Set(v.DoubleValue)
    case *protos.Value_FloatValue:
        vehicleDataGauge.WithLabelValues(fieldName, vin).Set(float64(v.FloatValue))
    case *protos.Value_IntValue:
        vehicleDataGauge.WithLabelValues(fieldName, vin).Set(float64(v.IntValue))
    case *protos.Value_LongValue:
        vehicleDataGauge.WithLabelValues(fieldName, vin).Set(float64(v.LongValue))
    case *protos.Value_BooleanValue:
        handleBooleanValue(v.BooleanValue, fieldName, vehicleDataGauge, vin)
    case *protos.Value_StringValue:
        handleStringValue(v.StringValue, fieldName, vehicleDataGauge, vin)
    case *protos.Value_Invalid:
        log.Printf("Invalid value received for field %s, setting as NaN", fieldName)
        vehicleDataGauge.WithLabelValues(fieldName, vin).Set(math.NaN())
    case *protos.Value_LocationValue:
        handleLocationValue(v.LocationValue, fieldName, vehicleDataGauge, vin)
    case *protos.Value_DoorValue:
        handleDoorValues(v.DoorValue, vehicleDataGauge, vin)
    case *protos.Value_TimeValue:
        handleTimeValue(v.TimeValue, fieldName, vehicleDataGauge, vin)
    default:
        log.Printf("Unhandled value type for field %s", fieldName)
    }
}

func handleBooleanValue(booleanValue bool, fieldName string, vehicleDataGauge *prometheus.GaugeVec, vin string) {
    var numericValue float64
    if booleanValue {
        numericValue = 1.0
    } else {
        numericValue = 0.0
    }
    vehicleDataGauge.WithLabelValues(fieldName, vin).Set(numericValue)
}

func handleStringValue(stringValue, fieldName string, vehicleDataGauge *prometheus.GaugeVec, vin string) {
    // Handle invalid string values more gracefully
    if stringValue == "<invalid>" || stringValue == "\u003cinvalid\u003e" {
        log.Printf("Invalid string value received for field %s, setting as NaN", fieldName)
        vehicleDataGauge.WithLabelValues(fieldName, vin).Set(math.NaN())
        return
    }

    // Try to parse the string value as a float64
    floatVal, err := strconv.ParseFloat(stringValue, 64)
    if err == nil {
        vehicleDataGauge.WithLabelValues(fieldName, vin).Set(floatVal)
    } else {
        log.Printf("Non-numeric string value received for field %s: %s, setting as NaN", fieldName, stringValue)
        vehicleDataGauge.WithLabelValues(fieldName, vin).Set(math.NaN())
    }
}

func handleLocationValue(locationValue *protos.LocationValue, fieldName string, vehicleDataGauge *prometheus.GaugeVec, vin string) {
    vehicleDataGauge.WithLabelValues("Latitude", vin).Set(locationValue.Latitude)
    vehicleDataGauge.WithLabelValues("Longitude", vin).Set(locationValue.Longitude)
}

func handleDoorValues(doors *protos.Doors, vehicleDataGauge *prometheus.GaugeVec, vin string) {
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
        vehicleDataGauge.WithLabelValues(doorName, vin).Set(numericValue)
    }
}

func handleTimeValue(timeValue *protos.Time, fieldName string, vehicleDataGauge *prometheus.GaugeVec, vin string) {
    totalSeconds := float64(timeValue.Hour*3600 + timeValue.Minute*60 + timeValue.Second)
    vehicleDataGauge.WithLabelValues(fieldName, vin).Set(totalSeconds)
}