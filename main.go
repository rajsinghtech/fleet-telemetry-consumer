package main

import (
    "bytes"
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "math"
    "net/http"
    "os"

    "strconv"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/confluentinc/confluent-kafka-go/kafka"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/teslamotors/fleet-telemetry/protos"
    "google.golang.org/protobuf/encoding/protojson"
    "google.golang.org/protobuf/proto"
)

// Define an S3 client and config
var s3Client *s3.Client
var s3Enabled bool
var s3Bucket string

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
    var configData map[string]interface{}
    if err := json.Unmarshal(configFile, &configData); err != nil {
        log.Fatalf("Failed to parse configuration file: %s", err)
    }

    // Load Kafka configuration
    kafkaConfig, ok := configData["kafka"].(map[string]interface{})
    if !ok {
        log.Fatalf("Kafka configuration missing or incorrect format")
    }

    // Convert the map to kafka.ConfigMap
    kafkaCfgMap := kafka.ConfigMap{}
    for key, value := range kafkaConfig {
        kafkaCfgMap[key] = value
    }

    // Check for AWS configuration
    awsConfig, awsOk := configData["aws"].(map[string]interface{})
    if awsOk {
        if bucket, exists := awsConfig["s3Bucket"].(string); exists && bucket != "" {
            s3Bucket = bucket
            initS3Client() // Initialize AWS S3 client if config exists
        } else {
            log.Println("S3 bucket is not configured correctly. S3 uploads will be disabled.")
            s3Enabled = false
        }
    } else {
        log.Println("AWS configuration missing. S3 uploads are not enabled.")
        s3Enabled = false
    }

    // Create the Kafka consumer
    consumer, err := kafka.NewConsumer(&kafkaCfgMap)
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

        // If S3 is enabled, upload to S3
        if s3Enabled {
            err = uploadToS3(vehicleData.Vin, vehicleDataJSON)
            if err != nil {
                log.Printf("Failed to upload data to S3: %v\n", err)
            }
        }

        // Process each Datum in the Payload
        for _, datum := range vehicleData.Data {
            fieldName := datum.Key.String() // Get the field name from the enum
            value := datum.Value
            processValue(fieldName, value, vehicleDataGauge, vehicleData.Vin)
        }
    }
}

// initS3Client initializes the S3 client with AWS SDK
func initS3Client() {
    cfg, err := config.LoadDefaultConfig(context.TODO())
    if err != nil {
        log.Fatalf("Unable to load AWS SDK config: %v", err)
    }
    s3Client = s3.NewFromConfig(cfg)
    s3Enabled = true
    log.Println("S3 uploads are enabled.")
}

// uploadToS3 uploads vehicle data to the specified S3 bucket
func uploadToS3(vin string, data []byte) error {
    key := fmt.Sprintf("vehicle-data/%s/%s.json", vin, time.Now().Format("2006-01-02T15-04-05"))

    input := &s3.PutObjectInput{
        Bucket: aws.String(s3Bucket),
        Key:    aws.String(key),
        Body:   bytes.NewReader(data),
        ContentType: aws.String("application/json"),
    }

    _, err := s3Client.PutObject(context.TODO(), input)
    if err != nil {
        return fmt.Errorf("unable to upload data to S3: %w", err)
    }

    log.Printf("Successfully uploaded data to S3: s3://%s/%s", s3Bucket, key)
    return nil
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