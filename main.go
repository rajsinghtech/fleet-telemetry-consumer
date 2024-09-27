package main

import (
    "bytes"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "math"
    "net/http"
    "os"
    "path/filepath"
    "strconv"
    "time"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/confluentinc/confluent-kafka-go/kafka"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/teslamotors/fleet-telemetry/protos"
    "google.golang.org/protobuf/encoding/protojson"
    "google.golang.org/protobuf/proto"
)

type S3Config struct {
    Endpoint  string `json:"endpoint"`
    Bucket    string `json:"bucket"`
    AccessKey string `json:"accessKey"`
    SecretKey string `json:"secretKey"`
    Region    string `json:"region"`
}

type KafkaConfig struct {
    BootstrapServers string `json:"bootstrap.servers"`
    GroupID          string `json:"group.id"`
    AutoOffsetReset  string `json:"auto.offset.reset"`
    Topic            string `json:"topic"`
}

var s3Svc *s3.S3
var s3Bucket string

func main() {
    configFilePath := flag.String("config", "config.json", "Path to the JSON configuration file")
    flag.Parse()

    // Read and parse configuration
    configFile, err := os.ReadFile(*configFilePath)
    if err != nil {
        log.Fatalf("Failed to read configuration file: %s", err)
    }

    var configMap map[string]interface{}
    if err := json.Unmarshal(configFile, &configMap); err != nil {
        log.Fatalf("Failed to parse configuration file: %s", err)
    }

    // Kafka configuration
    kafkaConfigMap := configMap["kafka"].(map[string]interface{})
    kafkaConfig := kafka.ConfigMap{}
    for key, value := range kafkaConfigMap {
        if key != "topic" {  // Exclude "topic" from KafkaConfigMap
            kafkaConfig[key] = value
        }
    }

    // Get the topic from the config
    topic, ok := kafkaConfigMap["topic"].(string)
    if !ok || topic == "" {
        log.Fatal("Kafka topic not specified in the configuration.")
    }

    // S3 configuration and connection test
    s3Config, s3Enabled := configMap["aws"].(map[string]interface{})
    if s3Enabled {
        log.Println("S3 is enabled, testing S3 connection...")

        s3ConfigStruct := parseS3Config(s3Config)
        s3Svc = configureS3(s3ConfigStruct)
        s3Bucket = s3ConfigStruct.Bucket

        if s3Svc == nil {
            log.Fatal("S3 configuration is invalid. Exiting.")
        }

        if !testS3Connection(s3Svc) {
            log.Fatal("S3 connection failed. Exiting.")
        }

        log.Println("S3 connection test successful.")
    } else {
        log.Println("S3 configuration not found. S3 upload is disabled.")
    }

    // Create Kafka consumer
    consumer, err := kafka.NewConsumer(&kafkaConfig)
    if err != nil {
        log.Fatalf("Failed to create consumer: %s", err)
    }
    defer consumer.Close()

    // Get topics and log them in one line
    metadata, err := consumer.GetMetadata(nil, true, 10000)
    if err != nil {
        log.Fatalf("Failed to get metadata: %s", err)
    }

    topics := make([]string, 0, len(metadata.Topics))
    for t := range metadata.Topics {
        topics = append(topics, t)
    }

    log.Printf("Available topics: %s", topics)
    log.Printf("Selected topic: %s", topic)

    // Subscribe to the Kafka topic
    err = consumer.Subscribe(topic, nil)
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

        // Process each Datum in the Payload
        for _, datum := range vehicleData.Data {
            fieldName := datum.Key.String() // Get the field name from the enum
            value := datum.Value
            processValue(fieldName, value, vehicleDataGauge, vehicleData.Vin)
        }

        // Upload to S3 (if enabled)
        if s3Svc != nil {
            err = uploadToS3(s3Svc, s3Bucket, vehicleDataJSON)
            if err != nil {
                log.Printf("Failed to upload vehicle data to S3: %v", err)
            }
        }
    }
}

// (Rest of the functions stay the same: parseS3Config, configureS3, testS3Connection, uploadToS3, processValue, handle*Value functions)

// Parse the S3 configuration from the map
func parseS3Config(s3Config map[string]interface{}) *S3Config {
    return &S3Config{
        Endpoint:  s3Config["endpoint"].(string),
        Bucket:    s3Config["bucket"].(string),
        AccessKey: s3Config["accessKey"].(string),
        SecretKey: s3Config["secretKey"].(string),
        Region:    s3Config["region"].(string),
    }
}

// Configure S3 client
func configureS3(s3Config *S3Config) *s3.S3 {
    if s3Config == nil || s3Config.Endpoint == "" || s3Config.Bucket == "" ||
        s3Config.AccessKey == "" || s3Config.SecretKey == "" || s3Config.Region == "" {
        log.Println("S3 configuration is incomplete.")
        return nil
    }

    sess, err := session.NewSession(&aws.Config{
        Region:      aws.String(s3Config.Region),
        Credentials: credentials.NewStaticCredentials(s3Config.AccessKey, s3Config.SecretKey, ""),
        Endpoint:    aws.String(s3Config.Endpoint),
    })
    if err != nil {
        log.Printf("Failed to create AWS session: %v", err)
        return nil
    }

    return s3.New(sess)
}

// Test the S3 connection by making a simple ListBuckets request
func testS3Connection(s3Svc *s3.S3) bool {
    _, err := s3Svc.ListBuckets(&s3.ListBucketsInput{})
    if err != nil {
        log.Printf("Failed to connect to S3: %v", err)
        return false
    }
    return true
}
// Example upload function (not used in connection test)
func uploadToS3(s3Svc *s3.S3, bucket string, data []byte) error {
	timestamp := time.Now().Format("2006/01/02/15-04-05")
	key := filepath.Join(timestamp, "data.json")

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}

	_, err := s3Svc.PutObject(input)
	if err != nil {
		log.Printf("Failed to upload file to S3: %v", err)
		return err
	}
	log.Printf("Successfully uploaded data to S3 at key: %s", key)
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
