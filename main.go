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
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/teslamotors/fleet-telemetry/protos"
)

// Config holds the entire configuration structure
type Config struct {
	Kafka KafkaConfig `json:"kafka"`
	AWS   *S3Config   `json:"aws,omitempty"`
}

// S3Config holds AWS S3 configuration
type S3Config struct {
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	Region    string `json:"region"`
}

// KafkaConfig holds Kafka consumer configuration
type KafkaConfig struct {
	BootstrapServers string `json:"bootstrap.servers"`
	GroupID          string `json:"group.id"`
	AutoOffsetReset  string `json:"auto.offset.reset"`
	Topic            string `json:"topic"`
}

// Service encapsulates the application's dependencies
type Service struct {
	Config          Config
	S3Client        *s3.S3
	KafkaConsumer   *kafka.Consumer
	PrometheusGauge *prometheus.GaugeVec
}

// NewService initializes the service with configurations
func NewService(cfg Config) (*Service, error) {
	service := &Service{
		Config: cfg,
	}

	// Initialize AWS S3 if configuration is provided
	if service.Config.AWS != nil {
		s3Client, err := configureS3(service.Config.AWS)
		if err != nil {
			return nil, fmt.Errorf("failed to configure S3: %w", err)
		}
		service.S3Client = s3Client

		if err := testS3Connection(s3Client); err != nil {
			return nil, fmt.Errorf("S3 connection test failed: %w", err)
		}
		log.Println("S3 connection established successfully.")
	} else {
		log.Println("AWS S3 configuration not provided. S3 uploads are disabled.")
	}

	// Initialize Kafka consumer
	consumer, err := configureKafka(service.Config.Kafka)
	if err != nil {
		return nil, fmt.Errorf("failed to configure Kafka consumer: %w", err)
	}
	service.KafkaConsumer = consumer

	// Initialize Prometheus metrics
	service.PrometheusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vehicle_data",
			Help: "Vehicle data metrics",
		},
		[]string{"field", "vin"},
	)
	prometheus.MustRegister(service.PrometheusGauge)

	return service, nil
}

// configureS3 sets up the AWS S3 client
func configureS3(s3Config *S3Config) (*s3.S3, error) {
	if err := validateS3Config(s3Config); err != nil {
		return nil, err
	}

	sess, err := session.NewSession(&aws.Config{
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String(s3Config.Region),
		Credentials:     credentials.NewStaticCredentials(s3Config.AccessKey, s3Config.SecretKey, ""),
		Endpoint:         aws.String(s3Config.Endpoint),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create AWS session: %w", err)
	}

	return s3.New(sess), nil
}

// validateS3Config ensures all required S3 configurations are present
func validateS3Config(cfg *S3Config) error {
	if cfg.Endpoint == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" || cfg.Region == "" {
		return fmt.Errorf("incomplete S3 configuration")
	}
	return nil
}

// testS3Connection verifies the connection to S3 by listing buckets
func testS3Connection(s3Svc *s3.S3) error {
	_, err := s3Svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return fmt.Errorf("failed to list S3 buckets: %w", err)
	}
	return nil
}

// configureKafka sets up the Kafka consumer
func configureKafka(kafkaCfg KafkaConfig) (*kafka.Consumer, error) {
	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  kafkaCfg.BootstrapServers,
		"group.id":           kafkaCfg.GroupID,
		"auto.offset.reset":  kafkaCfg.AutoOffsetReset,
		"enable.auto.commit": false, // Manual commit for better control
	}

	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create Kafka consumer: %w", err)
	}

	// Subscribe to the specified topic
	if err := consumer.SubscribeTopics([]string{kafkaCfg.Topic}, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe to Kafka topic '%s': %w", kafkaCfg.Topic, err)
	}

	return consumer, nil
}

// loadConfig reads and unmarshals the configuration file
func loadConfig(path string) (Config, error) {
	var cfg Config

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("error reading config file '%s': %w", path, err)
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("error unmarshalling config file: %w", err)
	}

	// Validate Kafka configuration
	if cfg.Kafka.BootstrapServers == "" || cfg.Kafka.GroupID == "" || cfg.Kafka.AutoOffsetReset == "" || cfg.Kafka.Topic == "" {
		return cfg, fmt.Errorf("incomplete Kafka configuration")
	}

	return cfg, nil
}

// uploadToS3 uploads data to the specified S3 bucket with a timestamped key
func uploadToS3(s3Svc *s3.S3, bucket string, data []byte) error {
	// Generate current time in UTC with microsecond precision
	now := time.Now().UTC()
	timestamp := now.Format("20060102T150405.000000Z") // Format: YYYYMMDDTHHMMSS.microsecondsZ

	// Define key structure based on the timestamp
	// Example: 2024/04/27/15/30/45/20240427T153045.123456Z.json
	key := fmt.Sprintf("%04d/%02d/%02d/%s.json",
		now.Year(),
		now.Month(),
		now.Day(),
		timestamp,
	)

	// Prepare the S3 PutObject input
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}

	// Upload the object to S3
	_, err := s3Svc.PutObject(input)
	if err != nil {
		return fmt.Errorf("failed to upload to S3 at key '%s': %w", key, err)
	}

	log.Printf("Successfully uploaded data to S3 at key: %s", key)
	return nil
}

// PayloadJSON represents the structure of the JSON payload
type PayloadJSON struct {
	Data      []struct {
		Key   string                 `json:"key"`
		Value map[string]interface{} `json:"value"`
	} `json:"data"`
	CreatedAt string `json:"createdAt"`
	Vin       string `json:"vin"`
}

// processValue handles different types of JSON values and updates Prometheus metrics
func processValue(fieldName string, value map[string]interface{}, gauge *prometheus.GaugeVec, vin string) {
	handled := false

	if v, ok := value["doubleValue"].(float64); ok {
		gauge.WithLabelValues(fieldName, vin).Set(v)
		handled = true
	}

	if v, ok := value["floatValue"].(float64); ok {
		gauge.WithLabelValues(fieldName, vin).Set(v)
		handled = true
	}

	if v, ok := value["intValue"].(float64); ok {
		gauge.WithLabelValues(fieldName, vin).Set(v)
		handled = true
	}

	if v, ok := value["longValue"].(float64); ok {
		gauge.WithLabelValues(fieldName, vin).Set(v)
		handled = true
	}

	if v, ok := value["booleanValue"].(bool); ok {
		numericValue := boolToFloat64(v)
		gauge.WithLabelValues(fieldName, vin).Set(numericValue)
		handled = true
	}

	if v, ok := value["stringValue"].(string); ok {
		handleStringValue(v, fieldName, gauge, vin)
		handled = true
	}

	if _, ok := value["invalid"].(bool); ok {
		log.Printf("Invalid value received for field '%s', setting as NaN", fieldName)
		gauge.WithLabelValues(fieldName, vin).Set(math.NaN())
		handled = true
	}

	if loc, ok := value["locationValue"].(map[string]interface{}); ok {
		if latitude, latOk := loc["latitude"].(float64); latOk {
			gauge.WithLabelValues("Latitude", vin).Set(latitude)
		}
		if longitude, lonOk := loc["longitude"].(float64); lonOk {
			gauge.WithLabelValues("Longitude", vin).Set(longitude)
		}
		handled = true
	}

	if doors, ok := value["doorValue"].(map[string]interface{}); ok {
		handleDoorValuesJSON(doors, gauge, vin)
		handled = true
	}

	if timeVal, ok := value["timeValue"].(map[string]interface{}); ok {
		hour, _ := getIntFromMap(timeVal, "hour")
		minute, _ := getIntFromMap(timeVal, "minute")
		second, _ := getIntFromMap(timeVal, "second")
		totalSeconds := float64(hour*3600 + minute*60 + second)
		gauge.WithLabelValues(fieldName, vin).Set(totalSeconds)
		handled = true
	}

	if !handled {
		log.Printf("Unhandled value type for field '%s': %v", fieldName, value)
	}
}

// boolToFloat64 converts a boolean to float64 (1.0 for true, 0.0 for false)
func boolToFloat64(value bool) float64 {
	if value {
		return 1.0
	}
	return 0.0
}

// handleStringValue processes string values, attempting to parse them as floats
func handleStringValue(stringValue, fieldName string, gauge *prometheus.GaugeVec, vin string) {
	if stringValue == "<invalid>" || stringValue == "\u003cinvalid\u003e" {
		log.Printf("Invalid string value received for field '%s', setting as NaN", fieldName)
		gauge.WithLabelValues(fieldName, vin).Set(math.NaN())
		return
	}

	floatVal, err := strconv.ParseFloat(stringValue, 64)
	if err == nil {
		gauge.WithLabelValues(fieldName, vin).Set(floatVal)
	} else {
		log.Printf("Non-numeric string value received for field '%s': '%s', setting as NaN", fieldName, stringValue)
		gauge.WithLabelValues(fieldName, vin).Set(math.NaN())
	}
}

// handleDoorValuesJSON processes door states from JSON and updates Prometheus metrics
func handleDoorValuesJSON(doors map[string]interface{}, gauge *prometheus.GaugeVec, vin string) {
	doorFields := map[string]bool{}

	for key, val := range doors {
		if boolVal, ok := val.(bool); ok {
			doorFields[key] = boolVal
		}
	}

	for doorName, state := range doorFields {
		numericValue := boolToFloat64(state)
		gauge.WithLabelValues(doorName, vin).Set(numericValue)
	}
}

// getIntFromMap retrieves an integer from a map with default value
func getIntFromMap(m map[string]interface{}, key string) (int, bool) {
	if val, ok := m[key].(float64); ok {
		return int(val), true
	}
	return 0, false
}

// startPrometheusServer launches the Prometheus metrics HTTP server
func startPrometheusServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	log.Printf("Starting Prometheus metrics server at %s/metrics", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Prometheus HTTP server failed: %v", err)
	}
}

// main is the entry point of the application
func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.json", "Path to the JSON configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	// Initialize service
	service, err := NewService(cfg)
	if err != nil {
		log.Fatalf("Service initialization error: %v", err)
	}

	// Start Prometheus metrics server
	go startPrometheusServer(":2112")

	// Begin consuming Kafka messages
	log.Println("Starting Kafka message consumption...")
	for {
		msg, err := service.KafkaConsumer.ReadMessage(-1)
		if err != nil {
			// Handle Kafka consumer errors
			if kafkaError, ok := err.(kafka.Error); ok && kafkaError.Code() == kafka.ErrAllBrokersDown {
				log.Printf("Kafka broker is down: %v", err)
				time.Sleep(5 * time.Second) // Wait before retrying
				continue
			}
			log.Printf("Error while consuming message: %v", err)
			continue
		}

		// Deserialize the Protobuf message
		vehicleData := &protos.Payload{}
		if err := proto.Unmarshal(msg.Value, vehicleData); err != nil {
			log.Printf("Failed to unmarshal Protobuf message: %v", err)
			continue
		}

		// Marshal vehicleData to JSON
		vehicleDataJSON, err := protojson.Marshal(vehicleData)
		if err != nil {
			log.Printf("Failed to marshal vehicleData to JSON: %v", err)
			continue
		}

		log.Printf("Received Vehicle Data: %s", string(vehicleDataJSON))

		// Unmarshal JSON into PayloadJSON
		var payload PayloadJSON
		if err := json.Unmarshal(vehicleDataJSON, &payload); err != nil {
			log.Printf("Failed to unmarshal vehicleData JSON: %v", err)
			continue
		}

		// Process each Datum in the Payload
		for _, datum := range payload.Data {
			fieldName := datum.Key
			value := datum.Value
			processValue(fieldName, value, service.PrometheusGauge, payload.Vin)
		}

		// Upload to S3 if enabled
		if service.S3Client != nil {
			if err := uploadToS3(service.S3Client, service.Config.AWS.Bucket, vehicleDataJSON); err != nil {
				log.Printf("Failed to upload vehicle data to S3: %v", err)
			}
		}

		// Commit the message offset after successful processing
		if _, err := service.KafkaConsumer.CommitMessage(msg); err != nil {
			log.Printf("Failed to commit Kafka message: %v", err)
		}
	}
}