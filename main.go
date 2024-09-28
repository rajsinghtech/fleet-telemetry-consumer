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
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/encoding/protojson"
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
	Config              Config
	S3Client            *s3.S3
	KafkaConsumer       *kafka.Consumer
	PrometheusGauge     *prometheus.GaugeVec
	PrometheusLatitude  *prometheus.GaugeVec
	PrometheusLongitude *prometheus.GaugeVec
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

	service.PrometheusLatitude = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vehicle_data_latitude",
			Help: "Vehicle latitude metrics",
		},
		[]string{"field", "vin"},
	)
	prometheus.MustRegister(service.PrometheusLatitude)

	service.PrometheusLongitude = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vehicle_data_longitude",
			Help: "Vehicle longitude metrics",
		},
		[]string{"field", "vin"},
	)
	prometheus.MustRegister(service.PrometheusLongitude)

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

// processValue handles different types of Protobuf values and updates Prometheus metrics
func processValue(datum *protos.Datum, service *Service, vin string) {
	fieldName := datum.Key.String()

	switch v := datum.Value.Value.(type) {
	case *protos.Value_StringValue:
		handleStringValue(v.StringValue, fieldName, service, vin)
	case *protos.Value_IntValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.IntValue))
	case *protos.Value_LongValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.LongValue))
	case *protos.Value_FloatValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.FloatValue))
	case *protos.Value_DoubleValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(v.DoubleValue)
	case *protos.Value_BooleanValue:
		numericValue := boolToFloat64(v.BooleanValue)
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(numericValue)
	case *protos.Value_LocationValue:
		// Update separate Latitude and Longitude metrics with the field name as a label
		service.PrometheusLatitude.WithLabelValues(fieldName, vin).Set(v.LocationValue.Latitude)
		service.PrometheusLongitude.WithLabelValues(fieldName, vin).Set(v.LocationValue.Longitude)
	case *protos.Value_DoorValue:
		handleDoorValues(v.DoorValue, service.PrometheusGauge, vin)
	case *protos.Value_TimeValue:
		totalSeconds := float64(v.TimeValue.Hour*3600 + v.TimeValue.Minute*60 + v.TimeValue.Second)
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(totalSeconds)
	// Handle enums by setting their integer values
	case *protos.Value_ChargingValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.ChargingValue))
	case *protos.Value_ShiftStateValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.ShiftStateValue))
	case *protos.Value_LaneAssistLevelValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.LaneAssistLevelValue))
	case *protos.Value_ScheduledChargingModeValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.ScheduledChargingModeValue))
	case *protos.Value_SentryModeStateValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.SentryModeStateValue))
	case *protos.Value_SpeedAssistLevelValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.SpeedAssistLevelValue))
	case *protos.Value_BmsStateValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.BmsStateValue))
	case *protos.Value_BuckleStatusValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.BuckleStatusValue))
	case *protos.Value_CarTypeValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.CarTypeValue))
	case *protos.Value_ChargePortValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.ChargePortValue))
	case *protos.Value_ChargePortLatchValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.ChargePortLatchValue))
	case *protos.Value_CruiseStateValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.CruiseStateValue))
	case *protos.Value_DriveInverterStateValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.DriveInverterStateValue))
	case *protos.Value_HvilStatusValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.HvilStatusValue))
	case *protos.Value_WindowStateValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.WindowStateValue))
	case *protos.Value_SeatFoldPositionValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.SeatFoldPositionValue))
	case *protos.Value_TractorAirStatusValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.TractorAirStatusValue))
	case *protos.Value_FollowDistanceValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.FollowDistanceValue))
	case *protos.Value_ForwardCollisionSensitivityValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.ForwardCollisionSensitivityValue))
	case *protos.Value_GuestModeMobileAccessValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.GuestModeMobileAccessValue))
	case *protos.Value_TrailerAirStatusValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.TrailerAirStatusValue))
	case *protos.Value_DetailedChargeStateValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.DetailedChargeStateValue))
	case *protos.Value_Invalid:
		log.Printf("Invalid value received for field '%s', setting as NaN", fieldName)
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(math.NaN())
	default:
		log.Printf("Unhandled value type for field '%s': %v", fieldName, v)
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
func handleStringValue(stringValue, fieldName string, service *Service, vin string) {
	if stringValue == "<invalid>" || stringValue == "\u003cinvalid\u003e" {
		log.Printf("Invalid string value received for field '%s', setting as NaN", fieldName)
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(math.NaN())
		return
	}

	floatVal, err := strconv.ParseFloat(stringValue, 64)
	if err == nil {
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(floatVal)
	} else {
		log.Printf("Non-numeric string value received for field '%s': '%s', setting as NaN", fieldName, stringValue)
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(math.NaN())
	}
}

// handleDoorValues processes door states from Protobuf and updates Prometheus metrics
func handleDoorValues(doors *protos.Doors, gauge *prometheus.GaugeVec, vin string) {
	doorFields := map[string]bool{
		"DriverFront":    doors.DriverFront,
		"PassengerFront": doors.PassengerFront,
		"DriverRear":     doors.DriverRear,
		"PassengerRear":  doors.PassengerRear,
		"TrunkFront":     doors.TrunkFront,
		"TrunkRear":      doors.TrunkRear,
	}

	for doorName, state := range doorFields {
		numericValue := boolToFloat64(state)
		gauge.WithLabelValues(doorName, vin).Set(numericValue)
	}
}

// startPrometheusServer launches the Prometheus metrics HTTP server
func startPrometheusServer(addr string, wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Run server in a separate goroutine
	go func() {
		log.Printf("Starting Prometheus metrics server at %s/metrics", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Prometheus HTTP server failed: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Shutdown the server gracefully
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Prometheus HTTP server shutdown failed: %v", err)
	} else {
		log.Println("Prometheus HTTP server shut down gracefully.")
	}
}

// main is the entry point of the application
func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.json", "Path to the JSON configuration file")
	promAddr := flag.String("prometheus.addr", ":2112", "Address for Prometheus metrics server")
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

	// Setup context and wait group for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Start Prometheus metrics server
	wg.Add(1)
	go startPrometheusServer(*promAddr, &wg, ctx)

	// Setup signal handling for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Begin consuming Kafka messages
	log.Println("Starting Kafka message consumption...")
consumeLoop:
	for {
		select {
		case sig := <-sigchan:
			log.Printf("Received signal: %v. Initiating shutdown...", sig)
			break consumeLoop
		default:
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

			log.Printf("Received Vehicle Data: %v", vehicleData)

			// Process each Datum in the Payload
			for _, datum := range vehicleData.Data {
				processValue(datum, service, vehicleData.Vin)
			}

			// Upload to S3 if enabled
			if service.S3Client != nil {
				vehicleDataJSON, err := protojson.Marshal(vehicleData)
				if err != nil {
					log.Printf("Failed to marshal vehicleData to JSON: %v", err)
					continue
				}
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

	// Initiate shutdown
	cancel()

	// Close Kafka consumer
	if err := service.KafkaConsumer.Close(); err != nil {
		log.Printf("Error closing Kafka consumer: %v", err)
	} else {
		log.Println("Kafka consumer closed successfully.")
	}

	// Wait for all goroutines to finish
	wg.Wait()

	log.Println("Application shut down gracefully.")
}