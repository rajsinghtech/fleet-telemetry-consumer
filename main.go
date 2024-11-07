package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/teslamotors/fleet-telemetry/protos" // Make sure this path is correct
)

// Define Prometheus metrics
var (
	messagesConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "messages_consumed_total",
		Help: "Total number of messages consumed from Kafka",
	})
	messagesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "messages_processed_total",
		Help: "Total number of messages processed successfully",
	})
	messagesFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "messages_failed_total",
		Help: "Total number of messages failed to process",
	})
)

func init() {
	prometheus.MustRegister(messagesConsumed)
	prometheus.MustRegister(messagesProcessed)
	prometheus.MustRegister(messagesFailed)
}

func main() {
	// Read environment variables
	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsBucketHost := os.Getenv("AWS_BUCKET_HOST")
	awsBucketName := os.Getenv("AWS_BUCKET_NAME")
	awsBucketPort := os.Getenv("AWS_BUCKET_PORT")
	awsBucketProtocol := os.Getenv("AWS_BUCKET_PROTOCOL")
	awsBucketRegion := os.Getenv("AWS_BUCKET_REGION")
	awsEnabled := os.Getenv("AWS_ENABLED")
	kafkaEnabled := os.Getenv("KAFKA_ENABLED")
	kafkaAutoOffsetReset := os.Getenv("KAFKA_AUTO_OFFSET_RESET")
	kafkaBootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	kafkaGroupID := os.Getenv("KAFKA_GROUP_ID")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	loadDaysStr := os.Getenv("LOAD_DAYS")
	localBasePath := os.Getenv("LOCAL_BASE_PATH")
	localEnabled := os.Getenv("LOCAL_ENABLED")
	postgresDBName := os.Getenv("POSTGRES_DBNAME")
	postgresEnabled := os.Getenv("POSTGRES_ENABLED")
	postgresHost := os.Getenv("POSTGRES_HOST")
	postgresPassword := os.Getenv("POSTGRES_PASSWORD")
	postgresPort := os.Getenv("POSTGRES_PORT")
	postgresSSLMode := os.Getenv("POSTGRES_SSLMODE")
	postgresUser := os.Getenv("POSTGRES_USER")
	prometheusAddr := os.Getenv("PROMETHEUS_ADDR")

	// Parse LOAD_DAYS
	loadDays, err := strconv.Atoi(loadDaysStr)
	if err != nil {
		log.Printf("Invalid LOAD_DAYS value '%s', defaulting to 0", loadDaysStr)
		loadDays = 0
	}

	// Create context and waitgroup for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Setup signal handling to gracefully shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		cancel()
	}()

	// Set up Prometheus metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(prometheusAddr, nil))
	}()

	// If KAFKA_ENABLED is true
	if kafkaEnabled == "true" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumeFromKafka(ctx, awsEnabled, kafkaBootstrapServers, kafkaGroupID, kafkaTopic, kafkaAutoOffsetReset, awsAccessKeyID, awsSecretAccessKey, awsBucketName, awsBucketRegion, awsBucketHost, awsBucketPort, awsBucketProtocol, localBasePath, localEnabled, postgresEnabled, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDBName, postgresSSLMode)
		}()
	}

	// If LOAD_DAYS is set and positive, load old data from S3 into Postgres
	if loadDays > 0 {
		// Load old data from S3 into Postgres
		if postgresEnabled == "true" && awsEnabled == "true" {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := loadOldData(ctx, loadDays, awsAccessKeyID, awsSecretAccessKey, awsBucketName, awsBucketRegion, awsBucketHost, awsBucketPort, awsBucketProtocol, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDBName, postgresSSLMode)
				if err != nil {
					log.Printf("Error loading old data: %s", err)
				}
			}()
		} else {
			log.Println("AWS and PostgreSQL must be enabled to load old data from S3 into Postgres")
		}
	}

	wg.Wait()
}

func consumeFromKafka(ctx context.Context, awsEnabled string, kafkaBootstrapServers string, kafkaGroupID string, kafkaTopic string, kafkaAutoOffsetReset string, awsAccessKeyID string, awsSecretAccessKey string, awsBucketName string, awsBucketRegion string, awsBucketHost string, awsBucketPort string, awsBucketProtocol string, localBasePath string, localEnabled string, postgresEnabled string, postgresHost string, postgresPort string, postgresUser string, postgresPassword string, postgresDBName string, postgresSSLMode string) {

	// Set up Kafka consumer
	config := &kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"group.id":          kafkaGroupID,
		"auto.offset.reset": kafkaAutoOffsetReset,
		"enable.auto.commit": false,
	}
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Subscribe to topic
	err = consumer.Subscribe(kafkaTopic, nil)
	if err != nil {
		log.Fatalf("failed to subscribe to topic %s: %s", kafkaTopic, err)
	}

	// Set up AWS S3 session if AWS_ENABLED
	var s3Client *s3.S3
	if awsEnabled == "true" {
		endpoint := fmt.Sprintf("%s://%s:%s", awsBucketProtocol, awsBucketHost, awsBucketPort)
		sess, err := session.NewSession(&aws.Config{
			S3ForcePathStyle: aws.Bool(true),
			Region:           aws.String(awsBucketRegion),
			Credentials:      credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""),
			Endpoint:         aws.String(endpoint),
		})
		if err != nil {
			log.Fatalf("failed to create AWS session: %s", err)
		}

		s3Client = s3.New(sess)
	}

	// Set up PostgreSQL connection if POSTGRES_ENABLED
	var db *sql.DB
	if postgresEnabled == "true" {
		connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", postgresHost, postgresPort, postgresUser, postgresPassword, postgresDBName, postgresSSLMode)
		db, err = sql.Open("postgres", connStr)
		if err != nil {
			log.Fatalf("failed to connect to PostgreSQL: %s", err)
		}
		defer db.Close()
		// Ensure the table exists
		err = createTelemetryTable(db)
		if err != nil {
			log.Fatalf("failed to create telemetry table: %s", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, exiting consumer")
			return
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				kafkaErr, ok := err.(kafka.Error)
				if ok && kafkaErr.Code() == kafka.ErrTimedOut {
					// Ignore timeout
					continue
				} else {
					log.Printf("Consumer error: %v (%v)\n", err, msg)
					messagesFailed.Inc()
					continue
				}
			}

			messagesConsumed.Inc()

			// **Enhanced log statement with message metadata**
			log.Printf("Consumed message Topic:%s Partition:%d Offset:%d Key:%s Timestamp:%s", *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), msg.Timestamp.String())

			// Process message
			payload := &protos.Payload{}
			err = proto.Unmarshal(msg.Value, payload)
			if err != nil {
				log.Printf("Failed to unmarshal protobuf message from Kafka at Offset:%d Partition:%d Error:%s", msg.TopicPartition.Offset, msg.TopicPartition.Partition, err)
				messagesFailed.Inc()
				continue
			}

			// Log the VIN and CreatedAt from the payload
			log.Printf("Processing Payload for VIN:%s CreatedAt:%s", payload.Vin, payload.CreatedAt.AsTime().String())

			// Store protobuf message in S3 as .pb file
			if awsEnabled == "true" {
				err := storeProtobufInS3(s3Client, awsBucketName, payload)
				if err != nil {
					log.Printf("Failed to store protobuf in S3 for VIN:%s Error:%s", payload.Vin, err)
					messagesFailed.Inc()
				} else {
					log.Printf("Successfully stored protobuf in S3 for VIN:%s", payload.Vin)
				}
			}

			// Store protobuf message in local filesystem if LOCAL_ENABLED
			if localEnabled == "true" {
				err := storeProtobufLocally(localBasePath, payload)
				if err != nil {
					log.Printf("Failed to store protobuf locally for VIN:%s Error:%s", payload.Vin, err)
					messagesFailed.Inc()
				} else {
					log.Printf("Successfully stored protobuf locally for VIN:%s", payload.Vin)
				}
			}

			// Marshal protobuf into JSON and store in PostgreSQL
			if postgresEnabled == "true" {
				err = storeProtobufInPostgres(db, payload)
				if err != nil {
					log.Printf("Failed to store protobuf in PostgreSQL for VIN:%s Error:%s", payload.Vin, err)
					messagesFailed.Inc()
				} else {
					log.Printf("Successfully stored protobuf in PostgreSQL for VIN:%s", payload.Vin)
				}
			}

			// Commit offsets manually if auto-commit is disabled
			if _, err := consumer.CommitMessage(msg); err != nil {
				log.Printf("Failed to commit message offset:%d Partition:%d Error:%s", msg.TopicPartition.Offset, msg.TopicPartition.Partition, err)
			} else {
				log.Printf("Committed message offset:%d Partition:%d", msg.TopicPartition.Offset, msg.TopicPartition.Partition)
			}

			messagesProcessed.Inc()
		}
	}
}

func storeProtobufInS3(s3Client *s3.S3, bucketName string, payload *protos.Payload) error {
	vin := payload.Vin
	createdAt := payload.CreatedAt.AsTime()
	objectKey := fmt.Sprintf("%s/%d/%d/%d/%d%02d%02d/%02d%02d%02d%09d.pb",
		vin,
		createdAt.Year(),
		int(createdAt.Month()),
		createdAt.Day(),
		createdAt.Year(), int(createdAt.Month()), createdAt.Day(),
		createdAt.Hour(), createdAt.Minute(), createdAt.Second(), createdAt.Nanosecond())

	// Serialize the protobuf message
	data, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal protobuf: %w", err)
	}

	// Put the object in S3
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to put object in S3: %w", err)
	}

	// **Log the S3 object key for tracking**
	log.Printf("Stored object in S3 Bucket:%s Key:%s for VIN:%s", bucketName, objectKey, vin)

	return nil
}

func storeProtobufLocally(basePath string, payload *protos.Payload) error {
	vin := payload.Vin
	createdAt := payload.CreatedAt.AsTime()
	filePath := filepath.Join(basePath,
		vin,
		fmt.Sprintf("%d", createdAt.Year()),
		fmt.Sprintf("%d", int(createdAt.Month())),
		fmt.Sprintf("%d", createdAt.Day()),
		fmt.Sprintf("%d%02d%02d", createdAt.Year(), int(createdAt.Month()), createdAt.Day()),
		fmt.Sprintf("%02d%02d%02d%09d.pb", createdAt.Hour(), createdAt.Minute(), createdAt.Second(), createdAt.Nanosecond()))

	// Make sure the directory exists
	dir := filepath.Dir(filePath)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Serialize the protobuf message
	data, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal protobuf: %w", err)
	}

	// Write the file
	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write file %s: %w", filePath, err)
	}

	// **Log the file path for tracking**
	log.Printf("Stored protobuf locally at Path:%s for VIN:%s", filePath, vin)

	return nil
}

func storeProtobufInPostgres(db *sql.DB, payload *protos.Payload) error {
	vin := payload.Vin
	createdAt := payload.CreatedAt.AsTime().UTC()
	marshaller := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}
	jsonData, err := marshaller.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal protobuf to JSON: %w", err)
	}

	// Insert into PostgreSQL
	query := `INSERT INTO telemetry_data (vin, created_at, data) VALUES ($1, $2, $3) ON CONFLICT (vin, created_at) DO UPDATE SET data = EXCLUDED.data`
	_, err = db.Exec(query, vin, createdAt, jsonData)
	if err != nil {
		return fmt.Errorf("failed to insert into PostgreSQL: %w", err)
	}

	// **Log successful insertion into PostgreSQL**
	log.Printf("Inserted data into PostgreSQL for VIN:%s CreatedAt:%s", vin, createdAt.String())

	return nil
}

func loadOldData(ctx context.Context, loadDays int, awsAccessKeyID string, awsSecretAccessKey string, awsBucketName string, awsBucketRegion string, awsBucketHost string, awsBucketPort string, awsBucketProtocol string, postgresHost string, postgresPort string, postgresUser string, postgresPassword string, postgresDBName string, postgresSSLMode string) error {
	// Set up AWS S3 session
		endpoint := fmt.Sprintf("%s://%s:%s", awsBucketProtocol, awsBucketHost, awsBucketPort)
		sess, err := session.NewSession(&aws.Config{
			S3ForcePathStyle: aws.Bool(true),
			Region:           aws.String(awsBucketRegion),
			Credentials:      credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""),
			Endpoint:         aws.String(endpoint),
		})
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %s", err)
	}

	s3Client := s3.New(sess)

	// Set up PostgreSQL connection
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", postgresHost, postgresPort, postgresUser, postgresPassword, postgresDBName, postgresSSLMode)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %s", err)
	}
	defer db.Close()

	err = createTelemetryTable(db)
	if err != nil {
		return fmt.Errorf("failed to create telemetry table: %s", err)
	}

	// For each day in LOAD_DAYS
	now := time.Now()
	for i := 0; i < loadDays; i++ {
		day := now.AddDate(0, 0, -i)
		log.Printf("Processing data for date: %s", day.Format("2006-01-02"))
		// Process objects for the day
		err := listAndProcessObjects(ctx, s3Client, awsBucketName, day, db)
		if err != nil {
			log.Printf("Failed to process objects for day %s: %s", day.Format("2006-01-02"), err)
			continue
		} else {
			log.Printf("Successfully processed data for date: %s", day.Format("2006-01-02"))
		}
	}

	return nil
}

func listAndProcessObjects(ctx context.Context, s3Client *s3.S3, bucketName string, day time.Time, db *sql.DB) error {
	// List all objects in the bucket
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	err := s3Client.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			key := *obj.Key

			// Parse object key to get date
			parts := strings.Split(key, "/")
			if len(parts) < 5 {
				continue
			}
			yearStr := parts[1]
			monthStr := parts[2]
			dayStr := parts[3]
			dateStr := fmt.Sprintf("%s-%s-%s", yearStr, monthStr, dayStr)
			objDate, err := time.Parse("2006-1-2", dateStr)
			if err != nil {
				continue
			}
			if objDate.Year() == day.Year() && objDate.Month() == day.Month() && objDate.Day() == day.Day() {
				log.Printf("Processing S3 object Key:%s LastModified:%s", key, obj.LastModified.String())
				err := processS3Object(s3Client, bucketName, key, db)
				if err != nil {
					log.Printf("Failed to process object %s: %s", key, err)
					continue
				} else {
					log.Printf("Successfully processed object %s", key)
				}
			}

			select {
			case <-ctx.Done():
				return false
			default:
			}
		}
		return !lastPage
	})
	return err
}

func processS3Object(s3Client *s3.S3, bucketName string, key string, db *sql.DB) error {
	// Get the object
	getObjectOutput, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object %s: %w", key, err)
	}
	defer getObjectOutput.Body.Close()

	// Read the object data
	data, err := io.ReadAll(getObjectOutput.Body)
	if err != nil {
		return fmt.Errorf("failed to read object body: %w", err)
	}

	// Unmarshal the protobuf message
	payload := &protos.Payload{}
	err = proto.Unmarshal(data, payload)
	if err != nil {
		return fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	// Log payload details
	log.Printf("Processing payload from S3 object Key:%s VIN:%s CreatedAt:%s", key, payload.Vin, payload.CreatedAt.AsTime().String())

	// Store protobuf message in PostgreSQL
	err = storeProtobufInPostgres(db, payload)
	if err != nil {
		return fmt.Errorf("failed to store protobuf in PostgreSQL: %w", err)
	}

	// Log successful processing
	log.Printf("Successfully processed S3 object Key:%s and stored data in PostgreSQL for VIN:%s", key, payload.Vin)

	return nil
}

func createTelemetryTable(db *sql.DB) error {
	query := `CREATE TABLE IF NOT EXISTS telemetry_data (
        vin text,
        created_at timestamp with time zone,
        data jsonb,
        PRIMARY KEY (vin, created_at)
    );`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create telemetry_data table: %w", err)
	}
	return nil
}
