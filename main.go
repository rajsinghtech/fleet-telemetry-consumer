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
	"google.golang.org/protobuf/proto"
	"github.com/teslamotors/fleet-telemetry/protos"
)

// Define the field keys as constants if they are not already defined
const (
	Field_VehicleSpeed = "vehicle_speed"
	// Add other field constants as needed
)

// Config holds the entire configuration structure
type Config struct {
	Kafka       KafkaConfig
	AWS         *S3Config
	Local       *LocalConfig
	PostgreSQL  *PostgreSQLConfig
	LoadDays    int // New field to specify the number of days to load
}

// S3Config holds AWS S3 configuration
type S3Config struct {
	Enabled   bool
	Protocol  string
	Host      string
	Port      string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
}

// LocalConfig holds local backup configuration
type LocalConfig struct {
	Enabled  bool
	BasePath string
}

// KafkaConfig holds Kafka consumer configuration
type KafkaConfig struct {
	BootstrapServers string
	GroupID          string
	AutoOffsetReset  string
	Topic            string
}

// PostgreSQLConfig holds PostgreSQL configuration
type PostgreSQLConfig struct {
	Enabled  bool
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// Service encapsulates the application's dependencies
type Service struct {
	Config             Config
	S3Client           *s3.S3
	LocalBackupEnabled bool
	LocalBasePath      string
	KafkaConsumer      *kafka.Consumer
	PrometheusMetrics  *PrometheusMetrics
	DB                 *sql.DB
}

// PrometheusMetrics holds the Prometheus metrics
type PrometheusMetrics struct {
	Gauge *prometheus.GaugeVec
}

// NewPrometheusMetrics initializes Prometheus metrics
func NewPrometheusMetrics() *PrometheusMetrics {
	return &PrometheusMetrics{
		Gauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vehicle_data",
				Help: "Vehicle telemetry data",
			},
			[]string{"field", "vin"},
		),
	}
}

// NewService initializes the service with configurations
func NewService(cfg Config) (*Service, error) {
	service := &Service{
		Config:             cfg,
		LocalBackupEnabled: cfg.Local != nil && cfg.Local.Enabled,
		LocalBasePath:      "",
		PrometheusMetrics:  NewPrometheusMetrics(),
	}

	if service.LocalBackupEnabled {
		service.LocalBasePath = cfg.Local.BasePath
	}

	// Initialize AWS S3 if enabled
	if err := initializeS3(service, cfg.AWS); err != nil {
		return nil, err
	}

	// Initialize Kafka consumer
	if err := initializeKafka(service, cfg.Kafka); err != nil {
		return nil, err
	}

	// Initialize PostgreSQL if enabled
	if err := initializePostgreSQL(service, cfg.PostgreSQL); err != nil {
		return nil, err
	}

	// Load existing S3 data if both S3 and PostgreSQL are enabled
	if service.S3Client != nil && service.DB != nil {
		if err := loadExistingS3Data(service); err != nil {
			return nil, fmt.Errorf("failed to load existing S3 data: %w", err)
		}
	}

	return service, nil
}

// Define the Metrics struct
type Metrics struct {
	Gauge     *prometheus.GaugeVec
	Latitude  *prometheus.GaugeVec
	Longitude *prometheus.GaugeVec
}

// configureS3 sets up the AWS S3 client
func configureS3(s3Config *S3Config) (*s3.S3, error) {
	if err := validateS3Config(s3Config); err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("%s://%s:%s", s3Config.Protocol, s3Config.Host, s3Config.Port)

	sess, err := session.NewSession(&aws.Config{
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String(s3Config.Region),
		Credentials:      credentials.NewStaticCredentials(s3Config.AccessKey, s3Config.SecretKey, ""),
		Endpoint:         aws.String(endpoint),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create AWS session: %w", err)
	}

	return s3.New(sess), nil
}

// validateS3Config ensures all required S3 configurations are present
func validateS3Config(cfg *S3Config) error {
	if cfg.Protocol == "" || cfg.Host == "" || cfg.Port == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" || cfg.Region == "" {
		return fmt.Errorf("incomplete S3 configuration")
	}
	return nil
}

// testS3Connection verifies the connection to S3 by listing objects in the specified bucket
func testS3Connection(s3Svc *s3.S3, bucket string) error {
	_, err := s3Svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return fmt.Errorf("failed to access S3 bucket '%s': %w", bucket, err)
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

// configurePostgreSQL sets up the PostgreSQL connection
func configurePostgreSQL(pgConfig *PostgreSQLConfig) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		pgConfig.Host,
		pgConfig.Port,
		pgConfig.User,
		pgConfig.Password,
		pgConfig.DBName,
		pgConfig.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to open PostgreSQL connection: %w", err)
	}

	// Verify the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("unable to verify PostgreSQL connection: %w", err)
	}

	return db, nil
}

// createTelemetryTable creates the telemetry_data table if it does not exist
func createTelemetryTable(db *sql.DB) error {
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS telemetry_data (
		id SERIAL PRIMARY KEY,
		vin TEXT,
		key INTEGER,
		value JSONB,
		created_at TIMESTAMPTZ,
        UNIQUE (vin, key, created_at)
	);
	`
	_, err := db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create telemetry_data table: %w", err)
	}
	return nil
}

// loadConfigFromEnv reads and validates the configuration from environment variables
func loadConfigFromEnv() (Config, error) {
	var cfg Config

	// Kafka configuration
	cfg.Kafka.BootstrapServers = os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	cfg.Kafka.GroupID = os.Getenv("KAFKA_GROUP_ID")
	cfg.Kafka.AutoOffsetReset = os.Getenv("KAFKA_AUTO_OFFSET_RESET")
	if cfg.Kafka.AutoOffsetReset == "" {
		cfg.Kafka.AutoOffsetReset = "earliest"
	}
	cfg.Kafka.Topic = os.Getenv("KAFKA_TOPIC")

	if cfg.Kafka.BootstrapServers == "" || cfg.Kafka.GroupID == "" || cfg.Kafka.Topic == "" {
		return cfg, fmt.Errorf("incomplete Kafka configuration")
	}

	// AWS S3 configuration
	awsEnabledStr := os.Getenv("AWS_ENABLED")
	awsEnabled, err := strconv.ParseBool(awsEnabledStr)
	if err != nil {
		awsEnabled = false
	}
	if awsEnabled {
		cfg.AWS = &S3Config{
			Enabled:   true,
			Protocol:  os.Getenv("AWS_BUCKET_PROTOCOL"),
			Host:      os.Getenv("AWS_BUCKET_HOST"),
			Port:      os.Getenv("AWS_BUCKET_PORT"),
			Bucket:    os.Getenv("AWS_BUCKET_NAME"),
			AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			Region:    os.Getenv("AWS_BUCKET_REGION"),
		}
		if err := validateS3Config(cfg.AWS); err != nil {
			return cfg, fmt.Errorf("invalid AWS configuration: %w", err)
		}
	}

	// Local backup configuration
	localEnabledStr := os.Getenv("LOCAL_ENABLED")
	localEnabled, err := strconv.ParseBool(localEnabledStr)
	if err != nil {
		localEnabled = false
	}
	if localEnabled {
		cfg.Local = &LocalConfig{
			Enabled:  true,
			BasePath: os.Getenv("LOCAL_BASE_PATH"),
		}
		if cfg.Local.BasePath == "" {
			return cfg, fmt.Errorf("LOCAL_BASE_PATH cannot be empty when local backup is enabled")
		}
	}

	// PostgreSQL configuration
	pgEnabledStr := os.Getenv("POSTGRES_ENABLED")
	pgEnabled, err := strconv.ParseBool(pgEnabledStr)
	if err != nil {
		pgEnabled = false
	}
	if pgEnabled {
		portStr := os.Getenv("POSTGRES_PORT")
		port, err := strconv.Atoi(portStr)
		if err != nil {
			port = 5432 // default PostgreSQL port
		}

		cfg.PostgreSQL = &PostgreSQLConfig{
			Enabled:  true,
			Host:     os.Getenv("POSTGRES_HOST"),
			Port:     port,
			User:     os.Getenv("POSTGRES_USER"),
			Password: os.Getenv("POSTGRES_PASSWORD"),
			DBName:   os.Getenv("POSTGRES_DBNAME"),
			SSLMode:  os.Getenv("POSTGRES_SSLMODE"),
		}
		if cfg.PostgreSQL.Host == "" || cfg.PostgreSQL.User == "" || cfg.PostgreSQL.Password == "" || cfg.PostgreSQL.DBName == "" {
			return cfg, fmt.Errorf("incomplete PostgreSQL configuration")
		}
		if cfg.PostgreSQL.SSLMode == "" {
			cfg.PostgreSQL.SSLMode = "disable"
		}
	}

	// LoadDays configuration
	loadDaysStr := os.Getenv("LOAD_DAYS")
	loadDays, err := strconv.Atoi(loadDaysStr)
	if err != nil || loadDays <= 0 {
		loadDays = 7 // Default to 7 days if not set or invalid
	}
	cfg.LoadDays = loadDays

	return cfg, nil
}

// uploadToS3 uploads Protobuf data to the specified S3 bucket with a vin/year/month/day/createdAt key structure
func uploadToS3(s3Svc *s3.S3, bucket, vin string, data []byte, createdAt time.Time) error {
	key := fmt.Sprintf("%s/%04d/%02d/%02d/%04d%02d%02dT%02d%02d%02d.%09dZ.pb",
		vin,
		createdAt.Year(),
		int(createdAt.Month()),
		createdAt.Day(),
		createdAt.Year(), int(createdAt.Month()), createdAt.Day(),
		createdAt.Hour(), createdAt.Minute(), createdAt.Second(), createdAt.Nanosecond(),
	)

	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/x-protobuf"),
	}

	_, err := s3Svc.PutObject(input)
	if err != nil {
		return fmt.Errorf("failed to upload to S3 at key '%s': %w", key, err)
	}

	log.Printf("Successfully uploaded data to S3 at key: %s", key)
	return nil
}

// backupLocally saves Protobuf data to the local filesystem with a vin/year/month/day/createdAt filename structure
func backupLocally(basePath, vin string, data []byte, createdAt time.Time) error {
	// Create the directories as per the existing structure
	dirPath := filepath.Join(basePath,
		vin,
		fmt.Sprintf("%04d", createdAt.Year()),
		fmt.Sprintf("%02d", createdAt.Month()),
		fmt.Sprintf("%02d", createdAt.Day()),
	)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories '%s': %w", dirPath, err)
	}

	// Use createdAt as the filename
	fileName := fmt.Sprintf("%04d%02d%02dT%02d%02d%02d.%09dZ.pb",
		createdAt.Year(), int(createdAt.Month()), createdAt.Day(),
		createdAt.Hour(), createdAt.Minute(), createdAt.Second(), createdAt.Nanosecond(),
	)

	// Full file path
	filePath := filepath.Join(dirPath, fileName)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file '%s': %w", filePath, err)
	}

	log.Printf("Successfully backed up data locally at: %s", filePath)
	return nil
}

// insertTelemetryData inserts telemetry data into PostgreSQL
func insertTelemetryData(db *sql.DB, vin string, datum *protos.Datum, createdAt time.Time) error {
	// Prepare the insert query with columns for each field
	insertQuery := `
	INSERT INTO telemetry_data (
		vin, created_at, drive_rail, charge_state, bms_fullchargecomplete, vehicle_speed, 
		odometer, pack_voltage, pack_current, soc, dcdc_enable, gear, isolation_resistance, 
		pedal_position, brake_pedal, di_state_r, di_heatsink_tr, di_axle_speed_r, 
		di_torquemotor, di_stator_temp_r, di_vbat_r, di_motor_current_r, location_latitude, 
		location_longitude, gps_state, gps_heading, num_brick_voltage_max, brick_voltage_max, 
		num_brick_voltage_min, brick_voltage_min, num_module_temp_max, module_temp_max, 
		num_module_temp_min, module_temp_min, rated_range, hvil, dc_charging_energy_in, 
		dc_charging_power, ac_charging_energy_in, ac_charging_power, charge_limit_soc, 
		fast_charger_present, est_battery_range, ideal_battery_range, battery_level, 
		time_to_full_charge, scheduled_charging_start_time, scheduled_charging_pending, 
		scheduled_departure_time, preconditioning_enabled, scheduled_charging_mode, 
		charge_amps, charge_enable_request, charger_phases, charge_port_cold_weather_mode, 
		charge_current_request, semitruck_tpms_pressure_re2r0, semitruck_tpms_pressure_re2r1, 
		tpms_last_seen_pressure_time_fl, tpms_last_seen_pressure_time_fr, 
		tpms_last_seen_pressure_time_rl, tpms_last_seen_pressure_time_rr, inside_temp, 
		outside_temp, seat_heater_left, seat_heater_right, seat_heater_rear_left, 
		seat_heater_rear_right, seat_heater_rear_center, auto_seat_climate_left, 
		auto_seat_climate_right, driver_seat_belt, passenger_seat_belt, driver_seat_occupied, 
		semitruck_passenger_seat_fold_position, lateral_acceleration, longitudinal_acceleration, 
		cruise_state, cruise_set_speed, lifetime_energy_used, lifetime_energy_used_drive, 
		semitruck_tractor_park_brake_status, semitruck_trailer_park_brake_status, 
		brake_pedal_pos, route_last_updated, route_line, miles_to_arrival, minutes_to_arrival, 
		origin_location, destination_location, car_type, trim, exterior_color, roof_color, 
		charge_port, charge_port_latch, experimental_1, experimental_2, experimental_3, 
		experimental_4, guest_mode_enabled, pin_to_drive_enabled, paired_phone_key_and_key_fob_qty, 
		cruise_follow_distance, automatic_blind_spot_camera, blind_spot_collision_warning_chime, 
		speed_limit_warning, forward_collision_warning, lane_departure_avoidance, 
		emergency_lane_departure_avoidance, automatic_emergency_braking_off, 
		lifetime_energy_gained_regen, di_state_f, di_state_rel, di_state_rer, di_heatsink_tf, 
		di_heatsink_trel, di_heatsink_trer, di_axle_speed_f, di_axle_speed_rel, di_axle_speed_rer, 
		di_slave_torque_cmd, di_torque_actual_r, di_torque_actual_f, di_torque_actual_rel, 
		di_torque_actual_rer, di_stator_temp_f, di_stator_temp_rel, di_stator_temp_rer, 
		di_vbat_f, di_vbat_rel, di_vbat_rer, di_motor_current_f, di_motor_current_rel, 
		di_motor_current_rer, energy_remaining, service_mode, bms_state, 
		guest_mode_mobile_access_state, deprecated_1, destination_name, di_inverter_tr, 
		di_inverter_tf, di_inverter_trel, di_inverter_trer, experimental_5, experimental_6, 
		experimental_7, experimental_8, experimental_9, experimental_10, experimental_11, 
		experimental_12, experimental_13, experimental_14, experimental_15, detailed_charge_state
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 
		$21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, 
		$39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, 
		$57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, 
		$75, $76, $77, $78, $79, $80, $81, $82, $83, $84, $85, $86, $87, $88, $89, $90, $91, $92, 
		$93, $94, $95, $96, $97, $98, $99, $100, $101, $102, $103, $104, $105, $106, $107, $108, 
		$109, $110, $111, $112, $113, $114, $115, $116, $117, $118, $119, $120, $121, $122, $123, 
		$124, $125, $126, $127, $128, $129, $130, $131, $132, $133, $134, $135, $136, $137, $138, 
		$139, $140, $141, $142, $143, $144, $145, $146, $147, $148, $149, $150, $151, $152, $153, 
		$154, $155, $156, $157, $158, $159, $160, $161, $162, $163, $164, $165, $166, $167, $168, 
		$169, $170, $171, $172, $173, $174, $175, $176, $177, $178, $179
	)
	`

	// Convert Datum to SQL values
	values := make([]interface{}, 179)
	values[0] = vin
	values[1] = createdAt

	for i := range protos.Field_name {
		if datum.Key == protos.Field(i) {
			switch v := datum.Value.Value.(type) {
			case *protos.Value_StringValue:
				values[i+2] = v.StringValue
			case *protos.Value_IntValue:
				values[i+2] = v.IntValue
			case *protos.Value_LongValue:
				values[i+2] = v.LongValue
			case *protos.Value_FloatValue:
				values[i+2] = v.FloatValue
			case *protos.Value_DoubleValue:
				values[i+2] = v.DoubleValue
			case *protos.Value_BooleanValue:
				values[i+2] = v.BooleanValue
			case *protos.Value_LocationValue:
				values[i+2] = v.LocationValue.Latitude
				values[i+3] = v.LocationValue.Longitude
			case *protos.Value_Invalid:
				values[i+2] = nil
			default:
				values[i+2] = nil
			}
		}
	}

	_, err := db.Exec(insertQuery, values...)
	if err != nil {
		return fmt.Errorf("failed to insert telemetry data: %w", err)
	}
	return nil
}

// processValue handles different types of Protobuf values and updates Prometheus metrics
func processValue(datum *protos.Datum, service *Service, vin string, createdAt time.Time) {
	fieldName := datum.Key.String()

	// Insert into PostgreSQL if enabled
	if service.DB != nil {
		if err := insertTelemetryData(service.DB, vin, datum, createdAt); err != nil {
			log.Printf("PostgreSQL insert error for VIN %s, key %d: %v", vin, datum.Key, err)
		}
	}

	// Update Prometheus metrics
	switch v := datum.Value.Value.(type) {
	case *protos.Value_StringValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(1)
	case *protos.Value_IntValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.IntValue))
	case *protos.Value_LongValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.LongValue))
	case *protos.Value_FloatValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.FloatValue))
	case *protos.Value_DoubleValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(v.DoubleValue)
	case *protos.Value_BooleanValue:
		numericValue := boolToFloat64(v.BooleanValue)
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(numericValue)
	case *protos.Value_LocationValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_latitude", vin).Set(v.LocationValue.Latitude)
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_longitude", vin).Set(v.LocationValue.Longitude)
	case *protos.Value_ChargingValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.ChargingValue))
	case *protos.Value_ShiftStateValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.ShiftStateValue))
	case *protos.Value_Invalid:
		log.Printf("Invalid value received for field '%s', setting as NaN", fieldName)
			service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(0)
	case *protos.Value_LaneAssistLevelValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.LaneAssistLevelValue))
	case *protos.Value_ScheduledChargingModeValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.ScheduledChargingModeValue))
	case *protos.Value_SentryModeStateValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.SentryModeStateValue))
	case *protos.Value_SpeedAssistLevelValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.SpeedAssistLevelValue))
	case *protos.Value_BmsStateValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.BmsStateValue))
	case *protos.Value_BuckleStatusValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.BuckleStatusValue))
	case *protos.Value_CarTypeValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.CarTypeValue))
	case *protos.Value_ChargePortValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.ChargePortValue))
	case *protos.Value_ChargePortLatchValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.ChargePortLatchValue))
	case *protos.Value_CruiseStateValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.CruiseStateValue))
	case *protos.Value_DoorValue:
		// Assuming DoorValue is a complex type, handle it accordingly
		// Example: service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_driver_front", vin).Set(boolToFloat64(v.DoorValue.DriverFront))
	case *protos.Value_DriveInverterStateValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.DriveInverterStateValue))
	case *protos.Value_HvilStatusValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.HvilStatusValue))
	case *protos.Value_WindowStateValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.WindowStateValue))
	case *protos.Value_SeatFoldPositionValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.SeatFoldPositionValue))
	case *protos.Value_TractorAirStatusValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.TractorAirStatusValue))
	case *protos.Value_FollowDistanceValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.FollowDistanceValue))
	case *protos.Value_ForwardCollisionSensitivityValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.ForwardCollisionSensitivityValue))
	case *protos.Value_GuestModeMobileAccessValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.GuestModeMobileAccessValue))
	case *protos.Value_TrailerAirStatusValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.TrailerAirStatusValue))
	case *protos.Value_TimeValue:
		// Assuming TimeValue is a complex type, handle it accordingly
		// Example: service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_hour", vin).Set(float64(v.TimeValue.Hour))
	case *protos.Value_DetailedChargeStateValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.DetailedChargeStateValue))
	default:
		log.Printf("Unhandled value type for field '%s'", fieldName)
	}
}

// boolToFloat64 converts a boolean to float64 (1.0 for true, 0.0 for false)
func boolToFloat64(value bool) float64 {
	if value {
		return 1.0
	}
	return 0.0
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

// startConsumerLoop begins consuming Kafka messages
func startConsumerLoop(service *Service, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Println("Starting Kafka message consumption...")
	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumption loop exiting due to context cancellation.")
			return
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

			log.Printf("Received Vehicle Data for VIN %s", vehicleData.Vin)

			// Convert created_at to time.Time
			createdAt := time.Unix(vehicleData.CreatedAt.Seconds, int64(vehicleData.CreatedAt.Nanos)).UTC()

			// Process each Datum in the Payload
			for _, datum := range vehicleData.Data {
				processValue(datum, service, vehicleData.Vin, createdAt)
			}

			// Serialize data as Protobuf for storage
			serializedData, err := proto.Marshal(vehicleData)
			if err != nil {
				log.Printf("Failed to marshal vehicleData to Protobuf: %v", err)
				continue
			}

			// Upload to S3 if enabled
			if service.S3Client != nil {
				if err := uploadToS3(service.S3Client, service.Config.AWS.Bucket, vehicleData.Vin, serializedData, createdAt); err != nil {
					log.Printf("Failed to upload vehicle data to S3: %v", err)
				}
			}

			// Backup locally if enabled
			if service.LocalBackupEnabled {
				if err := backupLocally(service.LocalBasePath, vehicleData.Vin, serializedData, createdAt); err != nil {
					log.Printf("Failed to backup vehicle data locally: %v", err)
				}
			}

			// Commit the message offset after successful processing
			if _, err := service.KafkaConsumer.CommitMessage(msg); err != nil {
				log.Printf("Failed to commit Kafka message: %v", err)
			}
		}
	}
}

// loadExistingS3Data loads all .pb files from the S3 bucket and processes them
func loadExistingS3Data(service *Service) error {
	if service.S3Client == nil || service.DB == nil {
		// Either S3 or PostgreSQL is not enabled; nothing to do
		return nil
	}

	bucket := service.Config.AWS.Bucket
	loadDays := service.Config.LoadDays

	// Calculate the cutoff date
	cutoffDate := time.Now().AddDate(0, 0, -loadDays)

	// List all .pb objects in the bucket
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	log.Printf("Listing all .pb files in S3 bucket: %s for the last %d days", bucket, loadDays)

	var continuationToken *string = nil
	for {
		if continuationToken != nil {
			listInput.ContinuationToken = continuationToken
		}

		result, err := service.S3Client.ListObjectsV2(listInput)
		if err != nil {
			return fmt.Errorf("failed to list objects in S3 bucket '%s': %w", bucket, err)
		}

		for _, object := range result.Contents {
			key := aws.StringValue(object.Key)
			if filepath.Ext(key) != ".pb" {
				continue // Skip non-.pb files
			}

			// Check if the object is within the loadDays range
			if object.LastModified.Before(cutoffDate) {
				continue
			}

			log.Printf("Processing S3 object: %s", key)

			// Download the .pb file
			getObjInput := &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			}

			resp, err := service.S3Client.GetObject(getObjInput)
			if err != nil {
				log.Printf("Failed to download S3 object '%s': %v", key, err)
				continue
			}

			defer resp.Body.Close()

			data, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Failed to read S3 object '%s': %v", key, err)
				continue
			}

			// Deserialize the Protobuf message
			vehicleData := &protos.Payload{}
			if err := proto.Unmarshal(data, vehicleData); err != nil {
				log.Printf("Failed to unmarshal Protobuf data from '%s': %v", key, err)
				continue
			}

			log.Printf("Loaded Vehicle Data for VIN %s from S3 key %s", vehicleData.Vin, key)

			// Convert created_at to time.Time
			createdAt := time.Unix(vehicleData.CreatedAt.Seconds, int64(vehicleData.CreatedAt.Nanos)).UTC()

			// Process each Datum in the Payload
			for _, datum := range vehicleData.Data {
				processValue(datum, service, vehicleData.Vin, createdAt)
			}

			// Optionally, you can delete the object after processing to prevent reprocessing
			// Uncomment the following lines if you choose to delete processed files
			/*
				deleteInput := &s3.DeleteObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				}
				_, err = service.S3Client.DeleteObject(deleteInput)
				if err != nil {
					log.Printf("Failed to delete S3 object '%s': %v", key, err)
				} else {
					log.Printf("Deleted S3 object '%s' after processing", key)
				}
			*/
		}

		if result.IsTruncated != nil && *result.IsTruncated {
			continuationToken = result.NextContinuationToken
		} else {
			break
		}
	}

	log.Println("Completed loading existing S3 data.")
	return nil
}

// initializeS3 sets up the AWS S3 client if enabled
func initializeS3(service *Service, s3Config *S3Config) error {
	if s3Config != nil && s3Config.Enabled {
		s3Client, err := configureS3(s3Config)
		if err != nil {
			return fmt.Errorf("failed to configure S3: %w", err)
		}
		service.S3Client = s3Client

		if err := testS3Connection(s3Client, s3Config.Bucket); err != nil {
			return fmt.Errorf("S3 connection test failed: %w", err)
		}
		log.Println("S3 connection established successfully.")
	} else {
		log.Println("AWS S3 uploads are disabled.")
	}
	return nil
}

// initializeKafka sets up the Kafka consumer
func initializeKafka(service *Service, kafkaCfg KafkaConfig) error {
	consumer, err := configureKafka(kafkaCfg)
	if err != nil {
		return fmt.Errorf("failed to configure Kafka consumer: %w", err)
	}
	service.KafkaConsumer = consumer
	return nil
}

// initializePostgreSQL sets up the PostgreSQL connection if enabled
func initializePostgreSQL(service *Service, pgConfig *PostgreSQLConfig) error {
	if pgConfig != nil && pgConfig.Enabled {
		db, err := configurePostgreSQL(pgConfig)
		if err != nil {
			return fmt.Errorf("failed to configure PostgreSQL: %w", err)
		}
		service.DB = db
		log.Println("PostgreSQL connection established successfully.")

		// Create table if it doesn't exist
		if err := createTelemetryTable(db); err != nil {
			return fmt.Errorf("failed to create telemetry table: %w", err)
		}
		log.Println("Telemetry table is ready.")
	} else {
		log.Println("PostgreSQL storage is disabled.")
	}
	return nil
}

// main is the entry point of the application
func main() {
	// Load configuration from environment variables
	cfg, err := loadConfigFromEnv()
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	// Read Prometheus address from environment variable, default to ":2112"
	promAddr := os.Getenv("PROMETHEUS_ADDR")
	if promAddr == "" {
		promAddr = ":2112"
	}

	// Initialize Prometheus metrics
	promMetrics := NewPrometheusMetrics()
	prometheus.MustRegister(promMetrics.Gauge)

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
	go startPrometheusServer(promAddr, &wg, ctx)

	// Start Kafka consumer loop
	wg.Add(1)
	go startConsumerLoop(service, ctx, &wg)

	// Setup signal handling for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	sig := <-sigchan
	log.Printf("Received signal: %v. Initiating shutdown...", sig)

	// Initiate shutdown
	cancel()

	// Close Kafka consumer
	if err := service.KafkaConsumer.Close(); err != nil {
		log.Printf("Error closing Kafka consumer: %v", err)
	} else {
		log.Println("Kafka consumer closed successfully.")
	}

	// Close PostgreSQL connection if enabled
	if service.DB != nil {
		if err := service.DB.Close(); err != nil {
			log.Printf("Error closing PostgreSQL connection: %v", err)
		} else {
			log.Println("PostgreSQL connection closed successfully.")
		}
	}

	// Wait for all goroutines to finish
	wg.Wait()

	log.Println("Application shut down gracefully.")
}
