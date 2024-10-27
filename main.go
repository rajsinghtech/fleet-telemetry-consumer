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
	"math"
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

func createTelemetryTable(db *sql.DB) error {
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS telemetry_data (
		vin TEXT,
		created_at TIMESTAMPTZ,
		vehicle_speed DOUBLE PRECISION,
		odometer DOUBLE PRECISION,
		pack_voltage DOUBLE PRECISION,
		pack_current DOUBLE PRECISION,
		soc DOUBLE PRECISION,
		dcdc_enable BOOLEAN,
		gear TEXT,
		isolation_resistance DOUBLE PRECISION,
		pedal_position DOUBLE PRECISION,
		brake_pedal BOOLEAN,
		di_state_r TEXT,
		di_heatsink_tr DOUBLE PRECISION,
		di_axle_speed_r DOUBLE PRECISION,
		di_torquemotor DOUBLE PRECISION,
		di_stator_temp_r DOUBLE PRECISION,
		di_vbat_r DOUBLE PRECISION,
		di_motor_current_r DOUBLE PRECISION,
		gps_state TEXT,
		gps_heading DOUBLE PRECISION,
		num_brick_voltage_max INTEGER,
		brick_voltage_max DOUBLE PRECISION,
		num_brick_voltage_min INTEGER,
		brick_voltage_min DOUBLE PRECISION,
		num_module_temp_max INTEGER,
		module_temp_max DOUBLE PRECISION,
		num_module_temp_min INTEGER,
		module_temp_min DOUBLE PRECISION,
		rated_range DOUBLE PRECISION,
		hvil_status TEXT,
		dc_charging_energy_in DOUBLE PRECISION,
		dc_charging_power DOUBLE PRECISION,
		ac_charging_energy_in DOUBLE PRECISION,
		ac_charging_power DOUBLE PRECISION,
		charge_limit_soc DOUBLE PRECISION,
		fast_charger_present BOOLEAN,
		est_battery_range DOUBLE PRECISION,
		ideal_battery_range DOUBLE PRECISION,
		battery_level INTEGER,
		time_to_full_charge DOUBLE PRECISION,
		scheduled_charging_start_time TIMESTAMPTZ,
		scheduled_charging_pending BOOLEAN,
		scheduled_departure_time TIMESTAMPTZ,
		preconditioning_enabled BOOLEAN,
		scheduled_charging_mode TEXT,
		charge_amps DOUBLE PRECISION,
		charge_enable_request BOOLEAN,
		charger_phases INTEGER,
		charge_port_cold_weather_mode BOOLEAN,
		charge_current_request DOUBLE PRECISION,
		charge_current_request_max DOUBLE PRECISION,
		battery_heater_on BOOLEAN,
		not_enough_power_to_heat BOOLEAN,
		supercharger_session_trip_planner BOOLEAN,
		door_state BOOLEAN,
		locked BOOLEAN,
		fd_window TEXT,
		fp_window TEXT,
		rd_window TEXT,
		rp_window TEXT,
		driver_front_door BOOLEAN,
		passenger_front_door BOOLEAN,
		driver_rear_door BOOLEAN,
		passenger_rear_door BOOLEAN,
		trunk_front BOOLEAN,
		trunk_rear BOOLEAN,
		vehicle_name TEXT,
		sentry_mode_state TEXT,
		speed_limit_mode BOOLEAN,
		current_limit_mph DOUBLE PRECISION,
		version TEXT,
		tpms_pressure_fl DOUBLE PRECISION,
		tpms_pressure_fr DOUBLE PRECISION,
		tpms_pressure_rl DOUBLE PRECISION,
		tpms_pressure_rr DOUBLE PRECISION,
		inside_temp DOUBLE PRECISION,
		outside_temp DOUBLE PRECISION,
		seat_heater_left INTEGER,
		seat_heater_right INTEGER,
		seat_heater_rear_left INTEGER,
		seat_heater_rear_right INTEGER,
		seat_heater_rear_center INTEGER,
		auto_seat_climate_left BOOLEAN,
		auto_seat_climate_right BOOLEAN,
		driver_seat_belt BOOLEAN,
		passenger_seat_belt BOOLEAN,
		driver_seat_occupied BOOLEAN,
		latitude DOUBLE PRECISION,
		longitude DOUBLE PRECISION,
		cruise_state TEXT,
		cruise_set_speed DOUBLE PRECISION,
		lifetime_energy_used DOUBLE PRECISION,
		lifetime_energy_used_drive DOUBLE PRECISION,
		brake_pedal_pos DOUBLE PRECISION,
		route_last_updated TIMESTAMPTZ,
		route_line TEXT,
		miles_to_arrival DOUBLE PRECISION,
		minutes_to_arrival DOUBLE PRECISION,
		origin_location TEXT,
		destination_location TEXT,
		car_type TEXT,
		trim TEXT,
		exterior_color TEXT,
		roof_color TEXT,
		charge_port TEXT,
		charge_port_latch TEXT,
		guest_mode_enabled BOOLEAN,
		pin_to_drive_enabled BOOLEAN,
		paired_phone_key_and_key_fob_qty INTEGER,
		cruise_follow_distance INTEGER,
		automatic_blind_spot_camera BOOLEAN,
		blind_spot_collision_warning_chime BOOLEAN,
		speed_limit_warning TEXT,
		forward_collision_warning TEXT,
		lane_departure_avoidance TEXT,
		emergency_lane_departure_avoidance BOOLEAN,
		automatic_emergency_braking_off BOOLEAN,
		lifetime_energy_gained_regen DOUBLE PRECISION,
		di_state_f TEXT,
		di_state_rel TEXT,
		di_state_rer TEXT,
		di_heatsink_tf DOUBLE PRECISION,
		di_heatsink_trel DOUBLE PRECISION,
		di_heatsink_trer DOUBLE PRECISION,
		di_axle_speed_f DOUBLE PRECISION,
		di_axle_speed_rel DOUBLE PRECISION,
		di_axle_speed_rer DOUBLE PRECISION,
		di_slave_torque_cmd DOUBLE PRECISION,
		di_torque_actual_r DOUBLE PRECISION,
		di_torque_actual_f DOUBLE PRECISION,
		di_torque_actual_rel DOUBLE PRECISION,
		di_torque_actual_rer DOUBLE PRECISION,
		di_stator_temp_f DOUBLE PRECISION,
		di_stator_temp_rel DOUBLE PRECISION,
		di_stator_temp_rer DOUBLE PRECISION,
		di_vbat_f DOUBLE PRECISION,
		di_vbat_rel DOUBLE PRECISION,
		di_vbat_rer DOUBLE PRECISION,
		di_motor_current_f DOUBLE PRECISION,
		di_motor_current_rel DOUBLE PRECISION,
		di_motor_current_rer DOUBLE PRECISION,
		energy_remaining DOUBLE PRECISION,
		service_mode BOOLEAN,
		bms_state TEXT,
		guest_mode_mobile_access_state TEXT,
		destination_name TEXT,
		di_inverter_tr DOUBLE PRECISION,
		di_inverter_tf DOUBLE PRECISION,
		di_inverter_trel DOUBLE PRECISION,
		di_inverter_trer DOUBLE PRECISION,
		detailed_charge_state TEXT
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
func insertTelemetryData(db *sql.DB, payload *protos.Payload) error {
    // Prepare the insert query with placeholders
    insertQuery := `
    INSERT INTO telemetry_data (
        vin,
        created_at,
        vehicle_speed,
        odometer,
        pack_voltage,
        pack_current,
        soc,
        dcdc_enable,
        gear,
        isolation_resistance,
        pedal_position,
        brake_pedal,
        di_state_r,
        di_heatsink_tr,
        di_axle_speed_r,
        di_torquemotor,
        di_stator_temp_r,
        di_vbat_r,
        di_motor_current_r,
        latitude,
        longitude,
        gps_state,
        gps_heading,
        num_brick_voltage_max,
        brick_voltage_max,
        num_brick_voltage_min,
        brick_voltage_min,
        num_module_temp_max,
        module_temp_max,
        num_module_temp_min,
        module_temp_min,
        rated_range,
        hvil_status,
        dc_charging_energy_in,
        dc_charging_power,
        ac_charging_energy_in,
        ac_charging_power,
        charge_limit_soc,
        fast_charger_present,
        est_battery_range,
        ideal_battery_range,
        battery_level,
        time_to_full_charge,
        scheduled_charging_start_time,
        scheduled_charging_pending,
        scheduled_departure_time,
        preconditioning_enabled,
        scheduled_charging_mode,
        charge_amps,
        charge_enable_request,
        charger_phases,
        charge_port_cold_weather_mode,
        charge_current_request,
        charge_current_request_max,
        battery_heater_on,
        not_enough_power_to_heat,
        supercharger_session_trip_planner,
        door_state,
        locked,
        fd_window,
        fp_window,
        rd_window,
        rp_window,
        driver_front_door,
        passenger_front_door,
        driver_rear_door,
        passenger_rear_door,
        trunk_front,
        trunk_rear,
        vehicle_name,
        sentry_mode_state,
        speed_limit_mode,
        current_limit_mph,
        version,
        tpms_pressure_fl,
        tpms_pressure_fr,
        tpms_pressure_rl,
        tpms_pressure_rr,
        inside_temp,
        outside_temp,
        seat_heater_left,
        seat_heater_right,
        seat_heater_rear_left,
        seat_heater_rear_right,
        seat_heater_rear_center,
        auto_seat_climate_left,
        auto_seat_climate_right,
        driver_seat_belt,
        passenger_seat_belt,
        driver_seat_occupied,
        cruise_state,
        cruise_set_speed,
        lifetime_energy_used,
        lifetime_energy_used_drive,
        brake_pedal_pos,
        route_last_updated,
        route_line,
        miles_to_arrival,
        minutes_to_arrival,
        origin_location,
        destination_location,
        car_type,
        trim,
        exterior_color,
        roof_color,
        charge_port,
        charge_port_latch,
        guest_mode_enabled,
        pin_to_drive_enabled,
        paired_phone_key_and_key_fob_qty,
        cruise_follow_distance,
        automatic_blind_spot_camera,
        blind_spot_collision_warning_chime,
        speed_limit_warning,
        forward_collision_warning,
        lane_departure_avoidance,
        emergency_lane_departure_avoidance,
        automatic_emergency_braking_off,
        lifetime_energy_gained_regen,
        di_state_f,
        di_state_rel,
        di_state_rer,
        di_heatsink_tf,
        di_heatsink_trel,
        di_heatsink_trer,
        di_axle_speed_f,
        di_axle_speed_rel,
        di_axle_speed_rer,
        di_slave_torque_cmd,
        di_torque_actual_r,
        di_torque_actual_f,
        di_torque_actual_rel,
        di_torque_actual_rer,
        di_stator_temp_f,
        di_stator_temp_rel,
        di_stator_temp_rer,
        di_vbat_f,
        di_vbat_rel,
        di_vbat_rer,
        di_motor_current_f,
        di_motor_current_rel,
        di_motor_current_rer,
        energy_remaining,
        service_mode,
        bms_state,
        guest_mode_mobile_access_state,
        destination_name,
        di_inverter_tr,
        di_inverter_tf,
        di_inverter_trel,
        di_inverter_trer,
        detailed_charge_state
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
        $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
        $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
        $51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
        $61, $62, $63, $64, $65, $66, $67, $68, $69, $70,
        $71, $72, $73, $74, $75, $76, $77, $78, $79, $80,
        $81, $82, $83, $84, $85, $86, $87, $88, $89, $90,
        $91, $92, $93, $94, $95, $96, $97, $98, $99, $100,
        $101, $102, $103, $104, $105, $106, $107, $108, $109, $110,
        $111, $112, $113, $114, $115, $116, $117, $118, $119, $120,
        $121, $122, $123, $124, $125, $126, $127, $128, $129, $130,
        $131, $132, $133, $134, $135, $136, $137, $138, $139, $140,
        $141, $142, $143, $144, $145, $146, $147, $148, $149, $150,
        $151, $152, $153, $154, $155, $156, $157, $158, $159, $160,
        $161, $162, $163, $164, $165
    )
    `

    // Initialize variables for each field
    var (
        vin       = payload.Vin
        createdAt = payload.CreatedAt.AsTime()

        vehicleSpeed                      sql.NullFloat64
        odometer                          sql.NullFloat64
        packVoltage                       sql.NullFloat64
        packCurrent                       sql.NullFloat64
        soc                               sql.NullFloat64
        dcdcEnable                        sql.NullBool
        gear                              sql.NullString
        isolationResistance               sql.NullFloat64
        pedalPosition                     sql.NullFloat64
        brakePedal                        sql.NullBool
        diStateR                          sql.NullString
        diHeatsinkTR                      sql.NullFloat64
        diAxleSpeedR                      sql.NullFloat64
        diTorquemotor                     sql.NullFloat64
        diStatorTempR                     sql.NullFloat64
        diVBatR                           sql.NullFloat64
        diMotorCurrentR                   sql.NullFloat64
        latitude                          sql.NullFloat64
        longitude                         sql.NullFloat64
        gpsState                          sql.NullString
        gpsHeading                        sql.NullFloat64
        numBrickVoltageMax                sql.NullInt64
        brickVoltageMax                   sql.NullFloat64
        numBrickVoltageMin                sql.NullInt64
        brickVoltageMin                   sql.NullFloat64
        numModuleTempMax                  sql.NullInt64
        moduleTempMax                     sql.NullFloat64
        numModuleTempMin                  sql.NullInt64
        moduleTempMin                     sql.NullFloat64
        ratedRange                        sql.NullFloat64
        hvilStatus                        sql.NullString
        dcChargingEnergyIn                sql.NullFloat64
        dcChargingPower                   sql.NullFloat64
        acChargingEnergyIn                sql.NullFloat64
        acChargingPower                   sql.NullFloat64
        chargeLimitSoc                    sql.NullFloat64
        fastChargerPresent                sql.NullBool
        estBatteryRange                   sql.NullFloat64
        idealBatteryRange                 sql.NullFloat64
        batteryLevel                      sql.NullInt64
        timeToFullCharge                  sql.NullFloat64
        scheduledChargingStartTime        sql.NullTime
        scheduledChargingPending          sql.NullBool
        scheduledDepartureTime            sql.NullTime
        preconditioningEnabled            sql.NullBool
        scheduledChargingMode             sql.NullString
        chargeAmps                        sql.NullFloat64
        chargeEnableRequest               sql.NullBool
        chargerPhases                     sql.NullInt64
        chargePortColdWeatherMode         sql.NullBool
        chargeCurrentRequest              sql.NullFloat64
        chargeCurrentRequestMax           sql.NullFloat64
        batteryHeaterOn                   sql.NullBool
        notEnoughPowerToHeat              sql.NullBool
        superchargerSessionTripPlanner    sql.NullBool
        doorState                         sql.NullBool
        locked                            sql.NullBool
        fdWindow                          sql.NullString
        fpWindow                          sql.NullString
        rdWindow                          sql.NullString
        rpWindow                          sql.NullString
        driverFrontDoor                   sql.NullBool
        passengerFrontDoor                sql.NullBool
        driverRearDoor                    sql.NullBool
        passengerRearDoor                 sql.NullBool
        trunkFront                        sql.NullBool
        trunkRear                         sql.NullBool
        vehicleName                       sql.NullString
        sentryModeState                   sql.NullString
        speedLimitMode                    sql.NullBool
        currentLimitMph                   sql.NullFloat64
        version                           sql.NullString
        tpmsPressureFl                    sql.NullFloat64
        tpmsPressureFr                    sql.NullFloat64
        tpmsPressureRl                    sql.NullFloat64
        tpmsPressureRr                    sql.NullFloat64
        insideTemp                        sql.NullFloat64
        outsideTemp                       sql.NullFloat64
        seatHeaterLeft                    sql.NullInt64
        seatHeaterRight                   sql.NullInt64
        seatHeaterRearLeft                sql.NullInt64
        seatHeaterRearRight               sql.NullInt64
        seatHeaterRearCenter              sql.NullInt64
        autoSeatClimateLeft               sql.NullBool
        autoSeatClimateRight              sql.NullBool
        driverSeatBelt                    sql.NullBool
        passengerSeatBelt                 sql.NullBool
        driverSeatOccupied                sql.NullBool
        cruiseState                       sql.NullString
        cruiseSetSpeed                    sql.NullFloat64
        lifetimeEnergyUsed                sql.NullFloat64
        lifetimeEnergyUsedDrive           sql.NullFloat64
        brakePedalPos                     sql.NullFloat64
        routeLastUpdated                  sql.NullTime
        routeLine                         sql.NullString
        milesToArrival                    sql.NullFloat64
        minutesToArrival                  sql.NullFloat64
        originLocation                    sql.NullString
        destinationLocation               sql.NullString
        carType                           sql.NullString
        trim                              sql.NullString
        exteriorColor                     sql.NullString
        roofColor                         sql.NullString
        chargePort                        sql.NullString
        chargePortLatch                   sql.NullString
        guestModeEnabled                  sql.NullBool
        pinToDriveEnabled                 sql.NullBool
        pairedPhoneKeyAndKeyFobQty        sql.NullInt64
        cruiseFollowDistance              sql.NullInt64
        automaticBlindSpotCamera          sql.NullBool
        blindSpotCollisionWarningChime    sql.NullBool
        speedLimitWarning                 sql.NullString
        forwardCollisionWarning           sql.NullString
        laneDepartureAvoidance            sql.NullString
        emergencyLaneDepartureAvoidance   sql.NullBool
        automaticEmergencyBrakingOff      sql.NullBool
        lifetimeEnergyGainedRegen         sql.NullFloat64
        di_state_f                         sql.NullString
        di_state_rel                       sql.NullString
        di_state_rer                       sql.NullString
        di_heatsink_tf                     sql.NullFloat64
        di_heatsink_trel                   sql.NullFloat64
        di_heatsink_trer                   sql.NullFloat64
        di_axle_speed_f                    sql.NullFloat64
        di_axle_speed_rel                  sql.NullFloat64
        di_axle_speed_rer                  sql.NullFloat64
        di_slave_torque_cmd                sql.NullFloat64
        di_torque_actual_r                 sql.NullFloat64
        di_torque_actual_f                 sql.NullFloat64
        di_torque_actual_rel               sql.NullFloat64
        di_torque_actual_rer               sql.NullFloat64
        di_stator_temp_f                   sql.NullFloat64
        di_stator_temp_rel                 sql.NullFloat64
        di_stator_temp_rer                 sql.NullFloat64
        di_vbat_f                         sql.NullFloat64
        di_vbat_rel                       sql.NullFloat64
        di_vbat_rer                       sql.NullFloat64
        di_motor_current_f                 sql.NullFloat64
        di_motor_current_rel               sql.NullFloat64
        di_motor_current_rer               sql.NullFloat64
        energyRemaining                   sql.NullFloat64
        serviceMode                       sql.NullBool
        bmsState                          sql.NullString
        guestModeMobileAccessState        sql.NullString
        destinationName                   sql.NullString
        di_inverter_tr                    sql.NullFloat64
        di_inverter_tf                    sql.NullFloat64
        di_inverter_trel                  sql.NullFloat64
        di_inverter_trer                  sql.NullFloat64
        detailedChargeState               sql.NullString
    )

    // Map Datum keys to variables
    for _, datum := range payload.Data {
        value := datum.Value

        switch datum.Key {
        case protos.Field_VehicleSpeed:
            if v, ok := value.GetValue().(*protos.Value_DoubleValue); ok {
                vehicleSpeed = sql.NullFloat64{Float64: v.DoubleValue}
            }
        case protos.Field_Odometer:
            if v, ok := value.GetValue().(*protos.Value_DoubleValue); ok {
                odometer = sql.NullFloat64{Float64: v.DoubleValue}
            }
        case protos.Field_PackVoltage:
            if v, ok := value.GetValue().(*protos.Value_DoubleValue); ok {
                packVoltage = sql.NullFloat64{Float64: v.DoubleValue}
            }
        case protos.Field_DiAxleSpeedR:
            if v, ok := value.GetValue().(*protos.Value_DoubleValue); ok {
                diAxleSpeedR = sql.NullFloat64{Float64: v.DoubleValue}
            }
        case protos.Field_PackCurrent:
            if v, ok := value.GetValue().(*protos.Value_DoubleValue); ok {
                packCurrent = sql.NullFloat64{Float64: v.DoubleValue}
            }
        case protos.Field_Soc:
            if v, ok := value.GetValue().(*protos.Value_DoubleValue); ok {
                soc = sql.NullFloat64{Float64: v.DoubleValue}
            }
        case protos.Field_DCDCEnable:
            if v, ok := value.GetValue().(*protos.Value_BooleanValue); ok {
                dcdcEnable = sql.NullBool{Bool: v.BooleanValue}
            }
        case protos.Field_Gear:
            if v, ok := value.GetValue().(*protos.Value_ShiftStateValue); ok {
                gearEnum := v.ShiftStateValue
                gear = sql.NullString{String: gearEnum.String()}
            }
        case protos.Field_IsolationResistance:
            if v, ok := value.GetValue().(*protos.Value_DoubleValue); ok {
                isolationResistance = sql.NullFloat64{Float64: v.DoubleValue}
            }
        case protos.Field_PedalPosition:
            if v, ok := value.GetValue().(*protos.Value_DoubleValue); ok {
                pedalPosition = sql.NullFloat64{Float64: v.DoubleValue}
            }
        case protos.Field_BrakePedal:
            if v, ok := value.GetValue().(*protos.Value_BooleanValue); ok {
                brakePedal = sql.NullBool{Bool: v.BooleanValue}
            }
        case protos.Field_DiStateR:
            if v, ok := value.GetValue().(*protos.Value_StringValue); ok {
                diStateR = sql.NullString{String: v.StringValue}
            }
        case protos.Field_DiHeatsinkTR:
            if v, ok := value.GetValue().(*protos.Value_DoubleValue); ok {
                diHeatsinkTR = sql.NullFloat64{Float64: v.DoubleValue}
            }
        case protos.Field_CruiseState:
            if v, ok := value.GetValue().(*protos.Value_CruiseStateValue); ok {
                cruiseStateEnum := v.CruiseStateValue
                cruiseState = sql.NullString{String: cruiseStateEnum.String()}
            }
        case protos.Field_DetailedChargeState:
            if v, ok := value.GetValue().(*protos.Value_DetailedChargeStateValue); ok {
                detailedChargeStateEnum := v.DetailedChargeStateValue
                detailedChargeState = sql.NullString{String: detailedChargeStateEnum.String()}
            }
        case protos.Field_Location:
            if v, ok := value.GetValue().(*protos.Value_LocationValue); ok {
                latitude = sql.NullFloat64{Float64: v.LocationValue.GetLatitude()}
                longitude = sql.NullFloat64{Float64: v.LocationValue.GetLongitude()}
            }
        case protos.Field_DoorState:
            if v, ok := value.GetValue().(*protos.Value_DoorValue); ok {
                doors := v.DoorValue
                driverFrontDoor = sql.NullBool{Bool: doors.DriverFront}
                passengerFrontDoor = sql.NullBool{Bool: doors.PassengerFront}
                driverRearDoor = sql.NullBool{Bool: doors.DriverRear}
                passengerRearDoor = sql.NullBool{Bool: doors.PassengerRear}
                trunkFront = sql.NullBool{Bool: doors.TrunkFront}
                trunkRear = sql.NullBool{Bool: doors.TrunkRear}
            }
        // Continue mapping for each additional field as defined in your .proto
        default:
            // Handle unknown fields if necessary
        }
    }

    // Execute the insert query
    _, err := db.Exec(insertQuery,
        vin,
        createdAt,
        vehicleSpeed.Float64,
        odometer.Float64,
        packVoltage.Float64,
        packCurrent.Float64,
        soc.Float64,
        dcdcEnable.Bool,
        gear.String,
        isolationResistance.Float64,
        pedalPosition.Float64,
        brakePedal.Bool,
        diStateR.String,
        diHeatsinkTR.Float64,
        diAxleSpeedR.Float64,
        diTorquemotor.Float64,
        diStatorTempR.Float64,
        diVBatR.Float64,
        diMotorCurrentR.Float64,
        latitude.Float64,
        longitude.Float64,
        gpsState.String,
        gpsHeading.Float64,
        numBrickVoltageMax.Int64,
        brickVoltageMax.Float64,
        numBrickVoltageMin.Int64,
        brickVoltageMin.Float64,
        numModuleTempMax.Int64,
        moduleTempMax.Float64,
        numModuleTempMin.Int64,
        moduleTempMin.Float64,
        ratedRange.Float64,
        hvilStatus.String,
        dcChargingEnergyIn.Float64,
        dcChargingPower.Float64,
        acChargingEnergyIn.Float64,
        acChargingPower.Float64,
        chargeLimitSoc.Float64,
        fastChargerPresent.Bool,
        estBatteryRange.Float64,
        idealBatteryRange.Float64,
        batteryLevel.Int64,
        timeToFullCharge.Float64,
        scheduledChargingStartTime.Time,
        scheduledChargingPending.Bool,
        scheduledDepartureTime.Time,
        preconditioningEnabled.Bool,
        scheduledChargingMode.String,
        chargeAmps.Float64,
        chargeEnableRequest.Bool,
        chargerPhases.Int64,
        chargePortColdWeatherMode.Bool,
        chargeCurrentRequest.Float64,
        chargeCurrentRequestMax.Float64,
        batteryHeaterOn.Bool,
        notEnoughPowerToHeat.Bool,
        superchargerSessionTripPlanner.Bool,
        doorState.Bool,
        locked.Bool,
        fdWindow.String,
        fpWindow.String,
        rdWindow.String,
        rpWindow.String,
        driverFrontDoor.Bool,
        passengerFrontDoor.Bool,
        driverRearDoor.Bool,
        passengerRearDoor.Bool,
        trunkFront.Bool,
        trunkRear.Bool,
        vehicleName.String,
        sentryModeState.String,
        speedLimitMode.Bool,
        currentLimitMph.Float64,
        version.String,
        tpmsPressureFl.Float64,
        tpmsPressureFr.Float64,
        tpmsPressureRl.Float64,
        tpmsPressureRr.Float64,
        insideTemp.Float64,
        outsideTemp.Float64,
        seatHeaterLeft.Int64,
        seatHeaterRight.Int64,
        seatHeaterRearLeft.Int64,
        seatHeaterRearRight.Int64,
        seatHeaterRearCenter.Int64,
        autoSeatClimateLeft.Bool,
        autoSeatClimateRight.Bool,
        driverSeatBelt.Bool,
        passengerSeatBelt.Bool,
        driverSeatOccupied.Bool,
        cruiseState.String,
        cruiseSetSpeed.Float64,
        lifetimeEnergyUsed.Float64,
        lifetimeEnergyUsedDrive.Float64,
        brakePedalPos.Float64,
        routeLastUpdated.Time,
        routeLine.String,
        milesToArrival.Float64,
        minutesToArrival.Float64,
        originLocation.String,
        destinationLocation.String,
        carType.String,
        trim.String,
        exteriorColor.String,
        roofColor.String,
        chargePort.String,
        chargePortLatch.String,
        guestModeEnabled.Bool,
        pinToDriveEnabled.Bool,
        pairedPhoneKeyAndKeyFobQty.Int64,
        cruiseFollowDistance.Int64,
        automaticBlindSpotCamera.Bool,
        blindSpotCollisionWarningChime.Bool,
        speedLimitWarning.String,
        forwardCollisionWarning.String,
        laneDepartureAvoidance.String,
        emergencyLaneDepartureAvoidance.Bool,
        automaticEmergencyBrakingOff.Bool,
        lifetimeEnergyGainedRegen.Float64,
        di_state_f.String,
        di_state_rel.String,
        di_state_rer.String,
        di_heatsink_tf.Float64,
        di_heatsink_trel.Float64,
        di_heatsink_trer.Float64,
        di_axle_speed_f.Float64,
        di_axle_speed_rel.Float64,
        di_axle_speed_rer.Float64,
        di_slave_torque_cmd.Float64,
        di_torque_actual_r.Float64,
        di_torque_actual_f.Float64,
        di_torque_actual_rel.Float64,
        di_torque_actual_rer.Float64,
        di_stator_temp_f.Float64,
        di_stator_temp_rel.Float64,
        di_stator_temp_rer.Float64,
        di_vbat_f.Float64,
        di_vbat_rel.Float64,
        di_vbat_rer.Float64,
        di_motor_current_f.Float64,
        di_motor_current_rel.Float64,
        di_motor_current_rer.Float64,
        energyRemaining.Float64,
        serviceMode.Bool,
        bmsState.String,
        guestModeMobileAccessState.String,
        destinationName.String,
        di_inverter_tr.Float64,
        di_inverter_tf.Float64,
        di_inverter_trel.Float64,
        di_inverter_trer.Float64,
        detailedChargeState.String,
    )
    if err != nil {
        return fmt.Errorf("failed to insert telemetry data: %w", err)
    }

    return nil
}

func mapCruiseStateString(value string) float64 {
    switch value {
    case "Unknown":
        return 0
    case "Off":
        return 1
    case "Standby":
        return 2
    case "On":
        return 3
    case "Standstill":
        return 4
    case "Override":
        return 5
    case "Fault":
        return 6
    case "PreFault":
        return 7
    case "PreCancel":
        return 8
    default:
        log.Printf("Unhandled CruiseState string value '%s'", value)
        return 0 // Default to Unknown
    }
}

func mapChargingStateString(value string) float64 {
    switch value {
    case "ChargeStateUnknown":
        return 0
    case "ChargeStateDisconnected":
        return 1
    case "ChargeStateNoPower":
        return 2
    case "ChargeStateStarting":
        return 3
    case "ChargeStateCharging":
        return 4
    case "ChargeStateComplete":
        return 5
    case "ChargeStateStopped":
        return 6
    default:
        log.Printf("Unhandled ChargingState string value '%s'", value)
        return 0 // Default to Unknown
    }
}

// processValue handles different types of Protobuf values and updates Prometheus metrics
func processValue(datum *protos.Datum, service *Service, vin string) {
    fieldName := datum.Key.String()

    // Update Prometheus metrics
    switch v := datum.Value.Value.(type) {
    case *protos.Value_StringValue:
        // Handle fields that should be mapped from string to numeric values
        if fieldName == "CruiseState" {
            cruiseStateValue := mapCruiseStateString(v.StringValue)
            service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(cruiseStateValue)
        } else if fieldName == "ChargingState" {
            chargingStateValue := mapChargingStateString(v.StringValue)
            service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(chargingStateValue)
        } else {
            // For other string values, you might set a label or an info metric
            service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(1)
        }
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
        // Since TimeValue is a complex type, you can break it down
        service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_hour", vin).Set(float64(v.TimeValue.Hour))
        service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_minute", vin).Set(float64(v.TimeValue.Minute))
        service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_second", vin).Set(float64(v.TimeValue.Second))
    case *protos.Value_DetailedChargeStateValue:
        service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.DetailedChargeStateValue))
    case *protos.Value_DoorValue:
        // Doors is a complex type; handle each door status separately
        service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_driver_front", vin).Set(boolToFloat64(v.DoorValue.DriverFront))
        service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_passenger_front", vin).Set(boolToFloat64(v.DoorValue.PassengerFront))
        service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_driver_rear", vin).Set(boolToFloat64(v.DoorValue.DriverRear))
        service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_passenger_rear", vin).Set(boolToFloat64(v.DoorValue.PassengerRear))
        service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_trunk_front", vin).Set(boolToFloat64(v.DoorValue.TrunkFront))
        service.PrometheusMetrics.Gauge.WithLabelValues(fieldName+"_trunk_rear", vin).Set(boolToFloat64(v.DoorValue.TrunkRear))
    case *protos.Value_Invalid:
        log.Printf("Invalid value received for field '%s', setting as NaN", fieldName)
        service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(math.NaN())
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
                processValue(datum, service, vehicleData.Vin)
            }

            // Insert into PostgreSQL if enabled, after processing the Payload
            if service.DB != nil {
                if err := insertTelemetryData(service.DB, vehicleData); err != nil {
                    log.Printf("PostgreSQL insert error for VIN %s: %v", vehicleData.Vin, err)
                }
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

            data, err := io.ReadAll(resp.Body)
            resp.Body.Close() // Ensure the body is closed after reading
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

            // Process each Datum in the Payload
            for _, datum := range vehicleData.Data {
                processValue(datum, service, vehicleData.Vin)
            }

            // Insert into PostgreSQL if enabled, after processing the Payload
            if service.DB != nil {
                if err := insertTelemetryData(service.DB, vehicleData); err != nil {
                    log.Printf("PostgreSQL insert error for VIN %s: %v", vehicleData.Vin, err)
                }
            }

            // Optionally, you can delete the object after processing to prevent reprocessing
            // Uncomment the following lines if you choose to delete processed files
            //
            // deleteInput := &s3.DeleteObjectInput{
            //     Bucket: aws.String(bucket),
            //     Key:    aws.String(key),
            // }
            // _, err = service.S3Client.DeleteObject(deleteInput)
            // if err != nil {
            //     log.Printf("Failed to delete S3 object '%s': %v", key, err)
            // } else {
            //     log.Printf("Deleted S3 object '%s' after processing", key)
            // }
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
