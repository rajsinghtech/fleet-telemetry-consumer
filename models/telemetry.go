package models

import (
	"time"
)

// TelemetryData represents a single telemetry data point
type TelemetryData struct {
	ID        uint      `gorm:"primaryKey"`
	VIN       string    `gorm:"index"`
	Key       string    `gorm:"index"`
	CreatedAt time.Time `gorm:"index"`

	// Value types (only one will be non-null)
	StringValue *string `gorm:"type:text"`
	IntValue    *int32
	LongValue   *int64
	FloatValue  *float32
	DoubleValue *float64
	BoolValue   *bool

	// Location specific fields
	Latitude  *float64
	Longitude *float64

	// Special enum values
	ChargingState               *string
	ShiftState                  *string
	LaneAssistLevel             *string
	ScheduledChargingMode       *string
	SentryModeState             *string
	SpeedAssistLevel            *string
	BMSState                    *string
	BuckleStatus                *string
	CarType                     *string
	ChargePort                  *string
	ChargePortLatch             *string
	DriveInverterState          *string
	HvilStatus                  *string
	WindowState                 *string
	SeatFoldPosition            *string
	TractorAirStatus            *string
	FollowDistance              *string
	ForwardCollisionSensitivity *string
	GuestModeMobileAccess       *string
	TrailerAirStatus            *string
	DetailedChargeState         *string
	HvacAutoMode                *string
	ClimateKeeperMode           *string
	HvacPowerState              *string
	FastCharger                 *string
	CableType                   *string
	TonneauTentMode             *string
	TonneauPosition             *string
	PowershareType              *string
	PowershareState             *string
	PowershareStopReason        *string
	DisplayState                *string
	DistanceUnit                *string
	TemperatureUnit             *string
	PressureUnit                *string
	ChargeUnitPreference        *string

	// Foreign key to TeslaVehicle
	VehicleID uint `gorm:"index"`
}
