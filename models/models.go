package models

import (
	"time"
)

// TeslaAccount represents a Tesla account in our system
type TeslaAccount struct {
	ID           uint `gorm:"primarykey"`
	CreatedAt    time.Time
	UpdatedAt    time.Time
	AccessToken  string
	RefreshToken string
	TokenType    string
	ExpiresIn    int
	ExpiresAt    time.Time
	Scope        string
	State        string
	LastSyncedAt time.Time
	// User information
	UserID      string `gorm:"uniqueIndex"` // sub from userinfo
	Name        string
	Email       string
	AccountType string
	TeslaID     string // account_id from userinfo
	Picture     string
	Locale      string
	CountryCode string
	// Relationships
	Vehicles []TeslaVehicle `gorm:"foreignKey:AccountID;references:ID"`
}

// TeslaVehicle represents a Tesla vehicle in our system
type TeslaVehicle struct {
	ID            uint `gorm:"primarykey"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
	AccountID     uint         `gorm:"not null"` // Foreign key to TeslaAccount
	Account       TeslaAccount `gorm:"foreignKey:AccountID"`
	TeslaID       int64
	VehicleID     int64
	VIN           string `gorm:"uniqueIndex"`
	DisplayName   string
	State         string
	InService     bool
	APIVersion    int
	AccessType    string
	HasVirtualKey bool
	LastSyncedAt  time.Time
}
