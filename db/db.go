package db

import (
	"fleet-telemetry-consumer/models"
	"fmt"
	"log"

	// "log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

// InitDB initializes the database connection
func InitDB(host, user, password, dbname string, port int) error {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable",
		host, user, password, dbname, port)

	var err error
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	// // Drop existing tables
	// err = DB.Migrator().DropTable(&models.TeslaVehicle{}, &models.TeslaAccount{})
	// if err != nil {
	// 	log.Printf("Warning: Failed to drop tables: %v", err)
	// }

	// Auto migrate the schemas
	err = DB.AutoMigrate(
		&models.TeslaAccount{},
		&models.TeslaVehicle{},
		&models.TelemetryData{},
	)
	if err != nil {
		return fmt.Errorf("failed to migrate database: %v", err)
	}

	log.Println("Database initialized successfully")
	return nil
}

// CreateOrUpdateTeslaAccount creates or updates a Tesla account in the database
func CreateOrUpdateTeslaAccount(account *models.TeslaAccount) error {
	result := DB.Save(account)
	return result.Error
}

// CreateOrUpdateTeslaVehicle creates or updates a Tesla vehicle in the database
func CreateOrUpdateTeslaVehicle(vehicle *models.TeslaVehicle) error {
	result := DB.Save(vehicle)
	return result.Error
}

// GetAllTeslaAccounts retrieves all Tesla accounts from the database
func GetAllTeslaAccounts() ([]models.TeslaAccount, error) {
	var accounts []models.TeslaAccount
	result := DB.Preload("Vehicles").Find(&accounts)
	return accounts, result.Error
}

// GetAllTeslaVehicles retrieves all Tesla vehicles from the database
func GetAllTeslaVehicles() ([]models.TeslaVehicle, error) {
	var vehicles []models.TeslaVehicle
	result := DB.Find(&vehicles)
	return vehicles, result.Error
}

// GetVehiclesByAccount retrieves all vehicles for a specific account
func GetVehiclesByAccount(accountID uint) ([]models.TeslaVehicle, error) {
	var vehicles []models.TeslaVehicle
	result := DB.Where("account_id = ?", accountID).Find(&vehicles)
	return vehicles, result.Error
}

// GetAccountByAccessToken retrieves a Tesla account by its access token
func GetAccountByAccessToken(accessToken string) (*models.TeslaAccount, error) {
	var account models.TeslaAccount
	result := DB.Where("access_token = ?", accessToken).Preload("Vehicles").First(&account)
	if result.Error != nil {
		return nil, result.Error
	}
	return &account, nil
}

// StoreTelemetryData stores a telemetry data point in the database
func StoreTelemetryData(data *models.TelemetryData) error {
	result := DB.Create(data)
	return result.Error
}

// GetTelemetryDataForVehicle retrieves telemetry data for a specific vehicle and key
func GetTelemetryDataForVehicle(vin string, key string, limit int) ([]models.TelemetryData, error) {
	var data []models.TelemetryData
	result := DB.Where("vin = ? AND key = ?", vin, key).
		Order("created_at DESC").
		Limit(limit).
		Find(&data)
	return data, result.Error
}

// GetLatestTelemetryDataForVehicle retrieves the latest telemetry data points for a vehicle
func GetLatestTelemetryDataForVehicle(vin string) (map[string]models.TelemetryData, error) {
	var data []models.TelemetryData
	subQuery := DB.Model(&models.TelemetryData{}).
		Select("DISTINCT ON (key) *").
		Where("vin = ?", vin).
		Order("key, created_at DESC")

	result := DB.Table("(?) as sub", subQuery).
		Find(&data)

	// Convert to map for easier access
	dataMap := make(map[string]models.TelemetryData)
	for _, d := range data {
		dataMap[d.Key] = d
	}

	return dataMap, result.Error
}
