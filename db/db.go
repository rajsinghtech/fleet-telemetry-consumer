package db

import (
	"fleet-telemetry-consumer/models"
	"fmt"
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

	// Create tables with new schema
	err = DB.AutoMigrate(&models.TeslaAccount{}, &models.TeslaVehicle{})
	if err != nil {
		return fmt.Errorf("failed to migrate database: %v", err)
	}

	return nil
}

// CreateOrUpdateTeslaAccount creates or updates a Tesla account in the database
func CreateOrUpdateTeslaAccount(account *models.TeslaAccount) error {
	// Try to find an existing account with the same UserID
	var existingAccount models.TeslaAccount
	result := DB.Where("user_id = ?", account.UserID).First(&existingAccount)

	if result.Error == nil {
		// Update existing account
		account.ID = existingAccount.ID
		return DB.Save(account).Error
	} else if result.Error == gorm.ErrRecordNotFound {
		// Create new account
		return DB.Create(account).Error
	}

	return result.Error
}

// CreateOrUpdateTeslaVehicle creates or updates a Tesla vehicle in the database
func CreateOrUpdateTeslaVehicle(vehicle *models.TeslaVehicle) error {
	// Try to find an existing vehicle with the same VIN
	var existingVehicle models.TeslaVehicle
	result := DB.Where("vin = ?", vehicle.VIN).First(&existingVehicle)

	if result.Error == nil {
		// Update existing vehicle
		vehicle.ID = existingVehicle.ID
		return DB.Save(vehicle).Error
	} else if result.Error == gorm.ErrRecordNotFound {
		// Create new vehicle
		return DB.Create(vehicle).Error
	}

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
