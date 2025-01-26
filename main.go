package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"fleet-telemetry-consumer/db"
	"fleet-telemetry-consumer/models"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html/v2"
)

type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
	IdToken      string `json:"id_token,omitempty"`
	Scope        string `json:"scope,omitempty"`
	State        string `json:"state,omitempty"`
	Issuer       string `json:"issuer,omitempty"`
}

type RegistrationResponse struct {
	ClientID       string      `json:"client_id"`
	Name           string      `json:"name"`
	Description    string      `json:"description"`
	Domain         string      `json:"domain"`
	CA             interface{} `json:"ca"`
	CreatedAt      string      `json:"created_at"`
	UpdatedAt      string      `json:"updated_at"`
	EnterpriseTier string      `json:"enterprise_tier"`
	PublicKey      string      `json:"public_key"`
}

type TeslaAPIResponse struct {
	Response *RegistrationResponse `json:"response"`
	Error    string                `json:"error"`
}

type AuthConfig struct {
	ClientID    string `json:"client_id"`
	RedirectURI string `json:"redirect_uri"`
	State       string `json:"state"`
	Scope       string `json:"scope"`
}

type VehicleResponse struct {
	Response   []Vehicle `json:"response"`
	Pagination struct {
		Previous interface{} `json:"previous"`
		Next     interface{} `json:"next"`
		Current  int         `json:"current"`
		PerPage  int         `json:"per_page"`
		Count    int         `json:"count"`
		Pages    int         `json:"pages"`
	} `json:"pagination"`
	Count int `json:"count"`
}

type Vehicle struct {
	ID             int64       `json:"id"`
	VehicleID      int64       `json:"vehicle_id"`
	VIN            string      `json:"vin"`
	DisplayName    string      `json:"display_name"`
	Color          interface{} `json:"color"`
	AccessType     string      `json:"access_type"`
	GranularAccess struct {
		HidePrivate bool `json:"hide_private"`
	} `json:"granular_access"`
	State           string `json:"state"`
	InService       bool   `json:"in_service"`
	IDS             string `json:"id_s"`
	CalendarEnabled bool   `json:"calendar_enabled"`
	APIVersion      int    `json:"api_version"`
}

// Update telemetry configuration struct
type TelemetryConfig struct {
	Config struct {
		PreferTyped bool     `json:"prefer_typed"`
		Port        int      `json:"port"`
		Exp         int64    `json:"exp"`
		AlertTypes  []string `json:"alert_types"`
		Fields      map[string]struct {
			ResendIntervalSeconds int `json:"resend_interval_seconds"`
			MinimumDelta          int `json:"minimum_delta"`
			IntervalSeconds       int `json:"interval_seconds"`
		} `json:"fields"`
		CA       string `json:"ca"`
		Hostname string `json:"hostname"`
	} `json:"config"`
	VINs []string `json:"vins"`
}

type UserInfo struct {
	Sub         string `json:"sub"`          // User ID
	Name        string `json:"name"`         // Full name
	Email       string `json:"email"`        // Email address
	AccountType string `json:"account_type"` // Type of account (e.g., "person")
	AccountID   string `json:"account_id"`   // Tesla account ID
	Picture     string `json:"picture"`      // Profile picture URL
	Locale      string `json:"locale"`       // User's locale
	CountryCode string `json:"country_code"` // User's country code
	UpdatedAt   int64  `json:"updated_at"`   // Last update timestamp
}

func generateToken() (*TokenResponse, error) {
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", strings.TrimSpace(string(clientID)))
	data.Set("client_secret", strings.TrimSpace(string(clientSecret)))
	data.Set("scope", "openid vehicle_device_data vehicle_cmds vehicle_charging_cmds")
	data.Set("audience", "https://fleet-api.prd.na.vn.cloud.tesla.com")

	resp, err := http.PostForm("https://auth.tesla.com/oauth2/v3/token", data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var tokenResp TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, err
	}

	return &tokenResp, nil
}

func registerPartnerAccount(token string) (*RegistrationResponse, error) {
	// Clean the domain - remove any https:// prefix and ensure lowercase
	domain := strings.TrimSpace(string(domainName))
	domain = strings.ToLower(domain)
	domain = strings.TrimPrefix(domain, "https://")
	domain = strings.TrimPrefix(domain, "http://")
	domain = strings.TrimSuffix(domain, "/")

	log.Printf("Using domain for registration: %s", domain)

	payload := map[string]string{
		"domain":      domain,
		"name":        "Tesla Fleet Telemetry Operator",
		"description": "Fleet telemetry data ingestion service",
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("error marshaling payload: %v", err)
	}

	log.Printf("Registration payload: %s", string(jsonPayload))

	req, err := http.NewRequest("POST",
		"https://fleet-api.prd.na.vn.cloud.tesla.com/api/1/partner_accounts",
		bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	log.Printf("Registration response status: %d", resp.StatusCode)
	log.Printf("Registration response body: %s", string(body))

	var teslaResp TeslaAPIResponse
	if err := json.Unmarshal(body, &teslaResp); err != nil {
		return nil, fmt.Errorf("error parsing response: %v", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("registration failed with status %d: %s", resp.StatusCode, teslaResp.Error)
	}

	if teslaResp.Response == nil {
		return nil, fmt.Errorf("empty response from Tesla API")
	}

	return teslaResp.Response, nil
}

func verifyPublicKey(token, domain string) (*RegistrationResponse, error) {
	// Clean the domain
	domain = strings.TrimSpace(domain)
	domain = strings.ToLower(domain)
	domain = strings.TrimPrefix(domain, "https://")
	domain = strings.TrimPrefix(domain, "http://")
	domain = strings.TrimSuffix(domain, "/")

	url := fmt.Sprintf("https://fleet-api.prd.na.vn.cloud.tesla.com/api/1/partner_accounts/public_key?domain=%s", domain)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating verification request: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making verification request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading verification response: %v", err)
	}

	log.Printf("Public key verification response status: %d", resp.StatusCode)
	log.Printf("Public key verification response body: %s", string(body))

	var teslaResp TeslaAPIResponse
	if err := json.Unmarshal(body, &teslaResp); err != nil {
		return nil, fmt.Errorf("error parsing verification response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("verification failed with status %d: %s", resp.StatusCode, teslaResp.Error)
	}

	if teslaResp.Response == nil {
		return nil, fmt.Errorf("empty verification response from Tesla API")
	}

	return teslaResp.Response, nil
}

var (
	clientID     []byte
	clientSecret []byte
	domainName   []byte
)

func getBaseURL(c *fiber.Ctx) string {
	protocol := "http"
	if c.Protocol() == "https" || c.Get("X-Forwarded-Proto") == "https" {
		protocol = "https"
	}
	return fmt.Sprintf("%s://%s", protocol, c.Hostname())
}

func getRedirectURI(c *fiber.Ctx) string {
	host := c.Hostname()
	// Use exact URIs as registered in Tesla Developer Portal
	if host == "localhost" || host == "localhost:3000" || strings.HasPrefix(host, "127.0.0.1") {
		return "http://localhost:3000/callback"
	}
	return "https://tesla.rajsingh.info/callback"
}

func generateAuthURL(c *fiber.Ctx) string {
	redirectURI := getRedirectURI(c)
	log.Printf("Using redirect URI: %s", redirectURI)

	// Build the URL in the exact same order as the example
	params := url.Values{}
	params.Set("client_id", strings.TrimSpace(string(clientID)))
	params.Set("locale", "en-US")
	params.Set("prompt", "login")
	params.Set("redirect_uri", redirectURI)
	params.Set("response_type", "code")
	params.Set("scope", "openid user_data vehicle_device_data vehicle_cmds vehicle_charging_cmds energy_device_data energy_cmds offline_access")
	params.Set("state", "abc123") // Match the example's state value

	authURL := fmt.Sprintf("https://auth.tesla.com/oauth2/v3/authorize?%s", params.Encode())
	log.Printf("Generated auth URL: %s", authURL)
	return authURL
}

func exchangeAuthCode(code string, c *fiber.Ctx) (*TokenResponse, error) {
	redirectURI := getRedirectURI(c)
	log.Printf("Using redirect URI for token exchange: %s", redirectURI)

	data := url.Values{}
	data.Set("grant_type", "authorization_code")
	data.Set("client_id", strings.TrimSpace(string(clientID)))
	data.Set("client_secret", strings.TrimSpace(string(clientSecret)))
	data.Set("code", code)
	data.Set("redirect_uri", redirectURI)
	data.Set("audience", "https://fleet-api.prd.na.vn.cloud.tesla.com")
	data.Set("scope", "openid offline_access vehicle_device_data vehicle_cmds vehicle_charging_cmds energy_device_data energy_cmds")

	req, err := http.NewRequest("POST", "https://auth.tesla.com/oauth2/v3/token", strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	log.Printf("Token exchange response status: %d", resp.StatusCode)
	log.Printf("Token exchange response body: %s", string(body))

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token exchange failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp TokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("error parsing response: %v", err)
	}

	return &tokenResp, nil
}

func getVehicles(token string) (*VehicleResponse, error) {
	req, err := http.NewRequest("GET", "https://fleet-api.prd.na.vn.cloud.tesla.com/api/1/vehicles", nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	log.Printf("Vehicles response status: %d", resp.StatusCode)
	log.Printf("Vehicles response body: %s", string(body))

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get vehicles with status %d: %s", resp.StatusCode, string(body))
	}

	var vehicleResp VehicleResponse
	if err := json.Unmarshal(body, &vehicleResp); err != nil {
		return nil, fmt.Errorf("error parsing response: %v", err)
	}

	return &vehicleResp, nil
}

func getVirtualKeyURL() string {
	domain := strings.TrimSpace(string(domainName))
	domain = strings.ToLower(domain)
	domain = strings.TrimPrefix(domain, "https://")
	domain = strings.TrimPrefix(domain, "http://")
	domain = strings.TrimSuffix(domain, "/")
	return fmt.Sprintf("https://www.tesla.com/_ak/%s", domain)
}

// Update configure telemetry function
func configureTelemetry(vin, accessToken string) error {
	// Tesla's proxy runs on localhost:4443 with HTTPS
	proxyURL := "https://localhost:4443"

	// Read the CA certificate
	caCert, err := os.ReadFile("./secrets/ssl/tls.crt")
	if err != nil {
		return fmt.Errorf("error reading CA certificate: %v", err)
	}

	config := TelemetryConfig{}
	config.Config.PreferTyped = true
	config.Config.Port = 8443
	config.Config.Exp = 1769366508 // Set to just below the maximum allowed timestamp
	config.Config.AlertTypes = []string{"service"}
	config.Config.Fields = map[string]struct {
		ResendIntervalSeconds int `json:"resend_interval_seconds"`
		MinimumDelta          int `json:"minimum_delta"`
		IntervalSeconds       int `json:"interval_seconds"`
	}{
		"Location": {
			IntervalSeconds: 15,
		},
		"GpsHeading": {
			IntervalSeconds: 15,
		},
		"Odometer": {
			IntervalSeconds: 600,
		},
		"VehicleSpeed": {
			IntervalSeconds: 30,
		},
		"LateralAcceleration": {
			IntervalSeconds: 30,
		},
		"MilesToArrival": {
			IntervalSeconds: 120,
		},
		"LifetimeEnergyUsedDrive": {
			IntervalSeconds: 120,
		},
		"DestinationLocation": {
			IntervalSeconds: 120,
		},
		"EstBatteryRange": {
			IntervalSeconds: 30,
		},
		"LongitudinalAcceleration": {
			IntervalSeconds: 30,
		},
	}
	config.Config.CA = string(caCert)
	config.Config.Hostname = "fleet-telemetry.tesla.rajsingh.info"
	config.VINs = []string{vin}

	jsonData, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("error marshaling config: %v", err)
	}

	// Log the request payload for debugging
	log.Printf("Telemetry configuration request: %s", string(jsonData))

	url := fmt.Sprintf("%s/api/1/vehicles/fleet_telemetry_config", proxyURL)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	// Create a custom HTTP client that skips TLS verification for localhost
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Skip verification since it's localhost
		},
	}
	client := &http.Client{Transport: tr}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to configure telemetry with status %d: %s", resp.StatusCode, string(body))
	}

	// Log the response for debugging
	log.Printf("Telemetry configuration response: %s", string(body))

	return nil
}

func getUserInfo(accessToken string) (*UserInfo, error) {
	req, err := http.NewRequest("GET", "https://auth.tesla.com/oauth2/v3/userinfo", nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+accessToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	log.Printf("User info response status: %d", resp.StatusCode)
	log.Printf("User info response body: %s", string(body))

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get user info with status %d: %s", resp.StatusCode, string(body))
	}

	var userInfo UserInfo
	if err := json.Unmarshal(body, &userInfo); err != nil {
		return nil, fmt.Errorf("error parsing response: %v", err)
	}

	return &userInfo, nil
}

func main() {
	var err error
	// Read credentials
	clientID, err = os.ReadFile("./secrets/fleet-api/CLIENT_ID")
	if err != nil {
		log.Fatal("Error reading CLIENT_ID:", err)
	}
	clientSecret, err = os.ReadFile("./secrets/fleet-api/CLIENT_SECRET")
	if err != nil {
		log.Fatal("Error reading CLIENT_SECRET:", err)
	}
	domainName, err = os.ReadFile("./secrets/fleet-api/DOMAIN")
	if err != nil {
		log.Fatal("Error reading DOMAIN:", err)
	}

	// Read database credentials from secrets
	dbHost, err := os.ReadFile("./secrets/pg/host")
	if err != nil {
		log.Fatal("Error reading database host:", err)
	}
	dbUser, err := os.ReadFile("./secrets/pg/user")
	if err != nil {
		log.Fatal("Error reading database user:", err)
	}
	dbPass, err := os.ReadFile("./secrets/pg/password")
	if err != nil {
		log.Fatal("Error reading database password:", err)
	}
	dbName, err := os.ReadFile("./secrets/pg/dbname")
	if err != nil {
		log.Fatal("Error reading database name:", err)
	}
	dbPortBytes, err := os.ReadFile("./secrets/pg/port")
	if err != nil {
		log.Fatal("Error reading database port:", err)
	}

	// Initialize database with credentials from secrets
	port := 5432
	if p, err := strconv.Atoi(strings.TrimSpace(string(dbPortBytes))); err == nil {
		port = p
	}
	err = db.InitDB(
		strings.TrimSpace(string(dbHost)),
		strings.TrimSpace(string(dbUser)),
		strings.TrimSpace(string(dbPass)),
		strings.TrimSpace(string(dbName)),
		port,
	)
	if err != nil {
		log.Fatal("Error initializing database:", err)
	}

	// Create a new engine
	engine := html.New("./views", ".html")

	// Add template functions
	engine.AddFunc("add", func(a, b int) int {
		return a + b
	})

	// Create new Fiber app with template engine
	app := fiber.New(fiber.Config{
		Views: engine,
	})

	// Serve static files
	app.Static("/secrets/fleet-api", "./secrets/fleet-api")

	// Serve the public key at the specified path
	app.Get("/.well-known/appspecific/com.tesla.3p.public-key.pem", func(c *fiber.Ctx) error {
		return c.SendFile("./secrets/fleet-api/public-key.pem")
	})

	// Generate token and register partner account endpoint
	app.Post("/generate-token", func(c *fiber.Ctx) error {
		token, err := generateToken()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Token generation failed: " + err.Error(),
			})
		}

		// Register partner account using the token
		regResp, err := registerPartnerAccount(token.AccessToken)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Registration failed: " + err.Error(),
				"token": token,
			})
		}

		// Verify the public key registration
		verifyResp, err := verifyPublicKey(token.AccessToken, string(domainName))
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error":        "Public key verification failed: " + err.Error(),
				"token":        token,
				"registration": regResp,
			})
		}

		return c.JSON(fiber.Map{
			"token":        token,
			"registration": regResp,
			"verification": verifyResp,
		})
	})

	// Add a separate endpoint for verification only
	app.Post("/verify-key", func(c *fiber.Ctx) error {
		token, err := generateToken()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Token generation failed: " + err.Error(),
			})
		}

		verifyResp, err := verifyPublicKey(token.AccessToken, string(domainName))
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Public key verification failed: " + err.Error(),
				"token": token,
			})
		}

		return c.JSON(fiber.Map{
			"token":        token,
			"verification": verifyResp,
		})
	})

	// Add the authorization endpoint
	app.Get("/auth", func(c *fiber.Ctx) error {
		authURL := generateAuthURL(c)
		log.Printf("Generated auth URL: %s", authURL)
		return c.Redirect(authURL)
	})

	// Add the callback endpoint
	app.Get("/callback", func(c *fiber.Ctx) error {
		code := c.Query("code")
		state := c.Query("state")

		if state != "abc123" {
			return c.Status(400).JSON(fiber.Map{
				"error": "Invalid state parameter",
			})
		}

		if code == "" {
			return c.Status(400).JSON(fiber.Map{
				"error": "No authorization code provided",
			})
		}

		token, err := exchangeAuthCode(code, c)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to exchange authorization code: " + err.Error(),
			})
		}

		// Get vehicles after obtaining the token
		vehicles, err := getVehicles(token.AccessToken)
		if err != nil {
			log.Printf("Warning: Failed to get vehicles: %v", err)
		}

		// Get user information
		userInfo, err := getUserInfo(token.AccessToken)
		if err != nil {
			log.Printf("Warning: Failed to get user info: %v", err)
		}

		// Store the Tesla account in the database
		teslaAccount := &models.TeslaAccount{
			AccessToken:  token.AccessToken,
			RefreshToken: token.RefreshToken,
			TokenType:    token.TokenType,
			ExpiresIn:    token.ExpiresIn,
			ExpiresAt:    time.Now().Add(time.Duration(token.ExpiresIn) * time.Second),
			Scope:        token.Scope,
			State:        token.State,
			LastSyncedAt: time.Now(),
		}

		// Add user information if available
		if userInfo != nil {
			teslaAccount.UserID = userInfo.Sub
			teslaAccount.Name = userInfo.Name
			teslaAccount.Email = userInfo.Email
			teslaAccount.AccountType = userInfo.AccountType
			teslaAccount.TeslaID = userInfo.AccountID
			teslaAccount.Picture = userInfo.Picture
			teslaAccount.Locale = userInfo.Locale
			teslaAccount.CountryCode = userInfo.CountryCode
		}

		if err := db.CreateOrUpdateTeslaAccount(teslaAccount); err != nil {
			log.Printf("Warning: Failed to store Tesla account: %v", err)
		}

		// Store the vehicles in the database
		for _, v := range vehicles.Response {
			teslaVehicle := &models.TeslaVehicle{
				AccountID:     teslaAccount.ID,
				TeslaID:       v.ID,
				VehicleID:     v.VehicleID,
				VIN:           v.VIN,
				DisplayName:   v.DisplayName,
				State:         v.State,
				InService:     v.InService,
				APIVersion:    v.APIVersion,
				AccessType:    v.AccessType,
				HasVirtualKey: !v.GranularAccess.HidePrivate, // If nothing is hidden, we likely have a virtual key
				LastSyncedAt:  time.Now(),
			}

			if err := db.CreateOrUpdateTeslaVehicle(teslaVehicle); err != nil {
				log.Printf("Warning: Failed to store vehicle %s: %v", v.VIN, err)
			}
		}

		// Set success cookie and redirect for browser requests
		c.Cookie(&fiber.Cookie{
			Name:    "auth_success",
			Value:   "true",
			Expires: time.Now().Add(5 * time.Second),
		})

		// Only return JSON if explicitly requested
		if c.Get("Accept") == "application/json" {
			return c.JSON(fiber.Map{
				"message":  "Successfully obtained user access token",
				"token":    token,
				"vehicles": vehicles.Response,
			})
		}

		// Default to redirect for all other requests (browsers)
		return c.Redirect("/dashboard")
	})

	// Update dashboard endpoint to handle success message
	app.Get("/dashboard", func(c *fiber.Ctx) error {
		accounts, err := db.GetAllTeslaAccounts()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to fetch accounts: " + err.Error(),
			})
		}

		// Check for auth success message
		showSuccess := c.Cookies("auth_success") == "true"

		return c.Render("dashboard", fiber.Map{
			"Title":       "Tesla Fleet Dashboard",
			"Accounts":    accounts,
			"ShowSuccess": showSuccess,
		})
	})

	// Add programming endpoint
	app.Post("/api/vehicles/:vin/program", func(c *fiber.Ctx) error {
		vin := c.Params("vin")

		// Find the vehicle in the database
		var vehicle models.TeslaVehicle
		result := db.DB.Where("vin = ?", vin).First(&vehicle)
		if result.Error != nil {
			return c.Status(404).JSON(fiber.Map{
				"error": "Vehicle not found",
			})
		}

		// Get the associated account
		var account models.TeslaAccount
		result = db.DB.First(&account, vehicle.AccountID)
		if result.Error != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to find associated account",
			})
		}

		// Configure telemetry using the account's access token
		err := configureTelemetry(vin, account.AccessToken)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to configure telemetry: " + err.Error(),
			})
		}

		// Update vehicle in database to mark as programmed
		vehicle.LastSyncedAt = time.Now()
		db.DB.Save(&vehicle)

		return c.JSON(fiber.Map{
			"message": "Vehicle successfully programmed for telemetry",
			"vin":     vin,
		})
	})

	// Add endpoint to get telemetry configuration
	app.Get("/api/vehicles/:vin/telemetry", func(c *fiber.Ctx) error {
		vin := c.Params("vin")

		// Find the vehicle in the database
		var vehicle models.TeslaVehicle
		dbResult := db.DB.Where("vin = ?", vin).First(&vehicle)
		if dbResult.Error != nil {
			return c.Status(404).JSON(fiber.Map{
				"error": "Vehicle not found",
			})
		}

		// Get the associated account
		var account models.TeslaAccount
		dbResult = db.DB.First(&account, vehicle.AccountID)
		if dbResult.Error != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to find associated account",
			})
		}

		// Create custom HTTP client that skips TLS verification for localhost
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		client := &http.Client{Transport: tr}

		// Make request to Tesla's proxy to get telemetry config
		url := fmt.Sprintf("https://localhost:4443/api/1/vehicles/%s/fleet_telemetry_config", vin)
		log.Printf("Getting telemetry config from: %s", url)

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to create request: " + err.Error(),
			})
		}

		req.Header.Set("Authorization", "Bearer "+account.AccessToken)

		resp, err := client.Do(req)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to get telemetry config: " + err.Error(),
			})
		}
		defer resp.Body.Close()

		// Read and log the response body
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to read response: " + err.Error(),
			})
		}
		log.Printf("Get telemetry config response (status %d): %s", resp.StatusCode, string(body))

		if resp.StatusCode != http.StatusOK {
			return c.Status(resp.StatusCode).JSON(fiber.Map{
				"error": fmt.Sprintf("Failed to get telemetry config with status %d: %s", resp.StatusCode, string(body)),
			})
		}

		// Parse and return the response
		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to parse response: " + err.Error(),
			})
		}

		return c.JSON(result)
	})

	// Add endpoint to delete telemetry configuration
	app.Delete("/api/vehicles/:vin/telemetry", func(c *fiber.Ctx) error {
		vin := c.Params("vin")

		// Find the vehicle in the database
		var vehicle models.TeslaVehicle
		dbResult := db.DB.Where("vin = ?", vin).First(&vehicle)
		if dbResult.Error != nil {
			return c.Status(404).JSON(fiber.Map{
				"error": "Vehicle not found",
			})
		}

		// Get the associated account
		var account models.TeslaAccount
		dbResult = db.DB.First(&account, vehicle.AccountID)
		if dbResult.Error != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to find associated account",
			})
		}

		// Create custom HTTP client that skips TLS verification for localhost
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		client := &http.Client{Transport: tr}

		// Make request to Tesla's proxy to delete telemetry config
		url := fmt.Sprintf("https://localhost:4443/api/1/vehicles/%s/fleet_telemetry_config", vin)
		log.Printf("Deleting telemetry config from: %s", url)

		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to create request: " + err.Error(),
			})
		}

		req.Header.Set("Authorization", "Bearer "+account.AccessToken)

		resp, err := client.Do(req)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to delete telemetry config: " + err.Error(),
			})
		}
		defer resp.Body.Close()

		// Read and log the response body
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to read response: " + err.Error(),
			})
		}
		log.Printf("Delete telemetry config response (status %d): %s", resp.StatusCode, string(body))

		if resp.StatusCode != http.StatusOK {
			return c.Status(resp.StatusCode).JSON(fiber.Map{
				"error": fmt.Sprintf("Failed to delete telemetry config with status %d: %s", resp.StatusCode, string(body)),
			})
		}

		return c.JSON(fiber.Map{
			"message": "Telemetry configuration successfully deleted",
			"vin":     vin,
		})
	})

	// Serve the main page
	app.Get("/", func(c *fiber.Ctx) error {
		return c.Render("index", fiber.Map{
			"Title": "Tesla Fleet Telemetry",
		})
	})

	log.Fatal(app.Listen(":3000"))
}
