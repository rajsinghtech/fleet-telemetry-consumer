package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

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

	resp, err := http.PostForm("https://auth.tesla.com/oauth2/v3/token", data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	log.Printf("Token exchange response status: %d", resp.StatusCode)
	log.Printf("Token exchange response body: %s", string(body))

	var tokenResp TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, err
	}

	return &tokenResp, nil
}

func main() {
	var err error
	// Read credentials
	clientID, err = os.ReadFile("static/CLIENT_ID")
	if err != nil {
		log.Fatal("Error reading CLIENT_ID:", err)
	}
	clientSecret, err = os.ReadFile("static/CLIENT_SECRET")
	if err != nil {
		log.Fatal("Error reading CLIENT_SECRET:", err)
	}
	domainName, err = os.ReadFile("static/DOMAIN")
	if err != nil {
		log.Fatal("Error reading DOMAIN:", err)
	}

	// Create a new engine
	engine := html.New("./views", ".html")

	// Create new Fiber app with template engine
	app := fiber.New(fiber.Config{
		Views: engine,
	})

	// Serve static files
	app.Static("/static", "./static")

	// Serve the public key at the specified path
	app.Get("/.well-known/appspecific/com.tesla.3p.public-key.pem", func(c *fiber.Ctx) error {
		return c.SendFile("./static/public-key.pem")
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

		if state != "abc123" { // Match the state from the auth request
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

		// Check if the request accepts JSON
		accepts := c.Accepts("application/json")
		if accepts == "application/json" {
			return c.JSON(fiber.Map{
				"token":   token,
				"message": "Successfully obtained user access token",
			})
		}

		// Otherwise, render the success page
		return c.Render("callback", fiber.Map{
			"Title": "Authorization Successful",
			"Token": token,
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
