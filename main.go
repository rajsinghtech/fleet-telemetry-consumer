package main

import (
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html/v2"
)

func main() {
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

	// Serve the main page
	app.Get("/", func(c *fiber.Ctx) error {
		return c.Render("index", fiber.Map{
			"Title": "Tesla Fleet Telemetry",
		})
	})

	log.Fatal(app.Listen(":3000"))
}
