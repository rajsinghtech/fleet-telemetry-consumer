package main

import (
    "encoding/json"
    "fmt"
    "log"

    "github.com/confluentinc/confluent-kafka-go/kafka"
    "google.golang.org/protobuf/proto"

    // Import your generated protobuf package here
    "github.com/teslamotors/fleet-telemetry/protos"
)

func main() {
    // Set up the Kafka consumer configuration
    config := kafka.ConfigMap{
        "bootstrap.servers": "10.0.100.12:31769", // Replace with your broker address
        "group.id":          "fleet-telemetry-consumer-group",
        "auto.offset.reset": "earliest",
    }

    // Create the consumer
    consumer, err := kafka.NewConsumer(&config)
    if err != nil {
        log.Fatalf("Failed to create consumer: %s", err)
    }
    defer consumer.Close()

    // Get topics
    metadata, err := consumer.GetMetadata(nil, true, 10000)
    if err != nil {
        log.Fatalf("Failed to get metadata: %s", err)
    }

    fmt.Println("Available topics:")
    for topic := range metadata.Topics {
        fmt.Println(topic)
    }
	
    // Subscribe to the topic
    err = consumer.Subscribe("tesla_V", nil)
    if err != nil {
        log.Fatalf("Failed to subscribe to topic: %s", err)
    }

    // Consume messages
    fmt.Println("Waiting for messages...")

    for {
        msg, err := consumer.ReadMessage(-1)
        if err != nil {
            // Handle errors
            log.Printf("Error while consuming: %v\n", err)
            continue
        }

        // Deserialize the Protobuf message
        vehicleData := &protos.Payload{} // Use the correct message type
        if err := proto.Unmarshal(msg.Value, vehicleData); err != nil {
            log.Printf("Failed to unmarshal Protobuf message: %v\n", err)
            continue
        }

        // Output the message to console
        vehicleDataJSON, err := json.MarshalIndent(vehicleData, "", "  ")
        if err != nil {
            log.Printf("Failed to marshal vehicle data to JSON: %v\n", err)
            continue
        }
        fmt.Printf("Received message: %s\n", vehicleDataJSON)
    }
}