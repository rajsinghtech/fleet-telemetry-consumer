package telemetry

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/teslamotors/fleet-telemetry/protos"
	"google.golang.org/protobuf/proto"
)

type Consumer struct {
	consumer *kafka.Consumer
	topic    string
	running  bool
}

func NewConsumer(brokers, topic, groupID string) (*Consumer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %v", err)
	}

	return &Consumer{
		consumer: consumer,
		topic:    topic,
	}, nil
}

func (c *Consumer) Start() error {
	if err := c.consumer.Subscribe(c.topic, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %v", c.topic, err)
	}

	log.Printf("Subscribed to topic: %s", c.topic)
	c.running = true

	// Handle graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Handle shutdown in a separate goroutine
	go func() {
		sig := <-sigchan
		log.Printf("Caught signal %v, initiating shutdown...", sig)
		c.running = false
		signal.Stop(sigchan)
		close(sigchan)
	}()

	// Start consuming messages
	log.Println("Starting to consume messages...")
	for c.running {
		msg, err := c.consumer.ReadMessage(100) // Add 100ms timeout
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrTimedOut {
				if !c.running {
					break
				}
				continue
			}
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Decode and handle the message
		if err := c.handleMessage(msg); err != nil {
			log.Printf("Error handling message: %v", err)
		}
	}

	log.Println("Shutting down consumer...")
	return c.consumer.Close()
}

func (c *Consumer) handleMessage(msg *kafka.Message) error {
	// The message value should be raw protobuf data
	// Unmarshal the protobuf message directly
	payload := &protos.Payload{}
	if err := proto.Unmarshal(msg.Value, payload); err != nil {
		return fmt.Errorf("failed to unmarshal protobuf: %v", err)
	}

	// Log the decoded message
	log.Printf("Received payload: %+v", payload)
	return nil
}
