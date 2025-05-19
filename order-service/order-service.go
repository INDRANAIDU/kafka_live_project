package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/segmentio/kafka-go"
)

// Models

// Order represents the incoming order payload from the client
type Order struct {
	ID       string  `json:"id"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
}

// OrderEvent represents the Kafka event structure to be published
type OrderEvent struct {
	OrderID   string  `json:"order_id"`
	Customer  string  `json:"customer"`
	Amount    float64 `json:"amount"`
	EventType string  `json:"event_type"`
}

// Kafka Writer Initialization

var kafkaWriter *kafka.Writer

// init initializes the Kafka writer
func init() {
	kafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        "order-service",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
}

// PublishOrderEvent sends the order event to Kafka
func PublishOrderEvent(event OrderEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.OrderID),
		Value: data,
		Time:  time.Now(),
	}

	err = kafkaWriter.WriteMessages(context.Background(), msg)
	if err != nil {
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}
	return nil
}

// Simple publisher to send a plain text message to Kafka
func publishToKafka(topic, message string) error {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	msg := kafka.Message{
		Key:   []byte("key"),
		Value: []byte(message),
		Time:  time.Now(),
	}

	return writer.WriteMessages(context.Background(), msg)
}

// HTTP Handlers

// HealthCheckHandler confirms service is running
func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Order service is healthy"))
}

// OrderHandler handles incoming order submissions
func OrderHandler(w http.ResponseWriter, r *http.Request) {
	var order Order

	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		log.Printf("Error decoding JSON: %v\n", err)
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	if err := ValidateOrder(order); err != nil {
		log.Printf("Validation error: %v\n", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event := CreateOrderEvent(order)

	if err := PublishOrderEvent(event); err != nil {
		log.Printf("Kafka publish error: %v\n", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Order received and published to Kafka"))
}

// TestSendOrderHandler is a test endpoint to publish a simple event
func TestSendOrderHandler(w http.ResponseWriter, r *http.Request) {
	err := publishToKafka("order-service", "Test order received event")
	if err != nil {
		http.Error(w, "Failed to publish: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte("Event published to Kafka"))
}

// Helper Functions

func ValidateOrder(order Order) error {
	if order.ID == "" || order.Customer == "" || order.Amount <= 0 {
		return fmt.Errorf("invalid order data: all fields required and amount must be positive")
	}
	return nil
}

func CreateOrderEvent(order Order) OrderEvent {
	return OrderEvent{
		OrderID:   order.ID,
		Customer:  order.Customer,
		Amount:    order.Amount,
		EventType: "OrderReceived",
	}
}

// Main

func main() {
	r := chi.NewRouter()

	r.Get("/health", HealthCheckHandler)
	r.Post("/send-order", TestSendOrderHandler)
	r.Post("/order", OrderHandler)

	// Graceful shutdown
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		log.Println("Shutting down server...")
		if err := kafkaWriter.Close(); err != nil {
			log.Printf("Failed to close Kafka writer: %v\n", err)
		}
		os.Exit(0)
	}()

	fmt.Println("Server listening on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Server failed: %v\n", err)
	}
}
