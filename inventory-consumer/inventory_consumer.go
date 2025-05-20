package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Event definitions
type OrderEvent struct {
	OrderID   string  `json:"order_id"`
	Customer  string  `json:"customer"`
	Amount    float64 `json:"amount"`
	EventType string  `json:"event_type"`
}

type ErrorEvent struct {
	FailedEvent OrderEvent `json:"failed_event"`
	Error       string     `json:"error"`
	Timestamp   string     `json:"timestamp"`
}

var processedOrders = make(map[string]bool)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "order-service",
		GroupID:   "inventory-consumer-group",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})

	writerConfirmed := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "OrderConfirmed",
	})

	writerDLQ := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "DeadLetterQueue",
	})

	defer reader.Close()
	defer writerConfirmed.Close()
	defer writerDLQ.Close()

	fmt.Println("📦 Inventory Consumer started...")

	ctx, cancel := context.WithCancel(context.Background())

	// Graceful shutdown
	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
		<-sigchan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("❌ Failed to read message: %v\n", err)
			continue
		}

		var event OrderEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("⚠️ Invalid JSON. Sending to DLQ: %v", err)
			sendError(writerDLQ, event, err)
			continue
		}

		if event.EventType != "OrderReceived" {
			log.Printf("⚠️ Unknown event type: %s\n", event.EventType)
			continue
		}

		// Idempotency
		if processedOrders[event.OrderID] {
			log.Printf("🟡 Duplicate order: %s (skipped)\n", event.OrderID)
			continue
		}

		log.Printf("✅ Processing order: %s for %s\n", event.OrderID, event.Customer)
		processedOrders[event.OrderID] = true

		// Simulate work
		time.Sleep(1 * time.Second)

		// Send OrderConfirmed event
		confirmEvent := OrderEvent{
			OrderID:   event.OrderID,
			Customer:  event.Customer,
			Amount:    event.Amount,
			EventType: "OrderConfirmed",
		}
		msg, _ := json.Marshal(confirmEvent)
		err = writerConfirmed.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.OrderID),
			Value: msg,
		})
		if err != nil {
			log.Printf("❌ Failed to publish to OrderConfirmed. Sending to DLQ.")
			sendError(writerDLQ, event, err)
		}
	}
}

func sendError(writer *kafka.Writer, event OrderEvent, err error) {
	errorEvent := ErrorEvent{
		FailedEvent: event,
		Error:       err.Error(),
		Timestamp:   time.Now().Format(time.RFC3339),
	}
	msg, _ := json.Marshal(errorEvent)
	_ = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(event.OrderID),
		Value: msg,
	})
}
