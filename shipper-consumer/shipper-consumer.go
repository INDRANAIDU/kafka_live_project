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

type OrderPickedAndPacked struct {
	OrderID   string  `json:"order_id"`
	Customer  string  `json:"customer"`
	Amount    float64 `json:"amount"`
	EventType string  `json:"event_type"`
}

type NotificationEvent struct {
	OrderID   string `json:"order_id"`
	Customer  string `json:"customer"`
	Message   string `json:"message"`
	EventType string `json:"event_type"`
}

type ErrorEvent struct {
	FailedEvent interface{} `json:"failed_event"`
	Error       string      `json:"error"`
	Timestamp   string      `json:"timestamp"`
}

var processedShipments = make(map[string]bool)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "OrderPickedAndPacked",
		GroupID: "shipper-consumer-group",
	})

	writerNotification := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "Notification",
	})

	writerDLQ := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "DeadLetterQueue",
	})

	defer reader.Close()
	defer writerNotification.Close()
	defer writerDLQ.Close()

	fmt.Println("🚚 Shipper Consumer started...")

	ctx, cancel := context.WithCancel(context.Background())

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

		var event OrderPickedAndPacked
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("⚠️ Invalid JSON. Sending to DLQ: %v", err)
			sendError(writerDLQ, string(m.Value), err)
			continue
		}

		if event.EventType != "OrderPickedAndPacked" {
			log.Printf("⚠️ Unexpected event type: %s\n", event.EventType)
			continue
		}

		if processedShipments[event.OrderID] {
			log.Printf("🟡 Duplicate shipment: %s (skipped)\n", event.OrderID)
			continue
		}

		log.Printf("✅ Shipping order: %s for %s\n", event.OrderID, event.Customer)
		processedShipments[event.OrderID] = true

		time.Sleep(1 * time.Second)

		notification := NotificationEvent{
			OrderID:   event.OrderID,
			Customer:  event.Customer,
			Message:   "Your order has been shipped!",
			EventType: "OrderShipped",
		}
		msg, _ := json.Marshal(notification)
		err = writerNotification.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.OrderID),
			Value: msg,
		})
		if err != nil {
			log.Printf("❌ Failed to publish notification. Sending to DLQ.")
			sendError(writerDLQ, string(m.Value), err)
		}
	}
}

func sendError(writer *kafka.Writer, event string, err error) {
	errorEvent := ErrorEvent{
		FailedEvent: event,
		Error:       err.Error(),
		Timestamp:   time.Now().Format(time.RFC3339),
	}
	msg, _ := json.Marshal(errorEvent)
	_ = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("shipper-error"),
		Value: msg,
	})
}
