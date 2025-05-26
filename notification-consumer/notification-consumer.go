// notification-consumer.go
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

var processedNotifications = make(map[string]bool)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "Notification",
		GroupID: "notification-consumer-group",
	})

	writerDLQ := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "DeadLetterQueue",
	})

	defer reader.Close()
	defer writerDLQ.Close()

	fmt.Println("🔔 Notification Consumer started...")

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

		var event NotificationEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("⚠️ Invalid JSON. Sending to DLQ: %v", err)
			sendError(writerDLQ, m.Value, err)
			continue
		}

		// Idempotency
		if processedNotifications[event.OrderID] {
			log.Printf("🟡 Duplicate notification: %s (skipped)\n", event.OrderID)
			continue
		}

		log.Printf("📨 Notification for OrderID %s: %s\n", event.OrderID, event.Message)
		processedNotifications[event.OrderID] = true
	}
}

func sendError(writer *kafka.Writer, event []byte, err error) {
	errorEvent := ErrorEvent{
		FailedEvent: string(event),
		Error:       err.Error(),
		Timestamp:   time.Now().Format(time.RFC3339),
	}
	msg, _ := json.Marshal(errorEvent)
	_ = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("notification-error"),
		Value: msg,
	})
}
