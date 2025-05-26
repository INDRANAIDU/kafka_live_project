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

type OrderEvent struct {
	OrderID   string  `json:"order_id"`
	Customer  string  `json:"customer"`
	Amount    float64 `json:"amount"`
	EventType string  `json:"event_type"`
}

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

var processedOrders = make(map[string]bool)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "OrderConfirmed",
		GroupID: "warehouse-consumer-group",
	})

	writerNotification := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "Notification",
	})

	writerPickedAndPacked := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "OrderPickedAndPacked",
	})

	writerDLQ := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "DeadLetterQueue",
	})

	defer reader.Close()
	defer writerNotification.Close()
	defer writerPickedAndPacked.Close()
	defer writerDLQ.Close()

	fmt.Println("🏭 Warehouse Consumer started...")

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

		var event OrderEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("⚠️ Invalid JSON. Sending to DLQ: %v", err)
			sendError(writerDLQ, string(m.Value), err)
			continue
		}

		if event.EventType != "OrderConfirmed" {
			log.Printf("⚠️ Unknown event type: %s\n", event.EventType)
			continue
		}

		if processedOrders[event.OrderID] {
			log.Printf("🟡 Duplicate order: %s (skipped)\n", event.OrderID)
			continue
		}

		log.Printf("📦 Fulfilling order: %s for %s\n", event.OrderID, event.Customer)
		processedOrders[event.OrderID] = true

		time.Sleep(1 * time.Second)

		// Send Notification event
		notification := NotificationEvent{
			OrderID:   event.OrderID,
			Customer:  event.Customer,
			Message:   "Your order is being fulfilled",
			EventType: "OrderFulfilled",
		}
		notificationMsg, _ := json.Marshal(notification)
		err = writerNotification.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.OrderID),
			Value: notificationMsg,
		})
		if err != nil {
			log.Printf("❌ Failed to publish to Notification. Sending to DLQ.")
			sendError(writerDLQ, string(m.Value), err)
			continue
		}

		// Send OrderPickedAndPacked event to shipper
		pickedPacked := OrderPickedAndPacked{
			OrderID:   event.OrderID,
			Customer:  event.Customer,
			Amount:    event.Amount,
			EventType: "OrderPickedAndPacked",
		}
		pickedPackedMsg, _ := json.Marshal(pickedPacked)
		err = writerPickedAndPacked.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.OrderID),
			Value: pickedPackedMsg,
		})
		if err != nil {
			log.Printf("❌ Failed to publish to OrderPickedAndPacked. Sending to DLQ.")
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
		Key:   []byte("warehouse-error"),
		Value: msg,
	})
}
