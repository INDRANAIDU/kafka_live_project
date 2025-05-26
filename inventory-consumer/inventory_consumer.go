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

type SystemEvent struct {
	Type        string      `json:"type"` // "Error" or "KPI"
	Timestamp   string      `json:"timestamp"`
	FailedEvent *OrderEvent `json:"failed_event,omitempty"`
	Error       string      `json:"error,omitempty"`
	KPIName     string      `json:"kpi_name,omitempty"`
	MetricName  string      `json:"metric_name,omitempty"`
	Value       float64     `json:"value,omitempty"`
}

var processedOrders = make(map[string]bool)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "order-service",
		GroupID:  "inventory-consumer-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	writerConfirmed := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "OrderConfirmed",
	})

	writerSystemEvents := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "system-events", // Unified topic for errors and KPIs
	})

	defer reader.Close()
	defer writerConfirmed.Close()
	defer writerSystemEvents.Close()

	fmt.Println("📦 Inventory Consumer started...")

	ctx, cancel := context.WithCancel(context.Background())

	// Graceful shutdown
	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
		<-sigchan
		fmt.Println("\n🛑 Shutting down gracefully...")
		cancel()
	}()

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context canceled, graceful shutdown
				break
			}
			log.Printf("❌ Failed to read message: %v\n", err)
			sendError(writerSystemEvents, OrderEvent{}, err)
			continue
		}

		var event OrderEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("⚠️ Invalid JSON. Sending to system-events: %v", err)
			sendError(writerSystemEvents, OrderEvent{}, err)
			continue
		}

		if event.EventType != "OrderReceived" {
			log.Printf("⚠️ Unknown event type: %s (skipped)\n", event.EventType)
			continue
		}

		// Idempotency
		if processedOrders[event.OrderID] {
			log.Printf("🟡 Duplicate order: %s (skipped)\n", event.OrderID)
			// Send KPI event for duplicate errors
			sendKPI(writerSystemEvents, "InventoryConsumerErrors", "errors_per_minute", 1)
			continue
		}

		log.Printf("✅ Processing order: %s for %s\n", event.OrderID, event.Customer)
		processedOrders[event.OrderID] = true

		startTime := time.Now()

		// Simulate processing work
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
			log.Printf("❌ Failed to publish to OrderConfirmed. Sending to system-events: %v", err)
			sendError(writerSystemEvents, event, err)
		} else {
			// Send KPI for processing latency
			latency := time.Since(startTime).Seconds()
			sendKPI(writerSystemEvents, "InventoryConsumerLatency", "processing_latency_seconds", latency)
		}
	}
	fmt.Println("🛑 Consumer stopped.")
}

func sendError(writer *kafka.Writer, event OrderEvent, err error) {
	systemEvent := SystemEvent{
		Type:        "Error",
		Timestamp:   time.Now().Format(time.RFC3339),
		FailedEvent: &event,
		Error:       err.Error(),
	}
	msg, _ := json.Marshal(systemEvent)
	e := writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(event.OrderID),
		Value: msg,
	})
	if e != nil {
		log.Printf("❌ Failed to write error to system-events: %v", e)
	} else {
		log.Printf("📥 Sent error to system-events: %s", systemEvent.Error)
	}
}

func sendKPI(writer *kafka.Writer, kpiName, metricName string, value float64) {
	systemEvent := SystemEvent{
		Type:       "KPI",
		Timestamp:  time.Now().Format(time.RFC3339),
		KPIName:    kpiName,
		MetricName: metricName,
		Value:      value,
	}
	msg, _ := json.Marshal(systemEvent)
	e := writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(kpiName),
		Value: msg,
	})
	if e != nil {
		log.Printf("❌ Failed to write KPI to system-events: %v", e)
	} else {
		log.Printf("📈 Sent KPI to system-events: %s = %f", metricName, value)
	}
}
