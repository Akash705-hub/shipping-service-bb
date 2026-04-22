package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// StartShippingWorker starts the background listener loop
func StartShippingWorker(service *ShippingService) {
	queueName := getEnvVar("SHIPPING_QUEUE_NAME")
	if queueName == "" {
		log.Fatal("SHIPPING_QUEUE_NAME not set")
	}

	client, err := buildServiceBusClient(&Config{
		ASBConnectionString:      os.Getenv("ASB_CONNECTION_STRING"),
		AzureServiceBusNamespace: os.Getenv("AZURE_SERVICEBUS_FULLYQUALIFIEDNAMESPACE"),
	})
	if err != nil {
		log.Fatalf("ASB connection failed: %v", err)
	}
	defer func() {
		if err := client.Close(context.Background()); err != nil {
			log.Printf("failed to close Service Bus client: %v", err)
		}
	}()

	receiver, err := client.NewReceiverForQueue(queueName, nil)
	if err != nil {
		log.Fatalf("Failed to create receiver: %v", err)
	}
	defer func() {
		if err := receiver.Close(context.Background()); err != nil {
			log.Printf("failed to close receiver: %v", err)
		}
	}()

	ctx := context.Background()
	log.Printf("Shipping Worker listening on queue: %s", queueName)

	for {
		messages, err := receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			log.Printf("Error receiving message: %v. Retrying in 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, msg := range messages {
			processMessage(ctx, service, receiver, msg)
		}
	}
}

// processMessage handles an individual shipping message
func processMessage(ctx context.Context, service *ShippingService, receiver *azservicebus.Receiver, msg *azservicebus.ReceivedMessage) {
	var req ShippingRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		log.Printf("Invalid message format: %v", err)
		abandonMessage(ctx, receiver, msg)
		return
	}

	log.Printf("Processing shipment for Order %s to %s", req.OrderID, req.Shipping.PostalCode)

	duration := calculateDuration(req.Shipping.PostalCode)
	trackingNum := fmt.Sprintf("TN-%04d-%s", rand.Intn(10000), req.OrderID)

	shipmentDetails := ShipmentRecord{
		OrderID:         req.OrderID,
		TrackingNumber:  trackingNum,
		Duration:        duration,
		Destination:     req.Shipping.PostalCode,
		ShippedAt:       time.Now(),
		EstimatedArrive: time.Now().Add(time.Duration(duration) * time.Second),
	}

	if err := service.UpdateShipment(req.OrderID, StatusShipped, shipmentDetails); err != nil {
		log.Printf("Failed to set Order %s to Status %d: %v", req.OrderID, StatusShipped, err)
		abandonMessage(ctx, receiver, msg)
		return
	}
	log.Printf("Order %s updated to Status %d (Shipped).", req.OrderID, StatusShipped)

	if err := receiver.CompleteMessage(ctx, msg, nil); err != nil {
		log.Printf("Failed to complete message for order %s: %v", req.OrderID, err)
		return
	}

	go simulateDelivery(service, req.OrderID, duration)
}

func abandonMessage(ctx context.Context, receiver *azservicebus.Receiver, msg *azservicebus.ReceivedMessage) {
	if err := receiver.AbandonMessage(ctx, msg, nil); err != nil {
		log.Printf("Failed to abandon message: %v", err)
	}
}

// simulateDelivery simulates the delivery process; in the future this could be a separate service, logistics-service
func simulateDelivery(service *ShippingService, orderID string, duration int) {
	log.Printf("Order %s is In Transit (%ds)...", orderID, duration)

	time.Sleep(time.Duration(duration) * time.Second)

	if err := service.MarkDelivered(orderID, StatusDelivered); err != nil {
		log.Printf("Failed to update status for %s to Status %d: %v", orderID, StatusDelivered, err)
	} else {
		log.Printf("Order %s Delivered!", orderID)
	}
}

// calculateDuration determines simulated shipping duration based on postal code
func calculateDuration(postalCode string) int {
	normalized := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(postalCode), " ", ""))
	if strings.HasPrefix(normalized, "K") {
		return 20 + rand.Intn(11) // 20-30s
	} else if strings.HasPrefix(normalized, "L") || strings.HasPrefix(normalized, "M") {
		return 30 + rand.Intn(11) // 30-40s
	}
	return 75 + rand.Intn(26) // 75-100s
}
