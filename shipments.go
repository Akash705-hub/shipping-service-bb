package main

import (
	"time"
)

// Request coming from Store Admin via HTTP
type ShippingRequest struct {
	OrderID  string       `json:"orderId"`
	Shipping ShippingInfo `json:"shipping"`
	Status   int          `json:"status"`
}

type ShippingInfo struct {
	Address1   string `json:"address1"`
	City       string `json:"city"`
	Province   string `json:"province"`
	PostalCode string `json:"postalCode"`
	Country    string `json:"country"`
}

// Record stored in MongoDB or CosmosDB
type ShipmentRecord struct {
	OrderID         string    `bson:"orderId" json:"orderId"`
	TrackingNumber  string    `bson:"trackingNumber" json:"trackingNumber"`
	Duration        int       `bson:"duration" json:"duration"`
	Destination     string    `bson:"destination" json:"destination"`
	ShippedAt       time.Time `bson:"shippedAt" json:"shippedAt"`
	EstimatedArrive time.Time `bson:"estimatedArrive" json:"estimatedArrive"`
}

// Repository Interface
type ShippingRepo interface {
	UpdateOrderShipmentInfo(orderID string, status int, shipment ShipmentRecord) error
	UpdateOrderDelivered(orderID string, status int) error
}

type ShippingService struct {
	repo ShippingRepo
}

func NewShippingService(repo ShippingRepo) *ShippingService {
	return &ShippingService{repo: repo}
}

func (s *ShippingService) UpdateShipment(orderID string, status int, shipment ShipmentRecord) error {
	return s.repo.UpdateOrderShipmentInfo(orderID, status, shipment)
}

func (s *ShippingService) MarkDelivered(orderID string, status int) error {
	return s.repo.UpdateOrderDelivered(orderID, status)
}
