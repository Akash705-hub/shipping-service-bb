package main

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBShippingRepo struct {
	client           *mongo.Client
	dbName           string
	ordersCollection *mongo.Collection
}

// NewMongoDBShippingRepo creates a new MongoDBShippingRepo
func NewMongoDBShippingRepo(mongoUri string, mongoDb string, mongoCollection string, mongoUser string, mongoPassword string) (*MongoDBShippingRepo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoUri))
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}
	log.Println("Connected to MongoDB")

	db := client.Database(mongoDb)
	ordersColl := db.Collection(mongoCollection)

	return &MongoDBShippingRepo{
		client:           client,
		dbName:           mongoDb,
		ordersCollection: ordersColl,
	}, nil
}

// UpdateOrderDelivered updates an order's status to delivered
func (r *MongoDBShippingRepo) UpdateOrderDelivered(orderID string, status int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"orderid": orderID}
	update := bson.M{"$set": bson.M{"status": status}}

	result, err := r.ordersCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		log.Printf("Order %s not found for simple status update.", orderID)
	}
	return nil
}

// UpdateOrderShipmentInfo is the comprehensive update method to set shipment info and status
func (r *MongoDBShippingRepo) UpdateOrderShipmentInfo(orderID string, status int, shipment ShipmentRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"orderid": orderID}
	update := bson.M{
		"$set": bson.M{
			"status":                  status,
			"shipping.duration":       shipment.Duration,
			"shipping.trackingNumber": shipment.TrackingNumber,
			"shipping.shippedAt":      shipment.ShippedAt,
		},
	}

	result, err := r.ordersCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		log.Printf("Order %s not found for update.", orderID)
	}
	return nil
}
