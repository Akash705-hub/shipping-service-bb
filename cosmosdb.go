package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

type PartitionKey struct {
	Key   string
	Value string
}

type CosmosDBServiceRepo struct {
	ordersContainer *azcosmos.ContainerClient
	partitionKey    PartitionKey
}

func NewCosmosDBServiceRepoWithManagedIdentity(cosmosDbEndpoint string, dbName string, containerName string, partitionKey PartitionKey) (*CosmosDBServiceRepo, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Printf("failed to create Cosmos DB workload identity credential: %v", err)
		return nil, err
	}

	client, err := azcosmos.NewClient(cosmosDbEndpoint, cred, nil)
	if err != nil {
		log.Printf("failed to create Cosmos DB client: %v", err)
		return nil, err
	}

	return createContainerClients(client, dbName, containerName, partitionKey)
}

func NewCosmosDBServiceRepo(cosmosDbEndpoint string, dbName string, containerName string, cosmosDbKey string, partitionKey PartitionKey) (*CosmosDBServiceRepo, error) {
	cred, err := azcosmos.NewKeyCredential(cosmosDbKey)
	if err != nil {
		log.Printf("failed to create Cosmos DB key credential: %v", err)
		return nil, err
	}

	client, err := azcosmos.NewClientWithKey(cosmosDbEndpoint, cred, nil)
	if err != nil {
		log.Printf("failed to create Cosmos DB client: %v", err)
		return nil, err
	}

	return createContainerClients(client, dbName, containerName, partitionKey)
}

func createContainerClients(client *azcosmos.Client, dbName string, containerName string, pk PartitionKey) (*CosmosDBServiceRepo, error) {
	ordersContainer, err := client.NewContainer(dbName, containerName)
	if err != nil {
		return nil, err
	}

	return &CosmosDBServiceRepo{
		ordersContainer: ordersContainer,
		partitionKey:    pk,
	}, nil
}

func (r *CosmosDBServiceRepo) findOrderDocumentID(ctx context.Context, orderID string) (string, error) {
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)
	query := "SELECT c.id FROM c WHERE c.orderId = @orderId"
	opt := &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{{Name: "@orderId", Value: orderID}},
	}

	pager := r.ordersContainer.NewQueryItemsPager(query, pk, opt)
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return "", err
		}

		for _, item := range resp.Items {
			var doc struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(item, &doc); err == nil && doc.ID != "" {
				return doc.ID, nil
			}
		}
	}
	return "", nil
}

func (r *CosmosDBServiceRepo) UpdateOrderDelivered(orderID string, status int) error {
	ctx := context.Background()
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)

	existingId, err := r.findOrderDocumentID(ctx, orderID)
	if err != nil {
		return err
	}
	if existingId == "" {
		log.Printf("Order %s not found in CosmosDB", orderID)
		return nil
	}

	patch := azcosmos.PatchOperations{}
	patch.AppendReplace("/status", status)

	_, err = r.ordersContainer.PatchItem(ctx, pk, existingId, patch, nil)
	return err
}

func (r *CosmosDBServiceRepo) UpdateOrderShipmentInfo(orderID string, status int, shipment ShipmentRecord) error {
	ctx := context.Background()
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)

	existingId, err := r.findOrderDocumentID(ctx, orderID)
	if err != nil {
		return err
	}
	if existingId == "" {
		log.Printf("Order %s not found in CosmosDB", orderID)
		return nil
	}

	patch := azcosmos.PatchOperations{}
	patch.AppendReplace("/status", status)
	patch.AppendReplace("/shipping/duration", shipment.Duration)
	patch.AppendReplace("/shipping/trackingNumber", shipment.TrackingNumber)
	patch.AppendReplace("/shipping/shippedAt", shipment.ShippedAt)

	_, err = r.ordersContainer.PatchItem(ctx, pk, existingId, patch, nil)
	return err
}
