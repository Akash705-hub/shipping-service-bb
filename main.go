package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

const (
	AZURE_COSMOS_DB_SQL_API  = "cosmosdbsql"
	DefaultMongoCollection   = "orders"
	StatusShipped           = 2
	StatusDelivered         = 3
)

type Config struct {
	DBAPIType                   string
	DBURI                       string
	DBName                      string
	ShippingQueueName           string
	ASBConnectionString         string
	AzureServiceBusNamespace    string
	CosmosContainerName         string
	CosmosPartitionKey          string
	CosmosPartitionValue        string
	UseWorkloadIdentityAuth     bool
	MongoCollectionName         string
	MongoUsername               string
	MongoPassword               string
	DBPassword                  string
	AppVersion                  string
}

func main() {
	rand.Seed(time.Now().UnixNano())

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	shippingService, err := initDatabase(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	go StartShippingWorker(shippingService)

	router := gin.Default()
	router.SetTrustedProxies([]string{"127.0.0.1", "::1", "172.18.0.0/16"})
	router.Use(cors.Default())
	router.POST("/", func(c *gin.Context) {
		handleShipRequest(c, cfg)
	})
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"version": cfg.AppVersion,
		})
	})

	log.Printf("Shipping Service running on :3003")
	if err := router.Run(":3003"); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func loadConfig() (*Config, error) {
	cfg := &Config{
		DBAPIType:                os.Getenv("ORDER_DB_API"),
		DBURI:                    getEnvVar("AZURE_COSMOS_RESOURCEENDPOINT", "MONGO_URI"),
		DBName:                   getEnvVar("SHIPPING_DB_NAME"),
		ShippingQueueName:        getEnvVar("SHIPPING_QUEUE_NAME"),
		ASBConnectionString:      os.Getenv("ASB_CONNECTION_STRING"),
		AzureServiceBusNamespace: os.Getenv("AZURE_SERVICEBUS_FULLYQUALIFIEDNAMESPACE"),
		CosmosContainerName:      os.Getenv("SHIPPING_DB_CONTAINER_NAME"),
		CosmosPartitionKey:       os.Getenv("SHIPPING_DB_PARTITION_KEY"),
		CosmosPartitionValue:     os.Getenv("SHIPPING_DB_PARTITION_VALUE"),
		UseWorkloadIdentityAuth:  os.Getenv("USE_WORKLOAD_IDENTITY_AUTH") == "true",
		MongoCollectionName:      os.Getenv("SHIPPING_DB_COLLECTION_NAME"),
		MongoUsername:            os.Getenv("SHIPPING_DB_USERNAME"),
		MongoPassword:            os.Getenv("SHIPPING_DB_PASSWORD"),
		DBPassword:               os.Getenv("SHIPPING_DB_PASSWORD"),
		AppVersion:               os.Getenv("APP_VERSION"),
	}

	if cfg.MongoCollectionName == "" {
		cfg.MongoCollectionName = DefaultMongoCollection
	}

	if cfg.DBURI == "" {
		return nil, fmt.Errorf("missing required database URI: AZURE_COSMOS_RESOURCEENDPOINT or MONGO_URI")
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("missing required database name: SHIPPING_DB_NAME")
	}
	if cfg.ShippingQueueName == "" {
		return nil, fmt.Errorf("missing required queue name: SHIPPING_QUEUE_NAME")
	}

	if cfg.DBAPIType == AZURE_COSMOS_DB_SQL_API {
		if cfg.CosmosContainerName == "" || cfg.CosmosPartitionKey == "" || cfg.CosmosPartitionValue == "" {
			return nil, fmt.Errorf("missing required Cosmos DB configuration")
		}
	}

	return cfg, nil
}

func handleShipRequest(c *gin.Context, cfg *Config) {
	var req ShippingRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload"})
		return
	}

	client, err := buildServiceBusClient(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to initialize Service Bus client"})
		return
	}
	defer func() {
		if err := client.Close(context.Background()); err != nil {
			log.Printf("failed to close Service Bus client: %v", err)
		}
	}()

	sender, err := client.NewSender(cfg.ShippingQueueName, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create queue sender"})
		return
	}
	defer func() {
		if err := sender.Close(context.Background()); err != nil {
			log.Printf("failed to close Service Bus sender: %v", err)
		}
	}()

	body, err := json.Marshal(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize request"})
		return
	}

	if err := sender.SendMessage(context.Background(), &azservicebus.Message{Body: body}, nil); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to enqueue"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"status": "Queued for shipping"})
}

func initDatabase(cfg *Config) (*ShippingService, error) {
	switch cfg.DBAPIType {
	case AZURE_COSMOS_DB_SQL_API:
		if cfg.UseWorkloadIdentityAuth {
			cosmosRepo, err := NewCosmosDBServiceRepoWithManagedIdentity(cfg.DBURI, cfg.DBName, cfg.CosmosContainerName, PartitionKey{cfg.CosmosPartitionKey, cfg.CosmosPartitionValue})
			if err != nil {
				return nil, err
			}
			return NewShippingService(cosmosRepo), nil
		}

		cosmosRepo, err := NewCosmosDBServiceRepo(cfg.DBURI, cfg.DBName, cfg.CosmosContainerName, cfg.DBPassword, PartitionKey{cfg.CosmosPartitionKey, cfg.CosmosPartitionValue})
		if err != nil {
			return nil, err
		}
		return NewShippingService(cosmosRepo), nil
	default:
		mongoRepo, err := NewMongoDBShippingRepo(cfg.DBURI, cfg.DBName, cfg.MongoCollectionName, cfg.MongoUsername, cfg.MongoPassword)
		if err != nil {
			return nil, err
		}
		return NewShippingService(mongoRepo), nil
	}
}

func buildServiceBusClient(cfg *Config) (*azservicebus.Client, error) {
	if cfg.ASBConnectionString != "" {
		return azservicebus.NewClientFromConnectionString(cfg.ASBConnectionString, nil)
	}
	if cfg.AzureServiceBusNamespace == "" {
		return nil, fmt.Errorf("missing Azure Service Bus namespace")
	}
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	return azservicebus.NewClient(cfg.AzureServiceBusNamespace, cred, nil)
}

func getEnvVar(varName string, fallbackVarNames ...string) string {
	value := os.Getenv(varName)
	if value == "" {
		for _, fallbackVarName := range fallbackVarNames {
			value = os.Getenv(fallbackVarName)
			if value != "" {
				break
			}
		}
	}
	return value
}
