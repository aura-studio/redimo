package redimo

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestClientBuilder(t *testing.T) {
	dynamoService := dynamodb.NewFromConfig(newConfig(t))
	c1 := NewClient(dynamoService)
	assert.Equal(t, c1.ddbClient, dynamoService)
	assert.True(t, c1.consistentReads)
	assert.Equal(t, "redimo", c1.tableName)
	assert.Equal(t, c1.partitionKey, c1.partitionKey)
	assert.False(t, c1.EventuallyConsistent().consistentReads)
	c2 := c1.Table("table2").Index("index2").EventuallyConsistent()
	assert.Equal(t, "table2", c2.tableName)
	assert.Equal(t, "index2", c2.indexName)
	assert.False(t, c2.consistentReads)
	assert.True(t, c1.consistentReads)
	assert.True(t, c2.StronglyConsistent().consistentReads)
}

func newClient(t *testing.T) Client {
	t.Parallel()

	tableName := uuid.New().String()
	indexName := "idx"
	partitionKey := "pk"
	sortKey := "sk"
	sortKeyNum := "skN"
	dynamoService := dynamodb.NewFromConfig(newConfig(t))
	_, err := dynamoService.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(partitionKey), AttributeType: "B"},
			{AttributeName: aws.String(sortKey), AttributeType: "B"},
			{AttributeName: aws.String(sortKeyNum), AttributeType: "N"},
		},
		BillingMode:            types.BillingModePayPerRequest,
		GlobalSecondaryIndexes: nil,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(partitionKey), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(sortKey), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(indexName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String(partitionKey), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String(sortKeyNum), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{
					NonKeyAttributes: nil,
					ProjectionType:   types.ProjectionTypeKeysOnly,
				},
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(0),
			WriteCapacityUnits: aws.Int64(0),
		},
		SSESpecification:    nil,
		StreamSpecification: nil,
		TableName:           aws.String(tableName),
		Tags:                nil,
	})
	assert.NoError(t, err)

	return NewClient(dynamoService).Table(tableName).Index(indexName).Attributes(partitionKey, sortKey, sortKeyNum)
}

func newConfig(t *testing.T) aws.Config {
	region := "us-west-1"
	credentialsProvider := credentials.NewStaticCredentialsProvider("ABCD", "EFGH", "IKJGL")
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
		if service == dynamodb.ServiceID {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           "http://localhost:8000",
				SigningRegion: region,
			}, nil
		}
		// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	sdkConfig, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentialsProvider),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	assert.NoError(t, err)

	return sdkConfig
}
