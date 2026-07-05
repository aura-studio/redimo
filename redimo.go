package redimo

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Client struct {
	ddbClient          *dynamodb.Client
	consistentReads    bool
	tableName          string
	indexName          string
	partitionKey       string
	sortKey            string
	sortKeyNum         string
	transactionActions int
	ctx                context.Context
}

// WithContext returns a copy of the Client whose DynamoDB calls use ctx, so callers
// can attach a deadline, timeout or cancellation to a request (e.g. per proxy command).
// It follows the same value-copy builder pattern as Table/Index/Attributes, so the
// original Client is unaffected. A Client that was never given a context defaults to
// context.Background() (see context()).
func (c Client) WithContext(ctx context.Context) Client {
	c.ctx = ctx
	return c
}

// context returns the Client's context, or context.Background() when none was set,
// so every DynamoDB call has a non-nil context without each call site special-casing.
func (c Client) context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}

	return context.Background()
}

func (c Client) EventuallyConsistent() Client {
	c.consistentReads = false
	return c
}

func (c Client) Table(tableName string) Client {
	c.tableName = tableName
	return c
}

func (c Client) Index(indexName string) Client {
	c.indexName = indexName
	return c
}

func (c Client) Attributes(pk string, sk string, skN string) Client {
	c.partitionKey = pk
	c.sortKey = sk
	c.sortKeyNum = skN

	return c
}

func (c Client) StronglyConsistent() Client {
	c.consistentReads = true
	return c
}

func (c Client) TransactionActions(actions int) Client {
	c.transactionActions = actions
	return c
}

func (c Client) ExistsTable() (bool, error) {
	_, err := c.ddbClient.DescribeTable(c.context(), &dynamodb.DescribeTableInput{
		TableName: aws.String(c.tableName),
	})
	if err == nil {
		return true, nil
	}
	var notFoundEx *types.ResourceNotFoundException
	if errors.As(err, &notFoundEx) {
		return false, nil
	}
	return false, fmt.Errorf("couldn't determine existence of table %v. Here's why: %w", c.tableName, err)
}

func (c Client) CreateTable(readCapacity int64, writeCapacity int64) error {
	if readCapacity == 0 && writeCapacity == 0 {
		return c.CreatePayPerRequestTable()
	}
	return c.CreateProvisionedTable(readCapacity, writeCapacity)
}

func (c Client) CreatePayPerRequestTable() error {
	_, err := c.ddbClient.CreateTable(c.context(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(c.partitionKey), AttributeType: "B"},
			{AttributeName: aws.String(c.sortKey), AttributeType: "B"},
			{AttributeName: aws.String(c.sortKeyNum), AttributeType: "N"},
		},
		BillingMode:            types.BillingModePayPerRequest,
		GlobalSecondaryIndexes: nil,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(c.partitionKey), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(c.sortKey), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(c.indexName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String(c.partitionKey), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String(c.sortKeyNum), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{
					NonKeyAttributes: nil,
					ProjectionType:   types.ProjectionTypeKeysOnly,
				},
			},
		},
		SSESpecification:    nil,
		StreamSpecification: nil,
		TableName:           aws.String(c.tableName),
		Tags:                nil,
	})

	if err != nil {
		return fmt.Errorf("couldn't create table %v. Here's why: %w", c.tableName, err)
	}
	return nil
}

func (c Client) CreateProvisionedTable(readCapacity int64, writeCapacity int64) error {
	_, err := c.ddbClient.CreateTable(c.context(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(c.partitionKey), AttributeType: "B"},
			{AttributeName: aws.String(c.sortKey), AttributeType: "B"},
			{AttributeName: aws.String(c.sortKeyNum), AttributeType: "N"},
		},
		BillingMode:            types.BillingModeProvisioned,
		GlobalSecondaryIndexes: nil,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(c.partitionKey), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(c.sortKey), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(c.indexName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String(c.partitionKey), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String(c.sortKeyNum), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{
					NonKeyAttributes: nil,
					ProjectionType:   types.ProjectionTypeKeysOnly,
				},
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(readCapacity),
			WriteCapacityUnits: aws.Int64(writeCapacity),
		},
		SSESpecification:    nil,
		StreamSpecification: nil,
		TableName:           aws.String(c.tableName),
		Tags:                nil,
	})
	if err != nil {
		return fmt.Errorf("couldn't create table %v. Here's why: %w", c.tableName, err)
	}

	return nil
}

func NewClient(service *dynamodb.Client) Client {
	return Client{
		ddbClient:          service,
		consistentReads:    true,
		tableName:          "redimo",
		indexName:          "idx",
		partitionKey:       "pk",
		sortKey:            "sk",
		sortKeyNum:         "skN",
		transactionActions: 100,
	}
}

const (
	vk = "val"
)

type Flag string

const (
	None            Flag = "-"
	Unconditionally      = None
	IfAlreadyExists Flag = "XX"
	IfNotExists     Flag = "NX"
)

type Flags []Flag

func (flags Flags) has(flag Flag) bool {
	for _, f := range flags {
		if f == flag {
			return true
		}
	}

	return false
}

// conditionFailureError reports whether err is a DynamoDB conditional-write
// failure — i.e. the condition was not met (a lost CAS), as opposed to a transient
// error (throttling, transaction conflict/in-progress, insufficient capacity) that
// the caller should RETRY. It matches the SDK's typed exceptions rather than error
// text, and for a cancelled transaction it inspects the per-item cancellation
// reasons so only an actual ConditionalCheckFailed counts — a throttled or
// conflicted transaction stays a retryable error and is not misreported as a lost
// CAS (which would exhaust the RMW retry loop under load).
func conditionFailureError(err error) bool {
	if err == nil {
		return false
	}

	var condFailed *types.ConditionalCheckFailedException
	if errors.As(err, &condFailed) {
		return true
	}

	var txnCancelled *types.TransactionCanceledException
	if errors.As(err, &txnCancelled) {
		for _, reason := range txnCancelled.CancellationReasons {
			if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
				return true
			}
		}
	}

	return false
}
