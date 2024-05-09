package redimo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

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
	_, err := c.ddbClient.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
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
	_, err := c.ddbClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(c.partitionKey), AttributeType: "S"},
			{AttributeName: aws.String(c.sortKey), AttributeType: "S"},
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
	_, err := c.ddbClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(c.partitionKey), AttributeType: "S"},
			{AttributeName: aws.String(c.sortKey), AttributeType: "S"},
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

	return fmt.Errorf("couldn't create table %v. Here's why: %w", c.tableName, err)
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

type expressionBuilder struct {
	conditions []string
	clauses    map[string][]string
	keys       map[string]struct{}
	values     map[string]types.AttributeValue
}

func (b *expressionBuilder) SET(clause string, key string, val types.AttributeValue) {
	b.clauses["SET"] = append(b.clauses["SET"], clause)
	b.keys[key] = struct{}{}
	b.values[key] = val
}

func (b *expressionBuilder) condition(condition string, references ...string) {
	b.conditions = append(b.conditions, condition)
	for _, ref := range references {
		b.keys[ref] = struct{}{}
	}
}

func (b *expressionBuilder) conditionExpression() *string {
	if len(b.conditions) == 0 {
		return nil
	}

	return aws.String(strings.Join(b.conditions, " AND "))
}

func (b *expressionBuilder) expressionAttributeNames() map[string]string {
	if len(b.keys) == 0 {
		return nil
	}

	out := make(map[string]string)

	for n := range b.keys {
		out["#"+n] = n
	}

	return out
}

func (b *expressionBuilder) expressionAttributeValues() map[string]types.AttributeValue {
	if len(b.values) == 0 {
		return nil
	}

	out := make(map[string]types.AttributeValue)

	for k, v := range b.values {
		out[":"+k] = v
	}

	return out
}

func (b *expressionBuilder) updateExpression() *string {
	if len(b.clauses) == 0 {
		return nil
	}

	clauses := make([]string, 0, len(b.clauses))

	for k, v := range b.clauses {
		clauses = append(clauses, k+" "+strings.Join(v, ", "))
	}

	return aws.String(strings.Join(clauses, " "))
}

func (b *expressionBuilder) addConditionEquality(attributeName string, value Value) {
	valueName := "cval" + strconv.Itoa(len(b.conditions))
	b.condition(fmt.Sprintf("#%v = :%v", attributeName, valueName), attributeName)
	b.values[valueName] = value.ToAV()
}

func (b *expressionBuilder) addConditionLessThan(attributeName string, value Value) {
	valueName := "cval" + strconv.Itoa(len(b.conditions))
	b.condition(fmt.Sprintf("#%v < :%v", attributeName, valueName), attributeName)
	b.values[valueName] = value.ToAV()
}

func (b *expressionBuilder) addConditionBeginWith(attributeName string, value Value) {
	valueName := "cval" + strconv.Itoa(len(b.conditions))
	b.condition(fmt.Sprintf("begins_with(#%v, :%v)", attributeName, valueName), attributeName)
	b.values[valueName] = value.ToAV()
}

func (b *expressionBuilder) addConditionLessThanOrEqualTo(attributeName string, value Value) {
	valueName := "cval" + strconv.Itoa(len(b.conditions))
	b.condition(fmt.Sprintf("#%v <= :%v", attributeName, valueName), attributeName)
	b.values[valueName] = value.ToAV()
}

func (b *expressionBuilder) updateSET(attributeName string, value Value) {
	b.SET(fmt.Sprintf("#%v = :%v", attributeName, attributeName), attributeName, value.ToAV())
}

func (b *expressionBuilder) updateSetAV(attributeName string, av types.AttributeValue) {
	b.SET(fmt.Sprintf("#%v = :%v", attributeName, attributeName), attributeName, av)
}

func (b *expressionBuilder) addConditionNotExists(attributeName string) {
	b.condition(fmt.Sprintf("attribute_not_exists(#%v)", attributeName), attributeName)
}

func (b *expressionBuilder) addConditionExists(attributeName string) {
	b.condition(fmt.Sprintf("attribute_exists(#%v)", attributeName), attributeName)
}

func newExpresionBuilder() expressionBuilder {
	return expressionBuilder{
		conditions: []string{},
		clauses:    make(map[string][]string),
		keys:       make(map[string]struct{}),
		values:     make(map[string]types.AttributeValue),
	}
}

type keyDef struct {
	pk string
	sk string
}

func (k keyDef) toAV(c Client) map[string]types.AttributeValue {
	m := map[string]types.AttributeValue{
		c.partitionKey: &types.AttributeValueMemberS{Value: k.pk},
		c.sortKey:      &types.AttributeValueMemberS{Value: compatibleWithEmtpySK(k.sk)},
	}

	return m
}

const emptySK = "/"

func compatibleWithEmtpySK(sk string) string {
	if sk == "" {
		return emptySK
	}

	return sk
}

func recoverFromEmptySK(sk string) string {
	if sk == emptySK {
		return ""
	}

	return sk
}

type itemDef struct {
	keyDef
	val ReturnValue
}

func parseKey(avm map[string]types.AttributeValue, c Client) keyDef {
	return keyDef{
		pk: ReturnValue{avm[c.partitionKey]}.String(),
		sk: recoverFromEmptySK(ReturnValue{avm[c.sortKey]}.String()),
	}
}

func parseItem(avm map[string]types.AttributeValue, c Client) (item itemDef) {
	item.keyDef = parseKey(avm, c)
	item.val = ReturnValue{avm[vk]}

	return
}

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

func conditionFailureError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()

	if strings.Contains(s, "ConditionalCheckFailedException") {
		return true
	}

	if strings.Contains(s, "TransactionInProgressException") {
		return true
	}

	if strings.Contains(s, "TransactionConflictException") {
		return true
	}

	if strings.Contains(s, "TransactionCanceledException") {
		return true
	}

	return false
}
