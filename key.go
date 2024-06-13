package redimo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c Client) DEL(key string) (deletedFields []string, err error) {
	fields, err := c.listSortKeys(key)
	if err != nil {
		return deletedFields, err
	}

	for _, field := range fields {
		resp, err := c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
			Key: keyDef{
				pk: key,
				sk: field,
			}.toAV(c),
			ReturnValues: types.ReturnValueAllOld,
			TableName:    aws.String(c.tableName),
		})
		if err != nil {
			return deletedFields, err
		}

		if len(resp.Attributes) > 0 {
			deletedFields = append(deletedFields, field)
		}
	}

	return
}

func (c Client) listSortKeys(key string) (sortKeys []string, err error) {
	hasMoreResults := true
	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, StringValue{key})

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.tableName),
			ProjectionExpression:      aws.String(c.sortKey),
			Select:                    types.SelectSpecificAttributes,
		})

		if err != nil {
			return sortKeys, err
		}

		for _, item := range resp.Items {
			parsedItem := parseItem(item, c)
			sortKeys = append(sortKeys, parsedItem.sk)
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) EXISTS(key string) (exists bool, err error) {
	var lastEvaluatedKey map[string]types.AttributeValue

	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, StringValue{key})

	resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(c.consistentReads),
		ExclusiveStartKey:         lastEvaluatedKey,
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		KeyConditionExpression:    builder.conditionExpression(),
		TableName:                 aws.String(c.tableName),
	})

	if err != nil {
		return exists, err
	}

	return len(resp.Items) > 0, nil
}

func (c Client) KEYS(key string) (keys []string, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionBeginWith(c.partitionKey, StringValue{key})

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.tableName),
			ProjectionExpression:      aws.String(c.sortKey),
			Select:                    types.SelectSpecificAttributes,
		})

		if err != nil {
			return keys, err
		}

		for _, item := range resp.Items {
			parsedItem := parseItem(item, c)
			keys = append(keys, parsedItem.sk)
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}
