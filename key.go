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

	if len(fields) == 0 {
		return deletedFields, nil
	}

	// ✅ 使用 BatchWriteItem 批量删除（最多 25 项/批）
	const batchSize = 25
	for batchStart := 0; batchStart < len(fields); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(fields) {
			batchEnd = len(fields)
		}

		batch := make([]types.WriteRequest, 0, batchEnd-batchStart)
		for _, field := range fields[batchStart:batchEnd] {
			batch = append(batch, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: keyDef{
						pk: key,
						sk: field,
					}.toAV(c),
				},
			})
		}

		_, err := c.ddbClient.BatchWriteItem(context.TODO(), &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				c.tableName: batch,
			},
		})
		if err != nil {
			return deletedFields, err
		}

		// ✅ 添加已删除的字段
		deletedFields = append(deletedFields, fields[batchStart:batchEnd]...)
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
	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, StringValue{key})

	resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(c.consistentReads),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		KeyConditionExpression:    builder.conditionExpression(),
		TableName:                 aws.String(c.tableName),
		Limit:                     aws.Int32(1),                   // ✅ 只取第一个
		ProjectionExpression:      aws.String(c.partitionKey),     // ✅ 只返回主键
		Select:                    types.SelectSpecificAttributes, // ✅ 最小化返回数据
	})

	if err != nil {
		return exists, err
	}

	return len(resp.Items) > 0, nil
}

// hkeys with pattern
// func (c Client) KEYS(key string, pattern string) (keys []string, err error) {
// 	hasMoreResults := true

// 	var lastEvaluatedKey map[string]types.AttributeValue

// 	for hasMoreResults {
// 		builder := newExpresionBuilder()
// 		builder.addConditionEquality(c.partitionKey, StringValue{key})
// 		builder.addConditionBeginWith(c.sortKey, StringValue{pattern})

// 		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
// 			ConsistentRead:            aws.Bool(c.consistentReads),
// 			ExclusiveStartKey:         lastEvaluatedKey,
// 			ExpressionAttributeNames:  builder.expressionAttributeNames(),
// 			ExpressionAttributeValues: builder.expressionAttributeValues(),
// 			KeyConditionExpression:    builder.conditionExpression(),
// 			TableName:                 aws.String(c.tableName),
// 			ProjectionExpression:      aws.String(c.sortKey),
// 			Select:                    types.SelectSpecificAttributes,
// 		})

// 		if err != nil {
// 			return keys, err
// 		}

// 		for _, item := range resp.Items {
// 			parsedItem := parseItem(item, c)
// 			keys = append(keys, parsedItem.sk)
// 		}

// 		if len(resp.LastEvaluatedKey) > 0 {
// 			lastEvaluatedKey = resp.LastEvaluatedKey
// 		} else {
// 			hasMoreResults = false
// 		}
// 	}

// 	return
// }
