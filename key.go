package redimo

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DEL removes the key by deleting EVERY item under its partition — the String value
// item, all collection members, the internal list indices, and the reserved #meta item.
// It returns the decoded sort keys it deleted (the value item decodes to "", #meta to
// MetaSK), so len(deletedFields) is the item count removed.
//
// Deletion is by each item's RAW stored primary key (pk + sk bytes, projected straight
// from the Query), NOT a decoded-then-re-encoded keyDef. That round-trip is lossy for the
// two reserved sort keys: in this format the value item's 0x00 decodes to "" and re-encodes to
// 0x01, and the #meta item's 0x02 decodes to MetaSK and re-encodes to 0x01||"#meta" — so
// re-encoding would delete the wrong (non-existent) items and leave the key alive.
// BatchWriteItem's UnprocessedItems are retried to completion by batchDeleteRawKeys.
func (c Client) DEL(key string) (deletedFields []string, err error) {
	var (
		rawKeys          []map[string]types.AttributeValue
		lastEvaluatedKey map[string]types.AttributeValue
	)

	for {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

		resp, qerr := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.tableName),
			ProjectionExpression:      aws.String(c.partitionKey + ", " + c.sortKey),
			Select:                    types.SelectSpecificAttributes,
		})
		if qerr != nil {
			return deletedFields, qerr
		}

		for _, item := range resp.Items {
			rawKeys = append(rawKeys, keyItemAV(item, c))
			deletedFields = append(deletedFields, parseItem(item, c).sk)
		}

		if len(resp.LastEvaluatedKey) == 0 {
			break
		}

		lastEvaluatedKey = resp.LastEvaluatedKey
	}

	if len(rawKeys) == 0 {
		return deletedFields, nil
	}

	_, err = c.batchDeleteRawKeys(rawKeys, MaxBatchWriteItems)

	return deletedFields, err
}


func (c Client) EXISTS(key string) (exists bool, err error) {
	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

	resp, err := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
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
// 		builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})
// 		builder.addConditionBeginWith(c.sortKey, StringValue{pattern})

// 		resp, err := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
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
