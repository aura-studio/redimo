package redimo

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c Client) HGET(key string, field string) (val ReturnValue, err error) {
	resp, err := c.ddbClient.GetItem(c.context(), &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key: keyDef{
			pk: key,
			sk: field,
		}.toAV(c),
		ProjectionExpression: aws.String(strings.Join([]string{vk}, ", ")),
		TableName:            aws.String(c.tableName),
	})
	if err == nil {
		val = parseItem(resp.Item, c).val
	}

	return
}

func (c Client) HMGET(key string, fields ...string) (values map[string]ReturnValue, err error) {
	if len(fields) == 0 {
		return make(map[string]ReturnValue), nil
	}

	// DynamoDB TransactGetItems rejects a transaction that references the same
	// item more than once, so collapse duplicate fields before batching. The
	// result is a map keyed by field name, so a caller that requested a field
	// more than once resolves every occurrence to the same entry — the
	// de-duplication is transparent to HMGET's contract.
	if len(fields) > 1 {
		seen := make(map[string]struct{}, len(fields))
		uniq := fields[:0:0]
		for _, f := range fields {
			if _, ok := seen[f]; ok {
				continue
			}
			seen[f] = struct{}{}
			uniq = append(uniq, f)
		}
		fields = uniq
	}

	values = make(map[string]ReturnValue)

	var (
		hasMoreFields = true
		leftFields    = fields
	)

	for hasMoreFields {
		if len(leftFields) > c.transactionActions {
			fields, hasMoreFields = leftFields[:c.transactionActions], true
			leftFields = leftFields[c.transactionActions:]
		} else {
			fields, hasMoreFields = leftFields, false
		}

		items := make([]types.TransactGetItem, len(fields))
		for i, field := range fields {
			items[i] = types.TransactGetItem{Get: &types.Get{
				Key: keyDef{
					pk: key,
					sk: field,
				}.toAV(c),
				ProjectionExpression: aws.String(strings.Join([]string{c.sortKey, vk}, ", ")),
				TableName:            aws.String(c.tableName),
			}}
		}

		resp, err := c.ddbClient.TransactGetItems(c.context(), &dynamodb.TransactGetItemsInput{
			TransactItems: items,
		})
		if err != nil {
			return values, err
		}

		for i, field := range fields {
			pi := parseItem(resp.Responses[i].Item, c)
			values[field] = pi.val
		}
	}

	return
}

func (c Client) HEXISTS(key string, field string) (exists bool, err error) {
	resp, err := c.ddbClient.GetItem(c.context(), &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key: keyDef{
			pk: key,
			sk: field,
		}.toAV(c),
		ProjectionExpression: aws.String(strings.Join([]string{c.partitionKey}, ", ")),
		TableName:            aws.String(c.tableName),
	})
	if err == nil && len(resp.Item) > 0 {
		exists = true
	}

	return
}

func (c Client) HGETALL(key string) (fieldValues map[string]ReturnValue, err error) {
	fieldValues = make(map[string]ReturnValue)
	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

		resp, err := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.tableName),
		})

		if err != nil {
			return fieldValues, err
		}

		for _, item := range resp.Items {
			if c.isMetaItem(item) { // never surface the reserved #meta item as a field
				continue
			}
			parsedItem := parseItem(item, c)
			fieldValues[parsedItem.sk] = parsedItem.val
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) HKEYS(key string, pattern string) (keys []string, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

		if pattern != "" {
			// Field names are stored as encodeSK(field); encode the prefix the
			// same way so begins_with matches the stored sort-key bytes.
			builder.addConditionBeginWith(c.sortKey, BytesValue{encodeSK(pattern)})
		}

		resp, err := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
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
			if c.isMetaItem(item) {
				continue
			}
			keys = append(keys, parseItem(item, c).sk)
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) HVALS(key string) (values []ReturnValue, err error) {
	all, err := c.HGETALL(key)
	if err == nil {
		for _, v := range all {
			values = append(values, v)
		}
	}

	return
}

// HLEN counts the fields of the hash at key (SCARD and ZCARD delegate here for
// their cardinality). It projects only the sort key and counts the items that are
// not the reserved #meta item, rather than using Select=Count: a raw Count over the
// partition would include the #meta bookkeeping item and overcount by one whenever a
// #meta item is present (as it always is for proxy-created keys). Counting non-meta
// items is correct whether or not a #meta item exists.
func (c Client) HLEN(key string) (count int32, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

		resp, err := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
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
			return count, err
		}

		for _, item := range resp.Items {
			if c.isMetaItem(item) {
				continue
			}
			count++
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}
