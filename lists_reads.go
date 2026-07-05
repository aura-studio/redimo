package redimo

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c Client) LINDEX(key string, index int64) (element ReturnValue, err error) {
	elements, err := c.lRange(key, index, index, true)

	if err != nil || len(elements) == 0 {
		return element, err
	}

	return elements[0], nil
}

func (c Client) LLEN(key string) (length int64, err error) {
	count, err := c.lLen(key)
	return int64(count), err
}

func (c Client) lLen(key string) (count int32, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		// Count the list's elements by querying the numeric-index LSI, which only
		// contains items that carry an skN (i.e. the element items). The reserved
		// #meta item — which now also holds the head/tail index counters il/ir —
		// has no skN and so is structurally absent from the index, giving the true
		// element count whether or not a #meta item exists.
		resp, err := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
			ConsistentRead:         aws.Bool(c.consistentReads),
			IndexName:              aws.String(c.indexName),
			ExclusiveStartKey:      lastEvaluatedKey,
			KeyConditionExpression: aws.String("#pk = :pk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": c.partitionKey,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberB{Value: []byte(key)},
			},
			TableName: aws.String(c.tableName),
			Select:    types.SelectCount,
		})

		if err != nil {
			return count, err
		}

		count += resp.Count

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) lRange(key string, start int64, end int64, forward bool) (elements []ReturnValue, err error) {
	llen, err := c.LLEN(key)
	if err != nil {
		return elements, err
	}

	if start < 0 {
		start = llen + start
	}

	if end < 0 {
		end = llen + end
	}

	if start < 0 {
		start = 0
	}

	if end >= llen {
		end = llen - 1
	}

	if start > end || start >= llen {
		return elements, nil
	}

	count := end - start + 1
	return c.lGeneralRange(key, start, count, forward, c.sortKeyNum)
}

func (c Client) LRANGE(key string, start, stop int64) (elements []ReturnValue, err error) {
	return c.lRange(key, start, stop, true)
}
