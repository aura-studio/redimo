package redimo

import (
	"math"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// pagedListItems is the shared pagination engine behind every list range read. It
// pages the key's partition, skipping the first offset items and collecting up to
// count of the raw items that follow (count <= 0 means "to the end of the range").
// addKeyConditions installs the per-page KeyConditionExpression on a fresh builder
// (partition-key equality alone, or equality plus a begins_with member prefix);
// useScoreIndex selects the numeric-index LSI (score/position order) versus a
// base-table Query. Elements/positions naturally exclude the #meta item: the LSI has
// no #meta entry (it carries no skN), and the base-table callers constrain the sort
// key with begins_with over the member hash, which #meta ([skPrefixMeta]) never
// matches.
func (c Client) pagedListItems(offset, count int64, forward, useScoreIndex bool,
	addKeyConditions func(b *expressionBuilder)) (items []map[string]types.AttributeValue, err error) {
	index := int64(0)
	remainingCount := count
	hasMoreResults := true

	var queryIndex *string
	if useScoreIndex {
		queryIndex = aws.String(c.indexName)
	}

	var lastKey map[string]types.AttributeValue

	for hasMoreResults {
		var queryLimit *int32
		if remainingCount > 0 {
			// Items still to evaluate = those still to skip + those still to collect. The old
			// formula remainingCount+offset-index double-counted the skip term and went NEGATIVE
			// once a page was truncated by DynamoDB's 1MB cap, so the next page's Limit was < 1
			// and every list whose elements exceed 1MB failed ALL reads with a ValidationException.
			skip := offset - index
			if skip < 0 {
				skip = 0
			}
			if need := remainingCount + skip; need > 0 && need <= math.MaxInt32 {
				queryLimit = aws.Int32(int32(need))
			}
			// need > MaxInt32: leave queryLimit nil; DynamoDB's 1MB per-Query cap still bounds the page.
		}

		builder := newExpresionBuilder()
		addKeyConditions(&builder)

		resp, qerr := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			IndexName:                 queryIndex,
			KeyConditionExpression:    builder.conditionExpression(),
			Limit:                     queryLimit,
			ScanIndexForward:          aws.Bool(forward),
			TableName:                 aws.String(c.tableName),
			Select:                    types.SelectAllAttributes,
		})

		if qerr != nil {
			return items, qerr
		}

		for _, item := range resp.Items {
			if index >= offset {
				items = append(items, item)
				remainingCount--
			}
			index++
		}

		if len(resp.LastEvaluatedKey) > 0 && remainingCount > 0 {
			lastKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return items, nil
}

// eqKeyCondition returns an addKeyConditions callback that constrains the query to a
// single partition key.
func (c Client) eqKeyCondition(key string) func(b *expressionBuilder) {
	return func(b *expressionBuilder) {
		b.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})
	}
}

func (c Client) lGeneralRange(key string, offset int64, count int64, forward bool, attribute string) (elements []ReturnValue, err error) {
	items, err := c.pagedListItems(offset, count, forward, attribute == c.sortKeyNum, c.eqKeyCondition(key))
	if err != nil {
		return make([]ReturnValue, 0), err
	}

	return listItemsToElements(items), nil
}

// parseVal is no longer needed - values are stored in val field
// sk now contains sha256(val)|index for fixed-length keys

func (c Client) lGeneralRangeWithItems(key string,
	offset int64, count int64,
	forward bool, attribute string) (elements []ReturnValue, items []map[string]types.AttributeValue, err error) {

	llen, err := c.LLEN(key)
	if err != nil {
		return elements, items, err
	}

	start := offset
	end := offset + count - 1

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
		return elements, items, nil
	}

	count = end - start + 1

	return c.lGeneralRangeWithItems_(key, start, count, forward, attribute)
}

func (c Client) lGeneralRangeWithItems_(key string,
	offset int64, count int64,
	forward bool, attribute string) (elements []ReturnValue, items []map[string]types.AttributeValue, err error) {
	items, err = c.pagedListItems(offset, count, forward, attribute == c.sortKeyNum, c.eqKeyCondition(key))
	if err != nil {
		return make([]ReturnValue, 0), nil, err
	}

	return listItemsToElements(items), items, nil
}
