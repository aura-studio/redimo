package redimo

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Member reclamation (fork v1.7 extension, redimos task 11.1).
//
// This file adds the storage primitive behind the proxy's lazy deleter. The proxy
// deletes a key in two steps: DeleteMeta removes the meta item first (making the
// key immediately logically absent, see meta.go), then a background deleter calls
// DeleteMembers to reclaim the key's remaining data items. Splitting the delete
// this way keeps the client-visible DEL O(1) while the (potentially large) member
// cleanup happens asynchronously and rate-limited.

// MaxBatchWriteItems is the DynamoDB hard limit on the number of write requests in
// a single BatchWriteItem call.
const MaxBatchWriteItems = 25

// DeleteMembers deletes all data-member items under pk — every item sharing the
// partition key except the reserved meta item (sk = "#meta"). It pages through the
// partition with Query (projecting only the key attributes) and reclaims the
// members with BatchWriteItem in batches of batchSize, retrying any
// UnprocessedItems returned by DynamoDB. It returns the number of member keys
// submitted for deletion.
//
// batchSize is clamped to the range [1, MaxBatchWriteItems]; a value <= 0 selects
// the DynamoDB per-call maximum. DeleteMembers is safe to call when the key has no
// members (it returns 0) and deliberately never removes the meta item, so a key
// that was concurrently recreated is not corrupted by an in-flight reclaim.
func (c Client) DeleteMembers(pk string, batchSize int) (deleted int, err error) {
	if batchSize <= 0 || batchSize > MaxBatchWriteItems {
		batchSize = MaxBatchWriteItems
	}

	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(pk)})

		resp, qerr := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			ProjectionExpression:      aws.String(strings.Join([]string{c.partitionKey, c.sortKey}, ", ")),
			TableName:                 aws.String(c.tableName),
		})
		if qerr != nil {
			return deleted, qerr
		}

		keys := make([]keyDef, 0, len(resp.Items))

		for _, item := range resp.Items {
			if c.isMetaItem(item) {
				// Never delete the meta item here; DeleteMeta owns its lifecycle.
				continue
			}

			keys = append(keys, parseKey(item, c))
		}

		n, derr := c.batchDeleteKeys(keys, batchSize)
		deleted += n

		if derr != nil {
			return deleted, derr
		}

		if len(resp.LastEvaluatedKey) == 0 {
			break
		}

		lastEvaluatedKey = resp.LastEvaluatedKey
	}

	return deleted, nil
}

// batchDeleteKeys issues BatchWriteItem delete requests for the given keys in
// batches of batchSize, retrying any UnprocessedItems until DynamoDB drains them.
// It returns the number of keys submitted for deletion.
func (c Client) batchDeleteKeys(keys []keyDef, batchSize int) (deleted int, err error) {
	for start := 0; start < len(keys); start += batchSize {
		end := start + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		requests := make([]types.WriteRequest, 0, end-start)
		for _, k := range keys[start:end] {
			requests = append(requests, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{Key: k.toAV(c)},
			})
		}

		unprocessed := map[string][]types.WriteRequest{c.tableName: requests}

		for len(unprocessed[c.tableName]) > 0 {
			resp, werr := c.ddbClient.BatchWriteItem(c.context(), &dynamodb.BatchWriteItemInput{
				RequestItems: unprocessed,
			})
			if werr != nil {
				return deleted, werr
			}

			if len(resp.UnprocessedItems[c.tableName]) == 0 {
				break
			}

			unprocessed = resp.UnprocessedItems
		}

		deleted += end - start
	}

	return deleted, nil
}

// batchPutItems writes the given items with BatchWriteItem in batches of batchSize,
// retrying any UnprocessedItems until DynamoDB drains them. It is the write-side twin of
// batchDeleteKeys, used to materialize many data items (e.g. a bulk LPUSH/RPUSH) in a few
// round-trips instead of one UpdateItem each. batchSize is clamped to [1,
// MaxBatchWriteItems]; a value <= 0 selects the DynamoDB per-call maximum. The items must
// have distinct primary keys (BatchWriteItem rejects duplicates within one call).
func (c Client) batchPutItems(items []map[string]types.AttributeValue, batchSize int) error {
	if batchSize <= 0 || batchSize > MaxBatchWriteItems {
		batchSize = MaxBatchWriteItems
	}

	for start := 0; start < len(items); start += batchSize {
		end := start + batchSize
		if end > len(items) {
			end = len(items)
		}

		requests := make([]types.WriteRequest, 0, end-start)
		for _, it := range items[start:end] {
			requests = append(requests, types.WriteRequest{
				PutRequest: &types.PutRequest{Item: it},
			})
		}

		unprocessed := map[string][]types.WriteRequest{c.tableName: requests}

		for len(unprocessed[c.tableName]) > 0 {
			resp, werr := c.ddbClient.BatchWriteItem(c.context(), &dynamodb.BatchWriteItemInput{
				RequestItems: unprocessed,
			})
			if werr != nil {
				return werr
			}

			if len(resp.UnprocessedItems[c.tableName]) == 0 {
				break
			}

			unprocessed = resp.UnprocessedItems
		}
	}

	return nil
}
