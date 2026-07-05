package redimo

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// doBatchWrites submits a single batch of pre-built write requests with
// BatchWriteItem and retries any UnprocessedItems DynamoDB returns until they
// drain. It is the shared submit-then-retry loop behind batchDeleteKeys and
// batchPutItems, which differ only in how they build the WriteRequest slice.
func (c Client) doBatchWrites(requests []types.WriteRequest) error {
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

	return nil
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

		if werr := c.doBatchWrites(requests); werr != nil {
			return deleted, werr
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

		if werr := c.doBatchWrites(requests); werr != nil {
			return werr
		}
	}

	return nil
}
