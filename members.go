package redimo

import (
	"strconv"
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

		keys := make([]map[string]types.AttributeValue, 0, len(resp.Items))

		for _, item := range resp.Items {
			if c.isMetaItem(item) {
				// Never delete the meta item here; DeleteMeta owns its lifecycle.
				continue
			}

			// Delete by the RAW stored key (pk + sk bytes), not a decoded keyDef: the
			// value item's 0x00 sort key no longer round-trips through decode/encode
			// (encodeSK("") is 0x01 in this format), so re-encoding would orphan it.
			keys = append(keys, keyItemAV(item, c))
		}

		n, derr := c.batchDeleteRawKeys(keys, batchSize)
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

// DeleteMembersIfDead reclaims a key's data-member items ONLY while the key is dead — its
// reserved #meta item absent — deleting them ATOMICALLY with that liveness check so a key
// recreated concurrently (a DEL-then-recreate) can never have its fresh data destroyed.
//
// It is the async lazy-deleter's member-reclaim primitive, and the correctness-critical
// counterpart to DeleteMembers: where DeleteMembers deletes unconditionally (and is reused by
// the synchronous live-collection rewrite LReplaceAll, which MUST clear a live key), this
// wraps each delete batch in a TransactWriteItems whose first action is a ConditionCheck that
// the #meta item does not exist. If the key was recreated (#meta present again) the whole
// transaction is cancelled: DeleteMembersIfDead stops and returns aborted=true, leaving the
// new incarnation's items intact.
//
// Because the dead-or-expired check and the member deletes commit as a single transaction,
// there is no window — unlike a separate LoadMeta guard, where a SET can land between the
// check and the delete — in which an acknowledged recreate is silently wiped. A key counts
// as dead when its #meta item is absent (already DEL'd) OR expired (exp <= nowEpoch), so a
// key surfaced as expired by the read path — whose #meta lingers until native TTL — is still
// reclaimed. batchSize is clamped to [1, transactionActions-1] to leave one action slot for
// the ConditionCheck. It returns the number of members deleted before completion or abort.
func (c Client) DeleteMembersIfDead(pk string, nowEpoch int64, batchSize int) (deleted int, aborted bool, err error) {
	maxDeletes := c.transactionActions - 1 // reserve one slot for the #meta ConditionCheck
	if maxDeletes < 1 {
		maxDeletes = 1
	}
	if batchSize <= 0 || batchSize > maxDeletes {
		batchSize = maxDeletes
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
			return deleted, false, qerr
		}

		keys := make([]map[string]types.AttributeValue, 0, len(resp.Items))

		for _, item := range resp.Items {
			if c.isMetaItem(item) {
				continue
			}

			// Raw stored key, not a decoded keyDef (see DeleteMembers): a value item's
			// 0x00 sort key would not survive a decode/encode round-trip in this format.
			keys = append(keys, keyItemAV(item, c))
		}

		n, ab, derr := c.transactDeleteKeysIfDead(pk, nowEpoch, keys, batchSize)
		deleted += n

		if derr != nil {
			return deleted, false, derr
		}

		if ab {
			// Key recreated (live and unexpired): stop reclaiming so it survives.
			return deleted, true, nil
		}

		if len(resp.LastEvaluatedKey) == 0 {
			break
		}

		lastEvaluatedKey = resp.LastEvaluatedKey
	}

	return deleted, false, nil
}

// transactDeleteKeysIfDead deletes the given keys in transactions of batchSize deletes, each
// gated by a leading ConditionCheck that pk's #meta item is absent OR expired (exp <=
// nowEpoch). A transaction cancelled by that condition (the key is live and unexpired — it
// was recreated) stops the reclaim and reports aborted=true; the keys already deleted by
// earlier transactions in this call are counted in deleted.
func (c Client) transactDeleteKeysIfDead(pk string, nowEpoch int64, keys []map[string]types.AttributeValue, batchSize int) (deleted int, aborted bool, err error) {
	metaDead := types.TransactWriteItem{
		ConditionCheck: &types.ConditionCheck{
			Key:                 c.metaItemKey(pk),
			TableName:           aws.String(c.tableName),
			ConditionExpression: aws.String("attribute_not_exists(#t) OR #exp <= :now"),
			ExpressionAttributeNames: map[string]string{
				"#t":   metaAttrType,
				"#exp": metaAttrExp,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":now": &types.AttributeValueMemberN{Value: strconv.FormatInt(nowEpoch, 10)},
			},
		},
	}

	for start := 0; start < len(keys); start += batchSize {
		end := start + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		items := make([]types.TransactWriteItem, 0, end-start+1)
		items = append(items, metaDead)

		for _, k := range keys[start:end] {
			items = append(items, types.TransactWriteItem{
				Delete: &types.Delete{
					Key:       k,
					TableName: aws.String(c.tableName),
				},
			})
		}

		_, werr := c.ddbClient.TransactWriteItems(c.context(), &dynamodb.TransactWriteItemsInput{
			TransactItems: items,
		})

		if conditionFailureError(werr) {
			return deleted, true, nil
		}

		if werr != nil {
			return deleted, false, werr
		}

		deleted += end - start
	}

	return deleted, false, nil
}
