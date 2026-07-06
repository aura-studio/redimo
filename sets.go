package redimo

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type setMember struct {
	pk string
	sk string
}

// toAV builds the set member's item as a pure pk+sk item (the member IS the sort
// key), the same flat layout as a hash field. Set members are never enumerated
// through the numeric LSI, so no skN is written — a random one would only bloat
// the index and make SADD non-idempotent at the attribute level.
func (sm setMember) toAV(c Client) map[string]types.AttributeValue {
	return sm.keyAV(c)
}

func (sm setMember) keyAV(c Client) map[string]types.AttributeValue {
	av := make(map[string]types.AttributeValue)
	av[c.partitionKey] = &types.AttributeValueMemberB{Value: []byte(sm.pk)}
	av[c.sortKey] = &types.AttributeValueMemberB{Value: encodeSK(sm.sk)}

	return av
}

// SADD adds the given string members to the set at the given key.
//
// Returns the members that were actually added and did not already exist in the set.
//
// Each member is written with an individual conditional PutItem (attribute_not_exists) and
// is counted as newly added only when the condition holds — i.e. it did not already exist.
// DynamoDB evaluates the condition atomically with the write, serialized per item, so the
// returned count is concurrency-EXACT: when several connections race to add the SAME member,
// exactly one condition succeeds (the rest fail with ConditionalCheckFailed), so a caller
// that maintains an O(1) cardinality counter (SCARD) from this count cannot over-count.
// (SADD previously used a single BatchGetItem existence snapshot then a batched write; the
// snapshot over-reported the added count under a concurrent same-member add, inflating SCARD
// above the true cardinality — the set contents were always correct. A conditional write is
// used rather than a plain PutItem + ReturnValue ALL_OLD because the condition check is
// atomic on both real DynamoDB and DynamoDB Local even under heavy same-partition contention
// with the #meta counter, whereas the returned-old-value can be raced on the local emulator.
// The trade-off is one round-trip per member instead of a handful for a bulk SADD; the
// per-member existence read is gone, so a single-member SADD is now cheaper. Callers that
// only need the write and take the count from a known result size — the *STORE builders —
// keep the batched fast path via saddUncounted.)
//
// Works similar to https://redis.io/commands/sadd
func (c Client) SADD(key string, members ...string) (addedMembers []string, err error) {
	members = dedupStrings(members)
	if len(members) == 0 {
		return nil, nil
	}

	for _, member := range members {
		builder := newExpresionBuilder()
		builder.addConditionNotExists(c.partitionKey)

		_, err := c.ddbClient.PutItem(c.context(), &dynamodb.PutItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Item:                      setMember{pk: key, sk: member}.toAV(c),
			TableName:                 aws.String(c.tableName),
		})
		if conditionFailureError(err) {
			continue // already a member — not newly added
		}
		if err != nil {
			return addedMembers, err
		}

		addedMembers = append(addedMembers, member)
	}

	return addedMembers, nil
}

// saddUncounted writes members to the set at key WITHOUT computing which were new. It keeps
// the batched-write fast path for callers — the *STORE result-set builders — that overwrite
// a freshly built destination and take the cardinality from the known result size, where
// SADD's per-member exact count is unnecessary and its extra round-trips would be wasteful.
func (c Client) saddUncounted(key string, members []string) error {
	members = dedupStrings(members)
	if len(members) == 0 {
		return nil
	}

	items := make([]map[string]types.AttributeValue, 0, len(members))
	for _, member := range members {
		items = append(items, setMember{pk: key, sk: member}.toAV(c))
	}

	return c.batchPutItems(items, MaxBatchWriteItems)
}

// SCARD returns the cardinality (the number of elements) in the set at key.
//
// Cost is O(size) / 1 WCU per 4KB of data counted.
//
// Works similar to https://redis.io/commands/scard
func (c Client) SCARD(key string) (count int32, err error) {
	return c.HLEN(key)
}

func (c Client) SDIFF(key string, subtractKeys ...string) (members []string, err error) {
	memberSet := make(map[string]struct{})
	startingList, err := c.SMEMBERS(key)

	if err != nil {
		return
	}

	for _, member := range startingList {
		memberSet[member] = struct{}{}
	}

	for _, otherKey := range subtractKeys {
		otherList, err := c.SMEMBERS(otherKey)
		if err != nil {
			return members, err
		}

		for _, member := range otherList {
			delete(memberSet, member)
		}
	}

	for member := range memberSet {
		members = append(members, member)
	}

	return
}

func (c Client) SDIFFSTORE(destinationKey string, sourceKey string, subtractKeys ...string) (count int32, err error) {
	members, err := c.SDIFF(sourceKey, subtractKeys...)
	if err == nil {
		err = c.saddUncounted(destinationKey, members)
	}

	return int32(len(members)), err
}

func (c Client) SINTER(key string, otherKeys ...string) (members []string, err error) {
	memberSet := make(map[string]struct{})
	startingList, err := c.SMEMBERS(key)

	if err != nil {
		return
	}

	for _, member := range startingList {
		memberSet[member] = struct{}{}
	}

	for _, otherKey := range otherKeys {
		otherList, err := c.SMEMBERS(otherKey)
		if err != nil {
			return members, err
		}

		otherSet := make(map[string]struct{})

		for _, member := range otherList {
			otherSet[member] = struct{}{}
		}

		for member := range memberSet {
			if _, ok := otherSet[member]; !ok {
				delete(memberSet, member)
			}
		}
	}

	for member := range memberSet {
		members = append(members, member)
	}

	return
}

func (c Client) SINTERSTORE(destinationKey string, sourceKey string, otherKeys ...string) (count int32, err error) {
	members, err := c.SINTER(sourceKey, otherKeys...)
	if err == nil {
		err = c.saddUncounted(destinationKey, members)
	}

	return int32(len(members)), err
}

func (c Client) SISMEMBER(key string, member string) (ok bool, err error) {
	// Only existence is needed, so project just the partition key rather than
	// fetching the whole member item (a member can carry a large value/score);
	// this trims the read payload for a hot membership check.
	resp, err := c.ddbClient.GetItem(c.context(), &dynamodb.GetItemInput{
		ConsistentRead:       aws.Bool(c.consistentReads),
		Key:                  setMember{pk: key, sk: member}.keyAV(c),
		ProjectionExpression: aws.String(c.partitionKey),
		TableName:            aws.String(c.tableName),
	})
	if err != nil || len(resp.Item) == 0 {
		return
	}

	return true, nil
}

func (c Client) SMEMBERS(key string) (members []string, err error) {
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
			return members, err
		}

		for _, item := range resp.Items {
			if c.isMetaItem(item) || c.isValueItem(item) { // skip the #meta and (stale) value items
				continue
			}
			members = append(members, parseItem(item, c).sk)
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) SMOVE(sourceKey string, destinationKey string, member string) (ok bool, err error) {
	builder := newExpresionBuilder()
	builder.addConditionExists(c.partitionKey)

	_, err = c.ddbClient.TransactWriteItems(c.context(), &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Delete: &types.Delete{
					ConditionExpression:       builder.conditionExpression(),
					ExpressionAttributeNames:  builder.expressionAttributeNames(),
					ExpressionAttributeValues: builder.expressionAttributeValues(),
					Key:                       setMember{pk: sourceKey, sk: member}.keyAV(c),
					TableName:                 aws.String(c.tableName),
				},
			},
			{
				Put: &types.Put{
					Item:      setMember{pk: destinationKey, sk: member}.toAV(c),
					TableName: aws.String(c.tableName),
				},
			},
		},
	})

	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (c Client) SPOP(key string, count int32) (members []string, err error) {
	members, err = c.SRANDMEMBER(key, count)
	if err == nil {
		_, err = c.SREM(key, members...)
	}

	return
}

func (c Client) SRANDMEMBER(key string, count int32) (members []string, err error) {
	if count < 0 {
		count = -count
	}

	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

	resp, err := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(c.consistentReads),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		KeyConditionExpression:    builder.conditionExpression(),
		Limit:                     aws.Int32(count),
		TableName:                 aws.String(c.tableName),
	})

	if err != nil {
		return members, err
	}

	for _, item := range resp.Items {
		if c.isMetaItem(item) || c.isValueItem(item) { // skip the #meta and (stale) value items
			continue
		}
		members = append(members, parseItem(item, c).sk)
	}

	return
}

// SREM removes the given members from the set at key and returns those that were actually
// present and removed. Each member is deleted with an individual conditional DeleteItem
// (attribute_exists) and is counted as removed only when the condition holds — i.e. it was
// actually present. DynamoDB evaluates the condition atomically with the delete, serialized
// per item, so the returned count is concurrency-EXACT: when several connections race to
// remove the SAME member, exactly one condition succeeds (the rest fail with
// ConditionalCheckFailed), so a caller maintaining an O(1) cardinality counter (SCARD) from
// this count cannot over-decrement. (SREM previously used a BatchGetItem existence snapshot
// then a batched delete; the snapshot over-reported removals under a concurrent same-member
// SREM, deflating SCARD below the true cardinality — the set contents were always correct.
// The same-partition-safe conditional check is used rather than a plain DeleteItem +
// ReturnValue ALL_OLD for the reason given on SADD.)
func (c Client) SREM(key string, members ...string) (removedMembers []string, err error) {
	members = dedupStrings(members)
	if len(members) == 0 {
		return nil, nil
	}

	for _, member := range members {
		builder := newExpresionBuilder()
		builder.addConditionExists(c.partitionKey)

		_, err := c.ddbClient.DeleteItem(c.context(), &dynamodb.DeleteItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: member}.toAV(c),
			TableName:                 aws.String(c.tableName),
		})
		if conditionFailureError(err) {
			continue // wasn't a member — nothing removed
		}
		if err != nil {
			return removedMembers, err
		}

		removedMembers = append(removedMembers, member)
	}

	return removedMembers, nil
}

func (c Client) SUNION(keys ...string) (members []string, err error) {
	memberSet := make(map[string]struct{})

	for _, key := range keys {
		setMembers, err := c.SMEMBERS(key)
		if err != nil {
			return members, err
		}

		for _, member := range setMembers {
			memberSet[member] = struct{}{}
		}
	}

	for member := range memberSet {
		members = append(members, member)
	}

	return
}

func (c Client) SUNIONSTORE(destinationKey string, sourceKeys ...string) (count int32, err error) {
	members, err := c.SUNION(sourceKeys...)
	if err == nil {
		err = c.saddUncounted(destinationKey, members)
	}

	return int32(len(members)), err
}
