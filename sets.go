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
// Members are written with BatchWriteItem (25 per call) after a single BatchGetItem
// existence snapshot, so a bulk SADD costs a handful of round-trips instead of one
// PutItem per member. The added set is exact at snapshot time; a concurrent write to the
// SAME member from another connection can make the count approximate (the set contents
// stay correct) — the same best-effort cross-connection contract as the other multi-item
// set operations.
//
// Works similar to https://redis.io/commands/sadd
func (c Client) SADD(key string, members ...string) (addedMembers []string, err error) {
	members = dedupStrings(members)
	if len(members) == 0 {
		return nil, nil
	}

	present, err := c.membersPresent(key, members)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]types.AttributeValue, 0, len(members))
	for _, member := range members {
		items = append(items, setMember{pk: key, sk: member}.toAV(c))
		if !present[member] {
			addedMembers = append(addedMembers, member)
		}
	}

	if err := c.batchPutItems(items, MaxBatchWriteItems); err != nil {
		return nil, err
	}

	return addedMembers, nil
}

// membersPresent reports which of the given members currently exist under key, via
// BatchGetItem (100 keys per call, projecting only the sort key and retrying
// UnprocessedKeys). It underpins the exact added/removed counts of the batched SADD/SREM.
func (c Client) membersPresent(key string, members []string) (map[string]bool, error) {
	present := make(map[string]bool, len(members))

	const batchGetMax = 100 // DynamoDB BatchGetItem hard cap

	for start := 0; start < len(members); start += batchGetMax {
		end := start + batchGetMax
		if end > len(members) {
			end = len(members)
		}

		keys := make([]map[string]types.AttributeValue, 0, end-start)
		for _, m := range members[start:end] {
			keys = append(keys, setMember{pk: key, sk: m}.keyAV(c))
		}

		reqItems := map[string]types.KeysAndAttributes{
			c.tableName: {
				Keys:                 keys,
				ConsistentRead:       aws.Bool(c.consistentReads),
				ProjectionExpression: aws.String(c.sortKey),
			},
		}

		for len(reqItems[c.tableName].Keys) > 0 {
			resp, err := c.ddbClient.BatchGetItem(c.context(), &dynamodb.BatchGetItemInput{
				RequestItems: reqItems,
			})
			if err != nil {
				return nil, err
			}

			for _, item := range resp.Responses[c.tableName] {
				if b, ok := item[c.sortKey].(*types.AttributeValueMemberB); ok {
					present[decodeSK(b.Value)] = true
				}
			}

			un, ok := resp.UnprocessedKeys[c.tableName]
			if !ok || len(un.Keys) == 0 {
				break
			}
			reqItems = resp.UnprocessedKeys
		}
	}

	return present, nil
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
		_, err = c.SADD(destinationKey, members...)
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
		_, err = c.SADD(destinationKey, members...)
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
			if c.isMetaItem(item) { // never surface the reserved #meta item as a member
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
		if c.isMetaItem(item) { // never surface the reserved #meta item as a member
			continue
		}
		members = append(members, parseItem(item, c).sk)
	}

	return
}

// SREM removes the given members from the set at key and returns those that were
// actually present. Like SADD it takes a single BatchGetItem existence snapshot (to
// derive the removed set) and then deletes the present members with BatchWriteItem, so a
// bulk SREM costs a few round-trips rather than one DeleteItem each. The removed set is
// exact at snapshot time; concurrent same-member writes can make it approximate (contents
// stay correct).
func (c Client) SREM(key string, members ...string) (removedMembers []string, err error) {
	members = dedupStrings(members)
	if len(members) == 0 {
		return nil, nil
	}

	present, err := c.membersPresent(key, members)
	if err != nil {
		return nil, err
	}

	keys := make([]keyDef, 0, len(members))
	for _, member := range members {
		if present[member] {
			removedMembers = append(removedMembers, member)
			keys = append(keys, keyDef{pk: key, sk: member})
		}
	}

	if len(keys) > 0 {
		if _, err := c.batchDeleteKeys(keys, MaxBatchWriteItems); err != nil {
			return nil, err
		}
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
		_, err = c.SADD(destinationKey, members...)
	}

	return int32(len(members)), err
}
