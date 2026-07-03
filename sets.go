package redimo

import (
	"context"
	"math/rand"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type setMember struct {
	pk string
	sk string
}

func (sm setMember) toAV(c Client) map[string]types.AttributeValue {
	av := sm.keyAV(c)
	av[c.sortKeyNum] = IntValue{rand.Int63()}.ToAV()

	return av
}

func (sm setMember) keyAV(c Client) map[string]types.AttributeValue {
	av := make(map[string]types.AttributeValue)
	av[c.partitionKey] = &types.AttributeValueMemberB{Value: []byte(sm.pk)}
	av[c.sortKey] = &types.AttributeValueMemberB{Value: encodeSK(sm.sk)}

	return av
}

// SADD adds the given string members to the set at the given key.
//
// Returns that members that were actually added and did not already exist in the set.
//
// Cost is O(1) / 1 WCU for each member, whether it already exists or not.
//
// Works similar to https://redis.io/commands/sadd
func (c Client) SADD(key string, members ...string) (addedMembers []string, err error) {
	for _, member := range members {
		resp, err := c.ddbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
			Item:         setMember{pk: key, sk: member}.toAV(c),
			ReturnValues: types.ReturnValueAllOld,
			TableName:    aws.String(c.tableName),
		})
		if err != nil {
			return addedMembers, err
		}

		if len(resp.Attributes) == 0 {
			addedMembers = append(addedMembers, member)
		}
	}

	return
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
	resp, err := c.ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key:            setMember{pk: key, sk: member}.keyAV(c),
		TableName:      aws.String(c.tableName),
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

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
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
			parsedItem := parseItem(item, c)
			members = append(members, parsedItem.sk)
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

	_, err = c.ddbClient.TransactWriteItems(context.TODO(), &dynamodb.TransactWriteItemsInput{
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

	resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
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
		parsedItem := parseItem(item, c)
		members = append(members, parsedItem.sk)
	}

	return
}

func (c Client) SREM(key string, members ...string) (removedMembers []string, err error) {
	for _, member := range members {
		resp, err := c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
			Key: setMember{
				pk: key,
				sk: member,
			}.keyAV(c),
			ReturnValues: types.ReturnValueAllOld,
			TableName:    aws.String(c.tableName),
		})
		if err != nil {
			return removedMembers, err
		}

		if len(resp.Attributes) > 0 {
			removedMembers = append(removedMembers, member)
		}
	}

	return
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
