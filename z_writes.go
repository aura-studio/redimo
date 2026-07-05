package redimo

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c Client) ZADD(key string, membersWithScores map[string]float64, flags Flags) (addedMembers []string, err error) {
	for member, score := range membersWithScores {
		builder := newExpresionBuilder()
		builder.updateSetAV(c.sortKeyNum, zScore{score}.ToAV())

		if flags.has(IfNotExists) {
			builder.addConditionNotExists(c.partitionKey)
		}

		if flags.has(IfAlreadyExists) {
			builder.addConditionExists(c.partitionKey)
		}

		resp, err := c.ddbClient.UpdateItem(c.context(), &dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: member}.toAV(c),
			ReturnValues:              types.ReturnValueAllOld,
			TableName:                 aws.String(c.tableName),
			UpdateExpression:          builder.updateExpression(),
		})
		if conditionFailureError(err) {
			continue
		}

		if err != nil {
			return addedMembers, err
		}

		if len(resp.Attributes) == 0 {
			addedMembers = append(addedMembers, member)
		}
	}

	return
}

func (c Client) ZINCRBY(key string, member string, delta float64) (newScore float64, err error) {
	builder := newExpresionBuilder()
	builder.keys[c.sortKeyNum] = struct{}{}
	builder.values["delta"] = zScore{delta}.ToAV()

	resp, err := c.ddbClient.UpdateItem(c.context(), &dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		Key: keyDef{
			pk: key,
			sk: member,
		}.toAV(c),
		ReturnValues:     types.ReturnValueAllNew,
		TableName:        aws.String(c.tableName),
		UpdateExpression: aws.String(fmt.Sprintf("ADD #%v :delta", c.sortKeyNum)),
	})
	if err != nil {
		return newScore, err
	}

	newScore = zScoreFromAV(resp.Attributes[c.sortKeyNum])

	return
}

func (c Client) ZPOPMAX(key string, count int32) (membersWithScores map[string]float64, err error) {
	return c.zPop(key, count, false)
}

func (c Client) ZPOPMIN(key string, count int32) (membersWithScores map[string]float64, err error) {
	return c.zPop(key, count, true)
}

func (c Client) zPop(key string, count int32, forward bool) (membersWithScores map[string]float64, err error) {
	// count <= 0 pops nothing (Redis ZPOPMIN/ZPOPMAX key 0 returns {} and removes
	// nothing). Guard here because zGeneralRange treats count<=0 as UNBOUNDED, which
	// would otherwise make ZPOPMIN(key, 0) drain the entire sorted set.
	if count <= 0 {
		return map[string]float64{}, nil
	}

	membersWithScores, err = c.zGeneralRange(key, negInf, posInf, 0, count, forward, c.sortKeyNum)
	if err != nil {
		return
	}

	poppedMembers := make(map[string]float64)

	for member, score := range membersWithScores {
		popped, err := c.ZREM(key, member)
		if err != nil {
			return poppedMembers, err
		}

		if len(popped) > 0 {
			poppedMembers[member] = score
		}
	}

	return poppedMembers, err
}

func (c Client) ZREM(key string, members ...string) (removedMembers []string, err error) {
	for _, member := range members {
		resp, err := c.ddbClient.DeleteItem(c.context(), &dynamodb.DeleteItemInput{
			Key:          keyDef{pk: key, sk: member}.toAV(c),
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

func (c Client) ZREMRANGEBYLEX(key string, min, max string) (removedMembers []string, err error) {
	membersWithScores, err := c.ZRANGEBYLEX(key, min, max, 0, 0)
	if err == nil {
		removedMembers, err = c.ZREM(key, zReadKeys(membersWithScores)...)
	}

	return
}

func zReadKeys(membersWithScores map[string]float64) []string {
	members := make([]string, 0, len(membersWithScores))
	for member := range membersWithScores {
		members = append(members, member)
	}

	return members
}

func (c Client) ZREMRANGEBYRANK(key string, start, stop int32) (removedMembers []string, err error) {
	membersWithScores, err := c.ZRANGE(key, start, stop)
	if err == nil {
		removedMembers, err = c.ZREM(key, zReadKeys(membersWithScores)...)
	}

	return
}

func (c Client) ZREMRANGEBYSCORE(key string, min, max float64) (removedMembers []string, err error) {
	membersWithScores, err := c.ZRANGEBYSCORE(key, min, max, 0, 0)
	if err == nil {
		removedMembers, err = c.ZREM(key, zReadKeys(membersWithScores)...)
	}

	return
}
