package redimo

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	ListSKIndexLeft  = "index_left"
	ListSKIndexRight = "index_right"
	ListSKIndexCount = "index_count"
)

type LSide string

const (
	Left  LSide = "LEFT"
	Right LSide = "RIGHT"
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

func (c Client) LPOP(key string) (element ReturnValue, err error) {
	_, items, err := c.lGeneralRangeWithItems(key, 0, 1, true, c.sortKeyNum)

	if err != nil || len(items) == 0 {
		return element, err
	}

	// delete item 0
	_, err = c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		Key:       keyDef{pk: key, sk: items[0][c.sortKey].(*types.AttributeValueMemberS).Value}.toAV(c),
		TableName: aws.String(c.tableName),
	})

	if err != nil {
		return element, err
	}

	element = ReturnValue{
		av: items[0][vk],
	}
	return
}

func (c Client) createLeftIndex(key string) (index int64, err error) {
	v, err := c.HINCRBY(fmt.Sprintf("_redimo/%v", key), ListSKIndexLeft, -1)
	return int64(v), err
}

func (c Client) createRightIndex(key string) (index int64, err error) {
	v, err := c.HINCRBY(fmt.Sprintf("_redimo/%v", key), ListSKIndexRight, 1)
	return int64(v), err
}

func (c Client) lLen(key string) (count int32, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, StringValue{key})

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.tableName),
			Select:                    types.SelectCount,
		})

		if err != nil {
			return count, err
		}

		count += resp.ScannedCount

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) LPUSH(key string, vElements ...interface{}) (newLength int64, err error) {
	return c.lPush(key, true, vElements...)
}

func genSk(val string, index int64) string {
	// val to base64
	b64 := base64.StdEncoding.EncodeToString([]byte(val))
	return fmt.Sprintf("%s|%v", b64, index)
}

func (c Client) lPush(key string, left bool, vElements ...interface{}) (newLength int64, err error) {
	length, err := c.LLEN(key)

	if err != nil {
		return length, err
	}

	for index, e := range vElements {
		builder := newExpresionBuilder()

		var score int64

		if left {
			score, err = c.createLeftIndex(key)
		} else {
			score, err = c.createRightIndex(key)
		}

		if err != nil {
			return length + int64(index), err
		}

		builder.updateSetAV(c.sortKeyNum, zScore{float64(score)}.ToAV())
		builder.updateSetAV(vk, e.(StringValue).ToAV())

		_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: genSk(e.(StringValue).S, score)}.toAV(c),
			ReturnValues:              types.ReturnValueAllOld,
			TableName:                 aws.String(c.tableName),
			UpdateExpression:          builder.updateExpression(),
		})

		if conditionFailureError(err) {
			continue
		}

		if err != nil {
			return length + int64(index), err
		}
	}

	return length + int64(len(vElements)), nil
}

func (c Client) RPUSH(key string, vElements ...interface{}) (newLength int64, err error) {
	return c.lPush(key, false, vElements...)
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

// offset Ϊ���
func (c Client) lGeneralRange(key string, offset int64, count int64, forward bool, attribute string) (elements []ReturnValue, err error) {
	elements = make([]ReturnValue, 0)
	index := int64(0)
	remainingCount := count
	hasMoreResults := true

	var lastKey map[string]types.AttributeValue

	for hasMoreResults {
		var queryLimit *int32
		if remainingCount > 0 {
			queryLimit = aws.Int32(int32(remainingCount) + int32(offset) - int32(index))
		}

		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, StringValue{key})

		var queryIndex *string
		if attribute == c.sortKeyNum {
			queryIndex = aws.String(c.indexName)
		}

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
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

		if err != nil {
			fmt.Printf("Error in lGeneralRange: %v", err)
			return elements, err
		}

		for _, item := range resp.Items {
			if index >= offset {
				val := parseVal(item[c.sortKey].(*types.AttributeValueMemberS).Value)

				elements = append(elements, ReturnValue{
					av: StringValue{val}.ToAV(),
				})
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

	return elements, nil
}

func parseVal(sk string) string {
	// sk = base64|index
	val := strings.Split(sk, "|")[0]
	decoded, err := base64.StdEncoding.DecodeString(val)
	if err != nil {
		panic(err)
	}
	return string(decoded)
}

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
	elements = make([]ReturnValue, 0)
	index := int64(0)
	remainingCount := count
	hasMoreResults := true

	var lastKey map[string]types.AttributeValue

	for hasMoreResults {
		var queryLimit *int32
		if remainingCount > 0 {
			queryLimit = aws.Int32(int32(remainingCount) + int32(offset) - int32(index))
		}

		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, StringValue{key})

		var queryIndex *string
		if attribute == c.sortKeyNum {
			queryIndex = aws.String(c.indexName)
		}

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
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

		if err != nil {
			fmt.Printf("Error in lGeneralRange: %v", err)
			return elements, items, err
		}

		for _, item := range resp.Items {
			if index >= offset {
				pi := parseItem(item, c)
				elements = append(elements, pi.val)
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

	return elements, items, nil
}

func (c Client) LRANGE(key string, start, stop int64) (elements []ReturnValue, err error) {
	return c.lRange(key, start, stop, true)
}

func (c Client) RPOP(key string) (element ReturnValue, err error) {
	_, items, err := c.lGeneralRangeWithItems(key, 0, 1, false, c.sortKeyNum)

	if err != nil || len(items) == 0 {
		return element, err
	}

	// delete item 0
	sk := items[0][c.sortKey].(*types.AttributeValueMemberS).Value

	result, err := c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		Key:          keyDef{pk: key, sk: sk}.toAV(c),
		TableName:    aws.String(c.tableName),
		ReturnValues: types.ReturnValueAllOld,
	})

	if err != nil {
		return element, err
	}

	if result.Attributes == nil {
		return element, nil
	}

	element = parseItem(items[0], c).val
	return
}

func (c Client) LPUSHX(key string, vElements ...interface{}) (newLength int64, err error) {
	exist, err := c.EXISTS(key)

	if err != nil || !exist {
		return 0, err
	}

	return c.LPUSH(key, vElements...)
}

func (c Client) RPUSHX(key string, vElements ...interface{}) (newLength int64, err error) {
	exist, err := c.EXISTS(key)

	if err != nil || !exist {
		return 0, err
	}

	return c.RPUSH(key, vElements...)
}

func (c Client) RPOPLPUSH(sourceKey string, destinationKey string) (element ReturnValue, err error) {
	element, err = c.RPOP(sourceKey)

	if err != nil {
		return element, err
	}

	_, err = c.LPUSH(destinationKey, StringValue{element.String()})

	if err != nil {
		return element, err
	}

	return
}

func (c Client) LSET(key string, index int64, element string) (ok bool, err error) {
	// get the element at the index
	_, items, err := c.lGeneralRangeWithItems(key, index, 1, true, c.sortKeyNum)

	if err != nil || len(items) == 0 {
		return false, err
	}

	item := items[0]
	skn := item[c.sortKeyNum].(*types.AttributeValueMemberN).Value

	sknn, err := strconv.ParseInt(skn, 10, 64)
	if err != nil {
		panic(err)
	}

	// delete old
	_, err = c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		Key:       keyDef{pk: key, sk: item[c.sortKey].(*types.AttributeValueMemberS).Value}.toAV(c),
		TableName: aws.String(c.tableName),
	})

	// add new
	builder := newExpresionBuilder()
	builder.updateSetAV(c.sortKeyNum, zScore{float64(sknn)}.ToAV())
	builder.updateSetAV(vk, StringValue{element}.ToAV())

	if err != nil {
		return false, err
	}

	_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		Key:                       keyDef{pk: key, sk: genSk(element, sknn)}.toAV(c),
		ReturnValues:              types.ReturnValueAllOld,
		TableName:                 aws.String(c.tableName),
		UpdateExpression:          builder.updateExpression(),
	})

	if err != nil {
		return false, nil
	}

	return true, err
}

func (c Client) lGeneralRangeWithItemsByMember(key string,
	start int64, end int64,
	forward bool, member string) (elements []ReturnValue, items []map[string]types.AttributeValue, err error) {
	llen, err := c.LLEN(key)
	if err != nil {
		return elements, items, err
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
		return elements, items, nil
	}

	count := end - start + 1
	return c.lGeneralRangeWithItemsByMember_(key, start, count, forward, member)
}

func (c Client) lGeneralRangeWithItemsByMember_(key string, offset int64, count int64,
	forward bool, member string) (elements []ReturnValue, items []map[string]types.AttributeValue, err error) {
	elements = make([]ReturnValue, 0)
	index := int64(0)
	remainingCount := count
	hasMoreResults := true

	var lastKey map[string]types.AttributeValue

	for hasMoreResults {
		var queryLimit *int32
		if remainingCount > 0 {
			queryLimit = aws.Int32(int32(remainingCount) + int32(offset) - int32(index))
		}

		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, StringValue{key})

		b64 := base64.StdEncoding.EncodeToString([]byte(member))
		builder.addConditionBeginWith(c.sortKey, StringValue{fmt.Sprintf("%v|", b64)})

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			Limit:                     queryLimit,
			ScanIndexForward:          aws.Bool(forward),
			TableName:                 aws.String(c.tableName),
			Select:                    types.SelectAllAttributes,
		})

		if err != nil {
			return elements, items, err
		}

		for _, item := range resp.Items {
			if index >= offset {
				elements = append(elements, ReturnValue{
					av: item[c.sortKey],
				})
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

	return elements, items, nil
}

func (c Client) getLRemItems(key string, member string, count int64) (newItems []map[string]types.AttributeValue, err error) {
	_, items, err := c.lGeneralRangeWithItemsByMember(key, 0, -1, true, member)

	if err != nil {
		return newItems, err
	}

	if count == 0 {
		return items, nil
	}

	if count > 0 {
		if count > int64(len(items)) {
			count = int64(len(items))
		}

		sort.Slice(items, func(i, j int) bool {
			return items[i][c.sortKeyNum].(*types.AttributeValueMemberN).Value < items[j][c.sortKeyNum].(*types.AttributeValueMemberN).Value
		})
		return items[:count], nil
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i][c.sortKeyNum].(*types.AttributeValueMemberN).Value > items[j][c.sortKeyNum].(*types.AttributeValueMemberN).Value
	})

	count = -count

	if count > int64(len(items)) {
		count = int64(len(items))
	}

	return items[:count], nil
}

// LREM removes [count] items from the list [key] that match [vElement]
func (c Client) LREM(key string, count int64, vElement interface{}) (newLength int64, success bool, err error) {
	member := vElement.(StringValue).ToAV().(*types.AttributeValueMemberS).Value
	var items []map[string]types.AttributeValue

	items, err = c.getLRemItems(key, member, count)

	if err != nil || len(items) == 0 {
		return 0, false, err
	}

	if count < 0 {
		count = -count
	}

	if count > int64(len(items)) || count == 0 {
		count = int64(len(items))
	}

	// delete [count] item
	for i := int64(0); i < count; i++ {
		item := items[i]

		_, err = c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
			Key:       keyDef{pk: key, sk: item[c.sortKey].(*types.AttributeValueMemberS).Value}.toAV(c),
			TableName: aws.String(c.tableName),
		})

		if err != nil {
			return 0, false, err
		}
	}

	newLength, err = c.LLEN(key)
	if err != nil {
		return 0, false, err
	}

	return newLength, true, nil
}

func (c Client) normalizeStartStop(llen int64, start int64, stop int64) (int64, int64) {
	end := stop

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
		return -1, -1
	}

	return start, end
}

func (c Client) lDelete(key string, start int64, stop int64) (newLength int64, err error) {
	llen, err := c.LLEN(key)
	if err != nil {
		return llen, err
	}

	if llen == 0 || stop < start {
		return llen, nil
	}

	if start < 0 || stop < 0 {
		return llen, nil
	}

	_, items, err := c.lGeneralRangeWithItems(key, start, stop-start+1, true, c.sortKeyNum)

	if err != nil {
		return llen, err
	}

	removeCount := int64(0)

	for _, item := range items {
		_, err = c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
			Key:       keyDef{pk: key, sk: item[c.sortKey].(*types.AttributeValueMemberS).Value}.toAV(c),
			TableName: aws.String(c.tableName),
		})

		if err != nil {
			return llen - removeCount, err
		}

		removeCount++
	}

	llen, err = c.LLEN(key)
	return llen, err
}

func (c Client) LTRIM(key string, start int64, stop int64) (newLength int64, err error) {
	llen, err := c.LLEN(key)
	if err != nil {
		return llen, err
	}

	if llen == 0 {
		return
	}

	start, stop = c.normalizeStartStop(llen, start, stop)

	if start == -1 {
		return llen, nil
	}

	llen, err = c.lDelete(key, stop+1, llen-1)

	if err != nil {
		return llen, err
	}

	llen, err = c.lDelete(key, 0, start-1)
	return llen, err
}
