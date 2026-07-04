package redimo

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

	// delete item 0 with condition to prevent concurrent duplicate deletion
	sk := decodeSK(items[0][c.sortKey].(*types.AttributeValueMemberB).Value)

	result, err := c.ddbClient.DeleteItem(c.context(), &dynamodb.DeleteItemInput{
		Key:                      keyDef{pk: key, sk: sk}.toAV(c),
		TableName:                aws.String(c.tableName),
		ReturnValues:             types.ReturnValueAllOld,
		ConditionExpression:      aws.String("attribute_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{"#pk": c.partitionKey},
	})

	if conditionFailureError(err) {
		// Element already deleted by another thread
		return ReturnValue{}, nil
	}

	if err != nil {
		return element, err
	}

	if result.Attributes == nil {
		return element, nil
	}

	element = listElement(items[0][vk])

	return
}

// createLeftIndex allocates the next head index (a fresh, strictly-decreasing
// value) for a left push, and createRightIndex the next tail index (strictly
// increasing) for a right push. Both are a single atomic ADD on the list's own
// #meta item, so concurrent pushes each observe a distinct index with no separate
// counter partition. See bumpListIndex.
func (c Client) createLeftIndex(key string) (index int64, err error) {
	return c.bumpListIndex(key, metaAttrIdxLeft, -1)
}

func (c Client) createRightIndex(key string) (index int64, err error) {
	return c.bumpListIndex(key, metaAttrIdxRight, 1)
}

// bumpListIndex atomically adds delta to a List index attribute (il/ir) on the
// key's #meta item and returns the NEW value. DynamoDB's ADD is atomic and
// ReturnValues=UPDATED_NEW hands each caller its own post-increment value, so
// racing pushes never share an index.
//
// The ADD targets the key's #meta item and will CREATE it (with only the il/ir
// attribute, and no type field) if it is absent — bumpListIndex does not itself
// establish the key's type. In proxy usage the type is established separately by
// EnsureType; a bare redimo caller that pushes without a prior type write gets a
// #meta item carrying only the index counters.
func (c Client) bumpListIndex(key, attr string, delta int64) (int64, error) {
	resp, err := c.ddbClient.UpdateItem(c.context(), &dynamodb.UpdateItemInput{
		Key:                      c.metaItemKey(key),
		TableName:                aws.String(c.tableName),
		UpdateExpression:         aws.String("ADD #idx :delta"),
		ExpressionAttributeNames: map[string]string{"#idx": attr},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":delta": &types.AttributeValueMemberN{Value: strconv.FormatInt(delta, 10)},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	if err != nil {
		return 0, err
	}
	return ReturnValue{resp.Attributes[attr]}.Int(), nil
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

func (c Client) LPUSH(key string, elements ...interface{}) (newLength int64, err error) {
	return c.lPush(key, true, elements...)
}

// listElementAV encodes a list element value for storage in the `val` attribute
// as DynamoDB Binary, so that arbitrary bytes (0x00-0xff) survive round-trips
// without the UTF-8 substitution that the String (S) type would apply. The
// element arrives as a redimo Value; string-shaped values carry their exact
// bytes in the Go string, which we forward as a byte slice losslessly.
func listElementAV(v Value) types.AttributeValue {
	switch tv := v.(type) {
	case StringValue:
		return BytesValue{[]byte(tv.S)}.ToAV()
	case BytesValue:
		return tv.ToAV()
	default:
		// Fall back to the value's own encoding (e.g. numeric values). These
		// were never binary-unsafe, so no conversion is needed.
		return v.ToAV()
	}
}

// listElement decodes a stored `val` attribute back into a ReturnValue. List
// element values are stored as Binary (see listElementAV); to preserve the
// historical string-oriented API (ReturnValue.String()), a Binary value is
// re-wrapped as a String-typed ReturnValue. This is lossless: the bytes read
// back from DynamoDB Binary are placed verbatim into a Go string, which can
// hold any byte sequence. Callers that want the raw bytes can still use
// ReturnValue.Bytes() on the original attribute.
func listElement(av types.AttributeValue) ReturnValue {
	if b, ok := av.(*types.AttributeValueMemberB); ok {
		return ReturnValue{av: &types.AttributeValueMemberS{Value: string(b.Value)}}
	}

	return ReturnValue{av: av}
}

// valueBytes extracts the raw bytes of a list element value, accepting either a
// StringValue or a BytesValue so callers can pass binary-safe elements uniformly
// (like the String/Hash families do) rather than being forced through StringValue.
// The bytes feed genSk's content hash, so a value's identity is its exact bytes
// regardless of which wrapper the caller used.
func valueBytes(v Value) []byte {
	switch tv := v.(type) {
	case BytesValue:
		return tv.B
	case StringValue:
		return []byte(tv.S)
	default:
		return ReturnValue{av: v.ToAV()}.Bytes()
	}
}

// genSk generates sort key from value and index.
// Format: sha256(val)|index
// - SHA256 ensures fixed-length (64 chars) keys regardless of value size
// - Same values will have same hash prefix, enabling efficient range queries for LREM
// - Index suffix ensures uniqueness for multiple instances of same value
func genSk(val string, index int64) string {
	// val to sha256 hash (fixed 64 chars)
	hash := sha256.Sum256([]byte(val))
	hashStr := hex.EncodeToString(hash[:])
	return fmt.Sprintf("%s|%v", hashStr, index)
}

// listItemIndex returns a list element's numeric position (its skN attribute)
// parsed as int64, for ordering elements by their true head/tail index rather
// than by the lexicographic order of the decimal-string Number attribute. A
// missing or unparseable index sorts as 0.
func listItemIndex(item map[string]types.AttributeValue, c Client) int64 {
	return ReturnValue{item[c.sortKeyNum]}.Int()
}

// lPush implements LPUSH/RPUSH.
// TODO: Optimize to use BatchWriteItem for better performance when pushing multiple elements.
// Current implementation makes N separate UpdateItem calls for N elements.
func (c Client) lPush(key string, left bool, elements ...interface{}) (newLength int64, err error) {
	vElements, err := ToValuesE(elements)
	if err != nil {
		return 0, err
	}

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

		builder.updateSetAV(c.sortKeyNum, IntValue{score}.ToAV())
		builder.updateSetAV(vk, listElementAV(e))

		_, err = c.ddbClient.UpdateItem(c.context(), &dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: genSk(string(valueBytes(e)), score)}.toAV(c),
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

func (c Client) RPUSH(key string, elements ...interface{}) (newLength int64, err error) {
	return c.lPush(key, false, elements...)
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
			queryLimit = aws.Int32(int32(remainingCount) + int32(offset) - int32(index))
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

// listItemsToElements decodes a slice of raw list items into their element values.
func listItemsToElements(items []map[string]types.AttributeValue) []ReturnValue {
	elements := make([]ReturnValue, 0, len(items))
	for _, item := range items {
		elements = append(elements, listElement(item[vk]))
	}

	return elements
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

func (c Client) LRANGE(key string, start, stop int64) (elements []ReturnValue, err error) {
	return c.lRange(key, start, stop, true)
}

func (c Client) RPOP(key string) (element ReturnValue, err error) {
	_, items, err := c.lGeneralRangeWithItems(key, 0, 1, false, c.sortKeyNum)

	if err != nil || len(items) == 0 {
		return element, err
	}

	// delete item 0
	sk := decodeSK(items[0][c.sortKey].(*types.AttributeValueMemberB).Value)

	result, err := c.ddbClient.DeleteItem(c.context(), &dynamodb.DeleteItemInput{
		Key:                      keyDef{pk: key, sk: sk}.toAV(c),
		TableName:                aws.String(c.tableName),
		ReturnValues:             types.ReturnValueAllOld,
		ConditionExpression:      aws.String("attribute_exists(#pk)"), // ← 确保元素存在
		ExpressionAttributeNames: map[string]string{"#pk": c.partitionKey},
	})

	if conditionFailureError(err) {
		// 元素已被其他线程删除，返回空
		return ReturnValue{}, nil
	}

	if err != nil {
		return element, err
	}

	if result.Attributes == nil {
		return element, nil
	}

	element = listElement(items[0][vk])

	return
}

func (c Client) LPUSHX(key string, elements ...interface{}) (newLength int64, err error) {
	exist, err := c.EXISTS(key)

	if err != nil || !exist {
		return 0, err
	}

	return c.LPUSH(key, elements...)
}

func (c Client) RPUSHX(key string, elements ...interface{}) (newLength int64, err error) {
	exist, err := c.EXISTS(key)

	if err != nil || !exist {
		return 0, err
	}

	return c.RPUSH(key, elements...)
}

// RPOPLPUSH atomically pops from source and pushes to destination.
// NOTE: This is implemented as two separate operations (RPOP + LPUSH),
// not a true atomic transaction. In case of failure between operations,
// the element may be lost. Consider this limitation in high-concurrency scenarios.
func (c Client) RPOPLPUSH(sourceKey string, destinationKey string) (element ReturnValue, err error) {
	element, err = c.RPOP(sourceKey)

	if err != nil || element.Empty() {
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
	_, err = c.ddbClient.DeleteItem(c.context(), &dynamodb.DeleteItemInput{
		Key:                      keyDef{pk: key, sk: decodeSK(item[c.sortKey].(*types.AttributeValueMemberB).Value)}.toAV(c),
		TableName:                aws.String(c.tableName),
		ConditionExpression:      aws.String("attribute_exists(#pk)"), // ← 确保元素存在
		ExpressionAttributeNames: map[string]string{"#pk": c.partitionKey},
	})

	// add new
	builder := newExpresionBuilder()
	builder.updateSetAV(c.sortKeyNum, IntValue{sknn}.ToAV())
	builder.updateSetAV(vk, listElementAV(StringValue{element}))

	if conditionFailureError(err) {
		// 元素已被其他线程删除，返回空
		return false, nil
	}

	if err != nil {
		return false, err
	}

	_, err = c.ddbClient.UpdateItem(c.context(), &dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		Key:                       keyDef{pk: key, sk: genSk(element, sknn)}.toAV(c),
		ReturnValues:              types.ReturnValueAllOld,
		TableName:                 aws.String(c.tableName),
		UpdateExpression:          builder.updateExpression(),
	})

	if err != nil {
		// Propagate the write failure instead of reporting a silent ok=false: the
		// old element was already deleted above, so the caller must learn the
		// re-insert failed rather than believe LSET was a clean no-op.
		return false, err
	}

	return true, nil
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
	// The list sort key is stored as encodeSK("sha256hex|index"); the begins_with
	// prefix must be encoded the same way (member prefix + "sha256hex|") to match the
	// stored bytes. The begins_with over the member hash also structurally excludes the
	// #meta item, whose sort key is [skPrefixMeta].
	hash := sha256.Sum256([]byte(member))
	hashStr := hex.EncodeToString(hash[:])
	prefix := BytesValue{encodeSK(fmt.Sprintf("%v|", hashStr))}

	items, err = c.pagedListItems(offset, count, forward, false, func(b *expressionBuilder) {
		b.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})
		b.addConditionBeginWith(c.sortKey, prefix)
	})
	if err != nil {
		return make([]ReturnValue, 0), nil, err
	}

	return listItemsToElements(items), items, nil
}

func (c Client) getLRemItems(key string, member string, count int64) (newItems []map[string]types.AttributeValue, err error) {
	_, items, err := c.lGeneralRangeWithItemsByMember(key, 0, -1, true, member)

	if err != nil {
		return newItems, err
	}

	if count == 0 {
		return items, nil
	}

	// The list index (skN) is a DynamoDB Number, stored as a decimal string.
	// Comparing those strings lexicographically is wrong ("100" < "20", "-2" < "-10"),
	// which for LREM's head/tail selection deletes the wrong occurrences. Order by the
	// PARSED numeric index instead so count>0 takes the head-most and count<0 the
	// tail-most matches, matching Redis.
	if count > 0 {
		if count > int64(len(items)) {
			count = int64(len(items))
		}

		sort.Slice(items, func(i, j int) bool {
			return listItemIndex(items[i], c) < listItemIndex(items[j], c)
		})
		return items[:count], nil
	}

	sort.Slice(items, func(i, j int) bool {
		return listItemIndex(items[i], c) > listItemIndex(items[j], c)
	})

	count = -count

	if count > int64(len(items)) {
		count = int64(len(items))
	}

	return items[:count], nil
}

// LREM removes [count] items from the list [key] that match [vElement]
func (c Client) LREM(key string, count int64, element interface{}) (newLength int64, success bool, err error) {
	vElement, err := ToValueE(element)
	if err != nil {
		return 0, false, err
	}

	member := string(valueBytes(vElement))
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

	// delete [count] items with condition to prevent concurrent issues
	actualDeleted := int64(0)
	for i := int64(0); i < count; i++ {
		item := items[i]

		_, err = c.ddbClient.DeleteItem(c.context(), &dynamodb.DeleteItemInput{
			Key:                      keyDef{pk: key, sk: decodeSK(item[c.sortKey].(*types.AttributeValueMemberB).Value)}.toAV(c),
			TableName:                aws.String(c.tableName),
			ConditionExpression:      aws.String("attribute_exists(#pk)"),
			ExpressionAttributeNames: map[string]string{"#pk": c.partitionKey},
		})

		if conditionFailureError(err) {
			// Item already deleted by concurrent operation, skip
			continue
		}

		if err != nil {
			return 0, false, err
		}
		actualDeleted++
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
		_, err = c.ddbClient.DeleteItem(c.context(), &dynamodb.DeleteItemInput{
			Key:                      keyDef{pk: key, sk: decodeSK(item[c.sortKey].(*types.AttributeValueMemberB).Value)}.toAV(c),
			TableName:                aws.String(c.tableName),
			ConditionExpression:      aws.String("attribute_exists(#pk)"),
			ExpressionAttributeNames: map[string]string{"#pk": c.partitionKey},
		})

		if conditionFailureError(err) {
			// Item already deleted, skip
			continue
		}

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
		// Redis semantics: an empty range (start > stop, or start beyond the
		// end of the list) trims away every element, emptying the list.
		return c.lDelete(key, 0, llen-1)
	}

	llen, err = c.lDelete(key, stop+1, llen-1)

	if err != nil {
		return llen, err
	}

	llen, err = c.lDelete(key, 0, start-1)
	return llen, err
}
