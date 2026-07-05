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

func (c Client) LPUSH(key string, elements ...any) (newLength int64, err error) {
	return c.lPush(key, true, elements...)
}

// listItemIndex returns a list element's numeric position (its skN attribute)
// parsed as int64, for ordering elements by their true head/tail index rather
// than by the lexicographic order of the decimal-string Number attribute. A
// missing or unparseable index sorts as 0.
func listItemIndex(item map[string]types.AttributeValue, c Client) int64 {
	return ReturnValue{item[c.sortKeyNum]}.Int()
}

// lPush implements LPUSH/RPUSH. All N element indices are allocated in ONE atomic
// index bump (ADD il/ir by ±N on the #meta item) and the N element items are written
// with BatchWriteItem (25 per call), instead of the old 2N sequential UpdateItems (one
// index bump + one write per element). Concurrent pushes each receive a distinct,
// non-overlapping index range from the atomic ADD, so element order is preserved.
func (c Client) lPush(key string, left bool, elements ...any) (newLength int64, err error) {
	vElements, err := ToValuesE(elements)
	if err != nil {
		return 0, err
	}

	length, err := c.LLEN(key)
	if err != nil {
		return length, err
	}

	n := int64(len(vElements))
	if n == 0 {
		return length, nil
	}

	// One atomic ADD allocates the whole contiguous index range and returns its far
	// end (the new il/ir). The per-element loop assigned element[i] the index
	// il0-(i+1) (LPUSH) or ir0+(i+1) (RPUSH); reconstruct exactly those from the
	// returned end so ordering is identical to the sequential path.
	attr, delta := metaAttrIdxRight, n
	if left {
		attr, delta = metaAttrIdxLeft, -n
	}

	endIndex, err := c.bumpListIndex(key, attr, delta)
	if err != nil {
		return length, err
	}

	items := make([]map[string]types.AttributeValue, n)
	for i, e := range vElements {
		var index int64
		if left {
			index = endIndex + (n - 1 - int64(i)) // il0-(i+1)
		} else {
			index = endIndex - (n - 1 - int64(i)) // ir0+(i+1)
		}

		items[i] = map[string]types.AttributeValue{
			c.partitionKey: &types.AttributeValueMemberB{Value: []byte(key)},
			c.sortKey:      &types.AttributeValueMemberB{Value: encodeSK(genSk(string(valueBytes(e)), index))},
			c.sortKeyNum:   IntValue{index}.ToAV(),
			vk:             listElementAV(e),
		}
	}

	if err := c.batchPutItems(items, MaxBatchWriteItems); err != nil {
		return length, err
	}

	return length + n, nil
}

func (c Client) RPUSH(key string, elements ...any) (newLength int64, err error) {
	return c.lPush(key, false, elements...)
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

func (c Client) LPUSHX(key string, elements ...any) (newLength int64, err error) {
	exist, err := c.EXISTS(key)

	if err != nil || !exist {
		return 0, err
	}

	return c.LPUSH(key, elements...)
}

func (c Client) RPUSHX(key string, elements ...any) (newLength int64, err error) {
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

// LSET sets the list element at index to element. element accepts any redimo-coercible
// value (string/[]byte/numeric/Value), matching LPUSH/RPUSH/LREM, so callers no longer
// have to stringify binary or numeric values.
func (c Client) LSET(key string, index int64, element any) (ok bool, err error) {
	vElement, err := ToValueE(element)
	if err != nil {
		return false, err
	}

	// get the element at the index
	_, items, err := c.lGeneralRangeWithItems(key, index, 1, true, c.sortKeyNum)

	if err != nil || len(items) == 0 {
		return false, err
	}

	item := items[0]
	skn := item[c.sortKeyNum].(*types.AttributeValueMemberN).Value

	sknn, err := strconv.ParseInt(skn, 10, 64)
	if err != nil {
		// A stored list index that will not parse means the item is corrupt; surface it
		// rather than crashing the process (this backs a network proxy).
		return false, fmt.Errorf("redimo: LSET: unparseable list index %q: %w", skn, err)
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
	builder.updateSetAV(vk, listElementAV(vElement))

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
		Key:                       keyDef{pk: key, sk: genSk(string(valueBytes(vElement)), sknn)}.toAV(c),
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
func (c Client) LREM(key string, count int64, element any) (newLength int64, success bool, err error) {
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

	// Delete the selected occurrences with BatchWriteItem (25 per call) instead of one
	// conditional DeleteItem each. BatchWriteItem deletes are idempotent — a key another
	// operation already removed is a harmless no-op — so the previous per-item
	// attribute_exists guard is unnecessary; the authoritative post-state is read back via
	// LLEN below. NOTE: getLRemItems still reads every occurrence of the value and orders
	// them numerically in memory (the base-table sort key is lexicographic on the index
	// suffix, so a pushed-down LIMIT would select the wrong occurrences); bounding that
	// read would need an order-preserving index encoding (storage-breaking, deferred).
	keys := make([]keyDef, 0, count)
	for i := int64(0); i < count; i++ {
		sk := decodeSK(items[i][c.sortKey].(*types.AttributeValueMemberB).Value)
		keys = append(keys, keyDef{pk: key, sk: sk})
	}

	if _, err := c.batchDeleteKeys(keys, MaxBatchWriteItems); err != nil {
		return 0, false, err
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
