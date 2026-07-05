package redimo

import (
	"errors"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// meta item layout (fork v1.7 extension).
//
// Every logical Redis key gets a single companion "meta" item that lives alongside
// its data items under the same partition key (pk). The meta item carries the
// bookkeeping required by the redis-dynamodb-proxy to implement type checking,
// O(1) cardinality and expiry semantics that are independent of DynamoDB's native
// TTL cleanup timing:
//
//	pk = "{db}:{key}"    // same pk as the key's data items
//	sk = "#meta"          // reserved sort key, distinct from any data-item sk
//	t   (S)  key type: str / hash / list / set / zset
//	exp (N)  expiry epoch seconds; absent = never expires. Declared here; the
//	         DynamoDB native-TTL registration and second-precision handling are
//	         owned by task 3.2.
//	cnt (N)  member count, maintained atomically via ADD (supports LLEN/HLEN/SCARD/ZCARD)
//
// The data-item layout used by Strings/Hashes/Lists/Sets/SortedSets is left
// untouched; the meta item is purely additive, so existing data remains readable.
const (
	// MetaSK is the reserved sort key value for a key's meta item.
	MetaSK = "#meta"

	// meta item attribute names.
	metaAttrType  = "t"
	metaAttrExp   = "exp"
	metaAttrCount = "cnt"

	// metaAttrIdxLeft / metaAttrIdxRight are List-only reserved attributes on the
	// #meta item: the monotonic head (decrementing) and tail (incrementing) index
	// counters that give list elements their order. Keeping them on the list's own
	// #meta item — instead of a separate "_redimo/<key>" partition — makes a List a
	// single self-contained partition like every other type, so DeleteMeta/
	// DeleteMembers reclaim the counters with the rest of the key (no orphan).
	metaAttrIdxLeft  = "il"
	metaAttrIdxRight = "ir"
)

// KeyType is the logical Redis type recorded in the meta item's `t` attribute.
type KeyType string

const (
	TypeString KeyType = "str"
	TypeHash   KeyType = "hash"
	TypeList   KeyType = "list"
	TypeSet    KeyType = "set"
	TypeZSet   KeyType = "zset"
)

// Meta is the decoded representation of a key's meta item.
type Meta struct {
	Type  KeyType // attribute t
	Exp   int64   // attribute exp, epoch seconds; 0 = never expires
	Count int64   // attribute cnt
}

// ErrWrongType is returned when a meta conditional write fails because the key
// already exists with a different type. It maps to the Redis
// "-WRONGTYPE Operation against a key holding the wrong kind of value" reply.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// metaItemKey returns the DynamoDB primary key of the reserved #meta item for the
// given logical key. The sort key uses the dedicated meta prefix (skPrefixMeta),
// distinct from any user member/field/value, so a key named literally "#meta"
// cannot collide with — and overwrite — its own metadata.
func (c Client) metaItemKey(key string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		c.partitionKey: &types.AttributeValueMemberB{Value: []byte(key)},
		c.sortKey:      &types.AttributeValueMemberB{Value: []byte{skPrefixMeta}},
	}
}

// isMetaItem reports whether a queried item is the reserved #meta item, detected by
// its dedicated sort-key prefix. Member-enumeration and sweep paths use this instead
// of comparing the decoded sk to "#meta", so a user member/field literally named
// "#meta" (0x01-prefixed) is correctly surfaced and never mistaken for the meta item.
func (c Client) isMetaItem(item map[string]types.AttributeValue) bool {
	b, ok := item[c.sortKey].(*types.AttributeValueMemberB)
	return ok && len(b.Value) > 0 && b.Value[0] == skPrefixMeta
}

// isValueItem reports whether a queried item is the reserved String value item, detected
// by its dedicated sort-key prefix skPrefixValue (0x00, written by valueItemKey). Since v3
// the value item and a collection's empty member ("", now 0x01) are structurally distinct,
// so the collection-enumeration readers (SMEMBERS/SRANDMEMBER/HGETALL/HKEYS/HLEN/HSCAN/
// ZSCAN/ZRANGE-lex) exclude a value item alongside the #meta item — a stale value item left
// by a not-yet-reclaimed type overwrite can no longer surface as a phantom empty member.
// The reclaim paths (DeleteMembers/SweepOrphans) do NOT use this: they must delete the
// value item as part of a key's data.
func (c Client) isValueItem(item map[string]types.AttributeValue) bool {
	b, ok := item[c.sortKey].(*types.AttributeValueMemberB)
	return ok && len(b.Value) > 0 && b.Value[0] == skPrefixValue
}

// EnsureType performs the meta conditional write that underpins every write command.
//
// It executes a single UpdateItem that atomically:
//   - creates the meta item if the key does not yet exist, or verifies the type
//     matches when it does (ConditionExpression: attribute_not_exists(t) OR t = :expected);
//   - sets t to the expected type and applies the count delta
//     (UpdateExpression: SET t = :expected ADD cnt :delta).
//
// It returns newCount, the member count AFTER the delta was applied, read back from the
// same atomic UpdateItem (ReturnValues=ALL_NEW). Callers that empty a collection use this
// authoritative post-write count to decide deletion, instead of a second racy read (see
// DeleteMetaIfEmpty). A zero cntDelta is valid (e.g. for String writes that keep no
// member count): the ADD clause is omitted entirely so the cnt attribute is not churned,
// and newCount reflects the existing count (0 when none is stored). When the key already
// exists with a different type the conditional check fails and ErrWrongType is returned
// without modifying any item.
func (c Client) EnsureType(key string, expected KeyType, cntDelta int64) (newCount int64, err error) {
	names := map[string]string{"#t": metaAttrType}
	values := map[string]types.AttributeValue{
		":expected": &types.AttributeValueMemberS{Value: string(expected)},
	}
	update := "SET #t = :expected"

	if cntDelta != 0 {
		update += " ADD #cnt :delta"
		names["#cnt"] = metaAttrCount
		values[":delta"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(cntDelta, 10)}
	}

	resp, err := c.ddbClient.UpdateItem(c.context(), &dynamodb.UpdateItemInput{
		Key:                       c.metaItemKey(key),
		TableName:                 aws.String(c.tableName),
		ConditionExpression:       aws.String("attribute_not_exists(#t) OR #t = :expected"),
		UpdateExpression:          aws.String(update),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		ReturnValues:              types.ReturnValueAllNew,
	})

	if conditionFailureError(err) {
		return 0, ErrWrongType
	}

	if err != nil {
		return 0, err
	}

	return parseMeta(resp.Attributes).Count, nil
}

// DeleteMetaIfEmpty removes the key's #meta item ONLY IF its member count is absent or
// <= 0. It is the concurrency-safe way to delete a collection that a count-adjusting write
// just emptied: a concurrent write that raised the count (adding a fresh member) makes the
// conditional check fail, so the meta item survives and the fresh member is not stranded
// under a deleted meta (an invisible orphan). Pair it with EnsureType's returned newCount:
// only call this when that post-write count is <= 0. existed reports whether a meta item
// was actually removed (false when the condition failed OR no meta item was present).
func (c Client) DeleteMetaIfEmpty(key string) (existed bool, err error) {
	resp, err := c.ddbClient.DeleteItem(c.context(), &dynamodb.DeleteItemInput{
		Key:                       c.metaItemKey(key),
		TableName:                 aws.String(c.tableName),
		ConditionExpression:       aws.String("attribute_not_exists(#cnt) OR #cnt <= :zero"),
		ExpressionAttributeNames:  map[string]string{"#cnt": metaAttrCount},
		ExpressionAttributeValues: map[string]types.AttributeValue{":zero": &types.AttributeValueMemberN{Value: "0"}},
		ReturnValues:              types.ReturnValueAllOld,
	})

	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return len(resp.Attributes) > 0, nil
}

// CreateTypeIfAbsent atomically creates the meta item with the given type ONLY IF
// the logical key is absent — either it has no meta item at all, or its meta item
// is already expired relative to nowEpoch (lazy expiry). It is the concurrency-safe
// gate for SETNX / SET NX: unlike EnsureType (which succeeds when the key already
// exists with a matching type), this conditions on
//
//	attribute_not_exists(#t) OR #exp <= :now
//
// so it fails for a live key of ANY type (a missing #exp on a never-expiring key
// makes the "#exp <= :now" clause false, so a live never-expiring key is correctly
// rejected). created is false (with a nil error) when the key is live — the
// conditional check failed and nothing was written.
//
// On success it resets the meta as a fresh key: SET #t and #cnt (a plain assign,
// not ADD, so an expired key's stale count is discarded) and REMOVE any #exp. Data
// items belonging to an overwritten expired key of another type are left for the
// proxy's lazy deleter / weekly sweeper, matching DeleteMeta's contract.
//
// Because the existence test and the type/count establishment happen in a single
// UpdateItem on the one meta item, any number of concurrent callers race on that
// item and exactly one observes created=true. This closes the read-then-write
// (TOCTOU) window a separate LoadMeta + EnsureType would leave open, so two racing
// SETNX on the same fresh key can no longer both report success.
func (c Client) CreateTypeIfAbsent(key string, keyType KeyType, cntDelta int64, nowEpoch int64) (created bool, err error) {
	_, err = c.ddbClient.UpdateItem(c.context(), &dynamodb.UpdateItemInput{
		Key:                 c.metaItemKey(key),
		TableName:           aws.String(c.tableName),
		ConditionExpression: aws.String("attribute_not_exists(#t) OR #exp <= :now"),
		UpdateExpression:    aws.String("SET #t = :type, #cnt = :delta REMOVE #exp"),
		ExpressionAttributeNames: map[string]string{
			"#t":   metaAttrType,
			"#cnt": metaAttrCount,
			"#exp": metaAttrExp,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":type":  &types.AttributeValueMemberS{Value: string(keyType)},
			":delta": &types.AttributeValueMemberN{Value: strconv.FormatInt(cntDelta, 10)},
			":now":   &types.AttributeValueMemberN{Value: strconv.FormatInt(nowEpoch, 10)},
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

// LoadMeta reads the meta item for the given key. found is false when the key has
// no meta item (i.e. the key is logically absent).
func (c Client) LoadMeta(key string) (meta Meta, found bool, err error) {
	resp, err := c.ddbClient.GetItem(c.context(), &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key:            c.metaItemKey(key),
		TableName:      aws.String(c.tableName),
	})
	if err != nil || len(resp.Item) == 0 {
		return meta, false, err
	}

	meta = parseMeta(resp.Item)

	return meta, true, nil
}

func parseMeta(item map[string]types.AttributeValue) (meta Meta) {
	meta.Type = KeyType(ReturnValue{item[metaAttrType]}.String())
	meta.Exp = ReturnValue{item[metaAttrExp]}.Int()
	meta.Count = ReturnValue{item[metaAttrCount]}.Int()

	return
}

// DeleteMeta removes only the meta item (sk = "#meta") for the given key, making
// the key immediately logically absent — a subsequent LoadMeta returns found=false
// and the read path treats the key as non-existent. It deliberately does NOT delete
// the key's data items: the proxy's lazy deleter (redimos task 11.1) reclaims those
// asynchronously via Query pk + BatchWriteItem, and the weekly sweeper mops up any
// orphan members. existed reports whether a meta item was present before deletion,
// which lets DEL distinguish a real delete from a no-op on a missing key.
func (c Client) DeleteMeta(key string) (existed bool, err error) {
	resp, err := c.ddbClient.DeleteItem(c.context(), &dynamodb.DeleteItemInput{
		Key:          c.metaItemKey(key),
		TableName:    aws.String(c.tableName),
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		return false, err
	}

	return len(resp.Attributes) > 0, nil
}

// IsExpired reports whether the meta indicates the key is expired relative to
// nowEpoch (epoch seconds). A key is expired when exp > 0 and exp <= now. The
// judgement depends only on meta.exp and the supplied clock, independent of when
// DynamoDB's native TTL actually removes the item.
func IsExpired(meta Meta, nowEpoch int64) bool {
	return meta.Exp > 0 && meta.Exp <= nowEpoch
}
