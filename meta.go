package redimo

import (
	"context"
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

// metaKeyDef returns the DynamoDB key of the meta item for the given logical key.
func metaKeyDef(key string) keyDef {
	return keyDef{pk: key, sk: MetaSK}
}

// EnsureType performs the meta conditional write that underpins every write command.
//
// It executes a single UpdateItem that atomically:
//   - creates the meta item if the key does not yet exist, or verifies the type
//     matches when it does (ConditionExpression: attribute_not_exists(t) OR t = :expected);
//   - sets t to the expected type and applies the count delta
//     (UpdateExpression: SET t = :expected ADD cnt :delta).
//
// A zero cntDelta is valid (e.g. for String writes that do not maintain a member
// count) and still establishes/verifies the type. When the key already exists with
// a different type the conditional check fails and ErrWrongType is returned without
// modifying any item.
func (c Client) EnsureType(key string, expected KeyType, cntDelta int64) error {
	_, err := c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		Key:                 metaKeyDef(key).toAV(c),
		TableName:           aws.String(c.tableName),
		ConditionExpression: aws.String("attribute_not_exists(#t) OR #t = :expected"),
		UpdateExpression:    aws.String("SET #t = :expected ADD #cnt :delta"),
		ExpressionAttributeNames: map[string]string{
			"#t":   metaAttrType,
			"#cnt": metaAttrCount,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expected": &types.AttributeValueMemberS{Value: string(expected)},
			":delta":    &types.AttributeValueMemberN{Value: strconv.FormatInt(cntDelta, 10)},
		},
	})

	if conditionFailureError(err) {
		return ErrWrongType
	}

	return err
}

// LoadMeta reads the meta item for the given key. found is false when the key has
// no meta item (i.e. the key is logically absent).
func (c Client) LoadMeta(key string) (meta Meta, found bool, err error) {
	resp, err := c.ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key:            metaKeyDef(key).toAV(c),
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
	resp, err := c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		Key:          metaKeyDef(key).toAV(c),
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
