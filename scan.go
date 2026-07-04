package redimo

import (
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Keyspace scan (fork v1.7 extension, redimos task 17.2).
//
// ScanMetaKeys is the storage primitive behind the proxy's SCAN command. Redis
// SCAN pages the keyspace with an opaque cursor; the proxy bridges Redis' uint64
// cursor to DynamoDB's LastEvaluatedKey via its own registry (internal/scan) and
// drives one Scan page per SCAN call through this method.
//
// It pages through the table returning the partition keys (pk) of LIVE meta items
// — items whose sort key is the reserved "#meta" value and whose exp attribute is
// absent or still in the future relative to nowEpoch. The expiry predicate is
// pushed into the DynamoDB FilterExpression so physically-present-but-logically-
// expired keys (whose native-TTL sweep has not yet run) are never surfaced,
// matching the read path's "correctness is guaranteed by read-path filtering"
// contract.
//
// Only the partition key is projected: SCAN reports key names, not values, so the
// data attributes are never read. A single call returns one page; lastEvaluatedKey
// is the DynamoDB pagination token to pass back as exclusiveStartKey on the next
// call, or nil when the scan has reached the end of the table. limit maps Redis'
// COUNT hint onto the DynamoDB Limit (the maximum number of items EVALUATED per
// page, applied before the filter, so a page may return fewer — even zero — keys
// while still yielding a non-nil lastEvaluatedKey); a value <= 0 leaves Limit
// unset so DynamoDB chooses the page size.
func (c Client) ScanMetaKeys(limit int32, exclusiveStartKey map[string]types.AttributeValue, nowEpoch int64) (pks []string, lastEvaluatedKey map[string]types.AttributeValue, err error) {
	input := &dynamodb.ScanInput{
		TableName:            aws.String(c.tableName),
		ExclusiveStartKey:    exclusiveStartKey,
		ProjectionExpression: aws.String("#pk"),
		FilterExpression:     aws.String("#sk = :meta AND (attribute_not_exists(#exp) OR #exp > :now)"),
		ExpressionAttributeNames: map[string]string{
			"#pk":  c.partitionKey,
			"#sk":  c.sortKey,
			"#exp": metaAttrExp,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			// The reserved #meta item's sort key is Binary [skPrefixMeta]; the filter
			// must use that exact Binary form, or "#sk = :meta" never matches and the
			// scan silently returns no keys.
			":meta": &types.AttributeValueMemberB{Value: []byte{skPrefixMeta}},
			":now":  &types.AttributeValueMemberN{Value: strconv.FormatInt(nowEpoch, 10)},
		},
	}
	if limit > 0 {
		input.Limit = aws.Int32(limit)
	}

	resp, err := c.ddbClient.Scan(c.context(), input)
	if err != nil {
		return nil, nil, err
	}

	pks = make([]string, 0, len(resp.Items))
	for _, item := range resp.Items {
		pks = append(pks, parseKey(item, c).pk)
	}

	if len(resp.LastEvaluatedKey) > 0 {
		lastEvaluatedKey = resp.LastEvaluatedKey
	}

	return pks, lastEvaluatedKey, nil
}

// HScanField is a single field/value pair returned by one page of HScanPage. It
// mirrors the field-item layout: Field is the item's sort key and Value is the
// item's value attribute.
type HScanField struct {
	Field string
	Value ReturnValue
}

// HScanPage is the storage primitive behind the proxy's HSCAN command (redimos
// task 13.2). Where ScanMetaKeys pages the WHOLE table for SCAN, HScanPage pages
// WITHIN a single partition key — the fields of one hash — so HSCAN reuses SCAN's
// cursor machinery but iterates a key's members instead of the keyspace.
//
// It Queries the given key's partition and returns one page of its field items,
// EXCLUDING the reserved meta item (sk == MetaSK) so the meta item is never
// surfaced as a hash field (matching HGETALL's filtering). limit maps Redis'
// COUNT hint onto the DynamoDB Limit (the maximum number of items EVALUATED per
// page, applied before the meta-item filter, so a page may return fewer — even
// zero — fields while still yielding a non-nil lastEvaluatedKey); a value <= 0
// leaves Limit unset so DynamoDB chooses the page size.
//
// exclusiveStartKey is the DynamoDB pagination token from the previous page's
// lastEvaluatedKey (nil starts a fresh page from the beginning of the partition);
// lastEvaluatedKey is the token to pass back on the next call, or nil when the
// partition has been fully paged (HSCAN then reports the terminating cursor 0).
func (c Client) HScanPage(key string, limit int32, exclusiveStartKey map[string]types.AttributeValue) (fields []HScanField, lastEvaluatedKey map[string]types.AttributeValue, err error) {
	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

	input := &dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(c.consistentReads),
		ExclusiveStartKey:         exclusiveStartKey,
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		KeyConditionExpression:    builder.conditionExpression(),
		TableName:                 aws.String(c.tableName),
	}
	if limit > 0 {
		input.Limit = aws.Int32(limit)
	}

	resp, err := c.ddbClient.Query(c.context(), input)
	if err != nil {
		return nil, nil, err
	}

	fields = collectNonMetaItems(c, resp.Items, func(item map[string]types.AttributeValue) HScanField {
		parsed := parseItem(item, c)
		return HScanField{Field: parsed.sk, Value: parsed.val}
	})

	if len(resp.LastEvaluatedKey) > 0 {
		lastEvaluatedKey = resp.LastEvaluatedKey
	}

	return fields, lastEvaluatedKey, nil
}

// collectNonMetaItems builds one result per non-#meta item in items, preserving order.
// It is the shared page-collection loop for HScanPage and ZScanPage, which differ only
// in the element type each constructs from a raw item.
func collectNonMetaItems[T any](c Client, items []map[string]types.AttributeValue, build func(map[string]types.AttributeValue) T) []T {
	out := make([]T, 0, len(items))
	for _, item := range items {
		if c.isMetaItem(item) {
			continue // never surface the reserved #meta item
		}
		out = append(out, build(item))
	}

	return out
}

// ZScanMember is a single member/score pair returned by one page of ZScanPage.
// Member is the item's sort key; Score is read from the numeric sort-key
// attribute (skN) the score index orders on, so ZSCAN can reply the member/score
// pairs Redis' ZSCAN wire shape carries.
type ZScanMember struct {
	Member string
	Score  float64
}

// ZScanPage is the storage primitive behind the proxy's ZSCAN command (redimos
// task 15.2). It is the Sorted Set analogue of HScanPage: where HScanPage pages a
// hash's field items, ZScanPage pages a sorted set's member items WITHIN a single
// partition key via a base-table Query, so ZSCAN reuses SCAN's cursor machinery
// but iterates one key's members instead of the keyspace.
//
// Unlike HScanPage — which projects the field value attribute — a sorted-set
// member carries its score in the numeric sort-key attribute (skN), so each item
// is decoded to a Member (its sort key) and a Score (skN). The reserved meta item
// (sk == MetaSK) is excluded so it is never surfaced as a member (matching
// ZRANGE's filtering).
//
// The page is Queried in base-table sort-key (member) order, NOT score order:
// ZSCAN — like SCAN/HSCAN/SSCAN — makes no ordering guarantee, and paging the base
// table lets the opaque LastEvaluatedKey resume cleanly. limit maps Redis' COUNT
// hint onto the DynamoDB Limit (the maximum number of items EVALUATED per page,
// applied before the meta-item filter, so a page may return fewer — even zero —
// members while still yielding a non-nil lastEvaluatedKey); a value <= 0 leaves
// Limit unset so DynamoDB chooses the page size.
//
// exclusiveStartKey is the DynamoDB pagination token from the previous page's
// lastEvaluatedKey (nil starts a fresh page from the beginning of the partition);
// lastEvaluatedKey is the token to pass back on the next call, or nil when the
// partition has been fully paged (ZSCAN then reports the terminating cursor 0).
func (c Client) ZScanPage(key string, limit int32, exclusiveStartKey map[string]types.AttributeValue) (members []ZScanMember, lastEvaluatedKey map[string]types.AttributeValue, err error) {
	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

	input := &dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(c.consistentReads),
		ExclusiveStartKey:         exclusiveStartKey,
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		KeyConditionExpression:    builder.conditionExpression(),
		TableName:                 aws.String(c.tableName),
	}
	if limit > 0 {
		input.Limit = aws.Int32(limit)
	}

	resp, err := c.ddbClient.Query(c.context(), input)
	if err != nil {
		return nil, nil, err
	}

	members = collectNonMetaItems(c, resp.Items, func(item map[string]types.AttributeValue) ZScanMember {
		parsed := parseItem(item, c)
		return ZScanMember{Member: parsed.sk, Score: zScoreFromAV(item[c.sortKeyNum])}
	})

	if len(resp.LastEvaluatedKey) > 0 {
		lastEvaluatedKey = resp.LastEvaluatedKey
	}

	return members, lastEvaluatedKey, nil
}
