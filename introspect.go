package redimo

import (
	"context"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// redimoInternalPrefix is the reserved partition-key namespace redimo uses for its
// own bookkeeping items (list index counters — see listMetaKey — and stream
// sequence/group state). Such partition keys are NOT user-visible Redis keys and are
// excluded from ScanKeys.
const redimoInternalPrefix = "_redimo/"

// setSkNThreshold is the magnitude at/above which a plain, non-negative integer skN
// is read as a random set-member marker rather than a sorted-set score. Set members
// store skN = rand.Int63() (uniform over [0, 2^63)), so a clear majority sit far above
// this line; genuine zset scores are usually smaller, fractional, negative, or
// exponent-formatted. See TypeOf for the full heuristic and its documented limits.
const setSkNThreshold = int64(1) << 52

// TypeOf reports the Redis type of key — "string", "list", "set", "zset" or "hash" —
// and whether the key exists ("none", false when it does not). redimo v1.6.1 stores
// no type tag, so the type is INFERRED from item shape. This call performs only reads
// and never mutates the table.
//
// Inference:
//   - list  : the reserved "_redimo/<key>" metadata item exists (unambiguous).
//   - string: a single value item at the empty sort-key sentinel ("/") with no skN.
//   - set / zset: members carry an skN. A set member's skN is a random int63 (a plain,
//     non-negative, large integer); a zset member's skN is the score, which is often
//     small, fractional, negative, or exponent-formatted. looksLikeSet classifies by
//     skN shape/magnitude across a sample of the key's items.
//   - hash  : members with a sort key but no skN.
//
// LIMITATION (inherent to a tagless format): a sorted set whose scores are ALL large
// plain integers (a majority ≥ 2^52 ≈ 4.5e15, e.g. nanosecond timestamps) can be
// reported as a set; a set can, with negligible probability, be reported as a zset.
// Every other type is distinguished exactly.
func (c Client) TypeOf(key string) (redisType string, exists bool, err error) {
	// 1) List: the metadata sibling "_redimo/<key>" carries the index_right counter
	// for every list and nothing else — a definitive, collision-free marker.
	metaResp, err := c.ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key: map[string]types.AttributeValue{
			c.partitionKey: &types.AttributeValueMemberS{Value: listMetaKey(key)},
			c.sortKey:      &types.AttributeValueMemberS{Value: ListSKIndexRight},
		},
		TableName: aws.String(c.tableName),
	})
	if err != nil {
		return "", false, err
	}
	if len(metaResp.Item) > 0 {
		return "list", true, nil
	}

	// 2) Sample the items stored at pk=key.
	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, StringValue{key})

	resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(c.consistentReads),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		KeyConditionExpression:    builder.conditionExpression(),
		Limit:                     aws.Int32(32),
		TableName:                 aws.String(c.tableName),
	})
	if err != nil {
		return "", false, err
	}
	if len(resp.Items) == 0 {
		return "none", false, nil
	}

	var skNs []string
	sawStringValueItem := false

	for _, item := range resp.Items {
		if skN, ok := item[c.sortKeyNum].(*types.AttributeValueMemberN); ok {
			skNs = append(skNs, skN.Value)
			continue
		}
		// No skN on this item: a string value item lives at the empty-SK sentinel.
		if sk, ok := item[c.sortKey].(*types.AttributeValueMemberS); ok && sk.Value == emptySK {
			sawStringValueItem = true
		}
	}

	switch {
	case len(skNs) > 0:
		if looksLikeSet(skNs) {
			return "set", true, nil
		}
		return "zset", true, nil
	case sawStringValueItem:
		return "string", true, nil
	default:
		return "hash", true, nil
	}
}

// looksLikeSet classifies a sample of skN strings as a set (true) or a sorted set
// (false). A single member whose skN is fractional, negative or exponent-formatted is
// conclusive proof of a real score (a set's rand-int63 skN never is), so it forces
// zset. Otherwise the decision is by majority magnitude, which is robust to the rare
// small random member of a set or large score of a zset.
func looksLikeSet(skNs []string) bool {
	if len(skNs) == 0 {
		return false
	}

	big := 0
	for _, s := range skNs {
		if strings.ContainsAny(s, ".eE-") {
			return false
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			// Not a plain int64 (e.g. a score too large to have avoided exponent
			// formatting) — treat as a score.
			return false
		}
		if n >= setSkNThreshold {
			big++
		}
	}

	return big*2 >= len(skNs)
}

// ScanKeys pages the distinct logical Redis keys in the table using a raw DynamoDB
// Scan that projects only the partition key. redimo-internal partition keys (the
// reserved "_redimo/" namespace used by list index counters and stream state) are
// excluded. Pass a nil startKey to begin; the returned nextKey is nil once iteration
// is complete. Distinct keys are deduplicated within a page; a key that straddles a
// page boundary may recur across pages, which matches Redis SCAN's duplicate
// semantics. This call performs only reads.
func (c Client) ScanKeys(limit int32, startKey map[string]types.AttributeValue) (keys []string, nextKey map[string]types.AttributeValue, err error) {
	input := &dynamodb.ScanInput{
		TableName:                aws.String(c.tableName),
		ProjectionExpression:     aws.String("#pk"),
		ExpressionAttributeNames: map[string]string{"#pk": c.partitionKey},
	}
	if limit > 0 {
		input.Limit = aws.Int32(limit)
	}
	if len(startKey) > 0 {
		input.ExclusiveStartKey = startKey
	}

	resp, err := c.ddbClient.Scan(context.TODO(), input)
	if err != nil {
		return nil, nil, err
	}

	seen := make(map[string]struct{}, len(resp.Items))
	for _, item := range resp.Items {
		pkAV, ok := item[c.partitionKey].(*types.AttributeValueMemberS)
		if !ok {
			continue
		}
		pk := pkAV.Value
		if strings.HasPrefix(pk, redimoInternalPrefix) {
			continue
		}
		if _, dup := seen[pk]; dup {
			continue
		}
		seen[pk] = struct{}{}
		keys = append(keys, pk)
	}

	if len(resp.LastEvaluatedKey) > 0 {
		nextKey = resp.LastEvaluatedKey
	}

	return keys, nextKey, nil
}
