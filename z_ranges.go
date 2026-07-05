package redimo

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// zBuildRangeCondition adds the min/max range bound to a sorted-set query's
// expression builder: it binds each present cap to the given value placeholder
// (lowerKey/upperKey) and emits a single BETWEEN condition when both caps are
// present, or a one-sided >= / <= comparison when only one is. The placeholder
// names are parameters so each caller reproduces its exact original expression
// (zGeneralCount uses min/max, zGeneralRange uses start/stop).
func zBuildRangeCondition(builder *expressionBuilder, lower, upper rangeCap, attribute, lowerKey, upperKey string) {
	if lower.present() {
		builder.values[lowerKey] = lower.ToAV()
	}

	if upper.present() {
		builder.values[upperKey] = upper.ToAV()
	}

	switch {
	case lower.present() && upper.present():
		builder.condition(fmt.Sprintf("#%v BETWEEN :%v AND :%v", attribute, lowerKey, upperKey), attribute)
	case lower.present():
		builder.condition(fmt.Sprintf("#%v >= :%v", attribute, lowerKey), attribute)
	case upper.present():
		builder.condition(fmt.Sprintf("#%v <= :%v", attribute, upperKey), attribute)
	}
}

func (c Client) ZCARD(key string) (count int32, err error) {
	return c.HLEN(key)
}

// ZScoredMember pairs a sorted-set member with its score. It is returned by
// ZMembersOrdered, which — unlike the map-returning range helpers (ZRANGE /
// ZRANGEBYSCORE) — preserves the score order DynamoDB yields so callers that need
// deterministic ranking (rank ranges, ZRANK, ZCOUNT) can layer on top of it.
type ZScoredMember struct {
	Member string
	Score  float64
}

// ZMembersOrdered returns every member of the sorted set at key together with its
// score, ordered by score: ascending when forward is true, descending otherwise.
// Members sharing a score are ordered by member value in the same direction,
// because the score index breaks ties on the base-table sort key. The reserved
// meta item carries no score attribute and is therefore not part of the score
// index, so it is naturally excluded.
//
// It is the ordered-read primitive the redimos proxy layers ZRANGE / ZREVRANGE /
// ZRANK / ZREVRANK / ZCOUNT / ZREMRANGEBY* on top of; the map-returning range
// helpers cannot express order because a Go map has none.
func (c Client) ZMembersOrdered(key string, forward bool) (members []ZScoredMember, err error) {
	hasMoreResults := true

	var lastKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

		resp, err := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			IndexName:                 aws.String(c.indexName),
			KeyConditionExpression:    builder.conditionExpression(),
			ScanIndexForward:          aws.Bool(forward),
			TableName:                 aws.String(c.tableName),
		})

		if err != nil {
			return members, err
		}

		for _, item := range resp.Items {
			// This queries the score LSI, which structurally excludes both the #meta and
			// value items (neither carries skN), so no filter is strictly required here.
			// Filter anyway, for defense-in-depth and consistency with the eight other
			// collection readers: should this ever move to the base table, or an skN ever be
			// attached to a 0x00/0x02 item, a phantom must still not surface.
			if c.isMetaItem(item) || c.isValueItem(item) {
				continue
			}
			pi := parseItem(item, c)
			members = append(members, ZScoredMember{
				Member: pi.sk,
				Score:  zScoreFromAV(item[c.sortKeyNum]),
			})
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return members, nil
}

func (c Client) ZCOUNT(key string, minScore, maxScore float64) (count int32, err error) {
	return c.zGeneralCount(key, zScore{minScore}, zScore{maxScore}, c.sortKeyNum)
}

func (c Client) zGeneralCount(key string, min rangeCap, max rangeCap, attribute string) (count int32, err error) {
	builder, scorePath, queryIndex := c.zBuildCountQuery(key, min, max, attribute)

	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		input := &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			IndexName:                 queryIndex,
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.tableName),
		}
		if scorePath {
			input.Select = types.SelectCount
		} else {
			input.Select = types.SelectSpecificAttributes
			input.ProjectionExpression = aws.String(c.sortKey)
		}

		resp, err := c.ddbClient.Query(c.context(), input)

		if err != nil {
			return count, err
		}

		if scorePath {
			count += resp.Count
		} else {
			for _, item := range resp.Items {
				if c.isMetaItem(item) || c.isValueItem(item) {
					continue
				}
				count++
			}
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

// zBuildCountQuery assembles the fixed (page-independent) parts of a zGeneralCount
// query: the partition-key + range-bound expression builder, whether this is the
// score path, and the index to query. It is the count-side twin of zBuildRangeQuery,
// letting zGeneralCount read as build-then-count.
//
// scorePath reports whether the count runs over the score LSI. The score path queries
// the score LSI, which never contains the skN-less #meta item, so Select=Count is both
// cheap and correct there. The lex path queries the base table, where an unbounded (or
// one-sided) range can include #meta ([skPrefixMeta]); Select=Count cannot exclude it
// and a FilterExpression+Count is unreliable on DynamoDB Local, so on the lex path the
// caller projects the sort key and counts non-meta items itself. queryIndex is set to
// the LSI only on the score path.
func (c Client) zBuildCountQuery(key string, min rangeCap, max rangeCap, attribute string) (builder expressionBuilder, scorePath bool, queryIndex *string) {
	builder = newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

	zBuildRangeCondition(&builder, min, max, attribute, "min", "max")

	scorePath = attribute == c.sortKeyNum

	if scorePath {
		queryIndex = aws.String(c.indexName)
	}

	return builder, scorePath, queryIndex
}

func (c Client) ZLEXCOUNT(key string, min string, max string) (count int32, err error) {
	return c.zGeneralCount(key, zLex{min}, zLex{max}, c.sortKey)
}

func (c Client) ZRANGE(key string, start, stop int32) (membersWithScores map[string]float64, err error) {
	return c.zRange(key, start, stop, true)
}

func (c Client) zRange(key string, start int32, stop int32, forward bool) (membersWithScores map[string]float64, err error) {
	if start < 0 && stop < 0 {
		return c.zGeneralRange(key, negInf, posInf, -stop-1, -start, !forward, c.sortKeyNum)
	}

	if start > 0 && stop < 0 {
		// Resolve the negative stop to a positional end and do a pure rank range, exactly like
		// the start>=0,stop>=0 case below. The old code turned stop into a SCORE upper bound,
		// which over-returned every member tied at that boundary score (and made the library
		// ZREMRANGEBYRANK over-delete). Rank ranges are tie-safe because they are positional.
		n, err := c.ZCARD(key)
		if err != nil {
			return membersWithScores, err
		}
		end := n + stop // stop < 0: 0-indexed inclusive end counted from the front
		if end < start {
			return membersWithScores, nil // empty range
		}

		return c.zGeneralRange(key, negInf, posInf, start, end-start+1, forward, c.sortKeyNum)
	}

	return c.zGeneralRange(key, negInf, posInf, start, stop-start+1, forward, c.sortKeyNum)
}

func (c Client) ZRANGEBYLEX(key string, min, max string, offset, count int32) (membersWithScores map[string]float64, err error) {
	return c.zGeneralRange(key, zLex{min}, zLex{max}, offset, count, true, c.sortKey)
}

func (c Client) ZRANGEBYSCORE(key string, min, max float64, offset, count int32) (membersWithScores map[string]float64, err error) {
	return c.zGeneralRange(key, zScore{min}, zScore{max}, offset, count, true, c.sortKeyNum)
}

func (c Client) zGeneralRange(key string,
	start rangeCap, stop rangeCap,
	offset int32, count int32,
	forward bool, attribute string) (membersWithScores map[string]float64, err error) {
	membersWithScores = make(map[string]float64)
	index := int32(0)
	remainingCount := count
	hasMoreResults := true

	var lastKey map[string]types.AttributeValue

	for hasMoreResults {
		query := c.zBuildRangeQuery(key, start, stop, offset, forward, attribute, index, remainingCount, lastKey)

		resp, err := c.ddbClient.Query(c.context(), query)

		if err != nil {
			return membersWithScores, err
		}

		c.zCollectRangePage(resp.Items, offset, membersWithScores, &index, &remainingCount)

		if len(resp.LastEvaluatedKey) > 0 && remainingCount > 0 {
			lastKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return membersWithScores, nil
}

// zBuildRangeQuery builds the QueryInput for one page of a zGeneralRange scan. It
// derives the page Limit from how many items are still to skip plus still to collect,
// assembles the partition-key + range-bound expression builder, and selects the score
// LSI (vs the base table) and forward/reverse direction — exactly the per-page setup
// zGeneralRange previously did inline, so its paging behaviour is unchanged.
//
// The Limit computation deliberately evaluates = items still to skip + items still to
// collect. remainingCount+offset-index double-counted the skip and underflowed on a
// 1MB-truncated multi-page range (twin of the pagedListItems bug). It is computed in
// int64 and clamped so the Limit is never < 1.
func (c Client) zBuildRangeQuery(key string,
	start rangeCap, stop rangeCap,
	offset int32,
	forward bool, attribute string,
	index int32, remainingCount int32,
	lastKey map[string]types.AttributeValue) *dynamodb.QueryInput {
	var queryLimit *int32
	if remainingCount > 0 {
		skip := int64(offset) - int64(index)
		if skip < 0 {
			skip = 0
		}
		if need := int64(remainingCount) + skip; need > 0 && need <= math.MaxInt32 {
			queryLimit = aws.Int32(int32(need))
		}
	}

	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

	zBuildRangeCondition(&builder, start, stop, attribute, "start", "stop")

	var queryIndex *string
	if attribute == c.sortKeyNum {
		queryIndex = aws.String(c.indexName)
	}

	return &dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(c.consistentReads),
		ExclusiveStartKey:         lastKey,
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		IndexName:                 queryIndex,
		KeyConditionExpression:    builder.conditionExpression(),
		Limit:                     queryLimit,
		ScanIndexForward:          aws.Bool(forward),
		TableName:                 aws.String(c.tableName),
	}
}

// zCollectRangePage folds one page of queried items into membersWithScores, advancing
// index and decrementing remainingCount through the pointers so paging across pages
// continues to skip offset items and collect the rest. It is the collection loop
// zGeneralRange previously ran inline; the offset-skipping, #meta filtering and count
// bookkeeping are byte-for-byte identical.
//
// The score path queries the LSI, which excludes the skN-less #meta item by
// construction; but the lex path (attribute == sortKey) queries the base table, where
// an unbounded range can return #meta ([skPrefixMeta]). Skip it BEFORE index++ so a
// filtered meta item never consumes an offset/count slot.
func (c Client) zCollectRangePage(items []map[string]types.AttributeValue, offset int32, membersWithScores map[string]float64, index *int32, remainingCount *int32) {
	for _, item := range items {
		if c.isMetaItem(item) || c.isValueItem(item) {
			continue
		}
		if *index >= offset {
			pi := parseItem(item, c)
			membersWithScores[pi.sk] = zScoreFromAV(item[c.sortKeyNum])
			*remainingCount--
		}
		*index++
	}
}

func (c Client) ZRANK(key string, member string) (rank int32, found bool, err error) {
	return c.zRank(key, member, true)
}

func (c Client) zRank(key string, member string, forward bool) (rank int32, ok bool, err error) {
	score, ok, err := c.ZSCORE(key, member)
	if err != nil || !ok {
		return
	}

	// Count members on the "before" side by score, then break ties lexically like Redis:
	// within one score, forward orders members ascending and reverse descending. Counting all
	// score<=s (the old `count-1`) returned the MAX tied rank for EVERY member sharing s.
	var countLE int32
	if forward {
		countLE, err = c.zGeneralCount(key, negInf, zScore{score}, c.sortKeyNum) // score <= s
	} else {
		countLE, err = c.zGeneralCount(key, zScore{score}, posInf, c.sortKeyNum) // score >= s
	}
	if err != nil {
		return 0, false, err
	}

	tie, err := c.ZRANGEBYSCORE(key, score, score, 0, 0) // members at exactly this score
	if err != nil {
		return 0, false, err
	}
	tieMembers := make([]string, 0, len(tie))
	for m := range tie {
		tieMembers = append(tieMembers, m)
	}
	sort.Strings(tieMembers)
	idx := sort.SearchStrings(tieMembers, member) // position within the tie, lexical ascending
	if !forward {
		idx = len(tieMembers) - 1 - idx // reverse orders the tie descending
	}

	rank = countLE - int32(len(tieMembers)) + int32(idx)
	return rank, true, nil
}

func (c Client) ZREVRANGE(key string, start, stop int32) (membersWithScores map[string]float64, err error) {
	return c.zRange(key, start, stop, false)
}

func (c Client) ZREVRANGEBYLEX(key string, max, min string, offset, count int32) (membersWithScores map[string]float64, err error) {
	return c.zGeneralRange(key, zLex{min}, zLex{max}, offset, count, false, c.sortKey)
}

func (c Client) ZREVRANGEBYSCORE(key string, max, min float64, offset, count int32) (membersWithScores map[string]float64, err error) {
	return c.zGeneralRange(key, zScore{min}, zScore{max}, offset, count, false, c.sortKeyNum)
}

func (c Client) ZREVRANK(key string, member string) (rank int32, found bool, err error) {
	return c.zRank(key, member, false)
}

func (c Client) ZSCORE(key string, member string) (score float64, found bool, err error) {
	resp, err := c.ddbClient.GetItem(c.context(), &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key: keyDef{
			pk: key,
			sk: member,
		}.toAV(c),
		ProjectionExpression: aws.String(strings.Join([]string{c.sortKeyNum}, ", ")),
		TableName:            aws.String(c.tableName),
	})
	if err == nil && len(resp.Item) > 0 {
		found = true
		score = zScoreFromAV(resp.Item[c.sortKeyNum])
	}

	return
}
