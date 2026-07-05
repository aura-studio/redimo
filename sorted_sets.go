package redimo

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type ZAggregation string

const (
	ZAggregationSum ZAggregation = "SUM"
	ZAggregationMin ZAggregation = "MIN"
	ZAggregationMax ZAggregation = "MAX"
)

var accumulators = map[ZAggregation]func(float64, float64) float64{
	ZAggregationSum: func(a float64, b float64) float64 {
		return a + b
	},
	ZAggregationMin: func(a float64, b float64) float64 {
		return min(a, b)
	},
	ZAggregationMax: func(a float64, b float64) float64 {
		return max(a, b)
	},
}

type rangeCap interface {
	Value
	present() bool
}
type zScore struct {
	score float64
}

func (zs zScore) ToAV() (av types.AttributeValue) {
	if zs.present() {
		av = &types.AttributeValueMemberN{
			Value: strconv.FormatFloat(zs.score, 'G', 17, 64),
		}
	}

	return
}

func (zs zScore) present() bool {
	return !math.IsInf(zs.score, +1) && !math.IsInf(zs.score, -1)
}

type zLex struct {
	lex string
}

func (zl zLex) ToAV() (av types.AttributeValue) {
	if zl.present() {
		// Members are stored in the sort key as encodeSK(member) (Binary). A lex
		// range bound must use the identical encoding so BETWEEN comparisons run
		// against the same byte representation. All members share the member
		// prefix, so byte ordering — and therefore lexical ordering — is
		// preserved.
		av = &types.AttributeValueMemberB{
			Value: encodeSK(zl.lex),
		}
	}

	return
}

func (zl zLex) present() bool {
	return zl.lex != ""
}

func zScoreFromAV(av types.AttributeValue) float64 {
	return ReturnValue{av}.Float()
}

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
	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, BytesValue{[]byte(key)})

	betweenRange := min.present() && max.present()

	if betweenRange {
		builder.condition(fmt.Sprintf("#%v BETWEEN :min AND :max", attribute), attribute)
	}

	if min.present() {
		builder.values["min"] = min.ToAV()

		if !betweenRange {
			builder.condition(fmt.Sprintf("#%v >= :min", attribute), attribute)
		}
	}

	if max.present() {
		builder.values["max"] = max.ToAV()

		if !betweenRange {
			builder.condition(fmt.Sprintf("#%v <= :max", attribute), attribute)
		}
	}

	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	// The score path queries the score LSI, which never contains the skN-less #meta
	// item, so Select=Count is both cheap and correct there. The lex path queries the
	// base table, where an unbounded (or one-sided) range can include #meta
	// ([skPrefixMeta]); Select=Count cannot exclude it and a FilterExpression+Count is
	// unreliable on DynamoDB Local, so on the lex path we project the sort key and count
	// non-meta items ourselves.
	scorePath := attribute == c.sortKeyNum

	var queryIndex *string
	if scorePath {
		queryIndex = aws.String(c.indexName)
	}

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
				if c.isMetaItem(item) {
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

func (c Client) ZINTERSTORE(destinationKey string, sourceKeys []string, aggregation ZAggregation, weights map[string]float64) (membersWithScores map[string]float64, err error) {
	set, err := c.ZINTER(sourceKeys, aggregation, weights)
	if err == nil {
		_, err = c.ZADD(destinationKey, set, Flags{})
	}

	return set, err
}

func (c Client) ZLEXCOUNT(key string, min string, max string) (count int32, err error) {
	return c.zGeneralCount(key, zLex{min}, zLex{max}, c.sortKey)
}

func (c Client) ZPOPMAX(key string, count int32) (membersWithScores map[string]float64, err error) {
	return c.zPop(key, count, false)
}

func (c Client) ZPOPMIN(key string, count int32) (membersWithScores map[string]float64, err error) {
	return c.zPop(key, count, true)
}

var negInf = zScore{math.Inf(-1)}
var posInf = zScore{math.Inf(+1)}

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
		var queryLimit *int32
		if remainingCount > 0 {
			// Evaluate = items still to skip + items still to collect. remainingCount+offset-index
			// double-counted the skip and underflowed on a 1MB-truncated multi-page range (twin of
			// the pagedListItems bug). Compute in int64 and clamp so the Limit is never < 1.
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

		if start.present() {
			builder.values["start"] = start.ToAV()
		}

		if stop.present() {
			builder.values["stop"] = stop.ToAV()
		}

		switch {
		case start.present() && stop.present():
			builder.condition(fmt.Sprintf("#%v BETWEEN :start AND :stop", attribute), attribute)
		case start.present():
			builder.condition(fmt.Sprintf("#%v >= :start", attribute), attribute)
		case stop.present():
			builder.condition(fmt.Sprintf("#%v <= :stop", attribute), attribute)
		}

		var queryIndex *string
		if attribute == c.sortKeyNum {
			queryIndex = aws.String(c.indexName)
		}

		resp, err := c.ddbClient.Query(c.context(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			IndexName:                 queryIndex,
			KeyConditionExpression:    builder.conditionExpression(),
			Limit:                     queryLimit,
			ScanIndexForward:          aws.Bool(forward),
			TableName:                 aws.String(c.tableName),
		})

		if err != nil {
			return membersWithScores, err
		}

		for _, item := range resp.Items {
			// The score path queries the LSI, which excludes the skN-less #meta item
			// by construction; but the lex path (attribute == sortKey) queries the base
			// table, where an unbounded range can return #meta ([skPrefixMeta]). Skip it
			// BEFORE index++ so a filtered meta item never consumes an offset/count slot.
			if c.isMetaItem(item) {
				continue
			}
			if index >= offset {
				pi := parseItem(item, c)
				membersWithScores[pi.sk] = zScoreFromAV(item[c.sortKeyNum])
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

	return membersWithScores, nil
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

func (c Client) ZUNIONSTORE(destinationKey string, sourceKeys []string, aggregation ZAggregation, weights map[string]float64) (membersWithScores map[string]float64, err error) {
	set, err := c.ZUNION(sourceKeys, aggregation, weights)
	if err == nil {
		_, err = c.ZADD(destinationKey, set, Flags{})
	}

	return set, err
}

func zGetWeight(weights map[string]float64, key string) float64 {
	if weights == nil {
		return 1
	}

	if w, ok := weights[key]; ok {
		return w
	}

	return 1
}
func (c Client) ZUNION(sourceKeys []string, aggregation ZAggregation, weights map[string]float64) (membersWithScores map[string]float64, err error) {
	membersWithScores = make(map[string]float64)

	for _, sourceKey := range sourceKeys {
		currentSet, err := c.ZRANGEBYSCORE(sourceKey, math.Inf(-1), math.Inf(+1), 0, 0)
		if err != nil {
			return membersWithScores, err
		}

		for member, score := range currentSet {
			if existingValue, ok := membersWithScores[member]; ok {
				membersWithScores[member] = accumulators[aggregation](existingValue, score*zGetWeight(weights, sourceKey))
			} else {
				membersWithScores[member] = score * zGetWeight(weights, sourceKey)
			}
		}
	}

	return
}

func (c Client) ZINTER(sourceKeys []string, aggregation ZAggregation, weights map[string]float64) (membersWithScores map[string]float64, err error) {
	membersWithScores, err = c.ZRANGEBYSCORE(sourceKeys[0], math.Inf(-1), math.Inf(+1), 0, 0)
	if err != nil {
		return
	}

	// Apply the FIRST source key's weight to the seed set. Previously the seed used the raw
	// scores of sourceKeys[0] and only sourceKeys[1:] had their weights applied, so the first
	// operand's WEIGHT was silently ignored.
	if w0 := zGetWeight(weights, sourceKeys[0]); w0 != 1 {
		for member := range membersWithScores {
			membersWithScores[member] *= w0
		}
	}

	for i := 1; i < len(sourceKeys); i++ {
		sourceKey := sourceKeys[i]
		currentSet, err := c.ZRANGEBYSCORE(sourceKey, math.Inf(-1), math.Inf(+1), 0, 0)

		if err != nil {
			return membersWithScores, err
		}

		for member, score := range membersWithScores {
			if currentSetValue, ok := currentSet[member]; ok {
				membersWithScores[member] = accumulators[aggregation](score, currentSetValue*zGetWeight(weights, sourceKey))
			} else {
				delete(membersWithScores, member)
			}
		}
	}

	return
}
