package redimo

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWithContextIsThreaded proves WithContext actually propagates the context into
// the underlying DynamoDB call: an already-cancelled context must abort the request
// rather than being ignored (the old code hard-coded context.TODO()).
func TestWithContextIsThreaded(t *testing.T) {
	c := newClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the call so the request cannot succeed

	_, err := c.WithContext(ctx).GET("any-key")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got %v", err)
}

// The tests in this file pin the v2.4.0 correctness fixes. Each one first creates a
// real #meta item with EnsureType (the way the redimos proxy does), so the
// meta-exclusion paths are actually exercised — redimo's other tests create keys via
// the data commands alone and so never materialise a #meta item.

// TestHLENExcludesMeta: HLEN/SCARD/ZCARD must not count the reserved #meta item.
func TestHLENExcludesMeta(t *testing.T) {
	c := newClient(t)

	assert.NoError(t, c.EnsureType("h", TypeHash, 0))

	_, err := c.HSET("h", "f1", "v1")
	assert.NoError(t, err)
	_, err = c.HSET("h", "f2", "v2")
	assert.NoError(t, err)
	_, err = c.HSET("h", "f3", "v3")
	assert.NoError(t, err)

	n, err := c.HLEN("h")
	assert.NoError(t, err)
	assert.Equal(t, int32(3), n) // 3 fields, NOT 3 + the #meta item
}

// TestSRANDMEMBERExcludesMeta: SRANDMEMBER must never leak "#meta" as a member, even
// when asked for more members than the set holds.
func TestSRANDMEMBERExcludesMeta(t *testing.T) {
	c := newClient(t)

	assert.NoError(t, c.EnsureType("s", TypeSet, 0))

	_, err := c.SADD("s", "m1", "m2", "m3")
	assert.NoError(t, err)

	members, err := c.SRANDMEMBER("s", 10) // over-fetch so the meta item would be returned
	assert.NoError(t, err)
	assert.Len(t, members, 3)
	assert.NotContains(t, members, MetaSK)
}

// TestZRANGEBYLEXExcludesMeta: the lex (base-table) range path must skip #meta. An
// unbounded "- +" range places no sort-key condition, so before the fix the base-table
// scan returned the #meta item as a member.
func TestZRANGEBYLEXExcludesMeta(t *testing.T) {
	c := newClient(t)

	assert.NoError(t, c.EnsureType("z", TypeZSet, 0))

	_, err := c.ZADD("z", map[string]float64{"a": 0, "b": 0, "c": 0}, Flags{})
	assert.NoError(t, err)

	// min == max == "" is the unbounded "-"/"+" lex range.
	got, err := c.ZRANGEBYLEX("z", "", "", 0, 0)
	assert.NoError(t, err)
	assert.Len(t, got, 3)
	_, hasMeta := got[MetaSK]
	assert.False(t, hasMeta, "ZRANGEBYLEX must not return the #meta item")
}

// TestZLEXCOUNTExcludesMeta: the lex count path must not count #meta.
func TestZLEXCOUNTExcludesMeta(t *testing.T) {
	c := newClient(t)

	assert.NoError(t, c.EnsureType("z", TypeZSet, 0))

	_, err := c.ZADD("z", map[string]float64{"a": 0, "b": 0, "c": 0}, Flags{})
	assert.NoError(t, err)

	n, err := c.ZLEXCOUNT("z", "", "")
	assert.NoError(t, err)
	assert.Equal(t, int32(3), n) // 3 members, NOT 3 + the #meta item
}

// TestLREMNumericOrder: LREM's head/tail selection must order occurrences by the
// numeric list index, not by the lexicographic order of the decimal-string skN. With
// duplicates at indices 2 and 11, LREM count=1 (head) must remove index 2 — leaving the
// duplicate at the tail. Under the old string comparison "11" < "2", it removed index
// 11 instead, leaving the duplicate near the head.
func TestLREMNumericOrder(t *testing.T) {
	c := newClient(t)

	assert.NoError(t, c.EnsureType("l", TypeList, 0))

	// Push order == index order: DUP lands at index 2 and index 11.
	_, err := c.RPUSH("l", "f1", "DUP", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "DUP")
	assert.NoError(t, err)

	_, ok, err := c.LREM("l", 1, "DUP")
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err := c.LRANGE("l", 0, -1)
	assert.NoError(t, err)

	got := make([]string, len(elements))
	for i, e := range elements {
		got[i] = e.String()
	}
	// The head-most DUP (index 2) is gone; the tail DUP (index 11) survives.
	assert.Equal(t, []string{"f1", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "DUP"}, got)
}

// TestHSETNXOnExistingHash guards the verified-correct behaviour that HSETNX can add a
// NEW field to an ALREADY-EXISTING hash. DynamoDB condition expressions are evaluated
// per item, so attribute_not_exists(#pk) on the targeted (key, field) item is a
// field-level "field absent" check, not a partition-level one.
func TestHSETNXOnExistingHash(t *testing.T) {
	c := newClient(t)

	// Make the hash partition already exist.
	_, err := c.HSET("h", "f1", "v1")
	assert.NoError(t, err)

	// Adding a brand-new field to the existing hash must succeed.
	ok, err := c.HSETNX("h", "f2", StringValue{"v2"})
	assert.NoError(t, err)
	assert.True(t, ok)

	got, err := c.HGET("h", "f2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", got.String())

	// Setting the same field again must fail and must not overwrite.
	ok, err = c.HSETNX("h", "f2", StringValue{"v3"})
	assert.NoError(t, err)
	assert.False(t, ok)

	got, err = c.HGET("h", "f2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", got.String())
}

// TestZMembersOrderedExcludesMeta guards the verified-correct behaviour that
// ZMembersOrdered (querying the score LSI) never surfaces #meta: the #meta item has no
// skN attribute and is therefore structurally absent from the index.
func TestZMembersOrderedExcludesMeta(t *testing.T) {
	c := newClient(t)

	assert.NoError(t, c.EnsureType("z", TypeZSet, 0))

	_, err := c.ZADD("z", map[string]float64{"a": 1, "b": 2, "cc": 3}, Flags{})
	assert.NoError(t, err)

	members, err := c.ZMembersOrdered("z", true)
	assert.NoError(t, err)

	names := make([]string, len(members))
	for i, m := range members {
		names[i] = m.Member
	}
	assert.Equal(t, []string{"a", "b", "cc"}, names)
	assert.NotContains(t, names, MetaSK)
}
