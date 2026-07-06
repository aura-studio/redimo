package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestEmptyMemberDistinctFromValueItem is the core regression guard for the sort-key format split
// separation. A collection's EMPTY member/field ("") now encodes to 0x01 (encodeSK),
// distinct from the reserved String VALUE item at 0x00 (valueItemKey). The two can coexist
// under one partition key — exactly the state a not-yet-reclaimed type overwrite produces —
// yet a collection read surfaces the genuine empty member (0x01) while filtering the stale
// value item (0x00): no phantom "", and no dropped real member.
func TestEmptyMemberDistinctFromValueItem(t *testing.T) {
	c := newClient(t)

	// A String value item lands at sort key 0x00 ...
	ok, err := c.SET("k", StringValue{"stale-value"})
	assert.NoError(t, err)
	assert.True(t, ok)

	// ... then (mimicking a proxy type overwrite) the key is repurposed as a Set whose
	// members include a GENUINE empty member "" (which lands at 0x01) alongside "x","y".
	_, err = c.EnsureType("k", TypeSet, 0)
	assert.NoError(t, err)
	_, err = c.SADD("k", "x", "y", "")
	assert.NoError(t, err)

	// SMEMBERS surfaces exactly {"", x, y}: the real empty member (0x01) is preserved while
	// the stale value item (0x00) is filtered — no phantom, no dropped member.
	members, err := c.SMEMBERS("k")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"", "x", "y"}, members)

	present, err := c.SISMEMBER("k", "")
	assert.NoError(t, err)
	assert.True(t, present, "the genuine empty member must be present")

	// SRANDMEMBER (over-fetch) must never leak the value item as an empty member either.
	rand, err := c.SRANDMEMBER("k", 10)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"", "x", "y"}, rand)

	card, err := c.SCARD("k")
	assert.NoError(t, err)
	assert.Equal(t, int32(3), card, "SCARD counts the 3 members, not the stale value item")
}

// TestEmptyFieldAndZMemberRoundTrip confirms an empty hash FIELD and an empty ZSET MEMBER
// round-trip correctly now that they live at 0x01 (previously 0x00, colliding with the
// value item). Enumeration, point lookup and lex range all see them.
func TestEmptyFieldAndZMemberRoundTrip(t *testing.T) {
	c := newClient(t)

	// Empty hash field "".
	_, err := c.HSET("h", "", "v-empty")
	assert.NoError(t, err)
	_, err = c.HSET("h", "f", "v-f")
	assert.NoError(t, err)

	keys, err := c.HKEYS("h", "")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"", "f"}, keys)

	got, err := c.HGET("h", "")
	assert.NoError(t, err)
	assert.Equal(t, "v-empty", got.String())

	all, err := c.HGETALL("h")
	assert.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Equal(t, "v-empty", all[""].String())

	// Empty zset member "".
	_, err = c.ZADD("z", map[string]float64{"": 1, "m": 2}, Flags{})
	assert.NoError(t, err)

	score, found, err := c.ZSCORE("z", "")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, float64(1), score)

	lex, err := c.ZRANGEBYLEX("z", "", "", 0, 0) // "" == unbounded "-"/"+"
	assert.NoError(t, err)
	assert.Len(t, lex, 2)
	_, hasEmpty := lex[""]
	assert.True(t, hasEmpty, "the empty zset member must be enumerable in a lex range")
}

// TestReclaimAndDeleteHandleValueItem guards that the whole-key reclaim/delete paths remove
// the value item at its raw 0x00 sort key. In this format a decoded "" no longer re-encodes to
// 0x00 (encodeSK("") is 0x01), so these paths delete the RAW stored key instead of a
// round-tripped keyDef.
func TestReclaimAndDeleteHandleValueItem(t *testing.T) {
	c := newClient(t)

	// DeleteMembers (the async lazy-deleter's reclaim primitive) must remove the value item.
	ok, err := c.SET("k", StringValue{"v"})
	assert.NoError(t, err)
	assert.True(t, ok)
	_, err = c.DeleteMembers("k", MaxBatchWriteItems)
	assert.NoError(t, err)
	exists, err := c.EXISTS("k")
	assert.NoError(t, err)
	assert.False(t, exists, "DeleteMembers must reclaim the 0x00 value item")

	// DEL must remove the value item AND a #meta item together (the #meta item's 0x02 sort
	// key is the other reserved key that does not survive a decode/encode round-trip).
	ok, err = c.SET("k2", StringValue{"v2"})
	assert.NoError(t, err)
	assert.True(t, ok)
	_, err = c.EnsureType("k2", TypeString, 0) // also materialise a #meta item
	assert.NoError(t, err)

	deleted, err := c.DEL("k2")
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(deleted), 2, "DEL should remove both the value and #meta items")

	exists, err = c.EXISTS("k2")
	assert.NoError(t, err)
	assert.False(t, exists, "DEL must leave no items behind")
}
