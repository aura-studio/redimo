package redimo

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Regression tests for the 2026-07 audit fixes.

// TestAuditListPaginationOver1MB covers the pagedListItems negative-Limit bug: a list whose
// elements total more than DynamoDB's 1MB page cap forces a multi-page Query, and the old
// Limit formula (remainingCount+offset-index) underflowed to a negative Limit on the second
// page, failing EVERY read with a ValidationException.
func TestAuditListPaginationOver1MB(t *testing.T) {
	c := newClient(t)

	const (
		n       = 30
		elemLen = 60000 // 30 * 60KB = 1.8MB > 1MB page cap
	)
	elems := make([]any, n)
	for i := range elems {
		// distinct first byte so we can spot-check ordering/content
		elems[i] = string(rune('A'+i%26)) + strings.Repeat("x", elemLen)
	}
	_, err := c.RPUSH("big", elems...)
	assert.NoError(t, err)

	got, err := c.LRANGE("big", 0, -1)
	assert.NoError(t, err)
	assert.Len(t, got, n, "LRANGE 0 -1 must return every element across pages")

	// A bounded window across the page boundary.
	win, err := c.LRANGE("big", 5, 20)
	assert.NoError(t, err)
	assert.Len(t, win, 16)

	// LINDEX past the first page.
	last, err := c.LINDEX("big", int64(n-1))
	assert.NoError(t, err)
	assert.Equal(t, elems[n-1], last.String())
}

func TestAuditZRankTies(t *testing.T) {
	c := newClient(t)
	_, err := c.ZADD("z", map[string]float64{"a": 1, "b": 2, "c": 2, "d": 2, "e": 3}, Flags{})
	assert.NoError(t, err)

	// Forward rank: score asc, ties lexical asc -> a0 b1 c2 d3 e4.
	for m, want := range map[string]int32{"a": 0, "b": 1, "c": 2, "d": 3, "e": 4} {
		r, found, err := c.ZRANK("z", m)
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equalf(t, want, r, "ZRANK %s", m)
	}
	// Reverse rank: e0 d1 c2 b3 a4.
	for m, want := range map[string]int32{"e": 0, "d": 1, "c": 2, "b": 3, "a": 4} {
		r, found, err := c.ZREVRANK("z", m)
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equalf(t, want, r, "ZREVRANK %s", m)
	}
}

// TestAuditZRangeNegStopTies covers ZRANGE(start>0, stop<0) over a tied-score boundary: the
// old code turned stop into a SCORE bound and returned EVERY member tied at that score (4
// here), so the library ZREMRANGEBYRANK over-deleted. The fix resolves the negative stop to a
// positional rank end, so the COUNT is exactly right (3). NOTE: redimo's zGeneralRange returns
// an UNORDERED map and relies on the DynamoDB LSI to order tied scores, which it does not do
// reliably by member — so WHICH tied member lands at a given rank is LSI-dependent. Callers
// needing Redis' exact lexical tie order sort client-side (this is what the redimos proxy
// does, and its zset dimensions are verified byte-for-byte against Redis 3.2). This test
// asserts the contractual part: the correct count and in-range bounds.
func TestAuditZRangeNegStopTies(t *testing.T) {
	c := newClient(t)
	// a1 b2 c2 d2 e2 f3 (ranks 0..5); ZRANGE 1 -3 => positional ranks [1,3] => exactly 3 members.
	_, err := c.ZADD("z", map[string]float64{"a": 1, "b": 2, "c": 2, "d": 2, "e": 2, "f": 3}, Flags{})
	assert.NoError(t, err)

	got, err := c.ZRANGE("z", 1, -3)
	assert.NoError(t, err)
	assert.Len(t, got, 3, "positional range [1,3] must return exactly 3, not over-return the whole tie")
	assert.NotContains(t, got, "a", "rank 0 is below start=1")
	assert.NotContains(t, got, "f", "rank 5 (score 3) is above the resolved end")
	for m := range got {
		assert.Equal(t, 2.0, got[m], "every returned member must be within the tied boundary score")
	}
}

func TestAuditZInterFirstSourceWeight(t *testing.T) {
	c := newClient(t)
	_, err := c.ZADD("z1", map[string]float64{"a": 1, "b": 2}, Flags{})
	assert.NoError(t, err)
	_, err = c.ZADD("z2", map[string]float64{"a": 10, "b": 20}, Flags{})
	assert.NoError(t, err)

	// SUM with weights z1*2, z2*3: a = 1*2 + 10*3 = 32, b = 2*2 + 20*3 = 64.
	res, err := c.ZINTER([]string{"z1", "z2"}, ZAggregationSum, map[string]float64{"z1": 2, "z2": 3})
	assert.NoError(t, err)
	assert.Equal(t, 32.0, res["a"])
	assert.Equal(t, 64.0, res["b"])
}

// TestAuditMGetMissingKeys covers the MGET empty-key collision: missing keys must not all
// collapse under the "" key, and present keys must be returned under their real names.
func TestAuditMGetMissingKeys(t *testing.T) {
	c := newClient(t)
	_, err := c.SET("k1", "v1")
	assert.NoError(t, err)
	_, err = c.SET("k3", "v3")
	assert.NoError(t, err)

	values, err := c.MGET("k1", "k2", "k3")
	assert.NoError(t, err)
	assert.Equal(t, "v1", values["k1"].String())
	assert.Equal(t, "v3", values["k3"].String())
	_, hasMissing := values["k2"]
	assert.False(t, hasMissing, "missing key k2 must be absent")
	_, hasEmpty := values[""]
	assert.False(t, hasEmpty, "no value may collapse under the empty key")
}
