package redimo

import (
	"fmt"
	"math"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
)

// These tests pin the v2.6.0 changes: EnsureType returning the post-ADD count, the
// conditional DeleteMetaIfEmpty, IntE overflow signalling, LSET accepting any value, and
// the batched LPUSH/RPUSH/SADD/SREM keeping their order/count contracts.

// TestEnsureTypeReturnsPostAddCount: EnsureType returns the count AFTER the delta is
// applied, read from the same atomic write.
func TestEnsureTypeReturnsPostAddCount(t *testing.T) {
	c := newClient(t)

	n, err := c.EnsureType("k", TypeSet, 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), n)

	n, err = c.EnsureType("k", TypeSet, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), n)

	n, err = c.EnsureType("k", TypeSet, -5)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// Zero delta establishes/verifies type and reports the current count without churning cnt.
	n, err = c.EnsureType("k", TypeSet, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

// TestDeleteMetaIfEmpty: only deletes when the count is <= 0.
func TestDeleteMetaIfEmpty(t *testing.T) {
	c := newClient(t)

	_, err := c.EnsureType("k", TypeSet, 1) // cnt = 1
	assert.NoError(t, err)

	deleted, err := c.DeleteMetaIfEmpty("k")
	assert.NoError(t, err)
	assert.False(t, deleted, "must not delete a non-empty collection")

	_, found, err := c.LoadMeta("k")
	assert.NoError(t, err)
	assert.True(t, found)

	_, err = c.EnsureType("k", TypeSet, -1) // cnt = 0
	assert.NoError(t, err)

	deleted, err = c.DeleteMetaIfEmpty("k")
	assert.NoError(t, err)
	assert.True(t, deleted, "must delete an emptied collection")

	_, found, err = c.LoadMeta("k")
	assert.NoError(t, err)
	assert.False(t, found)
}

// TestReturnValueIntE: overflow is signalled by IntE while Int silently clamps.
func TestReturnValueIntE(t *testing.T) {
	normal := ReturnValue{av: &types.AttributeValueMemberN{Value: "42"}}
	v, err := normal.IntE()
	assert.NoError(t, err)
	assert.Equal(t, int64(42), v)

	huge := ReturnValue{av: &types.AttributeValueMemberN{Value: "99999999999999999999999999"}}
	_, err = huge.IntE()
	assert.Error(t, err, "IntE must signal int64 overflow")
	assert.Equal(t, int64(math.MaxInt64), huge.Int(), "Int still clamps for back-compat")

	// A non-numeric / empty value is (0, nil), matching Int's absent==0 convention.
	empty := ReturnValue{}
	v, err = empty.IntE()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), v)
}

// TestLSETAcceptsBinary: LSET now takes any coercible value, including binary bytes.
func TestLSETAcceptsBinary(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l", "a", "b", "c")
	assert.NoError(t, err)

	bin := []byte{0x00, 0xff, 0x01, 0x02}
	ok, err := c.LSET("l", 1, bin)
	assert.NoError(t, err)
	assert.True(t, ok)

	got, err := c.LINDEX("l", 1)
	assert.NoError(t, err)
	assert.Equal(t, bin, []byte(got.String()), "binary element must round-trip losslessly")
}

// TestBulkPushOrderAcrossBatches: a bulk RPUSH larger than one BatchWriteItem (25) must
// still yield elements in insertion order, exercising the ranged index bump + multi-batch.
func TestBulkPushOrderAcrossBatches(t *testing.T) {
	c := newClient(t)

	const n = 60
	els := make([]any, n)
	want := make([]string, n)
	for i := range els {
		s := fmt.Sprintf("e%02d", i)
		els[i] = s
		want[i] = s
	}

	length, err := c.RPUSH("big", els...)
	assert.NoError(t, err)
	assert.Equal(t, int64(n), length)

	got, err := c.LRANGE("big", 0, -1)
	assert.NoError(t, err)
	gotStr := make([]string, len(got))
	for i, e := range got {
		gotStr[i] = e.String()
	}
	assert.Equal(t, want, gotStr)
}

// TestLPushOrder: LPUSH a b c yields [c, b, a] (each element prepended), matching Redis,
// after the batched ranged-index allocation.
func TestLPushOrder(t *testing.T) {
	c := newClient(t)

	_, err := c.LPUSH("lp", "a", "b", "c")
	assert.NoError(t, err)

	got, err := c.LRANGE("lp", 0, -1)
	assert.NoError(t, err)
	gotStr := make([]string, len(got))
	for i, e := range got {
		gotStr[i] = e.String()
	}
	assert.Equal(t, []string{"c", "b", "a"}, gotStr)
}

// TestSADDSREMCountsAfterBatching: added/removed counts stay exact through the batched
// pre-read path, including duplicate arguments and already-present members.
func TestSADDSREMCountsAfterBatching(t *testing.T) {
	c := newClient(t)

	added, err := c.SADD("s", "a", "b", "c")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, added)

	// b,c already present; d is new; the duplicate d is counted once.
	added, err = c.SADD("s", "b", "c", "d", "d")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"d"}, added)

	card, err := c.SCARD("s")
	assert.NoError(t, err)
	assert.Equal(t, int32(4), card)

	// a present, z absent -> only a removed.
	removed, err := c.SREM("s", "a", "z")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"a"}, removed)

	card, err = c.SCARD("s")
	assert.NoError(t, err)
	assert.Equal(t, int32(3), card)
}
