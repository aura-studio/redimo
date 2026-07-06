package redimo

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// meta_test.go — unit tests for the fork v1.7 meta/TTL extension (task 3.3).
//
// These tests exercise the DynamoDB-backed meta item behaviour against a local
// DynamoDB (localhost:8000) using the same conventions as the other *_test.go
// files in this package (newClient(t) provisions a unique throwaway table).
//
// Coverage (requirements 11.1, 11.2, 11.3):
//   - conditional-write type conflict returns ErrWrongType with no mutation;
//   - cnt is maintained atomically via ADD across multiple writes;
//   - exp write/clear boundaries via SetExpire / Persist (including missing key).

// TestEnsureTypeCreatesMeta verifies the very first EnsureType call creates the
// meta item, records the type, and seeds cnt from the supplied delta in a single
// conditional write (requirement 11.1, 11.3).
func TestEnsureTypeCreatesMeta(t *testing.T) {
	c := newClient(t)

	_, err := c.EnsureType("h1", TypeHash, 3)
	assert.NoError(t, err)

	meta, found, err := c.LoadMeta("h1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, TypeHash, meta.Type)
	assert.EqualValues(t, 3, meta.Count)
	assert.EqualValues(t, 0, meta.Exp) // no expiry set yet
}

// TestEnsureTypeZeroDeltaEstablishesType verifies a zero cntDelta (e.g. String
// writes that keep no member count) still establishes/verifies the type without
// touching cnt (requirement 11.1).
func TestEnsureTypeZeroDeltaEstablishesType(t *testing.T) {
	c := newClient(t)

	_, err := c.EnsureType("s1", TypeString, 0)
	assert.NoError(t, err)

	meta, found, err := c.LoadMeta("s1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, TypeString, meta.Type)
	assert.EqualValues(t, 0, meta.Count)
}

// TestEnsureTypeConflictReturnsWrongType verifies that when a key already exists
// with a given type, an EnsureType for a different type fails the conditional
// check, returns ErrWrongType, and leaves the meta item completely unmodified —
// neither the type nor the count changes (requirements 11.1, 11.2).
func TestEnsureTypeConflictReturnsWrongType(t *testing.T) {
	c := newClient(t)

	// Establish the key as a string with a known count.
	_, err := c.EnsureType("k1", TypeString, 1)
	assert.NoError(t, err)

	before, found, err := c.LoadMeta("k1")
	assert.NoError(t, err)
	assert.True(t, found)

	// Attempt to use the same key as a hash while also trying to bump cnt.
	_, err = c.EnsureType("k1", TypeHash, 10)
	assert.True(t, errors.Is(err, ErrWrongType), "expected ErrWrongType, got %v", err)

	// No mutation: type and count must be exactly as before the failed write.
	after, found, err := c.LoadMeta("k1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, TypeString, after.Type)
	assert.Equal(t, before.Count, after.Count)
	assert.EqualValues(t, 1, after.Count)
}

// TestEnsureTypeAtomicCountAdd verifies repeated EnsureType calls of the same
// type accumulate cnt via atomic ADD, including negative deltas (member removal),
// giving O(1) cardinality that always equals the net member count (requirement
// 11.3).
func TestEnsureTypeAtomicCountAdd(t *testing.T) {
	c := newClient(t)

	deltas := []int64{5, 3, -2, 10, -1}
	var want int64

	for _, d := range deltas {
		_, err := c.EnsureType("set1", TypeSet, d)
		assert.NoError(t, err)
		want += d

		meta, found, err := c.LoadMeta("set1")
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, TypeSet, meta.Type)
		assert.Equalf(t, want, meta.Count, "cnt after applying deltas up to %d", d)
	}

	assert.EqualValues(t, 15, want) // 5+3-2+10-1
}

// TestEnsureTypeExpiring covers the expire-if-needed type ensure: an expired key (of any
// type) is treated as absent and taken over, while live keys behave exactly like EnsureType.
func TestEnsureTypeExpiring(t *testing.T) {
	const now = 1000

	t.Run("absent creates", func(t *testing.T) {
		c := newClient(t)
		cnt, took, err := c.EnsureTypeExpiring("a", TypeSet, 2, now)
		assert.NoError(t, err)
		assert.False(t, took)
		assert.EqualValues(t, 2, cnt)
	})

	t.Run("live same-type adds", func(t *testing.T) {
		c := newClient(t)
		_, err := c.EnsureType("b", TypeSet, 1)
		assert.NoError(t, err)
		cnt, took, err := c.EnsureTypeExpiring("b", TypeSet, 3, now)
		assert.NoError(t, err)
		assert.False(t, took)
		assert.EqualValues(t, 4, cnt) // 1 + 3, not reset
	})

	t.Run("live wrong-type is WRONGTYPE", func(t *testing.T) {
		c := newClient(t)
		_, err := c.EnsureType("c", TypeString, 1)
		assert.NoError(t, err)
		_, took, err := c.EnsureTypeExpiring("c", TypeSet, 1, now)
		assert.True(t, errors.Is(err, ErrWrongType))
		assert.False(t, took)
	})

	t.Run("live wrong-type with future TTL is WRONGTYPE", func(t *testing.T) {
		c := newClient(t)
		_, err := c.EnsureType("f", TypeString, 1)
		assert.NoError(t, err)
		_, err = c.SetExpire("f", now+100) // still live
		assert.NoError(t, err)
		_, _, err = c.EnsureTypeExpiring("f", TypeSet, 1, now)
		assert.True(t, errors.Is(err, ErrWrongType))
	})

	t.Run("expired wrong-type is taken over", func(t *testing.T) {
		c := newClient(t)
		_, err := c.EnsureType("d", TypeString, 1)
		assert.NoError(t, err)
		_, err = c.SetExpire("d", now-500) // expired
		assert.NoError(t, err)
		cnt, took, err := c.EnsureTypeExpiring("d", TypeSet, 0, now)
		assert.NoError(t, err)
		assert.True(t, took)
		assert.EqualValues(t, 0, cnt)
		m, _, _ := c.LoadMeta("d")
		assert.Equal(t, TypeSet, m.Type)
		assert.EqualValues(t, 0, m.Count)
		assert.EqualValues(t, 0, m.Exp) // expiry cleared on takeover
	})

	t.Run("expired same-type is taken over (count reset, not added)", func(t *testing.T) {
		c := newClient(t)
		_, err := c.EnsureType("e", TypeSet, 5)
		assert.NoError(t, err)
		_, err = c.SetExpire("e", now-500) // expired
		assert.NoError(t, err)
		cnt, took, err := c.EnsureTypeExpiring("e", TypeSet, 0, now)
		assert.NoError(t, err)
		assert.True(t, took)
		assert.EqualValues(t, 0, cnt) // reset to delta 0, NOT 5+0
		m, _, _ := c.LoadMeta("e")
		assert.Equal(t, TypeSet, m.Type)
		assert.EqualValues(t, 0, m.Count)
		assert.EqualValues(t, 0, m.Exp)
	})
}

// TestLoadMetaMissingKey verifies LoadMeta reports found=false for a key that was
// never written, with no error and a zero-value Meta.
func TestLoadMetaMissingKey(t *testing.T) {
	c := newClient(t)

	meta, found, err := c.LoadMeta("ghost")
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, Meta{}, meta)
}

// TestSetExpireWritesAndPersistClears verifies the exp write/clear boundaries:
// SetExpire records exp on an existing key (found=true), and Persist removes it,
// leaving exp back at 0 (requirement 11.4 boundary; supports task 3.3 exp
// write/clear coverage).
func TestSetExpireWritesAndPersistClears(t *testing.T) {
	c := newClient(t)

	_, err := c.EnsureType("e1", TypeString, 0)
	assert.NoError(t, err)

	// Write exp.
	exp := time.Now().Add(time.Hour).Unix()
	found, err := c.SetExpire("e1", exp)
	assert.NoError(t, err)
	assert.True(t, found)

	meta, ok, err := c.LoadMeta("e1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, exp, meta.Exp)

	// Clear exp.
	found, err = c.Persist("e1")
	assert.NoError(t, err)
	assert.True(t, found)

	meta, ok, err = c.LoadMeta("e1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.EqualValues(t, 0, meta.Exp) // exp attribute removed -> reads back as 0

	// Type and existence are untouched by the exp lifecycle.
	assert.Equal(t, TypeString, meta.Type)
}

// TestSetExpireMissingKey verifies SetExpire on a key with no meta item returns
// found=false (the redimo backing for EXPIRE returning :0), without creating a
// meta item.
func TestSetExpireMissingKey(t *testing.T) {
	c := newClient(t)

	found, err := c.SetExpire("missing", time.Now().Add(time.Hour).Unix())
	assert.NoError(t, err)
	assert.False(t, found)

	_, exists, err := c.LoadMeta("missing")
	assert.NoError(t, err)
	assert.False(t, exists) // SetExpire must not create the key
}

// TestPersistMissingKey verifies Persist on a key with no meta item returns
// found=false without creating anything.
func TestPersistMissingKey(t *testing.T) {
	c := newClient(t)

	found, err := c.Persist("missing")
	assert.NoError(t, err)
	assert.False(t, found)

	_, exists, err := c.LoadMeta("missing")
	assert.NoError(t, err)
	assert.False(t, exists)
}

// TestSetExpireMillisTruncatesToSeconds verifies the millisecond-based expiry
// path stores exp at whole-second precision (Pika v3.2.2 has no sub-second
// precision), exercising the write boundary end-to-end against the backend.
func TestSetExpireMillisTruncatesToSeconds(t *testing.T) {
	c := newClient(t)

	_, err := c.EnsureType("e2", TypeString, 0)
	assert.NoError(t, err)

	// A ms epoch with a non-zero sub-second remainder.
	ms := int64(1_700_000_000_999)
	found, err := c.SetExpireMillis("e2", ms)
	assert.NoError(t, err)
	assert.True(t, found)

	meta, ok, err := c.LoadMeta("e2")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.EqualValues(t, 1_700_000_000, meta.Exp) // truncated, not rounded
}

// TestIsExpiredBoundaries verifies the pure expiry predicate at the exp<=now
// boundary, independent of any backend (requirement 11.4/11.5 judgement).
func TestIsExpiredBoundaries(t *testing.T) {
	t.Parallel()

	const now int64 = 1_700_000_000

	assert.False(t, IsExpired(Meta{Exp: 0}, now), "exp=0 means never expires")
	assert.False(t, IsExpired(Meta{Exp: now + 1}, now), "exp in the future")
	assert.True(t, IsExpired(Meta{Exp: now}, now), "exp == now is expired")
	assert.True(t, IsExpired(Meta{Exp: now - 1}, now), "exp in the past")
}
