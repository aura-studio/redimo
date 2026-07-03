package redimo

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

// lists_diff_test.go implements differential validation for the fork's List
// implementation (task 3.4 of the redis-dynamodb-proxy spec).
//
// Strategy: a pure in-memory reference model (refList) encodes the expected
// Redis list semantics. The same command sequence is applied to both the fork
// (redimo Client, backed by DynamoDB Local) and the reference model, then the
// resulting list state (and key return values) are compared byte-for-byte.
// This is the "oracle" pattern from the design doc's differential-testing
// strategy, using a Redis-semantics reference model as the oracle instead of a
// live Pika instance (the redimo fork is a library, not a RESP server).
//
// Covered commands: LPUSH/RPUSH/LPUSHX/RPUSHX, LPOP/RPOP, LRANGE/LINDEX,
// LSET, LTRIM, LREM, RPOPLPUSH.
//
// _需求 7.1, 7.3, 7.4, 7.5_
//
// NOTE (finding): LINSERT is listed in task 3.4 but is NOT implemented in the
// fork (no LINSERT method exists on Client). See TestListDiff_LINSERT_Missing.

// refList is a reference implementation of Redis list semantics used as the
// differential oracle.
type refList struct {
	items []string
}

func (r *refList) lpush(vals ...string) int {
	for _, v := range vals {
		r.items = append([]string{v}, r.items...)
	}
	return len(r.items)
}

func (r *refList) rpush(vals ...string) int {
	r.items = append(r.items, vals...)
	return len(r.items)
}

func (r *refList) lpushx(vals ...string) int {
	if len(r.items) == 0 {
		return 0
	}
	return r.lpush(vals...)
}

func (r *refList) rpushx(vals ...string) int {
	if len(r.items) == 0 {
		return 0
	}
	return r.rpush(vals...)
}

func (r *refList) lpop() (string, bool) {
	if len(r.items) == 0 {
		return "", false
	}
	v := r.items[0]
	r.items = r.items[1:]
	return v, true
}

func (r *refList) rpop() (string, bool) {
	if len(r.items) == 0 {
		return "", false
	}
	v := r.items[len(r.items)-1]
	r.items = r.items[:len(r.items)-1]
	return v, true
}

func (r *refList) llen() int64 { return int64(len(r.items)) }

// normalizeRange mirrors Redis LRANGE index normalization.
func normalizeRange(n, start, stop int64) (int64, int64, bool) {
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if start >= n || start > stop {
		return 0, 0, false
	}
	if stop >= n {
		stop = n - 1
	}
	return start, stop, true
}

func (r *refList) lrange(start, stop int64) []string {
	n := int64(len(r.items))
	s, e, ok := normalizeRange(n, start, stop)
	if !ok {
		return []string{}
	}
	out := make([]string, 0, e-s+1)
	for i := s; i <= e; i++ {
		out = append(out, r.items[i])
	}
	return out
}

func (r *refList) lindex(index int64) (string, bool) {
	n := int64(len(r.items))
	if index < 0 {
		index = n + index
	}
	if index < 0 || index >= n {
		return "", false
	}
	return r.items[index], true
}

// lset returns whether the index was in range (Redis errors on out-of-range;
// the fork returns ok=false, so we compare the in-range boolean).
func (r *refList) lset(index int64, val string) bool {
	n := int64(len(r.items))
	if index < 0 {
		index = n + index
	}
	if index < 0 || index >= n {
		return false
	}
	r.items[index] = val
	return true
}

// ltrim keeps [start, stop] (inclusive) using Redis normalization.
func (r *refList) ltrim(start, stop int64) {
	n := int64(len(r.items))
	s, e, ok := normalizeRange(n, start, stop)
	if !ok {
		r.items = []string{}
		return
	}
	kept := make([]string, 0, e-s+1)
	for i := s; i <= e; i++ {
		kept = append(kept, r.items[i])
	}
	r.items = kept
}

// lrem removes occurrences of val and returns (removedCount, remainingLen).
//
//	count>0: remove first count occurrences (head->tail)
//	count<0: remove last |count| occurrences (tail->head)
//	count==0: remove all
func (r *refList) lrem(count int64, val string) (removed int64, remainingLen int64) {
	if count >= 0 {
		// count==0 means remove all; otherwise remove first `count` from head.
		out := make([]string, 0, len(r.items))
		for _, v := range r.items {
			if v == val && (count == 0 || removed < count) {
				removed++
				continue
			}
			out = append(out, v)
		}
		r.items = out
		return removed, int64(len(r.items))
	}
	// count < 0: remove last |count| occurrences (scan from tail).
	limit := -count
	out := make([]string, len(r.items))
	copy(out, r.items)
	for i := len(out) - 1; i >= 0 && removed < limit; i-- {
		if out[i] == val {
			out = append(out[:i], out[i+1:]...)
			removed++
		}
	}
	r.items = out
	return removed, int64(len(r.items))
}

func (r *refList) rpoplpush(dst *refList) (string, bool) {
	v, ok := r.rpop()
	if !ok {
		return "", false
	}
	dst.lpush(v)
	return v, true
}

// assertStrings compares two string slices, treating a nil slice and an empty
// slice as equal (the fork's readStrings yields nil for an empty list while the
// reference model may yield a non-nil empty slice; that representation
// difference is not a Redis-semantic divergence).
func assertStrings(t *testing.T, want, got []string, msgAndArgs ...interface{}) {
	t.Helper()
	if len(want) == 0 && len(got) == 0 {
		return
	}
	assert.Equal(t, want, got, msgAndArgs...)
}

// assertListStateEqual compares the fork's full list contents against the
// reference model (the core differential assertion).
func assertListStateEqual(t *testing.T, c Client, key string, ref *refList, ctx string) {
	t.Helper()
	elements, err := c.LRANGE(key, 0, -1)
	assert.NoError(t, err, ctx)
	assertStrings(t, ref.items, readStrings(elements), "list state mismatch after %s", ctx)

	llen, err := c.LLEN(key)
	assert.NoError(t, err, ctx)
	assert.Equal(t, ref.llen(), llen, "LLEN mismatch after %s", ctx)
}

// --- Push / Pop / Range differential coverage (需求 7.1, 7.3) ---

func TestListDiff_PushPopRange(t *testing.T) {
	c := newClient(t)
	ref := &refList{}
	key := "diff:pushpop"

	// LPUSH single + multi
	n, err := c.LPUSH(key, StringValue{"a"})
	assert.NoError(t, err)
	assert.Equal(t, int64(ref.lpush("a")), n)
	assertListStateEqual(t, c, key, ref, "LPUSH a")

	n, err = c.LPUSH(key, StringValue{"b"}, StringValue{"c"})
	assert.NoError(t, err)
	assert.Equal(t, int64(ref.lpush("b", "c")), n)
	assertListStateEqual(t, c, key, ref, "LPUSH b c") // expect [c,b,a]

	// RPUSH multi
	n, err = c.RPUSH(key, StringValue{"d"}, StringValue{"e"})
	assert.NoError(t, err)
	assert.Equal(t, int64(ref.rpush("d", "e")), n)
	assertListStateEqual(t, c, key, ref, "RPUSH d e") // [c,b,a,d,e]

	// LRANGE variants (positive, negative, out-of-range) vs oracle
	rangeCases := [][2]int64{
		{0, -1}, {0, 0}, {1, 3}, {-3, -1}, {-100, 100}, {3, 1}, {10, 20}, {-2, -3},
	}
	for _, rc := range rangeCases {
		got, err := c.LRANGE(key, rc[0], rc[1])
		assert.NoError(t, err)
		assertStrings(t, ref.lrange(rc[0], rc[1]), readStrings(got),
			"LRANGE %d %d mismatch", rc[0], rc[1])
	}

	// LINDEX variants vs oracle
	for _, idx := range []int64{0, 2, 4, 5, -1, -5, -100, 100} {
		got, err := c.LINDEX(key, idx)
		assert.NoError(t, err)
		wantVal, wantPresent := ref.lindex(idx)
		assert.Equal(t, wantPresent, got.Present(), "LINDEX %d presence mismatch", idx)
		if wantPresent {
			assert.Equal(t, wantVal, got.String(), "LINDEX %d value mismatch", idx)
		}
	}

	// LPOP / RPOP drain, comparing each popped value
	for i := 0; i < 6; i++ {
		if i%2 == 0 {
			got, err := c.LPOP(key)
			assert.NoError(t, err)
			wantVal, wantOK := ref.lpop()
			assert.Equal(t, wantOK, !got.Empty(), "LPOP presence mismatch at step %d", i)
			if wantOK {
				assert.Equal(t, wantVal, got.String(), "LPOP value mismatch at step %d", i)
			}
		} else {
			got, err := c.RPOP(key)
			assert.NoError(t, err)
			wantVal, wantOK := ref.rpop()
			assert.Equal(t, wantOK, !got.Empty(), "RPOP presence mismatch at step %d", i)
			if wantOK {
				assert.Equal(t, wantVal, got.String(), "RPOP value mismatch at step %d", i)
			}
		}
		assertListStateEqual(t, c, key, ref, fmt.Sprintf("pop step %d", i))
	}

	// Pop on empty list
	got, err := c.LPOP(key)
	assert.NoError(t, err)
	assert.True(t, got.Empty())
	got, err = c.RPOP(key)
	assert.NoError(t, err)
	assert.True(t, got.Empty())
}

// --- LPUSHX / RPUSHX differential coverage (需求 7.1) ---

func TestListDiff_PushX(t *testing.T) {
	c := newClient(t)
	ref := &refList{}

	// PUSHX on non-existent key is a no-op returning 0
	n, err := c.LPUSHX("diff:pushx:none", StringValue{"x"})
	assert.NoError(t, err)
	assert.Equal(t, int64(ref.lpushx("x")), n)
	assert.Equal(t, int64(0), n)

	n, err = c.RPUSHX("diff:pushx:none2", StringValue{"x"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// Seed a key, then PUSHX should apply
	key := "diff:pushx"
	ref2 := &refList{}
	_, err = c.RPUSH(key, StringValue{"seed"})
	assert.NoError(t, err)
	ref2.rpush("seed")

	n, err = c.LPUSHX(key, StringValue{"l"})
	assert.NoError(t, err)
	assert.Equal(t, int64(ref2.lpushx("l")), n)
	assertListStateEqual(t, c, key, ref2, "LPUSHX l")

	n, err = c.RPUSHX(key, StringValue{"r"})
	assert.NoError(t, err)
	assert.Equal(t, int64(ref2.rpushx("r")), n)
	assertListStateEqual(t, c, key, ref2, "RPUSHX r")
}

// --- LSET differential coverage (需求 7.4) ---

func TestListDiff_LSET(t *testing.T) {
	c := newClient(t)
	ref := &refList{}
	key := "diff:lset"

	_, err := c.RPUSH(key, StringValue{"a"}, StringValue{"b"}, StringValue{"c"}, StringValue{"d"})
	assert.NoError(t, err)
	ref.rpush("a", "b", "c", "d")

	setCases := []struct {
		index int64
		val   string
	}{
		{0, "A"}, {3, "D"}, {-1, "Z"}, {-4, "start"}, {42, "oob"}, {-42, "oob-neg"},
	}
	for _, sc := range setCases {
		ok, err := c.LSET(key, sc.index, sc.val)
		assert.NoError(t, err, "LSET %d", sc.index)
		wantOK := ref.lset(sc.index, sc.val)
		assert.Equal(t, wantOK, ok, "LSET %d ok mismatch", sc.index)
		assertListStateEqual(t, c, key, ref, fmt.Sprintf("LSET %d %s", sc.index, sc.val))
	}
}

// --- LTRIM differential coverage (需求 7.4) ---

func TestListDiff_LTRIM(t *testing.T) {
	cases := []struct {
		name        string
		start, stop int64
	}{
		{"middle", 1, 3},
		{"headKeep", 0, 2},
		{"tailKeep", 2, 5},
		{"negative", -3, -1},
		{"emptyResult", 5, 2},
		{"allByNeg", 0, -1},
		{"single", 2, 2},
		{"outOfRange", 10, 20},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t)
			ref := &refList{}
			key := "diff:ltrim:" + tc.name

			seed := []interface{}{
				StringValue{"v0"}, StringValue{"v1"}, StringValue{"v2"},
				StringValue{"v3"}, StringValue{"v4"}, StringValue{"v5"},
			}
			_, err := c.RPUSH(key, seed...)
			assert.NoError(t, err)
			ref.rpush("v0", "v1", "v2", "v3", "v4", "v5")

			_, err = c.LTRIM(key, tc.start, tc.stop)
			assert.NoError(t, err)
			ref.ltrim(tc.start, tc.stop)
			assertListStateEqual(t, c, key, ref, fmt.Sprintf("LTRIM %d %d", tc.start, tc.stop))
		})
	}
}

// --- LREM differential coverage (需求 7.4) ---
//
// FINDING: The fork's LREM returns (remainingLength, success) rather than the
// Redis contract of (removedCount). We validate the resulting list STATE
// (contents + length) against the oracle, which is the correctness-critical
// invariant, and separately note the return-value divergence.
func TestListDiff_LREM(t *testing.T) {
	cases := []struct {
		name  string
		count int64
		val   string
	}{
		{"removeAll", 0, "x"},
		{"fromHead", 2, "x"},
		{"fromTail", -2, "x"},
		{"moreThanExist", 10, "x"},
		{"noMatch", 1, "absent"},
		{"headOne", 1, "y"},
		{"tailOne", -1, "y"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t)
			ref := &refList{}
			key := "diff:lrem:" + tc.name

			seed := []interface{}{
				StringValue{"x"}, StringValue{"y"}, StringValue{"x"},
				StringValue{"z"}, StringValue{"x"}, StringValue{"y"}, StringValue{"x"},
			}
			_, err := c.RPUSH(key, seed...)
			assert.NoError(t, err)
			ref.rpush("x", "y", "x", "z", "x", "y", "x")

			gotLen, gotSuccess, err := c.LREM(key, tc.count, StringValue{tc.val})
			assert.NoError(t, err)

			removed, remaining := ref.lrem(tc.count, tc.val)

			// Core differential assertion: resulting list state matches the oracle.
			assertListStateEqual(t, c, key, ref, fmt.Sprintf("LREM %d %s", tc.count, tc.val))

			// Fork contract: success indicates whether anything was removed.
			assert.Equal(t, removed > 0, gotSuccess, "LREM success mismatch")

			// Fork returns remaining length (divergence from Redis removed-count).
			if gotSuccess {
				assert.Equal(t, remaining, gotLen, "LREM returned length mismatch")
			}
		})
	}
}

// --- RPOPLPUSH differential coverage (需求 7.5) ---

func TestListDiff_RPOPLPUSH(t *testing.T) {
	c := newClient(t)
	src := &refList{}
	dst := &refList{}
	srcKey := "diff:rpoplpush:src"
	dstKey := "diff:rpoplpush:dst"

	_, err := c.RPUSH(srcKey, StringValue{"a"}, StringValue{"b"}, StringValue{"c"})
	assert.NoError(t, err)
	src.rpush("a", "b", "c")

	// Move all elements across two distinct keys.
	for i := 0; i < 3; i++ {
		got, err := c.RPOPLPUSH(srcKey, dstKey)
		assert.NoError(t, err)
		wantVal, wantOK := src.rpoplpush(dst)
		assert.Equal(t, wantOK, !got.Empty(), "RPOPLPUSH presence mismatch at %d", i)
		if wantOK {
			assert.Equal(t, wantVal, got.String(), "RPOPLPUSH value mismatch at %d", i)
		}
		assertListStateEqual(t, c, srcKey, src, fmt.Sprintf("RPOPLPUSH src step %d", i))
		assertListStateEqual(t, c, dstKey, dst, fmt.Sprintf("RPOPLPUSH dst step %d", i))
	}

	// RPOPLPUSH on empty source is a no-op returning nil.
	got, err := c.RPOPLPUSH(srcKey, dstKey)
	assert.NoError(t, err)
	assert.True(t, got.Empty())

	// Single-key rotation: tail moves to head.
	rotKey := "diff:rpoplpush:rot"
	rot := &refList{}
	_, err = c.RPUSH(rotKey, StringValue{"1"}, StringValue{"2"}, StringValue{"3"})
	assert.NoError(t, err)
	rot.rpush("1", "2", "3")

	for i := 0; i < 4; i++ {
		got, err := c.RPOPLPUSH(rotKey, rotKey)
		assert.NoError(t, err)
		// reference: pop tail then push head on the SAME list
		wantVal, wantOK := rot.rpop()
		if wantOK {
			rot.lpush(wantVal)
		}
		assert.Equal(t, wantOK, !got.Empty(), "rotate presence mismatch at %d", i)
		if wantOK {
			assert.Equal(t, wantVal, got.String(), "rotate value mismatch at %d", i)
		}
		assertListStateEqual(t, c, rotKey, rot, fmt.Sprintf("rotate step %d", i))
	}
}

// --- Randomized differential sequence (property-style, seeded) ---
//
// Generates a random sequence of list mutations and asserts the fork's list
// state matches the Redis-semantics oracle at every step. Provides broad
// coverage across command interleavings (需求 7.1, 7.3, 7.4, 7.5).
func TestListDiff_RandomizedSequence(t *testing.T) {
	const seed = 20240702
	rng := rand.New(rand.NewSource(seed))

	c := newClient(t)
	ref := &refList{}
	key := "diff:rand"

	vals := []string{"a", "b", "c", "d"} // small alphabet -> many duplicates for LREM

	const steps = 120
	for step := 0; step < steps; step++ {
		op := rng.Intn(9)
		v := vals[rng.Intn(len(vals))]
		desc := fmt.Sprintf("step %d op %d", step, op)

		switch op {
		case 0: // LPUSH
			gotN, err := c.LPUSH(key, StringValue{v})
			assert.NoError(t, err, desc)
			assert.Equal(t, int64(ref.lpush(v)), gotN, "LPUSH len "+desc)
		case 1: // RPUSH
			gotN, err := c.RPUSH(key, StringValue{v})
			assert.NoError(t, err, desc)
			assert.Equal(t, int64(ref.rpush(v)), gotN, "RPUSH len "+desc)
		case 2: // LPOP
			got, err := c.LPOP(key)
			assert.NoError(t, err, desc)
			wantV, wantOK := ref.lpop()
			assert.Equal(t, wantOK, !got.Empty(), "LPOP presence "+desc)
			if wantOK {
				assert.Equal(t, wantV, got.String(), "LPOP value "+desc)
			}
		case 3: // RPOP
			got, err := c.RPOP(key)
			assert.NoError(t, err, desc)
			wantV, wantOK := ref.rpop()
			assert.Equal(t, wantOK, !got.Empty(), "RPOP presence "+desc)
			if wantOK {
				assert.Equal(t, wantV, got.String(), "RPOP value "+desc)
			}
		case 4: // LSET random index
			if ref.llen() > 0 {
				idx := int64(rng.Intn(int(ref.llen())))
				ok, err := c.LSET(key, idx, v)
				assert.NoError(t, err, desc)
				assert.Equal(t, ref.lset(idx, v), ok, "LSET ok "+desc)
			}
		case 5: // LREM
			count := int64(rng.Intn(5) - 2) // -2..2
			_, _, err := c.LREM(key, count, StringValue{v})
			assert.NoError(t, err, desc)
			ref.lrem(count, v)
		case 6: // LTRIM
			n := ref.llen()
			if n > 0 {
				start := int64(rng.Intn(int(n)))
				stop := int64(rng.Intn(int(n)))
				_, err := c.LTRIM(key, start, stop)
				assert.NoError(t, err, desc)
				ref.ltrim(start, stop)
			}
		case 7: // LINDEX check (read-only)
			if ref.llen() > 0 {
				idx := int64(rng.Intn(int(ref.llen())))
				got, err := c.LINDEX(key, idx)
				assert.NoError(t, err, desc)
				wantV, wantOK := ref.lindex(idx)
				assert.Equal(t, wantOK, got.Present(), "LINDEX presence "+desc)
				if wantOK {
					assert.Equal(t, wantV, got.String(), "LINDEX value "+desc)
				}
			}
		case 8: // RPOPLPUSH self-rotation
			got, err := c.RPOPLPUSH(key, key)
			assert.NoError(t, err, desc)
			wantV, wantOK := ref.rpop()
			if wantOK {
				ref.lpush(wantV)
			}
			assert.Equal(t, wantOK, !got.Empty(), "rotate presence "+desc)
			if wantOK {
				assert.Equal(t, wantV, got.String(), "rotate value "+desc)
			}
		}

		// Differential state check after every mutation.
		assertListStateEqual(t, c, key, ref, desc)
	}
}

// TestListDiff_LINSERT_Missing documents that LINSERT (listed in task 3.4) is
// not implemented in the fork. When the fork adds LINSERT (spec task 16.2),
// replace this with a real differential test against the oracle.
func TestListDiff_LINSERT_Missing(t *testing.T) {
	t.Skip("LINSERT is not implemented in the redimo fork; differential coverage " +
		"deferred to spec task 16.2 (proxy-level LINSERT). See task 3.4 report.")
}
