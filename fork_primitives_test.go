package redimo

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
)

// These exercise the fork v1.7 storage primitives that back the redimos proxy's lazy
// deleter, SCAN and weekly sweeper directly against DynamoDB Local, rather than only
// indirectly through the proxy. Each asserts the primitive's contract around the reserved
// #meta item.

// TestDeleteMembersLeavesMeta: DeleteMembers unconditionally reclaims every data item under
// a pk EXCEPT the reserved #meta item, so it can be used both to clear a live collection for
// rewrite (LSET/LTRIM/... via LReplaceAll) and to reclaim an orphan after DeleteMeta. The
// DEL-then-recreate recreate-guard lives in the redimos lazy deleter, not this primitive.
func TestDeleteMembersLeavesMeta(t *testing.T) {
	c := newClient(t)

	_, err := c.EnsureType("h", TypeHash, 3)
	assert.NoError(t, err)
	for _, f := range []string{"f1", "f2", "f3"} {
		_, err := c.HSET("h", f, "v")
		assert.NoError(t, err)
	}

	deleted, err := c.DeleteMembers("h", 25)
	assert.NoError(t, err)
	assert.Equal(t, 3, deleted)

	// The #meta item remains (DeleteMeta owns its lifecycle, not DeleteMembers).
	_, found, err := c.LoadMeta("h")
	assert.NoError(t, err)
	assert.True(t, found, "DeleteMembers must not remove the #meta item")

	n, err := c.HLEN("h")
	assert.NoError(t, err)
	assert.Equal(t, int32(0), n)
}

// TestDeleteMembersIfDead: the fenced reclaim used by the async lazy deleter deletes a dead
// key's members (no #meta) but ABORTS on a live key (#meta present), leaving its data intact —
// this is what makes DEL-then-recreate linearizable: the #meta-absence check and the member
// deletes commit atomically, so a concurrent SET can never be wiped.
func TestDeleteMembersIfDead(t *testing.T) {
	c := newClient(t)

	const now = int64(1000)

	// Dead key: members present but no #meta (e.g. DeleteMeta already ran). Reclaim proceeds.
	_, err := c.SADD("dead", "a", "b", "c")
	assert.NoError(t, err)
	deleted, aborted, err := c.DeleteMembersIfDead("dead", now, 25)
	assert.NoError(t, err)
	assert.False(t, aborted, "a dead key (no #meta) must be reclaimed, not aborted")
	assert.Equal(t, 3, deleted)
	members, err := c.SMEMBERS("dead")
	assert.NoError(t, err)
	assert.Len(t, members, 0)

	// Live, unexpired key: #meta present (recreated). Reclaim must abort and delete nothing.
	_, err = c.EnsureType("live", TypeSet, 2)
	assert.NoError(t, err)
	_, err = c.SADD("live", "x", "y")
	assert.NoError(t, err)
	deleted, aborted, err = c.DeleteMembersIfDead("live", now, 25)
	assert.NoError(t, err)
	assert.True(t, aborted, "a live, unexpired key must abort the fenced reclaim")
	assert.Equal(t, 0, deleted, "no members may be deleted when the reclaim aborts")
	live, err := c.SMEMBERS("live")
	assert.NoError(t, err)
	assert.Len(t, live, 2, "the recreated key's members must survive")

	// Expired key: #meta present but exp <= now. The read path enqueues such a key without
	// removing its #meta, so the fence must still reclaim it.
	_, err = c.EnsureType("expired", TypeSet, 2)
	assert.NoError(t, err)
	_, err = c.SADD("expired", "p", "q")
	assert.NoError(t, err)
	_, err = c.SetExpire("expired", now-1) // exp=999 <= now=1000
	assert.NoError(t, err)
	deleted, aborted, err = c.DeleteMembersIfDead("expired", now, 25)
	assert.NoError(t, err)
	assert.False(t, aborted, "an expired key must be reclaimed, not aborted")
	assert.Equal(t, 2, deleted)
	exp, err := c.SMEMBERS("expired")
	assert.NoError(t, err)
	assert.Len(t, exp, 0, "the expired key's members must be reclaimed")
}

// TestScanMetaKeys: ScanMetaKeys pages the partition keys of LIVE meta items, excluding
// items whose exp is at or before nowEpoch.
func TestScanMetaKeys(t *testing.T) {
	c := newClient(t)

	for _, k := range []struct {
		name string
		typ  KeyType
	}{{"k1", TypeString}, {"k2", TypeHash}, {"k3", TypeSet}} {
		_, err := c.EnsureType(k.name, k.typ, 0)
		assert.NoError(t, err)
	}

	// An expired key: exp=100, scanned at now=1000 -> excluded.
	_, err := c.EnsureType("expired", TypeString, 0)
	assert.NoError(t, err)
	_, err = c.SetExpire("expired", 100)
	assert.NoError(t, err)

	const now = int64(1000)
	got := map[string]bool{}
	var lek map[string]types.AttributeValue
	for {
		pks, next, err := c.ScanMetaKeys(100, lek, now)
		assert.NoError(t, err)
		for _, pk := range pks {
			got[pk] = true
		}
		if len(next) == 0 {
			break
		}
		lek = next
	}

	assert.True(t, got["k1"] && got["k2"] && got["k3"], "all live keys must be scanned, got %v", got)
	assert.False(t, got["expired"], "expired key must be excluded from the scan")
}

// TestSweepOrphans: SweepOrphans reclaims data members whose owning pk has no #meta item,
// and leaves a live key (meta present) entirely untouched.
func TestSweepOrphans(t *testing.T) {
	c := newClient(t)

	// Orphan members: SADD writes member items but no #meta, so these pks are orphans.
	_, err := c.SADD("orphan1", "a", "b", "c")
	assert.NoError(t, err)
	_, err = c.SADD("orphan2", "x", "y")
	assert.NoError(t, err)

	// A live key: EnsureType materializes its #meta, so its members are not orphans.
	_, err = c.EnsureType("live", TypeSet, 2)
	assert.NoError(t, err)
	_, err = c.SADD("live", "m1", "m2")
	assert.NoError(t, err)

	reclaimed, err := c.SweepOrphans(25)
	assert.NoError(t, err)
	assert.Equal(t, 5, reclaimed, "must reclaim orphan1(3) + orphan2(2)")

	// The live key survives fully.
	_, found, err := c.LoadMeta("live")
	assert.NoError(t, err)
	assert.True(t, found)
	live, err := c.SMEMBERS("live")
	assert.NoError(t, err)
	assert.Len(t, live, 2)

	// The orphans are gone.
	o1, err := c.SMEMBERS("orphan1")
	assert.NoError(t, err)
	assert.Len(t, o1, 0)
	o2, err := c.SMEMBERS("orphan2")
	assert.NoError(t, err)
	assert.Len(t, o2, 0)
}
