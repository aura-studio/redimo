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

// TestDeleteMembersRecreateSafe: DeleteMembers reclaims a key's members ONLY when the key
// is logically absent (its #meta was removed, the normal post-DeleteMeta case). If the key
// is live/recreated (a #meta is present), DeleteMembers skips — so a DEL-then-recreate race
// cannot wipe the new incarnation's data.
func TestDeleteMembersRecreateSafe(t *testing.T) {
	c := newClient(t)

	_, err := c.EnsureType("h", TypeHash, 3)
	assert.NoError(t, err)
	for _, f := range []string{"f1", "f2", "f3"} {
		_, err := c.HSET("h", f, "v")
		assert.NoError(t, err)
	}

	// A live key (#meta present) must be left untouched: reclaiming here would wipe live data.
	deleted, err := c.DeleteMembers("h", 25)
	assert.NoError(t, err)
	assert.Equal(t, 0, deleted, "DeleteMembers must skip a live (recreated) key")
	n, err := c.HLEN("h")
	assert.NoError(t, err)
	assert.Equal(t, int32(3), n, "live key's members must survive")

	// Once the key is logically deleted (DeleteMeta removed the #meta), DeleteMembers reclaims.
	_, err = c.DeleteMeta("h")
	assert.NoError(t, err)

	deleted, err = c.DeleteMembers("h", 25)
	assert.NoError(t, err)
	assert.Equal(t, 3, deleted, "orphaned members must be reclaimed once #meta is gone")
	n, err = c.HLEN("h")
	assert.NoError(t, err)
	assert.Equal(t, int32(0), n)
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
