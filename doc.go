// Package redimo implements Redis data structures on top of a single Amazon
// DynamoDB table, exposing Strings, Hashes, Lists, Sets and Sorted Sets through a
// Client whose method names mirror the corresponding Redis commands (GET/SET,
// HGET/HSET, LPUSH/RPOP, SADD/SREM, ZADD/ZRANGE, and so on).
//
// # Single-table layout
//
// Every logical Redis key maps to one DynamoDB partition key (pk). A key's data
// items and its bookkeeping share that partition, distinguished by the sort key
// (sk), which is stored as DynamoDB Binary so it can carry arbitrary bytes. A
// reserved one-byte sk prefix keeps three namespaces disjoint:
//
//   - the single String value item (skPrefixValue, 0x00), addressed through the
//     dedicated valueItemKey — never through encodeSK;
//   - member-shaped items — hash fields, set/zset members and the internally
//     generated list indices — as skPrefixMember (0x01) followed by the raw member
//     bytes, so an EMPTY member ("") is the single byte 0x01;
//   - the single reserved #meta item (skPrefixMeta, 0x02).
//
// The prefix scheme preserves byte ordering between members so lexical ranges
// (ZRANGEBYLEX) stay correct, and guarantees a user member named literally "#meta"
// can never collide with a key's own metadata. A numeric attribute (skN) on scored
// items feeds a local secondary index used for score-ordered reads.
//
// # v3 sort-key change (BREAKING on-disk format)
//
// Through v2 the String value item AND a collection's empty member both encoded via
// encodeSK("") to the SAME byte, 0x00. Because a key's meta type made only one of them
// valid at a time they never coexisted deliberately, but after a type overwrite (SET
// over a set, DEL, rebuild) a not-yet-reclaimed String value item surfaced as a PHANTOM
// empty "" member in SMEMBERS/HKEYS. v3 separates them: the value item keeps sort key
// 0x00 (now written by valueItemKey, not encodeSK), while an empty member moves to 0x01
// (encodeSK("")). Collection reads exclude 0x00 (isValueItem) and so can never surface a
// stale value item, without ever dropping a genuine empty member.
//
// Migration: existing STRING data is UNAFFECTED — the value item's physical location
// (0x00) is unchanged, so GET/SET/INCR keep working across the upgrade with no rewrite.
// The one breaking case is a collection's EMPTY member/field written by v2 (stored at
// 0x00): v3 reads it as the value item and skips it, so it disappears from SMEMBERS/
// HGETALL/ZRANGEBYLEX and is uncounted by SCARD/HLEN/ZCARD. Empty members are rare and
// the v2 binary encoding is itself recent, so most deployments have none; a deployment
// that does can rewrite them before upgrading (SADD/HSET/ZADD the "" member again under
// v3, which relands it at 0x01). Set and zset empty members are also distinguishable by
// attribute for a bespoke migration scan — a set member carries no 'val' and a zset
// member carries 'skN', whereas the value item carries 'val' and no 'skN' (an empty hash
// FIELD, which also carries 'val', is the only shape indistinguishable from the value
// item at 0x00, so a hash-field rewrite must be driven by application knowledge). Because
// the on-disk format changes, v3 is a new major module version (…/redimo/v3).
//
// # The meta item
//
// Alongside its data items, each key carries a single reserved #meta item recording
// the key's logical type (str/hash/list/set/zset), an optional expiry epoch, an
// atomically maintained member count (backing O(1) LLEN/HLEN/SCARD/ZCARD), and, for
// lists, the head/tail index counters that order elements. Write commands funnel
// through a single conditional UpdateItem on this item (see EnsureType), which
// creates the key or verifies its type in one atomic step and returns the
// authoritative post-write count. Deleting the #meta item makes a key immediately
// logically absent while its data items are reclaimed asynchronously (DeleteMeta /
// DeleteMembers), keeping the client-visible DEL O(1).
//
// # Consistency
//
// A Client defaults to strongly consistent reads; EventuallyConsistent /
// StronglyConsistent toggle this per copy. The builder methods (Table, Index,
// Attributes, WithContext, TransactionActions) each return a modified copy, leaving
// the original untouched. Single-element collection mutations (SADD/SREM, HSET/HDEL,
// ZADD/ZREM) use per-element conditional writes (attribute_not_exists / attribute_exists),
// which DynamoDB evaluates atomically and serialized per item, so their added/removed
// counts — and hence SCARD/HLEN/ZCARD — are exact even under concurrent writes to the SAME
// element. The remaining best-effort case is a whole-collection read-modify-rewrite that
// cannot be expressed as one conditional write (list LTRIM/LSET/LREM): its contents stay
// valid but its element count (LLEN) can diverge from the rewrite under a concurrent push.
//
// # Key primitive families
//
// The exported surface groups into: the meta primitives (EnsureType,
// CreateTypeIfAbsent, LoadMeta, DeleteMeta, DeleteMetaIfEmpty) that gate every write;
// the member-reclamation primitives (DeleteMembers, DeleteMembersIfDead) behind the
// lazy deleter; batched member I/O (BatchWriteItem submit-and-retry for the *STORE
// builders and bulk pushes, BatchGetItem for MGET); and the per-type command methods for Strings,
// Hashes, Lists, Sets and Sorted Sets. It is designed to sit under a
// redis-dynamodb-proxy that speaks the Redis wire protocol on top of these calls.
package redimo
