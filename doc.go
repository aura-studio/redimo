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
// reserved one-byte sk prefix keeps three namespaces disjoint: the String value
// item (skPrefixValue, 0x00), member-shaped items — hash fields, set/zset members
// and the internally generated list indices (skPrefixMember, 0x01, followed by the
// raw member bytes), and the single reserved #meta item (skPrefixMeta, 0x02). The
// prefix scheme preserves byte ordering between members so lexical ranges
// (ZRANGEBYLEX) stay correct, and guarantees a user member named literally "#meta"
// can never collide with a key's own metadata. A numeric attribute (skN) on scored
// items feeds a local secondary index used for score-ordered reads.
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
// the original untouched. Multi-item mutations that cannot be expressed as one
// conditional write are best-effort across concurrent connections: contents stay
// correct, but added/removed counts can be approximate under a concurrent write to
// the same member.
//
// # Key primitive families
//
// The exported surface groups into: the meta primitives (EnsureType,
// CreateTypeIfAbsent, LoadMeta, DeleteMeta, DeleteMetaIfEmpty) that gate every write;
// the member-reclamation primitives (DeleteMembers, DeleteMembersIfDead) behind the
// lazy deleter; batched member I/O (BatchWriteItem submit-and-retry, BatchGetItem
// existence snapshots) shared by Sets; and the per-type command methods for Strings,
// Hashes, Lists, Sets and Sorted Sets. It is designed to sit under a
// redis-dynamodb-proxy that speaks the Redis wire protocol on top of these calls.
package redimo
