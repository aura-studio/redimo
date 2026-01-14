# Redimo - AI Coding Agent Instructions

## Project Overview

Redimo is a Go library that translates the Redis API to DynamoDB operations. It maps Redis data structures (strings, sets, lists, hashes, sorted sets, streams) to efficient DynamoDB calls, making DynamoDB more developer-friendly while maintaining cost/space/time efficiency.

**Key Insight**: This is a translation/adapter library, not a wrapper. Each Redis command must map to one or more DynamoDB operations (GetItem, UpdateItem, Query, etc.) and handle DynamoDB's constraints (25-item transactions, 1KB keys, 400KB values, optimistic concurrency).

## Architecture

### Core Structure
- **Client type** ([redimo.go](redimo.go)): Main entry point with builder pattern methods:
  - `Table(name)`, `Index(name)`, `Attributes(pk, sk, skN)` - configure DynamoDB target
  - `StronglyConsistent()` / `EventuallyConsistent()` - consistency control
  - `TransactionActions(n)` - batch transaction size
- **Data types are modular**: Each Redis data structure (strings, lists, sets, hashes, sorted sets, streams) has its own file (e.g., `strings.go`, `lists.go`)
- **Value interface** ([values.go](values.go)): Polymorphic serialization to DynamoDB AttributeValue
  - Built-in wrappers: `StringValue`, `BytesValue`, `IntValue`, `FloatValue`, `ReturnValue`
  - Supports any type via `ToValueE()` with automatic conversion for int/float families

### Table Schema Design
All data types use a **universal 3-attribute schema**:
- **pk** (partition key, string): Redis key name
- **sk** (sort key, string): Data-structure-specific sort key (field name, member ID, etc.)
- **skN** (numeric sort key): For range queries on numbers (indices, scores, timestamps)
- Optional **idx** (local secondary index): Enables queries on `skN` instead of `sk`
- **val** key: Stores the actual value as a DynamoDB AttributeValue

Different data structures pack different data into sk/skN:
- **Strings**: Single item per pk
- **Lists**: Items use numeric indices in skN, arbitrary left/right ordering via constants like `ListSKIndexLeft`
- **Sets**: Members in sk with random skN values (pseudo-randomness for partition distribution)
- **Hashes**: Field names in sk, values in val
- **Sorted Sets**: Members in sk, scores in skN
- **Streams**: Entry IDs in sk, consumer group state separate

### Expression Building Pattern
The `expressionBuilder` type ([redimo.go](redimo.go#L211)) constructs DynamoDB update/condition expressions:
- Accumulates SET clauses, conditions, attribute names (for reserved word escaping)
- Centralized placeholder generation to prevent collisions
- Example: `MSET` uses transactions with multiple builders for 25-item limit

## Development Patterns

### Redis Command Implementation
1. **Location**: Add methods to the appropriate file (`strings.go`, `lists.go`, etc.)
2. **Method naming**: Use exact Redis command name in caps (e.g., `GET`, `LPUSH`)
3. **Error handling**: Use go-sdk-v2 error types and wrap with context
4. **Consistency**: Respect client's `consistentReads` flag for GetItem/Query operations
5. **Documentation**: Include Redis docs link and note DynamoDB constraints

**Example pattern** (from `GET` in strings.go):
```go
func (c Client) GET(key string) (val ReturnValue, err error) {
    resp, err := c.ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
        ConsistentRead: aws.Bool(c.consistentReads),
        Key: keyDef{pk: key, sk: ""}.toAV(c),
        TableName: aws.String(c.tableName),
    })
    if err != nil || len(resp.Item) == 0 {
        return // empty ReturnValue signals key not found
    }
    val = ReturnValue{resp.Item[vk]}
    return
}
```

### Data Structure Operations
- **Range queries**: Use Query API with sort key conditions; handle `skN` for numeric ranges
- **Multi-item operations**: Split into batches ≤25 items for transactions
- **Soft deletes**: Some structures use markers (e.g., deleted stream entries) instead of hard deletes
- **Optimistic concurrency**: Catch `ConditionalCheckFailedException` for IfNotExists/IfAlreadyExists flags

### Test Patterns
- **Setup**: Each test creates isolated DynamoDB table via `newClient()` ([redimo_test.go](redimo_test.go#L27))
- **Parallel execution**: Tests use `t.Parallel()` for fast iteration
- **AWS LocalStack**: Connect via env vars in `newConfig()` (useful for CI/CD)
- **Assertions**: Import `testify/assert` for readable test output

**Test file layout**: One `*_test.go` per module (strings_test.go, lists_test.go, etc.)

## Critical Differences from Redis

### Size Constraints
- **Keys**: Max 1KB (includes partition + sort key)
- **Values**: Max 400KB per item
- **Transactions**: Max 25 items per transaction (MSET limited to 25 keys)

### Concurrency Model
- **Single-key pessimism** vs **multi-key optimism**: Operations on same key may retry; `ConditionalCheckFailedException` is normal
- **No global ordering**: Unlike Redis's single-threaded guarantee, operations across keys can interleave

### Unsupported Features
- Bit operations (GETBIT, SETBIT, BITCOUNT)
- HyperLogLog
- Pub/Sub (architectural limitation; streams are different)
- Lua scripting (use host language instead)
- Transactions with WATCH/MULTI/EXEC
- ACLs

## Common Development Tasks

### Adding a new Redis command
1. Identify its data type file (strings/lists/sets/hashes/sorted_sets/streams)
2. Check Redis docs for semantics
3. Write tests first in the corresponding `*_test.go`
4. Implement using DynamoDB operations respecting schema
5. Document the mapping and any constraints (call-outs for 25-item limits, etc.)

### Fixing a test
- Run: `go test -run TestName` (isolated test execution)
- LocalStack: Tests auto-create tables; inspect via AWS console if debugging
- Assertions: Add logs via `fmt.Sprintf` in test error messages

### Performance optimization
- Profile with `lists_benchmark_test.go` pattern (measure Queries vs GetItems)
- Consider batching operations to respect 25-item transaction limit
- Evaluate if operation should use index (skN) vs main sort key

## Key Files Reference

| File | Purpose |
|------|---------|
| [redimo.go](redimo.go) | Client struct, builder methods, expressionBuilder, table creation |
| [values.go](values.go) | Value interface, ToValue* converters, ReturnValue wrapper |
| [strings.go](strings.go) | GET, SET, APPEND, GETSET, INCR family |
| [lists.go](lists.go) | LPUSH, RPUSH, LPOP, RPOP, LRANGE, with complex index management |
| [sets.go](sets.go) | SADD, SREM, SMEMBERS, set operations |
| [hashes.go](hashes.go) | HGET, HSET, HDEL, HMGET, hash operations |
| [sorted_sets.go](sorted_sets.go) | ZADD, ZRANGE, ZRANK, scored member operations |
| [streams.go](streams.go) | XADD, XREAD, consumer groups with sequence tracking |
| [geo.go](geo.go) | Geohash-based geographic queries |
| [redimo_test.go](redimo_test.go) | Client setup, config, table creation utilities for tests |

## Dependencies

- **aws-sdk-go-v2**: DynamoDB client and types (v1.17.x)
- **testify**: assert/require in tests
- **uuid, geohash, geo, ulid**: Supporting libs for IDs and geospatial features
