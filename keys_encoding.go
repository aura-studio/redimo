package redimo

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type keyDef struct {
	pk string
	sk string
}

func (k keyDef) toAV(c Client) map[string]types.AttributeValue {
	m := map[string]types.AttributeValue{
		c.partitionKey: &types.AttributeValueMemberB{Value: []byte(k.pk)},
		c.sortKey:      &types.AttributeValueMemberB{Value: encodeSK(k.sk)},
	}

	return m
}

// Sort-key (sk) encoding.
//
// The sk is stored as DynamoDB Binary so it can hold arbitrary bytes (0x00-0xff)
// without the UTF-8 substitution the String (S) type applies. Three disjoint
// namespaces are separated by a reserved one-byte prefix:
//
//   - The String value item is the single reserved byte skPrefixValue (0x00). It is
//     NOT produced by encodeSK — the String commands (GET/SET/MGET/INCR/...) address
//     it through the dedicated valueItemKey(). This is what lets it coexist with a
//     collection's EMPTY member without collision (see below).
//   - Every "member-shaped" sk (hash field names, set/zset members, the internally
//     generated list indices) is encoded by encodeSK as skPrefixMember (0x01) followed
//     by the raw member bytes. An EMPTY member ("") therefore becomes the single byte
//     0x01 — distinct from the value-item marker 0x00.
//   - The single reserved #meta item is skPrefixMeta (0x02), addressed through
//     metaItemKey().
//
// Why the value item is NOT encodeSK("") (the fix in v3): the value item and a
// collection's empty member both have the logical sk "". Encoding BOTH via encodeSK("")
// mapped them to the SAME byte (0x00), so after a type overwrite (SET over a set, DEL,
// rebuild) a not-yet-reclaimed String value item surfaced as a PHANTOM empty "" member
// in SMEMBERS/HKEYS. Giving the value item a dedicated 0x00 sort key (valueItemKey) and
// moving the empty member to 0x01 (encodeSK("")) makes the two structurally distinct:
// collection reads exclude 0x00 (isValueItem) without ever dropping a real empty member.
//
// Because every member shares the 0x01 prefix, byte ordering between members is
// preserved (0x01||A < 0x01||B  ⟺  A < B), so ZRANGEBYLEX / zset lexical order remains
// correct. The empty member (0x01) sorts before all non-empty members, matching Redis,
// and the value-item marker (0x00) sorts before everything.
//
// BREAKING (v3): empty members written by v2 live at 0x00 (the old encodeSK("")); v3
// reads them as the value item and skips them. The value item's own location (0x00) is
// unchanged, so existing STRING data is fully compatible; only pre-v3 EMPTY members
// (rare) need a rewrite — see doc.go's migration note.
const (
	skPrefixValue  byte = 0x00
	skPrefixMember byte = 0x01
	// skPrefixMeta marks the single reserved #meta item's sort key. It is distinct
	// from the member prefix (0x01) so a user member/field/key named literally
	// "#meta" (which encodes as 0x01||"#meta") never collides with — and overwrites
	// — the key's own bookkeeping meta item. Meta detection is therefore by this
	// prefix byte (isMetaItem), NOT by the decoded string.
	skPrefixMeta byte = 0x02
)

// encodeSK encodes a member/field name to its Binary sort key: skPrefixMember (0x01)
// followed by the raw member bytes. An empty member ("") encodes to the single byte
// 0x01 — deliberately NOT 0x00, which is reserved for the String value item and never
// produced here (that item is addressed via valueItemKey). See the layout note above.
func encodeSK(sk string) []byte {
	out := make([]byte, 0, len(sk)+1)
	out = append(out, skPrefixMember)
	out = append(out, sk...)

	return out
}

// valueItemKey returns the DynamoDB primary key of a String key's reserved value item.
// Its sort key is the single reserved byte skPrefixValue (0x00), written directly (NOT
// via encodeSK) so it is distinct from the empty member (0x01) — the whole point of the
// v3 separation. Mirrors metaItemKey(). Every String command (GET/SET/SETCAS/GETSET/
// MGET/MSET/BatchGET/INCR*) addresses the value item through this, never keyDef{sk:""}.
func (c Client) valueItemKey(pk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		c.partitionKey: &types.AttributeValueMemberB{Value: []byte(pk)},
		c.sortKey:      &types.AttributeValueMemberB{Value: []byte{skPrefixValue}},
	}
}

// keyItemAV extracts the raw primary-key attributes (pk + sk, exactly as stored) from a
// queried item, WITHOUT the decodeSK/encodeSK round-trip keyDef.toAV would do. The
// reclaim paths (DeleteMembers/DeleteMembersIfDead/SweepOrphans) delete by these raw
// keys: since v3, decodeSK(0x00)=="" no longer re-encodes back to 0x00 (encodeSK("")
// is now 0x01), so round-tripping a value item's sort key through a keyDef would target
// the wrong item and orphan the value item. Deleting the raw bytes is exact for every
// item kind (value, member, list element).
func keyItemAV(item map[string]types.AttributeValue, c Client) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		c.partitionKey: item[c.partitionKey],
		c.sortKey:      item[c.sortKey],
	}
}

func decodeSK(sk []byte) string {
	if len(sk) == 0 {
		return ""
	}

	switch sk[0] {
	case skPrefixValue:
		return ""
	case skPrefixMeta:
		return MetaSK
	case skPrefixMember:
		return string(sk[1:])
	default:
		// Defensive: an sk not written by encodeSK (should not happen). Return
		// the raw bytes so behaviour degrades gracefully rather than panicking.
		return string(sk)
	}
}

type itemDef struct {
	keyDef
	val ReturnValue
}

func parseKey(avm map[string]types.AttributeValue, c Client) keyDef {
	return keyDef{
		pk: string(ReturnValue{avm[c.partitionKey]}.Bytes()),
		sk: decodeSK(ReturnValue{avm[c.sortKey]}.Bytes()),
	}
}

func parseItem(avm map[string]types.AttributeValue, c Client) (item itemDef) {
	item.keyDef = parseKey(avm, c)
	item.val = ReturnValue{avm[vk]}

	return
}
