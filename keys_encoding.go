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
// without the UTF-8 substitution the String (S) type applies. To keep two
// namespaces disjoint we reserve a one-byte prefix:
//
//   - The String value item (strings.go GET/SET/MGET/... always use sk="") is
//     encoded as the single reserved byte skPrefixValue (0x00).
//   - Every other, "member-shaped" sk (hash field names, set/zset/geo members,
//     and the internally generated list / stream / meta sort keys) is encoded as
//     skPrefixMember (0x01) followed by the raw member bytes. An empty member
//     ("") therefore becomes the single byte 0x01, which is distinct from the
//     value-item marker 0x00.
//
// This fixes two latent bugs from the previous "/"-sentinel scheme:
//   - a real member named "/" is no longer silently rewritten to "" on read;
//   - an empty member "" and a member "/" no longer collide onto the same sk.
//
// Because every member shares the 0x01 prefix, byte ordering between members is
// preserved (0x01||A < 0x01||B  ⟺  A < B), so ZRANGEBYLEX / zset lexical order
// remains correct. The empty member (0x01) sorts before all non-empty members,
// matching Redis, and the value-item marker (0x00) sorts before everything.
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

func encodeSK(sk string) []byte {
	if sk == "" {
		return []byte{skPrefixValue}
	}

	out := make([]byte, 0, len(sk)+1)
	out = append(out, skPrefixMember)
	out = append(out, sk...)

	return out
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
