package redimo

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// listElementAV encodes a list element value for storage in the `val` attribute
// as DynamoDB Binary, so that arbitrary bytes (0x00-0xff) survive round-trips
// without the UTF-8 substitution that the String (S) type would apply. The
// element arrives as a redimo Value; string-shaped values carry their exact
// bytes in the Go string, which we forward as a byte slice losslessly.
func listElementAV(v Value) types.AttributeValue {
	switch tv := v.(type) {
	case StringValue:
		return BytesValue{[]byte(tv.S)}.ToAV()
	case BytesValue:
		return tv.ToAV()
	default:
		// Fall back to the value's own encoding (e.g. numeric values). These
		// were never binary-unsafe, so no conversion is needed.
		return v.ToAV()
	}
}

// listElement decodes a stored `val` attribute back into a ReturnValue. List
// element values are stored as Binary (see listElementAV); to preserve the
// historical string-oriented API (ReturnValue.String()), a Binary value is
// re-wrapped as a String-typed ReturnValue. This is lossless: the bytes read
// back from DynamoDB Binary are placed verbatim into a Go string, which can
// hold any byte sequence. Callers that want the raw bytes can still use
// ReturnValue.Bytes() on the original attribute.
func listElement(av types.AttributeValue) ReturnValue {
	if b, ok := av.(*types.AttributeValueMemberB); ok {
		return ReturnValue{av: &types.AttributeValueMemberS{Value: string(b.Value)}}
	}

	return ReturnValue{av: av}
}

// valueBytes extracts the raw bytes of a list element value, accepting either a
// StringValue or a BytesValue so callers can pass binary-safe elements uniformly
// (like the String/Hash families do) rather than being forced through StringValue.
// The bytes feed genSk's content hash, so a value's identity is its exact bytes
// regardless of which wrapper the caller used.
func valueBytes(v Value) []byte {
	switch tv := v.(type) {
	case BytesValue:
		return tv.B
	case StringValue:
		return []byte(tv.S)
	default:
		return ReturnValue{av: v.ToAV()}.Bytes()
	}
}

// genSk generates sort key from value and index.
// Format: sha256(val)|index
// - SHA256 ensures fixed-length (64 chars) keys regardless of value size
// - Same values will have same hash prefix, enabling efficient range queries for LREM
// - Index suffix ensures uniqueness for multiple instances of same value
func genSk(val string, index int64) string {
	// val to sha256 hash (fixed 64 chars)
	hash := sha256.Sum256([]byte(val))
	hashStr := hex.EncodeToString(hash[:])
	return fmt.Sprintf("%s|%v", hashStr, index)
}

// listItemsToElements decodes a slice of raw list items into their element values.
func listItemsToElements(items []map[string]types.AttributeValue) []ReturnValue {
	elements := make([]ReturnValue, 0, len(items))
	for _, item := range items {
		elements = append(elements, listElement(item[vk]))
	}

	return elements
}
