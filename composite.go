package redimo

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Redis common data type constants.
const (
	CompositeTypeString = "string"
	CompositeTypeBytes  = "bytes"
	CompositeTypeInt    = "int"
	CompositeTypeList   = "list"
	CompositeTypeSet    = "set"
	CompositeTypeHash   = "hash"
	CompositeTypeZSet   = "zset"
)

// Backward-compatible aliases used by older callers.
const (
	RedimoTypeString = CompositeTypeString
	RedimoTypeBytes  = CompositeTypeBytes
	RedimoTypeInt    = CompositeTypeInt
	RedimoTypeList   = CompositeTypeList
	RedimoTypeSet    = CompositeTypeSet
	RedimoTypeHash   = CompositeTypeHash
	RedimoTypeZSet   = CompositeTypeZSet
)

// CompositeEncodedValue stores any value with explicit type metadata.
type CompositeEncodedValue struct {
	Type string          `json:"type"`
	Raw  json.RawMessage `json:"raw"`
}

// CompositeHashEntry represents one hash field.
type CompositeHashEntry struct {
	Field string                `json:"field"`
	Val   CompositeEncodedValue `json:"val"`
}

// Composite is a universal wire format for common Redis types.
type Composite struct {
	Key      string                  `json:"key"`
	Type     string                  `json:"type"`
	IntVal   *int64                  `json:"int,omitempty"`
	StrVal   *string                 `json:"str,omitempty"`
	BytesVal *string                 `json:"bytes,omitempty"`
	ListVal  []CompositeEncodedValue `json:"list,omitempty"`
	SetVal   []CompositeEncodedValue `json:"set,omitempty"`
	HashVal  []CompositeHashEntry    `json:"hash,omitempty"`
	ZSetVal  map[string]float64      `json:"zset,omitempty"`
}

func (c Composite) validate() error {
	if c.Key == "" {
		return fmt.Errorf("Composite: key is empty")
	}
	switch c.Type {
	case CompositeTypeString, CompositeTypeBytes, CompositeTypeInt,
		CompositeTypeList, CompositeTypeSet, CompositeTypeHash, CompositeTypeZSet:
		return nil
	}
	return fmt.Errorf("Composite: unknown type %q", c.Type)
}

// BuildComposite infers Redis type and builds a typed, serializable payload.
func BuildComposite(key string, value interface{}) (Composite, error) {
	if key == "" {
		return Composite{}, fmt.Errorf("BuildComposite: key is empty")
	}
	if value == nil {
		return Composite{}, fmt.Errorf("BuildComposite: nil value")
	}

	if uv, ok := unwrapPointer(value); ok {
		return BuildComposite(key, uv)
	}

	comp := Composite{Key: key}

	switch v := value.(type) {
	case ReturnValue:
		return buildCompositeFromReturnValue(key, v)
	case string:
		comp.Type = CompositeTypeString
		comp.StrVal = &v
		return comp, nil
	case []byte:
		s := base64.StdEncoding.EncodeToString(v)
		comp.Type = CompositeTypeBytes
		comp.BytesVal = &s
		return comp, nil
	case int:
		n := int64(v)
		comp.Type = CompositeTypeInt
		comp.IntVal = &n
		return comp, nil
	case int8:
		n := int64(v)
		comp.Type = CompositeTypeInt
		comp.IntVal = &n
		return comp, nil
	case int16:
		n := int64(v)
		comp.Type = CompositeTypeInt
		comp.IntVal = &n
		return comp, nil
	case int32:
		n := int64(v)
		comp.Type = CompositeTypeInt
		comp.IntVal = &n
		return comp, nil
	case int64:
		comp.Type = CompositeTypeInt
		comp.IntVal = &v
		return comp, nil
	case uint:
		n := int64(v)
		comp.Type = CompositeTypeInt
		comp.IntVal = &n
		return comp, nil
	case uint8:
		n := int64(v)
		comp.Type = CompositeTypeInt
		comp.IntVal = &n
		return comp, nil
	case uint16:
		n := int64(v)
		comp.Type = CompositeTypeInt
		comp.IntVal = &n
		return comp, nil
	case uint32:
		n := int64(v)
		comp.Type = CompositeTypeInt
		comp.IntVal = &n
		return comp, nil
	case uint64:
		if v > uint64(math.MaxInt64) {
			return Composite{}, fmt.Errorf("BuildComposite: uint64 overflow")
		}
		n := int64(v)
		comp.Type = CompositeTypeInt
		comp.IntVal = &n
		return comp, nil
	}

	rv := reflect.ValueOf(value)

	if m, ok := toStringFloatMap(rv); ok {
		comp.Type = CompositeTypeZSet
		comp.ZSetVal = m
		return comp, nil
	}

	if m, ok := toSetMembers(rv); ok {
		comp.Type = CompositeTypeSet
		comp.SetVal = m
		return comp, nil
	}

	if m, ok := toHashEntries(rv); ok {
		comp.Type = CompositeTypeHash
		comp.HashVal = m
		return comp, nil
	}

	if l, ok := toListValues(rv); ok {
		comp.Type = CompositeTypeList
		comp.ListVal = l
		return comp, nil
	}

	return Composite{}, fmt.Errorf("BuildComposite: unsupported type %T", value)
}

func unwrapPointer(v interface{}) (interface{}, bool) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr && !rv.IsNil() {
		return rv.Elem().Interface(), true
	}
	return nil, false
}

func encodeAny(v interface{}) (CompositeEncodedValue, error) {
	if rv, ok := v.(ReturnValue); ok {
		decoded, err := returnValueToInterface(rv)
		if err != nil {
			return CompositeEncodedValue{}, err
		}
		b, err := json.Marshal(decoded)
		if err != nil {
			return CompositeEncodedValue{}, err
		}
		return CompositeEncodedValue{Type: typeNameOf(decoded), Raw: b}, nil
	}

	b, err := json.Marshal(v)
	if err != nil {
		return CompositeEncodedValue{}, err
	}
	return CompositeEncodedValue{Type: typeNameOf(v), Raw: b}, nil
}

func buildCompositeFromReturnValue(key string, rv ReturnValue) (Composite, error) {
	decoded, err := returnValueToInterface(rv)
	if err != nil {
		return Composite{}, err
	}
	if decoded == nil {
		return Composite{}, fmt.Errorf("BuildComposite: ReturnValue is empty")
	}
	return BuildComposite(key, decoded)
}

func returnValueToInterface(rv ReturnValue) (interface{}, error) {
	if rv.Empty() {
		return nil, nil
	}
	return attributeValueToInterface(rv.ToAV())
}

func attributeValueToInterface(av types.AttributeValue) (interface{}, error) {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value, nil
	case *types.AttributeValueMemberB:
		return []byte(v.Value), nil
	case *types.AttributeValueMemberBOOL:
		return v.Value, nil
	case *types.AttributeValueMemberNULL:
		return nil, nil
	case *types.AttributeValueMemberN:
		return parseNumericString(v.Value)
	case *types.AttributeValueMemberL:
		list := make([]interface{}, 0, len(v.Value))
		for _, item := range v.Value {
			parsed, err := attributeValueToInterface(item)
			if err != nil {
				return nil, err
			}
			list = append(list, parsed)
		}
		return list, nil
	case *types.AttributeValueMemberM:
		m := make(map[string]interface{}, len(v.Value))
		for k, item := range v.Value {
			parsed, err := attributeValueToInterface(item)
			if err != nil {
				return nil, err
			}
			m[k] = parsed
		}
		return m, nil
	case *types.AttributeValueMemberSS:
		set := make(map[string]struct{}, len(v.Value))
		for _, item := range v.Value {
			set[item] = struct{}{}
		}
		return set, nil
	case *types.AttributeValueMemberNS:
		set := make(map[string]struct{}, len(v.Value))
		for _, item := range v.Value {
			set[item] = struct{}{}
		}
		return set, nil
	case *types.AttributeValueMemberBS:
		set := make(map[string]struct{}, len(v.Value))
		for _, item := range v.Value {
			set[base64.StdEncoding.EncodeToString(item)] = struct{}{}
		}
		return set, nil
	default:
		return nil, fmt.Errorf("BuildComposite: unsupported ReturnValue AV type %T", av)
	}
}

func parseNumericString(raw string) (interface{}, error) {
	if strings.ContainsAny(raw, ".eE") {
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i, nil
	}

	if u, err := strconv.ParseUint(raw, 10, 64); err == nil {
		if u <= math.MaxInt64 {
			return int64(u), nil
		}
		return u, nil
	}

	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func decodeAny(v CompositeEncodedValue) (interface{}, error) {
	switch v.Type {
	case "string":
		var out string
		if err := json.Unmarshal(v.Raw, &out); err != nil {
			return nil, err
		}
		return out, nil
	case "bool":
		var out bool
		if err := json.Unmarshal(v.Raw, &out); err != nil {
			return nil, err
		}
		return out, nil
	case "int", "int8", "int16", "int32", "int64":
		var out int64
		if err := json.Unmarshal(v.Raw, &out); err != nil {
			return nil, err
		}
		return out, nil
	case "uint", "uint8", "uint16", "uint32", "uint64":
		var out uint64
		if err := json.Unmarshal(v.Raw, &out); err != nil {
			return nil, err
		}
		return out, nil
	case "float32", "float64":
		var out float64
		if err := json.Unmarshal(v.Raw, &out); err != nil {
			return nil, err
		}
		return out, nil
	case "[]uint8":
		var out []byte
		if err := json.Unmarshal(v.Raw, &out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		var out interface{}
		if err := json.Unmarshal(v.Raw, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
}

func typeNameOf(v interface{}) string {
	if v == nil {
		return "nil"
	}
	return reflect.TypeOf(v).String()
}

func toStringFloatMap(v reflect.Value) (map[string]float64, bool) {
	if v.Kind() != reflect.Map || v.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	// Only treat as zset when the value type is a concrete numeric type,
	// not interface{}. This ensures map[string]interface{} with all-float
	// values is recognized as hash instead of zset.
	elemKind := v.Type().Elem().Kind()
	if elemKind == reflect.Interface {
		return nil, false
	}
	m := make(map[string]float64, v.Len())
	iter := v.MapRange()
	for iter.Next() {
		n, ok := toFloat64(iter.Value())
		if !ok {
			return nil, false
		}
		m[iter.Key().String()] = n
	}
	return m, len(m) > 0
}

func toFloat64(v reflect.Value) (float64, bool) {
	for v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return 0, false
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint()), true
	case reflect.Float32, reflect.Float64:
		return v.Float(), true
	default:
		return 0, false
	}
}

func toHashEntries(v reflect.Value) ([]CompositeHashEntry, bool) {
	if v.Kind() != reflect.Map || v.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	entries := make([]CompositeHashEntry, 0, v.Len())
	iter := v.MapRange()
	for iter.Next() {
		encoded, err := encodeAny(iter.Value().Interface())
		if err != nil {
			return nil, false
		}
		entries = append(entries, CompositeHashEntry{Field: iter.Key().String(), Val: encoded})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Field < entries[j].Field })
	return entries, len(entries) > 0
}

func toSetMembers(v reflect.Value) ([]CompositeEncodedValue, bool) {
	if v.Kind() != reflect.Map || v.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	elemType := v.Type().Elem()
	if !(elemType.Kind() == reflect.Struct && elemType.Size() == 0) && elemType.Kind() != reflect.Bool {
		return nil, false
	}

	members := make([]CompositeEncodedValue, 0, v.Len())
	iter := v.MapRange()
	for iter.Next() {
		if elemType.Kind() == reflect.Bool && !iter.Value().Bool() {
			continue
		}
		encoded, err := encodeAny(iter.Key().String())
		if err != nil {
			return nil, false
		}
		members = append(members, encoded)
	}
	sort.Slice(members, func(i, j int) bool {
		return string(members[i].Raw) < string(members[j].Raw)
	})
	return members, len(members) > 0
}

func toListValues(v reflect.Value) ([]CompositeEncodedValue, bool) {
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, false
	}
	if v.Type().Elem().Kind() == reflect.Uint8 {
		return nil, false
	}
	items := make([]CompositeEncodedValue, 0, v.Len())
	for i := 0; i < v.Len(); i++ {
		encoded, err := encodeAny(v.Index(i).Interface())
		if err != nil {
			return nil, false
		}
		items = append(items, encoded)
	}
	return items, len(items) > 0
}

// MarshalComposite serializes to JSON wire format.
func MarshalComposite(key string, value interface{}) (string, error) {
	comp, err := BuildComposite(key, value)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(comp)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// UnmarshalComposite deserializes from JSON and normalizes empty containers.
func UnmarshalComposite(raw string) (Composite, error) {
	var c Composite
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return c, err
	}
	if err := c.validate(); err != nil {
		return c, err
	}

	switch c.Type {
	case CompositeTypeList:
		if c.ListVal == nil {
			c.ListVal = make([]CompositeEncodedValue, 0)
		}
	case CompositeTypeSet:
		if c.SetVal == nil {
			c.SetVal = make([]CompositeEncodedValue, 0)
		}
	case CompositeTypeHash:
		if c.HashVal == nil {
			c.HashVal = make([]CompositeHashEntry, 0)
		}
	case CompositeTypeZSet:
		if c.ZSetVal == nil {
			c.ZSetVal = make(map[string]float64)
		}
	}

	return c, nil
}

// MarshalBinaryComposite serializes to a compact binary (gob) wire format.
func MarshalBinaryComposite(key string, value interface{}) ([]byte, error) {
	comp, err := BuildComposite(key, value)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(comp); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinaryComposite deserializes from the gob binary wire format.
func UnmarshalBinaryComposite(data []byte) (Composite, error) {
	var c Composite
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&c); err != nil {
		return c, err
	}
	if err := c.validate(); err != nil {
		return c, err
	}
	return c, nil
}

// SetComposite writes Composite back to storage using Redis-like commands.
func (c Client) SetComposite(comp Composite) error {
	if err := comp.validate(); err != nil {
		return err
	}

	switch comp.Type {
	case CompositeTypeString:
		if comp.StrVal == nil {
			return fmt.Errorf("SetComposite: string value is nil")
		}
		_, err := c.SET(comp.Key, *comp.StrVal)
		return err

	case CompositeTypeBytes:
		if comp.BytesVal == nil {
			return fmt.Errorf("SetComposite: bytes value is nil")
		}
		b, err := base64.StdEncoding.DecodeString(*comp.BytesVal)
		if err != nil {
			return err
		}
		_, err = c.SET(comp.Key, b)
		return err

	case CompositeTypeInt:
		if comp.IntVal == nil {
			return fmt.Errorf("SetComposite: int value is nil")
		}
		_, err := c.SET(comp.Key, *comp.IntVal)
		return err

	case CompositeTypeList:
		if _, err := c.DEL(comp.Key); err != nil {
			return err
		}
		args := make([]interface{}, 0, len(comp.ListVal))
		for _, item := range comp.ListVal {
			decoded, err := decodeAny(item)
			if err != nil {
				return err
			}
			args = append(args, normalizeRedisScalar(decoded))
		}
		if len(args) == 0 {
			return nil
		}
		_, err := c.RPUSH(comp.Key, args...)
		return err

	case CompositeTypeSet:
		if _, err := c.DEL(comp.Key); err != nil {
			return err
		}
		members := make([]string, 0, len(comp.SetVal))
		for _, item := range comp.SetVal {
			decoded, err := decodeAny(item)
			if err != nil {
				return err
			}
			members = append(members, toRedisString(decoded))
		}
		if len(members) == 0 {
			return nil
		}
		_, err := c.SADD(comp.Key, members...)
		return err

	case CompositeTypeHash:
		if _, err := c.DEL(comp.Key); err != nil {
			return err
		}
		m := make(map[string]interface{}, len(comp.HashVal))
		for _, entry := range comp.HashVal {
			decoded, err := decodeAny(entry.Val)
			if err != nil {
				return err
			}
			m[entry.Field] = normalizeRedisScalar(decoded)
		}
		if len(m) == 0 {
			return nil
		}
		return c.HMSET(comp.Key, m)

	case CompositeTypeZSet:
		if _, err := c.DEL(comp.Key); err != nil {
			return err
		}
		if len(comp.ZSetVal) == 0 {
			return nil
		}
		_, err := c.ZADD(comp.Key, comp.ZSetVal, Flags{})
		return err
	}

	return fmt.Errorf("SetComposite: unknown type %q", comp.Type)
}

func normalizeRedisScalar(v interface{}) interface{} {
	switch x := v.(type) {
	case nil:
		return ""
	case string, bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64, []byte:
		return x
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprintf("%v", x)
		}
		return string(b)
	}
}

func toRedisString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case bool:
		return strconv.FormatBool(x)
	case int:
		return strconv.Itoa(x)
	case int8, int16, int32, int64:
		return fmt.Sprintf("%d", x)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", x)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 64)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprintf("%v", x)
		}
		return string(b)
	}
}
