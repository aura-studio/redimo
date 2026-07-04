package redimo

import (
	"fmt"
	"math/big"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Value allows you to store values of any type supported by DynamoDB, as long as they implement this interface and
// encode themselves into a types.AttributeValue returned by ToAV.
//
// Every Redimo operation that stores data will accept the data as a Value. Some common value wrappers are provided,
// like StringValue, FloatValue, IntValue and BytesValue, allowing you to easily wrap the data you store.
//
// The output of most operations is a ReturnValue which has convenience methods to decode the data into these common types.
// ReturnValue also implements Value so you can call ToAV to access the raw types.AttributeValue, allowing you to
// do custom de-serialization.
//
// If you have a data that does not fit cleanly into one of the provide convenience wrapper types, you can implement the ToAV()
// method on any type to implement custom encoding. When you receive the data wrapped in a ReturnValue, the ToAV method can
// be used to access the raw dynamo.AttributeValue struct, allowing you to do custom decoding.
type Value interface {
	ToAV() types.AttributeValue
}

func ToValueE(data any) (value Value, err error) {
	switch data := data.(type) {
	case ReturnValue:
		value = data
	case StringValue:
		value = data
	case BytesValue:
		value = data
	case IntValue:
		value = data
	case FloatValue:
		value = data
	case string:
		value = StringValue{data}
	case []byte:
		value = BytesValue{data}
	case int:
		value = IntValue{int64(data)}
	case int8:
		value = IntValue{int64(data)}
	case int16:
		value = IntValue{int64(data)}
	case int32:
		value = IntValue{int64(data)}
	case int64:
		value = IntValue{int64(data)}
	case uint:
		value = IntValue{int64(data)}
	case uint8:
		value = IntValue{int64(data)}
	case uint16:
		value = IntValue{int64(data)}
	case uint32:
		value = IntValue{int64(data)}
	case uint64:
		value = IntValue{int64(data)}
	case float32:
		value = FloatValue{float64(data)}
	case float64:
		value = FloatValue{float64(data)}
	default:
		err = fmt.Errorf("ToValue: unsupported type: %T", data)
	}
	return value, err
}

func ToValue(data any) Value {
	value, err := ToValueE(data)
	if err != nil {
		panic(err)
	}
	return value
}

func ToValuesE(data []any) ([]Value, error) {
	values := make([]Value, len(data))
	for i, v := range data {
		value, err := ToValueE(v)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

func ToValues(data []any) []Value {
	values, err := ToValuesE(data)
	if err != nil {
		panic(err)
	}
	return values
}

func ToValueMapE(data any) (map[string]Value, error) {
	var valueMap map[string]Value
	switch data := data.(type) {
	case map[string]any:
		valueMap = make(map[string]Value, len(data))
		for k, v := range data {
			value, err := ToValueE(v)
			if err != nil {
				return nil, err
			}
			valueMap[k] = value
		}
	case map[string]string:
		valueMap = make(map[string]Value, len(data))
		for k, v := range data {
			valueMap[k] = StringValue{v}
		}
	case map[string][]byte:
		valueMap = make(map[string]Value, len(data))
		for k, v := range data {
			valueMap[k] = BytesValue{v}
		}
	case map[string]int64:
		valueMap = make(map[string]Value, len(data))
		for k, v := range data {
			valueMap[k] = IntValue{v}
		}
	case map[string]float64:
		valueMap = make(map[string]Value, len(data))
		for k, v := range data {
			valueMap[k] = FloatValue{v}
		}
	case map[string]Value:
		valueMap = data
	case map[string]ReturnValue:
		valueMap = make(map[string]Value, len(data))
		for k, v := range data {
			valueMap[k] = v
		}
	default:
		return valueMap, fmt.Errorf("ToValueMapE: unsupported type: %T", data)
	}

	return valueMap, nil
}

func ToValueMap(data any) map[string]Value {
	values, err := ToValueMapE(data)
	if err != nil {
		panic(err)
	}
	return values
}

// StringValue is a convenience value wrapper for a string, usable as
//
//	StringValue{"hello"}
type StringValue struct {
	S string
}

func (sv StringValue) ToAV() types.AttributeValue {
	return &types.AttributeValueMemberS{Value: sv.S}
}

// FloatValue is a convenience value wrapper for a float64, usable as
//
//	FloatValue{3.14}
type FloatValue struct {
	F float64
}

func (fv FloatValue) ToAV() types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatFloat(fv.F, 'G', 17, 64)}
}

// IntValue is a convenience value wrapper for an int64, usable as
//
//	IntValue{42}
type IntValue struct {
	I int64
}

func (iv IntValue) ToAV() types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(iv.I, 10)}
}

// BytesValue is a convenience wrapper for a byte slice, usable as
//
//	BytesValue{[]byte{1,2,3}}
type BytesValue struct {
	B []byte
}

func (bv BytesValue) ToAV() types.AttributeValue {
	return &types.AttributeValueMemberB{Value: bv.B}
}

// ReturnValue holds a value returned by DynamoDB. There are convenience methods used to coerce the held value into common types,
// but you can also retrieve the raw types.AttributeValue by calling ToAV if you would like to do custom decoding.
type ReturnValue struct {
	av types.AttributeValue
}

// ToAV returns the underlying types.AttributeValue, allow custom deserialization.
func (rv ReturnValue) ToAV() types.AttributeValue {
	return rv.av
}

// String returns the value as a string. If the value was not stored as a string, a zero-value / empty string
// will the returned. This method will not coerce numeric of byte values.
func (rv ReturnValue) String() string {
	if av, ok := rv.av.(*types.AttributeValueMemberS); ok {
		return av.Value
	}

	return ""
}

// Int returns the value as int64. Will be zero-valued if the value is not actually numeric. The value was originally
// a float, it will be truncated.
func (rv ReturnValue) Int() int64 {
	if av, ok := rv.av.(*types.AttributeValueMemberN); ok {
		if av.Value == "" {
			return 0
		}

		f, _, _ := new(big.Float).Parse(av.Value, 10)
		i, _ := f.Int64()
		return i
	}

	return 0
}

// Float returns the value as float64. Will be zero-valued if the value is not numeric. If the value
// was originally stored as an int, it will be converted to float64 based on parsing the string
// representation, so there is some scope for overflows being corrected silently.
func (rv ReturnValue) Float() float64 {
	if av, ok := rv.av.(*types.AttributeValueMemberN); ok {
		if av.Value == "" {
			return 0
		}

		f, _ := strconv.ParseFloat(av.Value, 64)
		return f
	}

	return 0
}

// Bytes returns the value as a byte slice. Will be nil if the value is not actually a byte slice.
func (rv ReturnValue) Bytes() []byte {
	if av, ok := rv.av.(*types.AttributeValueMemberB); ok {
		return av.Value
	}

	return nil
}

// Empty returns true if the value is empty or uninitialized. This
// indicates that the underlying DynamoDB operation did not return a value.
func (rv ReturnValue) Empty() bool {
	if rv.av == nil {
		return true
	}
	_, ok := rv.av.(*types.AttributeValueMemberNULL)
	return ok
}

// Interface returns the value as an interface{}. This is useful if you are not sure what type
//
// Caveat — sparse semantics: the numeric branches test against zero (Int() != 0 /
// Float() != 0) and the string branch against empty, so a value that is legitimately
// 0, 0.0 or "" is treated as "absent" and the method falls through to the next type
// or nil. Callers that must distinguish a stored 0 / 0.0 / "" from a missing value
// should use the typed accessors (Int/Float/String) together with Present() rather
// than Interface().
func (rv ReturnValue) Interface() any {
	switch {
	case rv.Empty():
		return nil
	case rv.String() != "":
		return rv.String()
	case rv.Bytes() != nil:
		return rv.Bytes()
	case rv.Int() != 0:
		return rv.Int()
	case rv.Float() != 0:
		return rv.Float()
	default:
		return nil
	}
}

// Present returns true if a value is present. It indicates that the underlying
// DynamoDB AttributeValue has a data in any one of its fields. If you already know
// the type of your value, you can call the convenience method (like String() or Int())
// or you can retrieve the underlying types.AttributeValue struct with ToAV and perform
// your down decoding.
func (rv ReturnValue) Present() bool {
	return !rv.Empty()
}

// Equals checks equality by comparing the underlying dynamodb.AttributeValues. If they
// both hold the same value, as indicated by the rules of reflect.DeepEqual, Equals will return true.
func (rv ReturnValue) Equals(ov ReturnValue) bool {
	return reflect.DeepEqual(rv.av, ov.av)
}
