package redimo

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
)

func TestLBasics(t *testing.T) {
	c := newClient(t)

	length, err := c.LPUSH("l1", StringValue{"twinkle"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), length)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle"}, readStrings(elements))

	length, err = c.LPUSH("l1", StringValue{"twinkle"})
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "twinkle"}, readStrings(elements))

	length, err = c.RPUSH("l1", StringValue{"little"}, StringValue{"star"})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "twinkle", "little", "star"}, readStrings(elements))

	element, err := c.LPOP("l1")
	assert.NoError(t, err)
	assert.Equal(t, "twinkle", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little", "star"}, readStrings(elements))

	element, err = c.RPOP("l1")
	assert.NoError(t, err)
	assert.Equal(t, "star", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little"}, readStrings(elements))

	count, err := c.LLEN("l1")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	length, err = c.LPUSHX("l1", StringValue{"wrinkle"})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, readStrings(elements))

	length, err = c.RPUSHX("l1", StringValue{"car"})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little", "car"}, readStrings(elements))

	elements, err = c.LRANGE("l1", 0, 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, readStrings(elements))

	elements, err = c.LRANGE("l1", 0, -2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, readStrings(elements))

	elements, err = c.LRANGE("l1", -3, -2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little"}, readStrings(elements))

	elements, err = c.LRANGE("l1", -2, -3)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	elements, err = c.LRANGE("l1", 3, 2)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	length, err = c.RPUSHX("nonexistentlist", StringValue{"car"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), length)

	length, err = c.LPUSHX("nonexistentlist", StringValue{"car"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), length)

	elements, err = c.LRANGE("nonexistentlist", 0, -1)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	element, err = c.LPOP("nonexistent")
	assert.NoError(t, err)
	assert.True(t, element.Empty())

	element, err = c.RPOP("nonexistent")
	assert.NoError(t, err)
	assert.True(t, element.Empty())
}

func readStrings(elements []ReturnValue) (strs []string) {
	for _, e := range elements {
		strs = append(strs, e.String())
	}

	return
}

func TestRPOPLPUSH(t *testing.T) {
	c := newClient(t)

	length, err := c.RPUSH("l1", StringValue{"one"}, StringValue{"two"}, StringValue{"three"}, StringValue{"four"})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), length)

	length, err = c.RPUSH("l2", StringValue{"five"}, StringValue{"six"}, StringValue{"seven"}, StringValue{"eight"})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), length)

	element, err := c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.Equal(t, "four", element.String())

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "one", "two", "three"}, readStrings(elements))

	element, err = c.RPOPLPUSH("l1", "l2")
	assert.NoError(t, err)
	assert.Equal(t, "three", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "one", "two"}, readStrings(elements))

	elements, err = c.LRANGE("l2", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"three", "five", "six", "seven", "eight"}, readStrings(elements))

	element, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.Equal(t, "two", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two", "four", "one"}, readStrings(elements))

	element, err = c.RPOPLPUSH("l1", "newList")
	assert.NoError(t, err)
	assert.Equal(t, "one", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two", "four"}, readStrings(elements))

	elements, err = c.LRANGE("newList", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"one"}, readStrings(elements))

	// Two item single list rotation - they should simply switch places
	element, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.Equal(t, "four", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "two"}, readStrings(elements))

	_, err = c.LPOP("l1")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two"}, readStrings(elements))

	// Single element single list rotation is a no-op
	element, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.Equal(t, "two", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two"}, readStrings(elements))
}

func TestListIndexBasedCRUD(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l1", StringValue{"inty"}, StringValue{"minty"}, StringValue{"papa"}, StringValue{"tinty"})
	assert.NoError(t, err)

	element, err := c.LINDEX("l1", 0)
	assert.NoError(t, err)
	assert.Equal(t, "inty", element.String())

	element, err = c.LINDEX("l1", 3)
	assert.NoError(t, err)
	assert.Equal(t, "tinty", element.String())

	element, err = c.LINDEX("l1", 4)
	assert.NoError(t, err)
	assert.False(t, element.Present())

	element, err = c.LINDEX("l1", 42)
	assert.NoError(t, err)
	assert.False(t, element.Present())

	element, err = c.LINDEX("l1", -1)
	assert.NoError(t, err)
	assert.True(t, element.Present())
	assert.Equal(t, "tinty", element.String())

	element, err = c.LINDEX("l1", -4)
	assert.NoError(t, err)
	assert.Equal(t, "inty", element.String())

	element, err = c.LINDEX("l1", -42)
	assert.NoError(t, err)
	assert.True(t, element.Empty())

	ok, err := c.LSET("l1", 1, "monty")
	assert.NoError(t, err)
	assert.True(t, ok)

	element, err = c.LINDEX("l1", 1)
	assert.NoError(t, err)
	assert.Equal(t, "monty", element.String())

	ok, err = c.LSET("l1", -2, "mama")
	assert.NoError(t, err)
	assert.True(t, ok)

	element, err = c.LINDEX("l1", -2)
	assert.NoError(t, err)
	assert.Equal(t, "mama", element.String())

	ok, err = c.LSET("l1", 42, "no chance")
	assert.NoError(t, err)
	assert.False(t, ok)

	count, err := c.LLEN("l1")
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"inty", "monty", "mama", "tinty"}, readStrings(elements))
}

func TestListValueBasedCRUD(t *testing.T) {
	c := newClient(t)

	length, err := c.RPUSH("l1", StringValue{"delta"}, StringValue{"beta"}, StringValue{"beta"}, StringValue{"delta"}, StringValue{"phi"})
	assert.NoError(t, err)
	assert.Equal(t, int64(5), length)

	length, ok, err := c.LREM("l1", 0, StringValue{"beta"})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(3), length)

	c.LREM("l1", 0, StringValue{"delta"})
	c.LPUSH("l1", StringValue{"delta"})
	c.LPUSH("l1", StringValue{"beta"})
	c.LPUSH("l1", StringValue{"alpha"})
	c.RPUSH("l1", StringValue{"omega"})

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "delta", "phi", "omega"}, readStrings(elements))

	length, ok, err = c.LREM("l1", 0, StringValue{"omega"})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(4), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "delta", "phi"}, readStrings(elements))

	length, ok, err = c.LREM("l1", 0, StringValue{"alpha"})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(3), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "delta", "phi"}, readStrings(elements))

	length, err = c.RPUSH("l1", StringValue{"delta"}, StringValue{"gamma"}, StringValue{"delta"}, StringValue{"mu"})
	assert.NoError(t, err)
	assert.Equal(t, int64(7), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "delta", "phi", "delta", "gamma", "delta", "mu"}, readStrings(elements))

	length, ok, err = c.LREM("l1", 1, StringValue{"delta"})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(6), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "phi", "delta", "gamma", "delta", "mu"}, readStrings(elements))

	length, ok, err = c.LREM("l1", -1, StringValue{"delta"})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(5), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "phi", "delta", "gamma", "mu"}, readStrings(elements))

	_, ok, err = c.LREM("l1", 1, StringValue{"no such element"})
	assert.NoError(t, err)
	assert.False(t, ok)
}

// Redis LTRIM semantics: an empty range (start > stop, or start past the end)
// empties the list; a valid range keeps exactly that slice. v1.6.1 returned
// early on the empty range and left the list untouched.
func TestLTRIM(t *testing.T) {
	c := newClient(t)

	seed := func(key string, values ...string) {
		elements := make([]interface{}, len(values))
		for i, v := range values {
			elements[i] = StringValue{v}
		}

		length, err := c.RPUSH(key, elements...)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(values)), length)
	}

	t.Run("start greater than stop empties the list", func(t *testing.T) {
		seed("ltrim1", "e0", "e1", "e2", "e3")

		length, err := c.LTRIM("ltrim1", 3, 1)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), length)

		elements, err := c.LRANGE("ltrim1", 0, -1)
		assert.NoError(t, err)
		assert.Empty(t, elements)

		exists, err := c.EXISTS("ltrim1")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("start beyond the end empties the list", func(t *testing.T) {
		seed("ltrim2", "e0", "e1", "e2", "e3")

		length, err := c.LTRIM("ltrim2", 100, 200)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), length)

		elements, err := c.LRANGE("ltrim2", 0, -1)
		assert.NoError(t, err)
		assert.Empty(t, elements)
	})

	t.Run("negative indices forming an empty range empty the list", func(t *testing.T) {
		seed("ltrim3", "e0", "e1", "e2", "e3", "e4")

		length, err := c.LTRIM("ltrim3", -1, -2)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), length)

		elements, err := c.LRANGE("ltrim3", 0, -1)
		assert.NoError(t, err)
		assert.Empty(t, elements)
	})

	t.Run("0 -1 keeps the whole list", func(t *testing.T) {
		seed("ltrim4", "e0", "e1", "e2", "e3")

		length, err := c.LTRIM("ltrim4", 0, -1)
		assert.NoError(t, err)
		assert.Equal(t, int64(4), length)

		elements, err := c.LRANGE("ltrim4", 0, -1)
		assert.NoError(t, err)
		assert.Equal(t, []string{"e0", "e1", "e2", "e3"}, readStrings(elements))
	})

	t.Run("0 0 keeps only the first element", func(t *testing.T) {
		seed("ltrim5", "e0", "e1", "e2", "e3")

		length, err := c.LTRIM("ltrim5", 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), length)

		elements, err := c.LRANGE("ltrim5", 0, -1)
		assert.NoError(t, err)
		assert.Equal(t, []string{"e0"}, readStrings(elements))
	})

	t.Run("missing key is a no-op", func(t *testing.T) {
		length, err := c.LTRIM("ltrim6", 0, 1)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), length)
	})

	t.Run("an emptied list accepts new pushes without index collision", func(t *testing.T) {
		seed("ltrim7", "e0", "e1", "e2")

		length, err := c.LTRIM("ltrim7", 5, 1)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), length)

		length, err = c.RPUSH("ltrim7", StringValue{"fresh"})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), length)

		elements, err := c.LRANGE("ltrim7", 0, -1)
		assert.NoError(t, err)
		assert.Equal(t, []string{"fresh"}, readStrings(elements))
	})
}

// A string element must encode to itself, so items written by earlier releases
// keep the sort key sha256(s)|index they were stored under and stay findable.
func TestListElementSkEncoding(t *testing.T) {
	t.Parallel()

	sk, err := listElementSk(StringValue{"twinkle"})
	assert.NoError(t, err)
	assert.Equal(t, "twinkle", sk)

	// Redis treats every value as a byte string, so these share one encoding.
	sk, err = listElementSk(IntValue{2})
	assert.NoError(t, err)
	assert.Equal(t, "2", sk)

	sk, err = listElementSk(BytesValue{[]byte("raw")})
	assert.NoError(t, err)
	assert.Equal(t, "raw", sk)

	// No defined string form: rejected rather than stored unfindably.
	_, err = listElementSk(ReturnValue{})
	assert.Error(t, err)
}

// LPUSH/RPUSH accept any Value, not just StringValue. Pushing an IntValue used to
// panic on a hard type assertion in lPush.
func TestListNonStringElements(t *testing.T) {
	c := newClient(t)

	length, err := c.RPUSH("l1", StringValue{"a"}, IntValue{2}, FloatValue{3.5}, BytesValue{[]byte{1, 2, 3}})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), length)

	// Each element round-trips with the type it was pushed as.
	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Len(t, elements, 4)
	assert.Equal(t, "a", elements[0].String())
	assert.Equal(t, int64(2), elements[1].Int())
	assert.Equal(t, 3.5, elements[2].Float())
	assert.Equal(t, []byte{1, 2, 3}, elements[3].Bytes())

	element, err := c.LINDEX("l1", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), element.Int())

	// LREM locates a non-string element through the same encoding.
	length, ok, err := c.LREM("l1", 0, IntValue{2})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(3), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Len(t, elements, 3)
	assert.Equal(t, 3.5, elements[1].Float())
}

// The sort key is a byte-string encoding, so a number and its decimal string are
// the same element to LREM even though each push stores its own typed value.
func TestListNumericAndStringElementsMatch(t *testing.T) {
	c := newClient(t)

	length, err := c.RPUSH("l1", IntValue{2}, StringValue{"2"}, StringValue{"keep"})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	length, ok, err := c.LREM("l1", 0, StringValue{"2"})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(1), length)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"keep"}, readStrings(elements))
}

// An element with no defined string form fails the whole call, leaving no partial
// prefix behind.
func TestListRejectsUnsupportedElement(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l1", StringValue{"first"}, ReturnValue{})
	assert.Error(t, err)

	count, err := c.LLEN("l1")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

// RPOPLPUSH moved the element as StringValue{element.String()}, which is empty for
// a non-string element.
func TestRPOPLPUSHPreservesElementType(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l1", IntValue{7})
	assert.NoError(t, err)

	element, err := c.RPOPLPUSH("l1", "l2")
	assert.NoError(t, err)
	assert.Equal(t, int64(7), element.Int())

	elements, err := c.LRANGE("l2", 0, -1)
	assert.NoError(t, err)
	assert.Len(t, elements, 1)
	assert.Equal(t, int64(7), elements[0].Int())
}

// A ReturnValue wrapping a BOOL attribute round-trips through the list: the
// encoding is "true"/"false" (strconv.FormatBool), the stored val keeps the
// BOOL attribute, and LREM finds the element through the same encoding.
func TestListBoolElementRoundTrip(t *testing.T) {
	c := newClient(t)

	boolAV := func(b bool) ReturnValue {
		return ReturnValue{av: &types.AttributeValueMemberBOOL{Value: b}}
	}

	// Encoding unit check: BOOL contributes its strconv.FormatBool form.
	sk, err := listElementSk(boolAV(true))
	assert.NoError(t, err)
	assert.Equal(t, "true", sk)

	sk, err = listElementSk(boolAV(false))
	assert.NoError(t, err)
	assert.Equal(t, "false", sk)

	// Round-trip: the element keeps its BOOL attribute.
	length, err := c.RPUSH("lbool", StringValue{"a"}, boolAV(true))
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	elements, err := c.LRANGE("lbool", 0, -1)
	assert.NoError(t, err)
	assert.Len(t, elements, 2)

	av, ok := elements[1].ToAV().(*types.AttributeValueMemberBOOL)
	assert.True(t, ok)
	assert.True(t, av.Value)

	// LREM locates the BOOL element through the same encoding.
	length, removed, err := c.LREM("lbool", 0, boolAV(true))
	assert.NoError(t, err)
	assert.True(t, removed)
	assert.Equal(t, int64(1), length)

	elements, err = c.LRANGE("lbool", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, readStrings(elements))
}
