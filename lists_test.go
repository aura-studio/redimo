package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLBasics(t *testing.T) {
	c := newClient(t)

	length, err := c.LPUSH("l1", "twinkle")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), length)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle"}, elements)

	_, err = c.LPUSH("l1", "twinkle")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "twinkle"}, elements)

	_, err = c.RPUSH("l1", "little", "star")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "twinkle", "little", "star"}, elements)

	element, found, err := c.LPOP("l1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "twinkle", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little", "star"}, elements)

	element, found, err = c.RPOP("l1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "star", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little"}, elements)

	count, err := c.LLEN("l1")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	_, err = c.LPUSHX("l1", "wrinkle")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, elements)

	_, err = c.RPUSHX("l1", "car")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little", "car"}, elements)

	elements, err = c.LRANGE("l1", 0, 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, elements)

	elements, err = c.LRANGE("l1", 0, -2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, elements)

	elements, err = c.LRANGE("l1", -3, -2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little"}, elements)

	elements, err = c.LRANGE("l1", -2, -3)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	elements, err = c.LRANGE("l1", 3, 2)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	_, err = c.RPUSHX("nonexistentlist", "car")
	assert.NoError(t, err)

	_, err = c.LPUSHX("nonexistentlist", "car")
	assert.NoError(t, err)

	elements, err = c.LRANGE("nonexistentlist", 0, -1)
	assert.NoError(t, err)
	assert.Empty(t, elements)
}

func TestRPOPLPUSH(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l1", "one", "two", "three", "four")
	assert.NoError(t, err)

	_, err = c.RPUSH("l2", "five", "six", "seven", "eight")
	assert.NoError(t, err)

	element, ok, err := c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "four", element)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "one", "two", "three"}, elements)

	element, ok, err = c.RPOPLPUSH("l1", "l2")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "three", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "one", "two"}, elements)

	elements, err = c.LRANGE("l2", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"three", "five", "six", "seven", "eight"}, elements)

	element, ok, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "two", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two", "four", "one"}, elements)

	element, ok, err = c.RPOPLPUSH("l1", "newList")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "one", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two", "four"}, elements)

	elements, err = c.LRANGE("newList", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"one"}, elements)

	// Two item single list rotation - they should simply switch places
	element, ok, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "four", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "two"}, elements)

	_, _, err = c.LPOP("l1")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two"}, elements)

	// Single element single list rotation is a no-op
	element, ok, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "two", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two"}, elements)
}

func TestListIndexBasedCRUD(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l1", "inty", "minty", "papa", "tinty")
	assert.NoError(t, err)

	element, found, err := c.LINDEX("l1", 0)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "inty", element)

	element, found, err = c.LINDEX("l1", 3)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "tinty", element)

	_, found, err = c.LINDEX("l1", 4)
	assert.NoError(t, err)
	assert.False(t, found)

	_, found, err = c.LINDEX("l1", 42)
	assert.NoError(t, err)
	assert.False(t, found)

	element, found, err = c.LINDEX("l1", -1)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "tinty", element)

	element, found, err = c.LINDEX("l1", -4)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "inty", element)

	_, found, err = c.LINDEX("l1", -42)
	assert.NoError(t, err)
	assert.False(t, found)

	ok, err := c.LSET("l1", 1, "monty")
	assert.NoError(t, err)
	assert.True(t, ok)

	element, found, err = c.LINDEX("l1", 1)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "monty", element)

	ok, err = c.LSET("l1", -2, "mama")
	assert.NoError(t, err)
	assert.True(t, ok)

	element, found, err = c.LINDEX("l1", -2)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "mama", element)

	ok, err = c.LSET("l1", 42, "no chance")
	assert.NoError(t, err)
	assert.False(t, ok)

	count, err := c.LLEN("l1")
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"inty", "monty", "mama", "tinty"}, elements)
}

func TestListValueBasedCRUD(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l1", "beta", "delta", "phi")
	assert.NoError(t, err)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "delta", "phi"}, elements)

	_, ok, err := c.LINSERT("l1", Left, "delta", "gamma")
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "gamma", "delta", "phi"}, elements)

	_, ok, err = c.LINSERT("l1", Left, "beta", "alpha")
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "gamma", "delta", "phi"}, elements)

	_, ok, err = c.LINSERT("l1", Right, "phi", "omega")
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "gamma", "delta", "phi", "omega"}, elements)

	_, ok, err = c.LREM("l1", Left, "gamma")
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "delta", "phi", "omega"}, elements)

	_, ok, err = c.LREM("l1", Left, "omega")
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "delta", "phi"}, elements)

	_, ok, err = c.LREM("l1", Left, "alpha")
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "delta", "phi"}, elements)

	_, err = c.RPUSH("l1", "delta", "gamma", "delta", "mu")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "delta", "phi", "delta", "gamma", "delta", "mu"}, elements)

	_, ok, err = c.LREM("l1", Left, "delta")
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "phi", "delta", "gamma", "delta", "mu"}, elements)

	_, ok, err = c.LREM("l1", Right, "delta")
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "phi", "delta", "gamma", "mu"}, elements)
}