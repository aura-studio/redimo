package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicHashes(t *testing.T) {
	c := newClient(t)
	savedCount, err := c.HSET("k1", map[string]Value{"f1": StringValue{"v1"}, "f2": StringValue{"v2"}})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, savedCount)
	val, err := c.HGET("k1", "f1")
	assert.NoError(t, err)
	assert.Equal(t, StringValue{"v1"}, val)

	val, err = c.HGET("k1", "f2")
	assert.NoError(t, err)
	assert.Equal(t, StringValue{"v2"}, val)

	val, err = c.HGET("nosuchkey", "no such field")
	assert.NoError(t, err)
	assert.Nil(t, val)
}
