package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicKey(t *testing.T) {
	c := newClient(t)

	val, err := c.GET("hello")
	assert.NoError(t, err)
	assert.False(t, val.Present())

	savedFields, err := c.HSET("k1", map[string]Value{"f1": StringValue{"v1"}, "f2": StringValue{"v2"}})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(savedFields))
	assert.Equal(t, savedFields["f2"], StringValue{"v2"})

	exists, err := c.EXISTS("k1")
	assert.NoError(t, err)
	assert.True(t, exists)

	deletedFields, err := c.DEL("k1")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(deletedFields))
	assert.ElementsMatch(t, []string{"f1", "f2"}, deletedFields)

	exists, err = c.EXISTS("k1")
	assert.NoError(t, err)
	assert.False(t, exists)

	// for i := 0; i < 100; i++ {
	// 	_, err := c.HSET("a", map[string]Value{fmt.Sprintf("b%d", i): StringValue{"c"}})
	// 	assert.NoError(t, err)
	// }

	// keys, err := c.KEYS("a", "b1")
	// assert.NoError(t, err)
	// assert.True(t, len(keys) == 11)
}
