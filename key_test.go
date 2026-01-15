package redimo

import (
	"fmt"
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
}

// 测试DEL不存在的key
func TestDELNonExistent(t *testing.T) {
	c := newClient(t)

	deletedFields, err := c.DEL("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(deletedFields))
}

// 测试DEL单个字段
func TestDELSingleField(t *testing.T) {
	c := newClient(t)

	_, err := c.HSET("k1", map[string]Value{"f1": StringValue{"v1"}})
	assert.NoError(t, err)

	deletedFields, err := c.DEL("k1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(deletedFields))
	assert.Equal(t, deletedFields[0], "f1")

	exists, err := c.EXISTS("k1")
	assert.NoError(t, err)
	assert.False(t, exists)
}

// 测试DEL大量字段（测试批处理，>25项）
func TestDELBulkFields(t *testing.T) {
	c := newClient(t)

	// 创建50个字段
	fields := make(map[string]Value)
	for i := 0; i < 50; i++ {
		fields[fmt.Sprintf("f%d", i)] = StringValue{fmt.Sprintf("v%d", i)}
	}

	savedCount, err := c.HSET("k_bulk", fields)
	assert.NoError(t, err)
	assert.Equal(t, 50, len(savedCount))

	// 验证字段都存在
	exists, err := c.EXISTS("k_bulk")
	assert.NoError(t, err)
	assert.True(t, exists)

	// 删除所有字段（会触发批处理）
	deletedFields, err := c.DEL("k_bulk")
	assert.NoError(t, err)
	assert.Equal(t, 50, len(deletedFields))

	// 验证所有字段已删除
	exists, err = c.EXISTS("k_bulk")
	assert.NoError(t, err)
	assert.False(t, exists)
}

// 测试DEL多个key
func TestDELMultipleKeys(t *testing.T) {
	c := newClient(t)

	// 设置3个key
	c.HSET("k1", map[string]Value{"f1": StringValue{"v1"}})
	c.HSET("k2", map[string]Value{"f2": StringValue{"v2"}, "f3": StringValue{"v3"}})
	c.HSET("k3", map[string]Value{"f4": StringValue{"v4"}})

	// 验证都存在
	exists1, _ := c.EXISTS("k1")
	exists2, _ := c.EXISTS("k2")
	exists3, _ := c.EXISTS("k3")
	assert.True(t, exists1)
	assert.True(t, exists2)
	assert.True(t, exists3)

	// 依次删除
	del1, err := c.DEL("k1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(del1))

	del2, err := c.DEL("k2")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(del2))

	del3, err := c.DEL("k3")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(del3))

	// 验证都不存在
	exists1, _ = c.EXISTS("k1")
	exists2, _ = c.EXISTS("k2")
	exists3, _ = c.EXISTS("k3")
	assert.False(t, exists1)
	assert.False(t, exists2)
	assert.False(t, exists3)
}

// 测试DEL字符串值
func TestDELStringValue(t *testing.T) {
	c := newClient(t)

	_, err := c.SET("str_key", StringValue{"hello"})
	assert.NoError(t, err)

	exists, err := c.EXISTS("str_key")
	assert.NoError(t, err)
	assert.True(t, exists)

	deletedFields, err := c.DEL("str_key")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(deletedFields))

	exists, err = c.EXISTS("str_key")
	assert.NoError(t, err)
	assert.False(t, exists)
}

// 测试重复DEL同一个key
func TestDELRepeatDelete(t *testing.T) {
	c := newClient(t)

	_, err := c.HSET("k1", map[string]Value{"f1": StringValue{"v1"}})
	assert.NoError(t, err)

	// 第一次删除
	deleted1, err := c.DEL("k1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(deleted1))

	// 第二次删除（key已不存在）
	deleted2, err := c.DEL("k1")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(deleted2))

	// 第三次删除
	deleted3, err := c.DEL("k1")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(deleted3))
}

// 测试DEL100个字段（触发多批次批处理）
func TestDEL100Fields(t *testing.T) {
	c := newClient(t)

	fields := make(map[string]Value)
	for i := 0; i < 100; i++ {
		fields[fmt.Sprintf("field_%d", i)] = StringValue{fmt.Sprintf("value_%d", i)}
	}

	savedCount, err := c.HSET("k_100", fields)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(savedCount))

	// 验证key存在
	exists, err := c.EXISTS("k_100")
	assert.NoError(t, err)
	assert.True(t, exists)

	// 删除所有（会分4批：25+25+25+25）
	deletedFields, err := c.DEL("k_100")
	assert.NoError(t, err)
	assert.Equal(t, 100, len(deletedFields))

	// 验证都删除了
	exists, err = c.EXISTS("k_100")
	assert.NoError(t, err)
	assert.False(t, exists)
}

// 测试DEL验证返回的字段名正确
func TestDELReturnedFieldNames(t *testing.T) {
	c := newClient(t)

	fieldMap := map[string]Value{
		"name":  StringValue{"Alice"},
		"age":   StringValue{"30"},
		"email": StringValue{"alice@example.com"},
	}

	_, err := c.HSET("user", fieldMap)
	assert.NoError(t, err)

	deletedFields, err := c.DEL("user")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(deletedFields))
	assert.ElementsMatch(t, []string{"name", "age", "email"}, deletedFields)
}

// 测试DEL混合类型字段值
func TestDELMixedFieldTypes(t *testing.T) {
	c := newClient(t)

	fieldMap := map[string]Value{
		"str":   StringValue{"text"},
		"num":   IntValue{42},
		"float": FloatValue{3.14},
		"bytes": BytesValue{[]byte("binary")},
	}

	_, err := c.HSET("mixed", fieldMap)
	assert.NoError(t, err)

	deletedFields, err := c.DEL("mixed")
	assert.NoError(t, err)
	assert.Equal(t, 4, len(deletedFields))
	assert.ElementsMatch(t, []string{"str", "num", "float", "bytes"}, deletedFields)
}

// 测试DEL字段名顺序
func TestDELFieldOrder(t *testing.T) {
	c := newClient(t)

	// 按特定顺序设置字段
	c.HSET("k", map[string]Value{"z": StringValue{"1"}})
	c.HSET("k", map[string]Value{"a": StringValue{"2"}})
	c.HSET("k", map[string]Value{"m": StringValue{"3"}})

	deletedFields, err := c.DEL("k")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(deletedFields))
	// 验证包含所有字段
	assert.ElementsMatch(t, []string{"z", "a", "m"}, deletedFields)
}
