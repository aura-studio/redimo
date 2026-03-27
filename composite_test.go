package redimo

import (
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeRoundTrip_CommonTypes(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		input   interface{}
		typ     string
		assertC func(t *testing.T, c Composite)
	}{
		{
			name:  "string",
			key:   "k:string",
			input: "hello",
			typ:   CompositeTypeString,
			assertC: func(t *testing.T, c Composite) {
				require.NotNil(t, c.StrVal)
				assert.Equal(t, "hello", *c.StrVal)
			},
		},
		{
			name:  "bytes",
			key:   "k:bytes",
			input: []byte{1, 2, 3, 4},
			typ:   CompositeTypeBytes,
			assertC: func(t *testing.T, c Composite) {
				require.NotNil(t, c.BytesVal)
				assert.Equal(t, base64.StdEncoding.EncodeToString([]byte{1, 2, 3, 4}), *c.BytesVal)
			},
		},
		{
			name:  "int",
			key:   "k:int",
			input: int64(42),
			typ:   CompositeTypeInt,
			assertC: func(t *testing.T, c Composite) {
				require.NotNil(t, c.IntVal)
				assert.Equal(t, int64(42), *c.IntVal)
			},
		},
		{
			name:  "list",
			key:   "k:list",
			input: []interface{}{"x", 2, true},
			typ:   CompositeTypeList,
			assertC: func(t *testing.T, c Composite) {
				require.Len(t, c.ListVal, 3)
				v0, err := decodeAny(c.ListVal[0])
				require.NoError(t, err)
				v1, err := decodeAny(c.ListVal[1])
				require.NoError(t, err)
				v2, err := decodeAny(c.ListVal[2])
				require.NoError(t, err)
				assert.Equal(t, "x", v0)
				assert.Equal(t, int64(2), v1)
				assert.Equal(t, true, v2)
			},
		},
		{
			name:  "set",
			key:   "k:set",
			input: map[string]struct{}{"a": {}, "b": {}},
			typ:   CompositeTypeSet,
			assertC: func(t *testing.T, c Composite) {
				require.Len(t, c.SetVal, 2)
				got := map[string]bool{}
				for _, m := range c.SetVal {
					v, err := decodeAny(m)
					require.NoError(t, err)
					got[v.(string)] = true
				}
				assert.True(t, got["a"])
				assert.True(t, got["b"])
			},
		},
		{
			name:  "hash",
			key:   "k:hash",
			input: map[string]interface{}{"n": 7, "s": "v"},
			typ:   CompositeTypeHash,
			assertC: func(t *testing.T, c Composite) {
				require.Len(t, c.HashVal, 2)
				got := map[string]interface{}{}
				for _, e := range c.HashVal {
					v, err := decodeAny(e.Val)
					require.NoError(t, err)
					got[e.Field] = v
				}
				assert.Equal(t, int64(7), got["n"])
				assert.Equal(t, "v", got["s"])
			},
		},
		{
			name:  "zset",
			key:   "k:zset",
			input: map[string]float64{"m1": 1.5, "m2": 2},
			typ:   CompositeTypeZSet,
			assertC: func(t *testing.T, c Composite) {
				require.Len(t, c.ZSetVal, 2)
				assert.InDelta(t, 1.5, c.ZSetVal["m1"], 0.0001)
				assert.InDelta(t, 2.0, c.ZSetVal["m2"], 0.0001)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := MarshalComposite(tt.key, tt.input)
			require.NoError(t, err)

			c, err := UnmarshalComposite(raw)
			require.NoError(t, err)
			assert.Equal(t, tt.key, c.Key)
			assert.Equal(t, tt.typ, c.Type)
			tt.assertC(t, c)
		})
	}
}

func TestBuildComposite_FromReturnValue(t *testing.T) {
	tests := []struct {
		name  string
		rv    ReturnValue
		type_ string
	}{
		{
			name:  "rv string",
			rv:    ReturnValue{av: &types.AttributeValueMemberS{Value: "abc"}},
			type_: CompositeTypeString,
		},
		{
			name:  "rv number",
			rv:    ReturnValue{av: &types.AttributeValueMemberN{Value: "123"}},
			type_: CompositeTypeInt,
		},
		{
			name: "rv list",
			rv: ReturnValue{av: &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "x"},
				&types.AttributeValueMemberN{Value: "2"},
			}}},
			type_: CompositeTypeList,
		},
		{
			name:  "rv string set",
			rv:    ReturnValue{av: &types.AttributeValueMemberSS{Value: []string{"a", "b"}}},
			type_: CompositeTypeSet,
		},
		{
			name: "rv map",
			rv: ReturnValue{av: &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"a": &types.AttributeValueMemberS{Value: "x"},
				"b": &types.AttributeValueMemberN{Value: "9"},
			}}},
			type_: CompositeTypeHash,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := BuildComposite("rv:key", tt.rv)
			require.NoError(t, err)
			assert.Equal(t, tt.type_, c.Type)
		})
	}
}

func TestCompositeRoundTrip_WithDynamoDB(t *testing.T) {
	c := newClient(t)

	t.Run("string two-keys isolation", func(t *testing.T) {
		keyA := "comp:string:a"
		keyB := "comp:string:b"

		compA, err := BuildComposite(keyA, "hello-a")
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compA))
		rvA1, err := c.GET(keyA)
		require.NoError(t, err)
		assert.Equal(t, "hello-a", rvA1.String())

		compB, err := BuildComposite(keyB, rvA1)
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compB))
		rvB1, err := c.GET(keyB)
		require.NoError(t, err)
		assert.Equal(t, "hello-a", rvB1.String())

		rvA2, err := c.GET(keyA)
		require.NoError(t, err)
		rvB2, err := c.GET(keyB)
		require.NoError(t, err)
		assert.Equal(t, "hello-a", rvA2.String())
		assert.Equal(t, "hello-a", rvB2.String())
	})

	t.Run("bytes two-keys isolation", func(t *testing.T) {
		keyA := "comp:bytes:a"
		keyB := "comp:bytes:b"

		compA, err := BuildComposite(keyA, []byte{1, 2, 3})
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compA))
		rvA1, err := c.GET(keyA)
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3}, rvA1.Bytes())

		compB, err := BuildComposite(keyB, rvA1)
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compB))
		rvB1, err := c.GET(keyB)
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3}, rvB1.Bytes())

		rvA2, err := c.GET(keyA)
		require.NoError(t, err)
		rvB2, err := c.GET(keyB)
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3}, rvA2.Bytes())
		assert.Equal(t, []byte{1, 2, 3}, rvB2.Bytes())
	})

	t.Run("int two-keys isolation", func(t *testing.T) {
		keyA := "comp:int:a"
		keyB := "comp:int:b"

		compA, err := BuildComposite(keyA, int64(123))
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compA))
		rvA1, err := c.GET(keyA)
		require.NoError(t, err)
		assert.Equal(t, int64(123), rvA1.Int())

		compB, err := BuildComposite(keyB, rvA1)
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compB))
		rvB1, err := c.GET(keyB)
		require.NoError(t, err)
		assert.Equal(t, int64(123), rvB1.Int())

		rvA2, err := c.GET(keyA)
		require.NoError(t, err)
		rvB2, err := c.GET(keyB)
		require.NoError(t, err)
		assert.Equal(t, int64(123), rvA2.Int())
		assert.Equal(t, int64(123), rvB2.Int())
	})

	t.Run("list two-keys isolation", func(t *testing.T) {
		keyA := "comp:list:a"
		keyB := "comp:list:b"

		compA, err := BuildComposite(keyA, []interface{}{"a", int64(2), float64(3.5)})
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compA))
		rvA1, err := c.LRANGE(keyA, 0, -1)
		require.NoError(t, err)
		require.Len(t, rvA1, 3)
		assert.Equal(t, "a", rvA1[0].String())
		assert.Equal(t, int64(2), rvA1[1].Int())

		compB, err := BuildComposite(keyB, rvA1)
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compB))
		listB1, err := c.LRANGE(keyB, 0, -1)
		require.NoError(t, err)
		require.Len(t, listB1, 3)
		assert.Equal(t, "a", listB1[0].String())
		assert.Equal(t, int64(2), listB1[1].Int())

		listA2, err := c.LRANGE(keyA, 0, -1)
		require.NoError(t, err)
		listB2, err := c.LRANGE(keyB, 0, -1)
		require.NoError(t, err)
		assert.Equal(t, "a", listA2[0].String())
		assert.Equal(t, "a", listB2[0].String())
	})

	t.Run("set two-keys isolation", func(t *testing.T) {
		keyA := "comp:set:a"
		keyB := "comp:set:b"

		compA, err := BuildComposite(keyA, map[string]struct{}{"x": {}, "y": {}})
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compA))
		setMembersA, err := c.SMEMBERS(keyA)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"x", "y"}, setMembersA)
		rvA1 := make(map[string]struct{}, len(setMembersA))
		for _, m := range setMembersA {
			rvA1[m] = struct{}{}
		}

		compB, err := BuildComposite(keyB, rvA1)
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compB))
		setB1, err := c.SMEMBERS(keyB)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"x", "y"}, setB1)

		setA2, err := c.SMEMBERS(keyA)
		require.NoError(t, err)
		setB2, err := c.SMEMBERS(keyB)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"x", "y"}, setA2)
		assert.ElementsMatch(t, []string{"x", "y"}, setB2)
	})

	t.Run("hash two-keys isolation", func(t *testing.T) {
		keyA := "comp:hash:a"
		keyB := "comp:hash:b"

		compA, err := BuildComposite(keyA, map[string]interface{}{"n": int64(7), "s": "v"})
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compA))
		rvA1, err := c.HGETALL(keyA)
		require.NoError(t, err)
		assert.Equal(t, int64(7), rvA1["n"].Int())
		assert.Equal(t, "v", rvA1["s"].String())

		compB, err := BuildComposite(keyB, rvA1)
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compB))
		hashB1, err := c.HGETALL(keyB)
		require.NoError(t, err)
		assert.Equal(t, int64(7), hashB1["n"].Int())
		assert.Equal(t, "v", hashB1["s"].String())

		hashA2, err := c.HGETALL(keyA)
		require.NoError(t, err)
		hashB2, err := c.HGETALL(keyB)
		require.NoError(t, err)
		assert.Equal(t, int64(7), hashA2["n"].Int())
		assert.Equal(t, int64(7), hashB2["n"].Int())
	})

	t.Run("zset two-keys isolation", func(t *testing.T) {
		keyA := "comp:zset:a"
		keyB := "comp:zset:b"

		compA, err := BuildComposite(keyA, map[string]float64{"m1": 1.1, "m2": 2.2})
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compA))
		rvA1, err := c.ZRANGE(keyA, 0, -1)
		require.NoError(t, err)
		assert.InDelta(t, 1.1, rvA1["m1"], 0.0001)
		assert.InDelta(t, 2.2, rvA1["m2"], 0.0001)

		compB, err := BuildComposite(keyB, rvA1)
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(compB))
		zsetB1, err := c.ZRANGE(keyB, 0, -1)
		require.NoError(t, err)
		assert.InDelta(t, 1.1, zsetB1["m1"], 0.0001)
		assert.InDelta(t, 2.2, zsetB1["m2"], 0.0001)

		zsetA2, err := c.ZRANGE(keyA, 0, -1)
		require.NoError(t, err)
		zsetB2, err := c.ZRANGE(keyB, 0, -1)
		require.NoError(t, err)
		assert.InDelta(t, 1.1, zsetA2["m1"], 0.0001)
		assert.InDelta(t, 1.1, zsetB2["m1"], 0.0001)
	})

	t.Run("list binary element", func(t *testing.T) {
		key := "comp:list:binary"
		binaryData := []byte{0xDE, 0xAD, 0xBE, 0xEF}

		comp, err := BuildComposite(key, []interface{}{"before", binaryData, int64(99)})
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(comp))

		items, err := c.LRANGE(key, 0, -1)
		require.NoError(t, err)
		require.Len(t, items, 3)
		assert.Equal(t, "before", items[0].String())
		assert.Equal(t, binaryData, items[1].Bytes())
		assert.Equal(t, int64(99), items[2].Int())
	})

	t.Run("set binary-safe members", func(t *testing.T) {
		key := "comp:set:binary"
		// set 成员只支持字符串，用 base64 编码在字符串成员中安全保存二进制数据
		bin1 := base64.StdEncoding.EncodeToString([]byte{0xDE, 0xAD})
		bin2 := base64.StdEncoding.EncodeToString([]byte{0xBE, 0xEF})

		comp, err := BuildComposite(key, map[string]struct{}{bin1: {}, bin2: {}})
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(comp))

		members, err := c.SMEMBERS(key)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{bin1, bin2}, members)

		// 验证每个成员都能解码回原始二进制
		for _, m := range members {
			decoded, decErr := base64.StdEncoding.DecodeString(m)
			require.NoError(t, decErr)
			assert.Len(t, decoded, 2)
		}
	})

	t.Run("hash binary field value", func(t *testing.T) {
		key := "comp:hash:binary"
		binaryData := []byte{0xDE, 0xAD, 0xBE, 0xEF}

		comp, err := BuildComposite(key, map[string]interface{}{"bin": binaryData, "str": "text", "num": int64(42)})
		require.NoError(t, err)
		require.NoError(t, c.SetComposite(comp))

		fields, err := c.HGETALL(key)
		require.NoError(t, err)
		require.Len(t, fields, 3)
		assert.Equal(t, binaryData, fields["bin"].Bytes())
		assert.Equal(t, "text", fields["str"].String())
		assert.Equal(t, int64(42), fields["num"].Int())
	})
}

func TestCompositeRoundTrip_BinaryFormat(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		input   interface{}
		typ     string
		assertC func(t *testing.T, c Composite)
	}{
		{
			name:  "string",
			key:   "bin:string",
			input: "hello-bin",
			typ:   CompositeTypeString,
			assertC: func(t *testing.T, c Composite) {
				require.NotNil(t, c.StrVal)
				assert.Equal(t, "hello-bin", *c.StrVal)
			},
		},
		{
			name:  "bytes",
			key:   "bin:bytes",
			input: []byte{0xFF, 0x00, 0xAB, 0x0D},
			typ:   CompositeTypeBytes,
			assertC: func(t *testing.T, c Composite) {
				require.NotNil(t, c.BytesVal)
				assert.Equal(t, base64.StdEncoding.EncodeToString([]byte{0xFF, 0x00, 0xAB, 0x0D}), *c.BytesVal)
			},
		},
		{
			name:  "int",
			key:   "bin:int",
			input: int64(9876543210),
			typ:   CompositeTypeInt,
			assertC: func(t *testing.T, c Composite) {
				require.NotNil(t, c.IntVal)
				assert.Equal(t, int64(9876543210), *c.IntVal)
			},
		},
		{
			name:  "list with binary member",
			key:   "bin:list",
			input: []interface{}{"text", []byte{0x01, 0x02, 0x03}, int64(42)},
			typ:   CompositeTypeList,
			assertC: func(t *testing.T, c Composite) {
				require.Len(t, c.ListVal, 3)
				v0, err := decodeAny(c.ListVal[0])
				require.NoError(t, err)
				assert.Equal(t, "text", v0)
				v1, err := decodeAny(c.ListVal[1])
				require.NoError(t, err)
				assert.Equal(t, []byte{0x01, 0x02, 0x03}, v1)
				v2, err := decodeAny(c.ListVal[2])
				require.NoError(t, err)
				assert.Equal(t, int64(42), v2)
			},
		},
		{
			name:  "set",
			key:   "bin:set",
			input: map[string]struct{}{"alpha": {}, "beta": {}, "gamma": {}},
			typ:   CompositeTypeSet,
			assertC: func(t *testing.T, c Composite) {
				require.Len(t, c.SetVal, 3)
				got := map[string]bool{}
				for _, m := range c.SetVal {
					v, err := decodeAny(m)
					require.NoError(t, err)
					got[v.(string)] = true
				}
				assert.True(t, got["alpha"])
				assert.True(t, got["beta"])
				assert.True(t, got["gamma"])
			},
		},
		{
			name:  "hash with binary field",
			key:   "bin:hash",
			input: map[string]interface{}{"raw": []byte{0xDE, 0xAD}, "num": int64(255), "str": "ok"},
			typ:   CompositeTypeHash,
			assertC: func(t *testing.T, c Composite) {
				require.Len(t, c.HashVal, 3)
				got := map[string]interface{}{}
				for _, e := range c.HashVal {
					v, err := decodeAny(e.Val)
					require.NoError(t, err)
					got[e.Field] = v
				}
				assert.Equal(t, []byte{0xDE, 0xAD}, got["raw"])
				assert.Equal(t, int64(255), got["num"])
				assert.Equal(t, "ok", got["str"])
			},
		},
		{
			name:  "zset",
			key:   "bin:zset",
			input: map[string]float64{"a": 0.1, "b": 99.9},
			typ:   CompositeTypeZSet,
			assertC: func(t *testing.T, c Composite) {
				require.Len(t, c.ZSetVal, 2)
				assert.InDelta(t, 0.1, c.ZSetVal["a"], 0.0001)
				assert.InDelta(t, 99.9, c.ZSetVal["b"], 0.0001)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := MarshalBinaryComposite(tt.key, tt.input)
			require.NoError(t, err)
			assert.NotEmpty(t, data)

			c, err := UnmarshalBinaryComposite(data)
			require.NoError(t, err)
			assert.Equal(t, tt.key, c.Key)
			assert.Equal(t, tt.typ, c.Type)
			tt.assertC(t, c)
		})
	}
}
