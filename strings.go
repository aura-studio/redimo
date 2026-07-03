package redimo

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// GET fetches the value at the given key. If the key does not exist, the ReturnValue will be Empty().
//
// Works similar to https://redis.io/commands/get
func (c Client) GET(key string) (val ReturnValue, err error) {
	resp, err := c.ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key:            keyDef{pk: key, sk: ""}.toAV(c),
		TableName:      aws.String(c.tableName),
	})
	if err != nil || len(resp.Item) == 0 {
		return
	}

	val = ReturnValue{resp.Item[vk]}

	return
}

// SET stores the given Value at the given key. If called as SET("key", "value", None), SET is
// unconditional and is not expected to fail.
//
// The condition flags IfNotExists and IfAlreadyExists can be specified, and if they are
// the SET becomes conditional and will return false if the condition fails.
//
// Works similar to https://redis.io/commands/set
func (c Client) SET(key string, vValue interface{}, flags ...Flag) (ok bool, err error) {
	value, err := ToValueE(vValue)
	if err != nil {
		return
	}
	builder := newExpresionBuilder()

	builder.updateSET(vk, value)

	for _, flag := range flags {
		if flag == IfNotExists {
			builder.addConditionNotExists(c.partitionKey)
		}

		if flag == IfAlreadyExists {
			builder.addConditionExists(c.partitionKey)
		}
	}

	_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		UpdateExpression:          builder.updateExpression(),
		Key: keyDef{
			pk: key,
			sk: "",
		}.toAV(c),
		TableName: aws.String(c.tableName),
	})
	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return
	}

	return true, nil
}

// SETNX is equivalent to SET(key, value, Flags{IfNotExists})
//
// Works similar to https://redis.io/commands/setnx
func (c Client) SETNX(key string, value Value) (ok bool, err error) {
	return c.SET(key, value, IfNotExists)
}

// SETCAS is a compare-and-set on the string value at key: it writes newValue only
// if the current stored value still matches the caller's compare-and-set
// precondition. When oldExists is true the current value must equal oldValue; when
// oldExists is false the value item must not exist. It returns ok=false (and makes
// no write) when the precondition fails — i.e. a concurrent writer changed the
// value between the caller's read and this write — which callers use to drive an
// optimistic-concurrency retry loop for read-modify-write commands (APPEND /
// SETRANGE and the read-modify-write INCR reconciliation). Any other error is
// returned as-is.
//
// SETCAS does not depend on read consistency: the DynamoDB conditional expression
// is evaluated against the current item at write time, so two concurrent
// read-modify-write attempts cannot both succeed with a stale base — the loser's
// condition fails and it retries with the winner's value.
func (c Client) SETCAS(key string, newValue Value, oldValue Value, oldExists bool) (ok bool, err error) {
	builder := newExpresionBuilder()
	builder.updateSET(vk, newValue)

	if oldExists {
		builder.addConditionEquality(vk, oldValue)
	} else {
		builder.addConditionNotExists(vk)
	}

	_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		UpdateExpression:          builder.updateExpression(),
		Key: keyDef{
			pk: key,
			sk: "",
		}.toAV(c),
		TableName: aws.String(c.tableName),
	})
	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

// GETSET gets the value at the key and atomically sets it to a new value.
//
// Works similar to https://redis.io/commands/getset
func (c Client) GETSET(key string, value Value) (oldValue ReturnValue, err error) {
	builder := newExpresionBuilder()
	builder.updateSET(vk, value)

	resp, err := c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		UpdateExpression:          builder.updateExpression(),
		Key: keyDef{
			pk: key,
			sk: "",
		}.toAV(c),
		ReturnValues: types.ReturnValueAllOld,
		TableName:    aws.String(c.tableName),
	})

	if err != nil || len(resp.Attributes) == 0 {
		return
	}

	oldValue = parseItem(resp.Attributes, c).val

	return
}

// MGET fetches the given keys atomically in a transaction. The call is limited to 25 keys and 4MB.
// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html
//
// Works similar to https://redis.io/commands/mget
func (c Client) MGET(keys ...string) (values map[string]ReturnValue, err error) {
	values = make(map[string]ReturnValue)
	inputRequests := make([]types.TransactGetItem, len(keys))

	for i, key := range keys {
		inputRequests[i] = types.TransactGetItem{
			Get: &types.Get{
				Key: keyDef{
					pk: key,
					sk: "",
				}.toAV(c),
				ProjectionExpression: aws.String(strings.Join([]string{vk, c.partitionKey}, ", ")),
				TableName:            aws.String(c.tableName),
			},
		}
	}

	resp, err := c.ddbClient.TransactGetItems(context.TODO(), &dynamodb.TransactGetItemsInput{
		TransactItems: inputRequests,
	})

	if err != nil {
		return
	}

	for _, item := range resp.Responses {
		pi := parseItem(item.Item, c)
		values[pi.pk] = pi.val
	}

	return
}

// MSET sets the given keys and values atomically in a transaction. The call is limited to 25 keys and 4MB.
// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
//
// Works similar to https://redis.io/commands/mset
func (c Client) MSET(vFieldMap interface{}) (err error) {
	fieldMap, err := ToValueMapE(vFieldMap)
	if err != nil {
		return err
	}
	_, err = c.mset(fieldMap, Flags{})
	return err
}

// MSETNX sets the given keys and values atomically in a transaction, but only if none of the given
// keys exist. If one or more of the keys already exist, nothing will be changed and MSETNX will return false.
//
// Works similar to https://redis.io/commands/msetnx
func (c Client) MSETNX(vFieldMap interface{}) (ok bool, err error) {
	fieldMap, err := ToValueMapE(vFieldMap)
	if err != nil {
		return ok, err
	}

	ok, err = c.mset(fieldMap, Flags{IfNotExists})
	return
}

func (c Client) mset(data map[string]Value, flags Flags) (ok bool, err error) {
	inputs := make([]types.TransactWriteItem, 0, len(data))

	for k, v := range data {
		builder := newExpresionBuilder()

		if flags.has(IfNotExists) {
			builder.addConditionNotExists(c.partitionKey)
		}

		builder.updateSET(vk, v)

		inputs = append(inputs, types.TransactWriteItem{
			Update: &types.Update{
				ConditionExpression:       builder.conditionExpression(),
				ExpressionAttributeNames:  builder.expressionAttributeNames(),
				ExpressionAttributeValues: builder.expressionAttributeValues(),
				Key: keyDef{
					pk: k,
					sk: "",
				}.toAV(c),
				TableName:        aws.String(c.tableName),
				UpdateExpression: builder.updateExpression(),
			},
		})
	}

	_, err = c.ddbClient.TransactWriteItems(context.TODO(), &dynamodb.TransactWriteItemsInput{
		ClientRequestToken: nil,
		TransactItems:      inputs,
	})

	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

// INCRBYFLOAT increments the number stored at the key with the given float64 delta (n = n + delta) and returns
// the new value. If the key does not exist, it will be initialized with zero before applying
// the operation.
//
// The delta can be positive or negative, and a zero delta is effectively a no-op.
//
// If there is an existing value at the key with a non-numeric type (string, bytes, etc.)
// the operation will throw an error. If the existing value is numeric, the operation
// can continue irrespective of how it was initially set.
//
// Cost is O(1) or 1 WCU.
//
// Works similar to https://redis.io/commands/incrbyfloat
func (c Client) INCRBYFLOAT(key string, delta float64) (after float64, err error) {
	rv, err := c.incr(key, FloatValue{delta})
	if err == nil {
		after = rv.Float()
	}

	return
}

func (c Client) incr(key string, value Value) (newValue ReturnValue, err error) {
	builder := newExpresionBuilder()
	builder.keys[vk] = struct{}{}
	resp, err := c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: builder.expressionAttributeNames(),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":delta": value.ToAV(),
		},
		Key:              keyDef{pk: key, sk: ""}.toAV(c),
		ReturnValues:     types.ReturnValueAllNew,
		TableName:        aws.String(c.tableName),
		UpdateExpression: aws.String("ADD #val :delta"),
	})

	if err == nil {
		newValue = ReturnValue{resp.Attributes[vk]}
	}

	return
}

// INCR increments the number stored at the key by 1 (n = n + 1) and returns the new value. If the
// key does not exist, it will be initialized with zero before applying the operation.
//
// If there is an existing value at the key with a non-numeric type (string, bytes, etc.)
// the operation will throw an error. If the existing value is numeric, the operation
// can continue irrespective of how it was initially set.
//
// Cost is O(1) or 1 WCU.
//
// Works similar to https://redis.io/commands/incr
func (c Client) INCR(key string) (after int64, err error) {
	return c.INCRBY(key, 1)
}

// DECR decrements the number stored at the key by 1 (n = n - 1) and returns the new value. If the
// key does not exist, it will be initialized with zero before applying the operation.
//
// If there is an existing value at the key with a non-numeric type (string, bytes, etc.)
// the operation will throw an error. If the existing value is numeric, the operation
// can continue irrespective of how it was initially set.
//
// Cost is O(1) or 1 WCU.
//
// Works similar to https://redis.io/commands/decr
func (c Client) DECR(key string) (after int64, err error) {
	return c.INCRBY(key, -1)
}

// INCRBY increments the number stored at the key with the given delta (n = n + delta) and returns the new value. If the
// key does not exist, it will be initialized with zero before applying the operation.
//
// If there is an existing value at the key with a non-numeric type (string, bytes, etc.)
// the operation will throw an error. If the existing value is numeric, the operation
// can continue irrespective of how it was initially set.
//
// Cost is O(1) or 1 WCU.
//
// Works similar to https://redis.io/commands/incrby
func (c Client) INCRBY(key string, delta int64) (after int64, err error) {
	rv, err := c.incr(key, IntValue{delta})
	if err == nil {
		after = rv.Int()
	}

	return
}

// DECRBY decrements the number stored at the key with the given delta (n = n - delta) and returns the new value. If the
// key does not exist, it will be initialized with zero before applying the operation.
//
// If there is an existing value at the key with a non-numeric type (string, bytes, etc.)
// the operation will throw an error. If the existing value is numeric, the operation
// can continue irrespective of how it was initially set.
//
// Cost is O(1) or 1 WCU.
//
// Works similar to https://redis.io/commands/decrby
func (c Client) DECRBY(key string, delta int64) (after int64, err error) {
	return c.INCRBY(key, -delta)
}
