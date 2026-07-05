package redimo

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	ErrArgsAmountNotCorrect = errors.New("args amount not correct")
	ErrKeyMustBeString      = errors.New("key must be a string")
)

func (c Client) HSET(key string, values ...any) (newlySavedFields map[string]Value, err error) {
	var fieldMap = map[string]Value{}

	switch len(values) {
	case 1:
		fieldMap, err = ToValueMapE(values[0])
		if err != nil {
			return newlySavedFields, err
		}
	case 2:
		k, ok := values[0].(string)
		if !ok {
			return newlySavedFields, ErrKeyMustBeString
		}

		v, err := ToValueE(values[1])
		if err != nil {
			return newlySavedFields, err
		}

		fieldMap = map[string]Value{
			k: v,
		}
	default:
		return newlySavedFields, ErrArgsAmountNotCorrect
	}

	newlySavedFields = make(map[string]Value)

	for field, value := range fieldMap {
		builder := newExpresionBuilder()
		builder.updateSetAV(vk, value.ToAV())

		resp, err := c.ddbClient.UpdateItem(c.context(), &dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: field}.toAV(c),
			ReturnValues:              types.ReturnValueAllOld,
			TableName:                 aws.String(c.tableName),
			UpdateExpression:          builder.updateExpression(),
		})

		if err != nil {
			return newlySavedFields, err
		}

		if len(resp.Attributes) < 1 {
			newlySavedFields[field] = value
		}
	}

	return
}

func (c Client) HMSET(key string, vFieldMap any) (err error) {
	fieldMap, err := ToValueMapE(vFieldMap)
	if err != nil {
		return err
	}

	var fields []string
	for field := range fieldMap {
		fields = append(fields, field)
	}

	var (
		hasMoreFields = true
		leftFields    = fields
	)
	for hasMoreFields {
		if len(leftFields) > c.transactionActions {
			fields, hasMoreFields = leftFields[:c.transactionActions], true
			leftFields = leftFields[c.transactionActions:]
		} else {
			fields, hasMoreFields = leftFields, false
		}

		items := make([]types.TransactWriteItem, len(fields))
		for i, field := range fields {
			v := fieldMap[field]
			builder := newExpresionBuilder()
			builder.updateSET(vk, v)

			items[i] = types.TransactWriteItem{
				Update: &types.Update{
					ConditionExpression:       builder.conditionExpression(),
					ExpressionAttributeNames:  builder.expressionAttributeNames(),
					ExpressionAttributeValues: builder.expressionAttributeValues(),
					Key: keyDef{
						pk: key,
						sk: field,
					}.toAV(c),
					TableName:        aws.String(c.tableName),
					UpdateExpression: builder.updateExpression(),
				},
			}
		}

		_, err = c.ddbClient.TransactWriteItems(c.context(), &dynamodb.TransactWriteItemsInput{
			TransactItems: items,
		})
		if err != nil {
			return err
		}
	}
	return
}

func (c Client) HDEL(key string, fields ...string) (deletedFields []string, err error) {
	for _, field := range fields {
		resp, err := c.ddbClient.DeleteItem(c.context(), &dynamodb.DeleteItemInput{
			Key: keyDef{
				pk: key,
				sk: field,
			}.toAV(c),
			ReturnValues: types.ReturnValueAllOld,
			TableName:    aws.String(c.tableName),
		})
		if err != nil {
			return deletedFields, err
		}

		if len(resp.Attributes) > 0 {
			deletedFields = append(deletedFields, field)
		}
	}

	return
}

func (c Client) HINCRBYFLOAT(key string, field string, delta float64) (after float64, err error) {
	rv, err := c.hIncr(key, field, FloatValue{delta})
	if err == nil {
		after = rv.Float()
	}

	return
}

func (c Client) hIncr(key string, field string, delta Value) (after ReturnValue, err error) {
	// A hash field is a member-shaped item: encode sk=field (encodeSK => 0x01||field).
	return c.doIncr(keyDef{pk: key, sk: field}.toAV(c), delta)
}

func (c Client) HINCRBY(key string, field string, delta int64) (after int64, err error) {
	rv, err := c.hIncr(key, field, IntValue{delta})

	if err == nil {
		after = rv.Int()
	}

	return
}

// HSETCAS conditionally sets a hash field's value: newValue is written only if
// the field's current value still matches the base the caller observed —
// oldValue when oldExists is true, or the field being absent when oldExists is
// false. It is the hash-field analogue of SETCAS (strings.go) and backs an atomic
// HINCRBY / HINCRBYFLOAT read-modify-write over binary-stored field values: two
// connections incrementing the same field concurrently cannot both succeed on a
// stale base — the loser's condition fails and it returns ok=false so the caller
// re-reads and re-applies its delta on the winner's value.
//
// It does not depend on read consistency: the DynamoDB conditional expression is
// evaluated against the current item at write time.
func (c Client) HSETCAS(key string, field string, newValue Value, oldValue Value, oldExists bool) (ok bool, err error) {
	builder := newExpresionBuilder()
	builder.updateSET(vk, newValue)

	if oldExists {
		builder.addConditionEquality(vk, oldValue)
	} else {
		builder.addConditionNotExists(vk)
	}

	_, err = c.ddbClient.UpdateItem(c.context(), &dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		UpdateExpression:          builder.updateExpression(),
		Key: keyDef{
			pk: key,
			sk: field,
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

func (c Client) HSETNX(key string, field string, value Value) (ok bool, err error) {
	builder := newExpresionBuilder()
	builder.updateSET(vk, value)
	builder.addConditionNotExists(c.partitionKey)

	_, err = c.ddbClient.UpdateItem(c.context(), &dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		Key: keyDef{
			pk: key,
			sk: field,
		}.toAV(c),
		TableName:        aws.String(c.tableName),
		UpdateExpression: builder.updateExpression(),
	})

	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}
