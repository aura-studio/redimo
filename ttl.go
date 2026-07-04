package redimo

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// TTL support (fork v1.7 extension, task 3.2).
//
// This file builds on the meta item declared in meta.go. The `exp` (N) attribute
// of a key's meta item stores the expiry as epoch **seconds** and simultaneously
// serves as the attribute registered for DynamoDB's native TTL. Two concerns are
// owned here:
//
//  1. Writing/clearing `exp` on the meta item at second precision (SetExpire /
//     SetExpireMillis / SetExpireAt / Persist). Any sub-second input is truncated
//     to whole seconds, matching Pika v3.2.2 which has no millisecond precision.
//  2. Registering `exp` as the DynamoDB native TTL attribute (EnableNativeTTL /
//     NativeTTLStatus). Native TTL only guarantees the *eventual* cleanup of the
//     meta item (up to ~48h lag); read-path correctness is enforced by IsExpired
//     against meta.exp, independent of native-TTL timing (see meta.go).
//
// The meta layout itself is NOT re-declared here; metaAttrExp / metaAttrType /
// metaItemKey come from meta.go.

// TTLAttributeName is the meta-item attribute (`exp`) that holds the expiry epoch
// in seconds and is the attribute to register for DynamoDB native TTL. Table
// provisioning / operators should enable TTL on this attribute (see
// EnableNativeTTL).
const TTLAttributeName = metaAttrExp

// SecondsFromMillis truncates a millisecond epoch timestamp to whole epoch
// seconds. `exp` is stored at second precision, so millisecond-based commands
// (PEXPIRE / PEXPIREAT) truncate the sub-second remainder rather than round.
func SecondsFromMillis(msEpoch int64) int64 {
	return msEpoch / 1000
}

// SecondsFromTime returns the epoch-second representation of t, discarding any
// sub-second component (time.Time.Unix already truncates to whole seconds).
func SecondsFromTime(t time.Time) int64 {
	return t.Unix()
}

// SetExpire records the key's expiry as epoch seconds in the meta item's `exp`
// attribute (O(1)). It only touches an existing key: found is false when the key
// has no meta item, mirroring Redis EXPIRE returning :0 for a missing key. A
// non-positive expEpochSeconds is stored verbatim; the read path treats exp <= now
// as expired via IsExpired.
func (c Client) SetExpire(key string, expEpochSeconds int64) (found bool, err error) {
	_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		Key:                 c.metaItemKey(key),
		TableName:           aws.String(c.tableName),
		ConditionExpression: aws.String("attribute_exists(#t)"),
		UpdateExpression:    aws.String("SET #exp = :exp"),
		ExpressionAttributeNames: map[string]string{
			"#t":   metaAttrType,
			"#exp": metaAttrExp,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":exp": &types.AttributeValueMemberN{Value: strconv.FormatInt(expEpochSeconds, 10)},
		},
	})

	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

// SetExpireMillis sets the key's expiry from a millisecond epoch timestamp,
// truncating sub-second precision to whole seconds before writing `exp`. It backs
// the PEXPIRE / PEXPIREAT commands (Pika v3.2.2 has no millisecond precision, so
// truncation is the correct behaviour).
func (c Client) SetExpireMillis(key string, expEpochMillis int64) (found bool, err error) {
	return c.SetExpire(key, SecondsFromMillis(expEpochMillis))
}

// SetExpireAt sets the key's expiry to the wall-clock instant t, stored at second
// precision (sub-second component discarded).
func (c Client) SetExpireAt(key string, t time.Time) (found bool, err error) {
	return c.SetExpire(key, SecondsFromTime(t))
}

// Persist removes the `exp` attribute from the key's meta item, making the key
// never-expiring. found is false when the key has no meta item.
func (c Client) Persist(key string) (found bool, err error) {
	_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		Key:                 c.metaItemKey(key),
		TableName:           aws.String(c.tableName),
		ConditionExpression: aws.String("attribute_exists(#t)"),
		UpdateExpression:    aws.String("REMOVE #exp"),
		ExpressionAttributeNames: map[string]string{
			"#t":   metaAttrType,
			"#exp": metaAttrExp,
		},
	})

	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

// EnableNativeTTL registers the meta item's `exp` attribute as the table's
// DynamoDB native TTL attribute so DynamoDB eventually reclaims expired meta items
// on its own. This is an idempotent, operator/provisioning-time call. Correctness
// of expiry does NOT depend on native TTL timing — the read path filters on
// meta.exp via IsExpired regardless of when DynamoDB physically deletes the item.
func (c Client) EnableNativeTTL() error {
	_, err := c.ddbClient.UpdateTimeToLive(context.TODO(), &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(c.tableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String(TTLAttributeName),
			Enabled:       aws.Bool(true),
		},
	})

	return err
}

// NativeTTLStatus returns the table's current DynamoDB native TTL configuration,
// letting callers verify that TTL is enabled on the `exp` attribute.
func (c Client) NativeTTLStatus() (*types.TimeToLiveDescription, error) {
	resp, err := c.ddbClient.DescribeTimeToLive(context.TODO(), &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(c.tableName),
	})
	if err != nil {
		return nil, err
	}

	return resp.TimeToLiveDescription, nil
}
