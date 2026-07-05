package redimo

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/assert"
)

// TestDoBatchWritesRetriesUnprocessedItems proves the UnprocessedItems drain loop in
// doBatchWrites actually re-submits: it must issue a SECOND BatchWriteItem when the
// FIRST response reports unprocessed writes, and it must stop (returning nil) once the
// response drains.
//
// It is hermetic — no real DynamoDB. The redimo Client wraps a concrete
// *dynamodb.Client, so behaviour is injected with an AWS SDK v2 test middleware: a
// smithy Initialize middleware that intercepts every BatchWriteItem, counts the call,
// and short-circuits the stack (it never calls next, so the request is never serialized
// or sent). On the first call it returns a canned output whose UnprocessedItems echo the
// submitted requests; on the second it returns an empty map, ending the loop.
//
// The interceptor sits on the Initialize step (not Finalize) because that is the only
// step where in.Parameters still holds the typed *dynamodb.BatchWriteItemInput — by the
// Finalize step the operation has already been serialized into an opaque HTTP request.
func TestDoBatchWritesRetriesUnprocessedItems(t *testing.T) {
	const tableName = "batch-retry-test-table"

	var calls int32

	// interceptor swaps BatchWriteItem's response for a canned one that carries
	// UnprocessedItems on the first call and none on the second. Placed at the front of
	// the Initialize step and returning without invoking next, it prevents any real send.
	interceptor := middleware.InitializeMiddlewareFunc(
		"batchWriteUnprocessedInterceptor",
		func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
			middleware.InitializeOutput, middleware.Metadata, error,
		) {
			input, ok := in.Parameters.(*dynamodb.BatchWriteItemInput)
			if !ok {
				// Not a BatchWriteItem — pass through untouched.
				return next.HandleInitialize(ctx, in)
			}

			n := atomic.AddInt32(&calls, 1)

			out := &dynamodb.BatchWriteItemOutput{}
			if n == 1 {
				// Report every submitted write as unprocessed so the drain loop must retry.
				out.UnprocessedItems = map[string][]types.WriteRequest{
					tableName: input.RequestItems[tableName],
				}
			}

			return middleware.InitializeOutput{Result: out}, middleware.Metadata{}, nil
		},
	)

	ddbClient := dynamodb.NewFromConfig(newConfig(t), dynamodb.WithAPIOptions(
		func(stack *middleware.Stack) error {
			return stack.Initialize.Add(interceptor, middleware.Before)
		},
	))

	c := NewClient(ddbClient).Table(tableName)

	requests := []types.WriteRequest{
		{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberB{Value: []byte("k")},
			"sk": &types.AttributeValueMemberB{Value: encodeSK("m")},
		}}},
	}

	err := c.doBatchWrites(requests)

	assert.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls),
		"doBatchWrites should re-submit once (2 BatchWriteItem calls) to drain UnprocessedItems")
}
