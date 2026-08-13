package redimo

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// introspectClient builds a v1.6.1 client against DynamoDB Local (endpoint from
// REDIMO_DDB_ENDPOINT) on a fresh, isolated table. It is env-gated so the package's
// offline `go test` stays unaffected.
func introspectClient(t *testing.T) Client {
	endpoint := os.Getenv("REDIMO_DDB_ENDPOINT")
	if endpoint == "" {
		t.Skip("set REDIMO_DDB_ENDPOINT to run introspect tests against DynamoDB Local")
	}

	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{PartitionID: "aws", URL: endpoint, SigningRegion: region}, nil
	})
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	assert.NoError(t, err)

	svc := dynamodb.NewFromConfig(cfg)
	tableName := "introspect" + uuid.New().String()
	_, err = svc.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: "S"},
			{AttributeName: aws.String("sk"), AttributeType: "S"},
			{AttributeName: aws.String("skN"), AttributeType: "N"},
		},
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{{
			IndexName: aws.String("idx"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("skN"), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
		TableName: aws.String(tableName),
	})
	assert.NoError(t, err)
	t.Cleanup(func() {
		_, _ = svc.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	})

	return NewClient(svc).Table(tableName)
}

// TestTypeOfInference asserts one key of each Redis type is classified correctly,
// including a large-integer-scored zset (seconds timestamps stay below the set
// threshold) and a missing key.
func TestTypeOfInference(t *testing.T) {
	c := introspectClient(t)

	_, err := c.SET("str1", StringValue{"hello"})
	assert.NoError(t, err)
	_, err = c.HSET("hash1", map[string]Value{"f1": StringValue{"v1"}, "f2": StringValue{"v2"}})
	assert.NoError(t, err)
	_, err = c.RPUSH("list1", StringValue{"a"}, StringValue{"b"}, StringValue{"c"})
	assert.NoError(t, err)
	_, err = c.SADD("set1", "m1", "m2", "m3")
	assert.NoError(t, err)
	_, err = c.ZADD("zset1", map[string]float64{"one": 1, "two": 2.5, "three": 3}, nil)
	assert.NoError(t, err)
	_, err = c.ZADD("zsetTS", map[string]float64{"a": 1700000000, "b": 1700000001}, nil)
	assert.NoError(t, err)

	for key, want := range map[string]string{
		"str1": "string", "hash1": "hash", "list1": "list",
		"set1": "set", "zset1": "zset", "zsetTS": "zset",
	} {
		got, exists, err := c.TypeOf(key)
		assert.NoError(t, err)
		assert.True(t, exists, "key %s should exist", key)
		assert.Equal(t, want, got, "TypeOf(%s)", key)
	}

	got, exists, err := c.TypeOf("nope")
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Equal(t, "none", got)
}

// TestScanKeysEnumeratesAndExcludesInternal asserts every user key is surfaced and the
// reserved "_redimo/" list-metadata partition key is excluded.
func TestScanKeysEnumeratesAndExcludesInternal(t *testing.T) {
	c := introspectClient(t)

	_, _ = c.SET("k:str", StringValue{"v"})
	_, _ = c.HSET("k:hash", map[string]Value{"f": StringValue{"v"}})
	_, _ = c.RPUSH("k:list", StringValue{"a"}, StringValue{"b"})
	_, _ = c.SADD("k:set", "m1", "m2")
	_, _ = c.ZADD("k:zset", map[string]float64{"x": 1}, nil)

	seen := map[string]bool{}
	var lek map[string]types.AttributeValue
	for {
		keys, next, err := c.ScanKeys(100, lek)
		assert.NoError(t, err)
		for _, k := range keys {
			seen[k] = true
		}
		if len(next) == 0 {
			break
		}
		lek = next
	}

	for _, want := range []string{"k:str", "k:hash", "k:list", "k:set", "k:zset"} {
		assert.True(t, seen[want], "ScanKeys should surface %s", want)
	}
	assert.False(t, seen["_redimo/k:list"], "ScanKeys must exclude _redimo/ internal keys")
}

// TestLooksLikeSetHeuristic pins the set-vs-zset skN heuristic.
func TestLooksLikeSetHeuristic(t *testing.T) {
	assert.True(t, looksLikeSet([]string{"8472931846291823", "4729384756392100", "9012837465012381"}))
	assert.False(t, looksLikeSet([]string{"1", "2", "3"}))
	assert.False(t, looksLikeSet([]string{"1.5"}))
	assert.False(t, looksLikeSet([]string{"-3"}))
	assert.False(t, looksLikeSet([]string{"1E+20"}))
	assert.False(t, looksLikeSet([]string{"1700000000", "1700000001"}))
}
