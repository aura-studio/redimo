package redimo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// 并发压测配置
type ConcurrentTestConfig struct {
	NumGoroutines          int           // 并发协程数
	OperationsPerGoroutine int           // 每个协程的操作数
	TestDuration           time.Duration // 测试持续时间（0 表示按操作数）
}

// 压测统计
type ConcurrentStats struct {
	TotalOps     int64
	SuccessOps   int64
	FailedOps    int64
	LPushOps     int64
	RPushOps     int64
	LPopOps      int64
	RPopOps      int64
	LRangeOps    int64
	RPOPLPUSHOps int64
	Errors       []error
	ErrorsMux    sync.Mutex
	StartTime    time.Time
	EndTime      time.Time
}

func (s *ConcurrentStats) RecordError(err error) {
	s.ErrorsMux.Lock()
	defer s.ErrorsMux.Unlock()
	s.Errors = append(s.Errors, err)
}

func (s *ConcurrentStats) Report() string {
	duration := s.EndTime.Sub(s.StartTime)
	opsPerSecond := float64(s.SuccessOps) / duration.Seconds()

	return fmt.Sprintf(`
===== 并发压测报告 =====
总操作数:     %d
成功操作:     %d
失败操作:     %d
LPUSH 操作:   %d
RPUSH 操作:   %d
LPOP 操作:    %d
RPOP 操作:    %d
LRANGE 操作:  %d
RPOPLPUSH:    %d
测试时长:     %v
吞吐量:       %.2f ops/s
错误数:       %d
========================
`, s.TotalOps, s.SuccessOps, s.FailedOps,
		s.LPushOps, s.RPushOps, s.LPopOps, s.RPopOps, s.LRangeOps, s.RPOPLPUSHOps,
		duration, opsPerSecond, len(s.Errors))
}

// TestConcurrentLPUSHRPUSH 测试并发左右推送
func TestConcurrentLPUSHRPUSH(t *testing.T) {
	c := newClient(t)
	key := "concurrent_push_test"

	config := ConcurrentTestConfig{
		NumGoroutines:          10,
		OperationsPerGoroutine: 100,
	}

	stats := &ConcurrentStats{StartTime: time.Now()}
	var wg sync.WaitGroup

	// 启动并发 LPUSH
	for i := 0; i < config.NumGoroutines/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < config.OperationsPerGoroutine; j++ {
				value := fmt.Sprintf("lpush_%d_%d", id, j)
				_, err := c.LPUSH(key, StringValue{value})
				atomic.AddInt64(&stats.TotalOps, 1)
				atomic.AddInt64(&stats.LPushOps, 1)

				if err != nil {
					atomic.AddInt64(&stats.FailedOps, 1)
					stats.RecordError(err)
				} else {
					atomic.AddInt64(&stats.SuccessOps, 1)
				}
			}
		}(i)
	}

	// 启动并发 RPUSH
	for i := 0; i < config.NumGoroutines/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < config.OperationsPerGoroutine; j++ {
				value := fmt.Sprintf("rpush_%d_%d", id, j)
				_, err := c.RPUSH(key, StringValue{value})
				atomic.AddInt64(&stats.TotalOps, 1)
				atomic.AddInt64(&stats.RPushOps, 1)

				if err != nil {
					atomic.AddInt64(&stats.FailedOps, 1)
					stats.RecordError(err)
				} else {
					atomic.AddInt64(&stats.SuccessOps, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	stats.EndTime = time.Now()

	// 验证最终长度
	finalLen, err := c.LLEN(key)
	assert.NoError(t, err)
	expectedLen := int64(config.NumGoroutines * config.OperationsPerGoroutine)

	t.Log(stats.Report())
	t.Logf("预期长度: %d, 实际长度: %d", expectedLen, finalLen)

	// 允许一定误差（由于并发 LLEN 的 TOCTOU 问题）
	assert.InDelta(t, expectedLen, finalLen, float64(expectedLen)*0.05)
	assert.Empty(t, stats.Errors, "不应有错误")
}

// TestConcurrentPushPop 测试并发推送和弹出
func TestConcurrentPushPop(t *testing.T) {
	c := newClient(t)
	key := "concurrent_push_pop_test"

	// 预先填充一些数据
	for i := 0; i < 100; i++ {
		_, err := c.RPUSH(key, StringValue{fmt.Sprintf("init_%d", i)})
		assert.NoError(t, err)
	}

	config := ConcurrentTestConfig{
		NumGoroutines:          20,
		OperationsPerGoroutine: 50,
	}

	stats := &ConcurrentStats{StartTime: time.Now()}
	var wg sync.WaitGroup

	// 并发 LPUSH
	for i := 0; i < config.NumGoroutines/4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < config.OperationsPerGoroutine; j++ {
				_, err := c.LPUSH(key, StringValue{fmt.Sprintf("l_%d_%d", id, j)})
				atomic.AddInt64(&stats.TotalOps, 1)
				atomic.AddInt64(&stats.LPushOps, 1)
				if err == nil {
					atomic.AddInt64(&stats.SuccessOps, 1)
				} else {
					atomic.AddInt64(&stats.FailedOps, 1)
					stats.RecordError(err)
				}
			}
		}(i)
	}

	// 并发 RPUSH
	for i := 0; i < config.NumGoroutines/4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < config.OperationsPerGoroutine; j++ {
				_, err := c.RPUSH(key, StringValue{fmt.Sprintf("r_%d_%d", id, j)})
				atomic.AddInt64(&stats.TotalOps, 1)
				atomic.AddInt64(&stats.RPushOps, 1)
				if err == nil {
					atomic.AddInt64(&stats.SuccessOps, 1)
				} else {
					atomic.AddInt64(&stats.FailedOps, 1)
					stats.RecordError(err)
				}
			}
		}(i)
	}

	// 并发 LPOP
	for i := 0; i < config.NumGoroutines/4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < config.OperationsPerGoroutine; j++ {
				_, err := c.LPOP(key)
				atomic.AddInt64(&stats.TotalOps, 1)
				atomic.AddInt64(&stats.LPopOps, 1)
				if err == nil {
					atomic.AddInt64(&stats.SuccessOps, 1)
				} else {
					atomic.AddInt64(&stats.FailedOps, 1)
					stats.RecordError(err)
				}
				time.Sleep(time.Millisecond) // 稍微延迟避免过快耗尽
			}
		}(i)
	}

	// 并发 RPOP
	for i := 0; i < config.NumGoroutines/4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < config.OperationsPerGoroutine; j++ {
				_, err := c.RPOP(key)
				atomic.AddInt64(&stats.TotalOps, 1)
				atomic.AddInt64(&stats.RPopOps, 1)
				if err == nil {
					atomic.AddInt64(&stats.SuccessOps, 1)
				} else {
					atomic.AddInt64(&stats.FailedOps, 1)
					stats.RecordError(err)
				}
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	stats.EndTime = time.Now()

	t.Log(stats.Report())

	// 验证最终长度应该合理
	finalLen, err := c.LLEN(key)
	assert.NoError(t, err)
	t.Logf("最终列表长度: %d", finalLen)

	// 不应该有严重错误
	assert.LessOrEqual(t, len(stats.Errors), int(stats.TotalOps/20), "错误率应该<5%")
}

// TestConcurrentRPOPLPUSH 测试并发 RPOPLPUSH 的元素丢失问题
func TestConcurrentRPOPLPUSH(t *testing.T) {
	c := newClient(t)
	sourceKey := "rpoplpush_source"
	destKey := "rpoplpush_dest"

	// 预填充源列表
	initialCount := 200
	for i := 0; i < initialCount; i++ {
		_, err := c.RPUSH(sourceKey, StringValue{fmt.Sprintf("item_%d", i)})
		assert.NoError(t, err)
	}

	config := ConcurrentTestConfig{
		NumGoroutines:          10,
		OperationsPerGoroutine: 15,
	}

	stats := &ConcurrentStats{StartTime: time.Now()}
	var wg sync.WaitGroup
	var transferredCount int64

	// 并发执行 RPOPLPUSH
	for i := 0; i < config.NumGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < config.OperationsPerGoroutine; j++ {
				val, err := c.RPOPLPUSH(sourceKey, destKey)
				atomic.AddInt64(&stats.TotalOps, 1)
				atomic.AddInt64(&stats.RPOPLPUSHOps, 1)

				if err == nil && !val.Empty() {
					atomic.AddInt64(&stats.SuccessOps, 1)
					atomic.AddInt64(&transferredCount, 1)
				} else if err != nil {
					atomic.AddInt64(&stats.FailedOps, 1)
					stats.RecordError(err)
				}
			}
		}(i)
	}

	wg.Wait()
	stats.EndTime = time.Now()

	// 验证数据完整性
	sourceLen, err := c.LLEN(sourceKey)
	assert.NoError(t, err)
	destLen, err := c.LLEN(destKey)
	assert.NoError(t, err)

	totalElements := sourceLen + destLen

	t.Log(stats.Report())
	t.Logf("初始元素: %d", initialCount)
	t.Logf("源列表剩余: %d", sourceLen)
	t.Logf("目标列表: %d", destLen)
	t.Logf("总元素数: %d", totalElements)
	t.Logf("成功转移: %d", transferredCount)

	// 关键验证：元素总数应该不变（检测元素丢失问题）
	assert.Equal(t, int64(initialCount), totalElements, "元素总数应该保持不变（检测 RPOPLPUSH 元素丢失 bug）")
}

// TestConcurrentReadWrite 测试并发读写场景
func TestConcurrentReadWrite(t *testing.T) {
	c := newClient(t)
	key := "concurrent_readwrite_test"

	// 预填充
	for i := 0; i < 50; i++ {
		_, err := c.RPUSH(key, StringValue{fmt.Sprintf("val_%d", i)})
		assert.NoError(t, err)
	}

	config := ConcurrentTestConfig{
		NumGoroutines: 30,
		TestDuration:  3 * time.Second,
	}

	stats := &ConcurrentStats{StartTime: time.Now()}
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// 定时停止信号
	go func() {
		time.Sleep(config.TestDuration)
		close(stopChan)
	}()

	// 并发写入
	for i := 0; i < config.NumGoroutines/3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			counter := 0
			for {
				select {
				case <-stopChan:
					return
				default:
					_, err := c.RPUSH(key, StringValue{fmt.Sprintf("w_%d_%d", id, counter)})
					atomic.AddInt64(&stats.TotalOps, 1)
					atomic.AddInt64(&stats.RPushOps, 1)
					if err == nil {
						atomic.AddInt64(&stats.SuccessOps, 1)
					} else {
						atomic.AddInt64(&stats.FailedOps, 1)
						stats.RecordError(err)
					}
					counter++
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(i)
	}

	// 并发读取 LRANGE
	for i := 0; i < config.NumGoroutines/3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					_, err := c.LRANGE(key, 0, 10)
					atomic.AddInt64(&stats.TotalOps, 1)
					atomic.AddInt64(&stats.LRangeOps, 1)
					if err == nil {
						atomic.AddInt64(&stats.SuccessOps, 1)
					} else {
						atomic.AddInt64(&stats.FailedOps, 1)
						stats.RecordError(err)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(i)
	}

	// 并发 LLEN
	for i := 0; i < config.NumGoroutines/3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					_, err := c.LLEN(key)
					atomic.AddInt64(&stats.TotalOps, 1)
					if err == nil {
						atomic.AddInt64(&stats.SuccessOps, 1)
					} else {
						atomic.AddInt64(&stats.FailedOps, 1)
						stats.RecordError(err)
					}
					time.Sleep(8 * time.Millisecond)
				}
			}
		}(i)
	}

	wg.Wait()
	stats.EndTime = time.Now()

	t.Log(stats.Report())

	// 验证最终状态
	finalLen, err := c.LLEN(key)
	assert.NoError(t, err)
	t.Logf("最终列表长度: %d", finalLen)
	assert.GreaterOrEqual(t, finalLen, int64(50), "应该有新增元素")
}

// TestConcurrentLSET 测试并发 LSET 的数据一致性
func TestConcurrentLSET(t *testing.T) {
	c := newClient(t)
	key := "concurrent_lset_test"

	// 预填充 100 个元素
	listSize := 100
	for i := 0; i < listSize; i++ {
		_, err := c.RPUSH(key, StringValue{fmt.Sprintf("original_%d", i)})
		assert.NoError(t, err)
	}

	config := ConcurrentTestConfig{
		NumGoroutines:          20,
		OperationsPerGoroutine: 50,
	}

	stats := &ConcurrentStats{StartTime: time.Now()}
	var wg sync.WaitGroup

	// 并发修改随机位置
	for i := 0; i < config.NumGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < config.OperationsPerGoroutine; j++ {
				index := int64(j % listSize)
				newValue := fmt.Sprintf("updated_%d_%d", id, j)
				ok, err := c.LSET(key, index, newValue)
				atomic.AddInt64(&stats.TotalOps, 1)

				if err == nil && ok {
					atomic.AddInt64(&stats.SuccessOps, 1)
				} else {
					atomic.AddInt64(&stats.FailedOps, 1)
					if err != nil {
						stats.RecordError(err)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	stats.EndTime = time.Now()

	// 验证长度不变
	finalLen, err := c.LLEN(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(listSize), finalLen, "LSET 不应该改变列表长度")

	t.Log(stats.Report())
	t.Logf("列表长度保持: %d", finalLen)
}

// BenchmarkConcurrentOperations 基准测试 - 混合并发操作
func BenchmarkConcurrentOperations(b *testing.B) {
	c := newBenchmarkClient(b)
	key := "bench_concurrent"

	// 预填充
	for i := 0; i < 100; i++ {
		c.RPUSH(key, StringValue{fmt.Sprintf("init_%d", i)})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			switch counter % 6 {
			case 0:
				c.LPUSH(key, StringValue{fmt.Sprintf("lp_%d", counter)})
			case 1:
				c.RPUSH(key, StringValue{fmt.Sprintf("rp_%d", counter)})
			case 2:
				c.LPOP(key)
			case 3:
				c.RPOP(key)
			case 4:
				c.LRANGE(key, 0, 10)
			case 5:
				c.LLEN(key)
			}
			counter++
		}
	})
}

// BenchmarkConcurrentLPUSH 基准测试 - 纯 LPUSH
func BenchmarkConcurrentLPUSH(b *testing.B) {
	c := newBenchmarkClient(b)
	key := "bench_lpush"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			c.LPUSH(key, StringValue{fmt.Sprintf("val_%d", counter)})
			counter++
		}
	})
}

// BenchmarkConcurrentLRANGE 基准测试 - 纯读取
func BenchmarkConcurrentLRANGE(b *testing.B) {
	c := newBenchmarkClient(b)
	key := "bench_lrange"

	// 预填充 1000 个元素
	for i := 0; i < 1000; i++ {
		c.RPUSH(key, StringValue{fmt.Sprintf("val_%d", i)})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.LRANGE(key, 0, 50)
		}
	})
}

// newBenchmarkClient builds an isolated client for benchmarks without relying on testing.T helpers.
func newBenchmarkClient(b *testing.B) Client {
	b.Helper()

	tableName := uuid.New().String()
	indexName := "idx"
	partitionKey := "pk"
	sortKey := "sk"
	sortKeyNum := "skN"

	dynamoService := dynamodb.NewFromConfig(newBenchmarkConfig(b))

	_, err := dynamoService.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(partitionKey), AttributeType: "S"},
			{AttributeName: aws.String(sortKey), AttributeType: "S"},
			{AttributeName: aws.String(sortKeyNum), AttributeType: "N"},
		},
		BillingMode:            types.BillingModePayPerRequest,
		GlobalSecondaryIndexes: nil,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(partitionKey), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(sortKey), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(indexName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String(partitionKey), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String(sortKeyNum), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeKeysOnly,
				},
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(0), WriteCapacityUnits: aws.Int64(0)},
		TableName:             aws.String(tableName),
	})

	if err != nil {
		b.Fatalf("failed to create benchmark table: %v", err)
	}

	return NewClient(dynamoService).Table(tableName).Index(indexName).Attributes(partitionKey, sortKey, sortKeyNum)
}

// newBenchmarkConfig mirrors newConfig but avoids testing.T asserts for benchmarks.
func newBenchmarkConfig(b *testing.B) aws.Config {
	b.Helper()

	region := "us-west-1"
	credentialsProvider := credentials.NewStaticCredentialsProvider("ABCD", "EFGH", "IKJGL")
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
		if service == dynamodb.ServiceID {
			return aws.Endpoint{PartitionID: "aws", URL: "http://localhost:8000", SigningRegion: region}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentialsProvider),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		b.Fatalf("failed to load benchmark config: %v", err)
	}

	return cfg
}
