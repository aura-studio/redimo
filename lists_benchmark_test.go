package redimo

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const (
	StepActionLPush = iota
	StepActionRPush
	StepActionLPop
	StepActionRPop
	StepActionLRange // negative test ok
	StepActionLIndex // negative test
	StepActionLSet   // negative test
	StepActionLRem0
	StepActionLRem1  // negative test
	StepActionLRemN1 // negative test
	StepActionLTrim  // negative test
	StepActionRPopLPush
	StepActionLLen
	StepActionLPush1
	StepActionLPush2
	StepActionRPush1
	StepActionRPush2
	StepActionMax
)

type BenchClient struct {
	List        []string
	Client      Client
	TableName   string
	Rand        *rand.Rand
	EnableCheck bool
	Lock        sync.Mutex
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func TestSingleThread(t *testing.T) {
	c := newBenchClient(t)

	for i := 0; i < 10000; i++ {
		if !c.Step() {
			break
		}
	}
}

func RunThread(c *BenchClient, wg *sync.WaitGroup) {
	for i := 0; i < 4000; i++ {
		if !c.Step() {
			break
		}
	}

	wg.Done()
}

func TestMultiThread(t *testing.T) {
	c := newBenchClient(t)
	c.EnableCheck = false

	wg := new(sync.WaitGroup)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go RunThread(c, wg)
	}

	wg.Wait()
}

func (bc *BenchClient) stringWithCharset(length int64, charset string) string {
	r := bc.Rand
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

func (b *BenchClient) String() string {
	length := b.Rand.Int63()%5 + 3
	return b.stringWithCharset(length, charset)
}

func newBenchClient(t *testing.T) *BenchClient {
	c := newClient(t)

	seed := time.Now().UnixNano()
	// seed := int64(1715156636706749400)
	fmt.Printf("Seed %d\n", seed)

	return &BenchClient{
		Client:      c,
		TableName:   "l1",
		Rand:        rand.New(rand.NewSource(seed)),
		EnableCheck: true,
	}
}

func (b *BenchClient) AssertErrNil(err error) {
	if err != nil {
		fmt.Printf("Error %v\n", err)
		panic(err)
	}
}

func (b *BenchClient) CheckEqual() {
	if !b.EnableCheck {
		b.Lock.Lock()
		defer b.Lock.Unlock()
		elements, err := b.Client.LRANGE(b.TableName, 0, -1)
		b.AssertErrNil(err)

		b.List = make([]string, 0)
		for _, e := range elements {
			b.List = append(b.List, e.String())
		}
		return
	}

	elements, err := b.Client.LRANGE(b.TableName, 0, -1)
	b.AssertErrNil(err)

	if len(elements) != len(b.List) {
		b.Panic("Not equal")
	}

	for i, e := range elements {
		if e.String() != b.List[i] {
			b.Panic("Not equal")
		}
	}
}

func (b *BenchClient) IsSizeLimit() bool {
	return len(b.List) > 100
}

func (b *BenchClient) ActionLPush() {
	if b.IsSizeLimit() {
		return
	}

	s := b.String()
	b.List = append([]string{s}, b.List...)
	fmt.Printf("LPush %s\n", s)
	fmt.Printf("List %v\n", b.List)

	_, err := b.Client.LPUSH(b.TableName, StringValue{s})
	b.AssertErrNil(err)
	b.CheckEqual()
}

func (b *BenchClient) ActionRPush() {
	if b.IsSizeLimit() {
		return
	}

	s := b.String()
	b.List = append(b.List, s)
	fmt.Printf("RPush %s\n", s)
	fmt.Printf("List %v\n", b.List)

	_, err := b.Client.RPUSH(b.TableName, StringValue{s})
	b.AssertErrNil(err)
	b.CheckEqual()
}

func (b *BenchClient) ActionLPop() {
	if len(b.List) == 0 {
		return
	}

	s := b.List[0]
	b.List = b.List[1:]
	fmt.Printf("LPop %s\n", s)
	fmt.Printf("List %v\n", b.List)

	_, err := b.Client.LPOP(b.TableName)
	b.AssertErrNil(err)
	b.CheckEqual()
}

func (b *BenchClient) ActionRPop() {
	if len(b.List) == 0 {
		return
	}

	s := b.List[len(b.List)-1]
	b.List = b.List[:len(b.List)-1]
	fmt.Printf("RPop %s\n", s)
	fmt.Printf("List %v\n", b.List)

	_, err := b.Client.RPOP(b.TableName)
	b.AssertErrNil(err)
	b.CheckEqual()
}

func (b *BenchClient) ActionLRange() {
	if len(b.List) == 0 {
		return
	}

	start := int64(b.Rand.Intn(len(b.List)))
	end := int64(b.Rand.Intn(len(b.List)))

	if start > end {
		start, end = end, start
	}

	virtualStart := start
	virtualEnd := end
	llen := int64(len(b.List))

	// rand negative
	if b.Rand.Intn(2) == 0 {
		virtualStart = -(llen - start)
	}

	// rand negative
	if b.Rand.Intn(2) == 0 {
		virtualEnd = -(llen - end)
	}

	rangeList := make([]string, 0)
	for i := start; i <= end; i++ {
		rangeList = append(rangeList, b.List[i])
	}

	fmt.Printf("LRange %d %d virtual %d %d\n", start, end, virtualStart, virtualEnd)
	fmt.Printf("Range list %v\n", rangeList)

	elements, err := b.Client.LRANGE(b.TableName, virtualStart, virtualEnd)
	b.AssertErrNil(err)

	if len(elements) != len(rangeList) {
		b.Panic("Not equal")
	}

	if b.EnableCheck {
		for i, e := range elements {
			if e.String() != rangeList[i] {
				b.Panic("Not equal")
			}
		}
	}
}

func (b *BenchClient) ActionLIndex() {
	if len(b.List) == 0 {
		return
	}

	index := int64(b.Rand.Intn(len(b.List)))
	s := b.List[index]

	virtualIndex := index

	// rand negative
	if b.Rand.Intn(2) == 0 {
		virtualIndex = -(int64(len(b.List)) - int64(index))
	}

	fmt.Printf("LIndex %d virtual %d %s\n", index, virtualIndex, s)

	element, err := b.Client.LINDEX(b.TableName, int64(virtualIndex))
	b.AssertErrNil(err)

	if element.String() != s {
		b.Panic("Not equal")
	}
}

func (b *BenchClient) ActionLSet() {
	if len(b.List) == 0 {
		return
	}

	index := int64(b.Rand.Intn(len(b.List)))
	s := b.String()
	b.List[index] = s

	virtualIndex := index

	// rand negative
	if b.Rand.Intn(2) == 0 {
		virtualIndex = -(int64(len(b.List)) - int64(index))
	}

	fmt.Printf("LSet %d virtual %d %s\n", index, virtualIndex, s)
	fmt.Printf("List %v\n", b.List)

	ok, err := b.Client.LSET(b.TableName, virtualIndex, s)
	b.AssertErrNil(err)

	if !ok {
		b.Panic("Not equal")
	}

	b.CheckEqual()
}

func (b *BenchClient) ActionLRem0() {
	if len(b.List) == 0 {
		return
	}

	i := b.Rand.Intn(len(b.List))
	s := b.List[i]
	newList := make([]string, 0)
	for _, e := range b.List {
		if e != s {
			newList = append(newList, e)
		}
	}
	b.List = newList

	fmt.Printf("LRem %d %s\n", i, s)
	fmt.Printf("List %v\n", b.List)

	_, ok, err := b.Client.LREM(b.TableName, 0, StringValue{s})
	b.AssertErrNil(err)

	if !ok {
		b.Panic("Not equal")
	}

	b.CheckEqual()
}

func (b *BenchClient) ActionLRem1() {
	if len(b.List) == 0 {
		return
	}

	i := b.Rand.Intn(len(b.List))
	s := b.List[i]

	count := b.Rand.Intn(3) + 1
	rawCount := count
	totalCount := int64(0)

	for _, e := range b.List {
		if e == s {
			totalCount++
		}
	}

	newList := make([]string, 0)
	for _, e := range b.List {
		if e == s && count > 0 {
			count--
		} else {
			newList = append(newList, e)
		}
	}

	fmt.Printf("LRem1 %d %s count %d/%d\n", i, s, rawCount, totalCount)
	fmt.Printf("Lrem1 List %v newList %v\n", b.List, newList)

	b.List = newList

	_, ok, err := b.Client.LREM(b.TableName, int64(count), StringValue{s})
	b.AssertErrNil(err)

	if !ok {
		b.Panic("Not equal")
	}

	b.CheckEqual()
}

func reverse(s []string) []string {
	for i := len(s)/2 - 1; i >= 0; i-- {
		opp := len(s) - 1 - i
		s[i], s[opp] = s[opp], s[i]
	}
	return s
}

func (b *BenchClient) ActionLRemN1() {
	if len(b.List) == 0 {
		return
	}

	index := b.Rand.Intn(len(b.List))
	s := b.List[index]

	count := b.Rand.Intn(3) + 1
	rawCount := count
	totalCount := int64(0)

	for _, e := range b.List {
		if e == s {
			totalCount++
		}
	}

	newList := make([]string, 0)
	for i := len(b.List) - 1; i >= 0; i-- {
		e := b.List[i]
		if e == s && count > 0 {
			count--
		} else {
			newList = append(newList, e)
		}
	}

	// reverse newList
	newList = reverse(newList)

	fmt.Printf("LRemN1 %d %s count %d/%d\n", index, s, rawCount, totalCount)
	fmt.Printf("LremN1 List %v newList %v\n", b.List, newList)

	b.List = newList

	_, ok, err := b.Client.LREM(b.TableName, -int64(count), StringValue{s})
	b.AssertErrNil(err)

	if !ok {
		b.Panic("Not equal")
	}

	b.CheckEqual()
}

func (b *BenchClient) ActionLTrim() {
	if len(b.List) == 0 {
		return
	}

	llen := int64(len(b.List))

	start := int64(b.Rand.Intn(len(b.List)))
	end := int64(b.Rand.Intn(len(b.List)))

	if start > end {
		start, end = end, start
	}

	newList := make([]string, 0)
	for i, e := range b.List {
		if int64(i) >= start && int64(i) <= end {
			newList = append(newList, e)
		}
	}

	b.List = newList

	virtualStart := start
	virtualEnd := end

	// rand negative
	if b.Rand.Intn(2) == 0 {
		virtualStart = -(llen - int64(start))
	}

	// rand negative
	if b.Rand.Intn(2) == 0 {
		virtualEnd = -(llen - int64(end))
	}

	fmt.Printf("LTrim %d %d virtual %d %d\n", start, end, virtualStart, virtualEnd)
	fmt.Printf("List %v\n", b.List)

	_, err := b.Client.LTRIM(b.TableName, int64(virtualStart), int64(virtualEnd))
	b.AssertErrNil(err)
	b.CheckEqual()
}

func (b *BenchClient) ActionRPopLPush() {
	if len(b.List) == 0 {
		return
	}

	s := b.List[len(b.List)-1]
	b.List = b.List[:len(b.List)-1]
	b.List = append([]string{s}, b.List...)

	fmt.Printf("RPopLPush %s\n", s)
	fmt.Printf("List %v\n", b.List)

	element, err := b.Client.RPOPLPUSH(b.TableName, b.TableName)
	b.AssertErrNil(err)

	if element.String() != s {
		b.Panic("Not equal")
	}

	b.CheckEqual()
}

func (b *BenchClient) Panic(v interface{}) {
	if !b.EnableCheck {
		return
	}

	panic(v)
}

func (b *BenchClient) ActionLLen() {
	count, err := b.Client.LLEN(b.TableName)
	b.AssertErrNil(err)

	if count != int64(len(b.List)) {
		b.Panic("Not equal")
	}
}

func (b *BenchClient) Step() bool {
	switch b.Rand.Intn(StepActionMax) {
	case StepActionLPush:
		b.ActionLPush()
	case StepActionRPush:
		b.ActionRPush()
	case StepActionLPop:
		b.ActionLPop()
	case StepActionRPop:
		b.ActionRPop()
	case StepActionLRange:
		b.ActionLRange()
	case StepActionLIndex:
		b.ActionLIndex()
	case StepActionLSet:
		b.ActionLSet()
	case StepActionLRem0:
		b.ActionLRem0()
	case StepActionLRem1:
		b.ActionLRem1()
	case StepActionLRemN1:
		b.ActionLRemN1()
	case StepActionLTrim:
		b.ActionLTrim()
	case StepActionRPopLPush:
		b.ActionRPopLPush()
	case StepActionLLen:
		b.ActionLLen()
	case StepActionLPush1:
		b.ActionLPush()
	case StepActionLPush2:
		b.ActionLPush()
	case StepActionRPush1:
		b.ActionRPush()
	case StepActionRPush2:
		b.ActionRPush()
	}

	return true
}
