package redimo

import (
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type ZAggregation string

const (
	ZAggregationSum ZAggregation = "SUM"
	ZAggregationMin ZAggregation = "MIN"
	ZAggregationMax ZAggregation = "MAX"
)

var accumulators = map[ZAggregation]func(float64, float64) float64{
	ZAggregationSum: func(a float64, b float64) float64 {
		return a + b
	},
	ZAggregationMin: func(a float64, b float64) float64 {
		return min(a, b)
	},
	ZAggregationMax: func(a float64, b float64) float64 {
		return max(a, b)
	},
}

type rangeCap interface {
	Value
	present() bool
}
type zScore struct {
	score float64
}

func (zs zScore) ToAV() (av types.AttributeValue) {
	if zs.present() {
		av = &types.AttributeValueMemberN{
			Value: strconv.FormatFloat(zs.score, 'G', 17, 64),
		}
	}

	return
}

func (zs zScore) present() bool {
	return !math.IsInf(zs.score, +1) && !math.IsInf(zs.score, -1)
}

type zLex struct {
	lex string
}

func (zl zLex) ToAV() (av types.AttributeValue) {
	if zl.present() {
		// Members are stored in the sort key as encodeSK(member) (Binary). A lex
		// range bound must use the identical encoding so BETWEEN comparisons run
		// against the same byte representation. All members share the member
		// prefix, so byte ordering — and therefore lexical ordering — is
		// preserved.
		av = &types.AttributeValueMemberB{
			Value: encodeSK(zl.lex),
		}
	}

	return
}

func (zl zLex) present() bool {
	return zl.lex != ""
}

func zScoreFromAV(av types.AttributeValue) float64 {
	return ReturnValue{av}.Float()
}

var negInf = zScore{math.Inf(-1)}
var posInf = zScore{math.Inf(+1)}
