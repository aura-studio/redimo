package redimo

import (
	"math"
)

func (c Client) ZINTERSTORE(destinationKey string, sourceKeys []string, aggregation ZAggregation, weights map[string]float64) (membersWithScores map[string]float64, err error) {
	set, err := c.ZINTER(sourceKeys, aggregation, weights)
	if err == nil {
		_, err = c.ZADD(destinationKey, set, Flags{})
	}

	return set, err
}

func (c Client) ZUNIONSTORE(destinationKey string, sourceKeys []string, aggregation ZAggregation, weights map[string]float64) (membersWithScores map[string]float64, err error) {
	set, err := c.ZUNION(sourceKeys, aggregation, weights)
	if err == nil {
		_, err = c.ZADD(destinationKey, set, Flags{})
	}

	return set, err
}

func zGetWeight(weights map[string]float64, key string) float64 {
	if weights == nil {
		return 1
	}

	if w, ok := weights[key]; ok {
		return w
	}

	return 1
}
func (c Client) ZUNION(sourceKeys []string, aggregation ZAggregation, weights map[string]float64) (membersWithScores map[string]float64, err error) {
	membersWithScores = make(map[string]float64)

	for _, sourceKey := range sourceKeys {
		currentSet, err := c.ZRANGEBYSCORE(sourceKey, math.Inf(-1), math.Inf(+1), 0, 0)
		if err != nil {
			return membersWithScores, err
		}

		for member, score := range currentSet {
			if existingValue, ok := membersWithScores[member]; ok {
				membersWithScores[member] = accumulators[aggregation](existingValue, score*zGetWeight(weights, sourceKey))
			} else {
				membersWithScores[member] = score * zGetWeight(weights, sourceKey)
			}
		}
	}

	return
}

func (c Client) ZINTER(sourceKeys []string, aggregation ZAggregation, weights map[string]float64) (membersWithScores map[string]float64, err error) {
	membersWithScores, err = c.ZRANGEBYSCORE(sourceKeys[0], math.Inf(-1), math.Inf(+1), 0, 0)
	if err != nil {
		return
	}

	// Apply the FIRST source key's weight to the seed set. Previously the seed used the raw
	// scores of sourceKeys[0] and only sourceKeys[1:] had their weights applied, so the first
	// operand's WEIGHT was silently ignored.
	if w0 := zGetWeight(weights, sourceKeys[0]); w0 != 1 {
		for member := range membersWithScores {
			membersWithScores[member] *= w0
		}
	}

	for i := 1; i < len(sourceKeys); i++ {
		sourceKey := sourceKeys[i]
		currentSet, err := c.ZRANGEBYSCORE(sourceKey, math.Inf(-1), math.Inf(+1), 0, 0)

		if err != nil {
			return membersWithScores, err
		}

		for member, score := range membersWithScores {
			if currentSetValue, ok := currentSet[member]; ok {
				membersWithScores[member] = accumulators[aggregation](score, currentSetValue*zGetWeight(weights, sourceKey))
			} else {
				delete(membersWithScores, member)
			}
		}
	}

	return
}
