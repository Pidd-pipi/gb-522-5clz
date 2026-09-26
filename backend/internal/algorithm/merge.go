package algorithm

import (
	"fmt"
	"math"
)

// MatchByDistance pairs previously reviewed events with newly detected events
// using one-to-one nearest matching on distance. previous and detected hold
// distances in metres, ordered from near to far. The result contains the
// matched detected index for every previous index, or -1 when no detected
// event lies within toleranceM. The tolerance must be positive.
func MatchByDistance(previous, detected []float64, toleranceM float64) ([]int, error) {
	if toleranceM <= 0 {
		return nil, fmt.Errorf("match tolerance must be positive")
	}
	used := make(map[int]bool, len(detected))
	matches := make([]int, len(previous))
	for i, priorDistance := range previous {
		match, best := -1, math.MaxFloat64
		for j, newDistance := range detected {
			if used[j] {
				continue
			}
			delta := math.Abs(priorDistance - newDistance)
			if delta <= toleranceM && delta < best {
				match, best = j, delta
			}
		}
		matches[i] = match
		if match >= 0 {
			used[match] = true
		}
	}
	return matches, nil
}
