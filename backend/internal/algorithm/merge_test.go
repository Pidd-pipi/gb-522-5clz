package algorithm

import (
	"reflect"
	"testing"
)

func TestMatchByDistance(t *testing.T) {
	cases := []struct {
		name      string
		previous  []float64
		detected  []float64
		tolerance float64
		want      []int
	}{
		{"empty inputs", nil, nil, 10, []int{}},
		{"exact pairs", []float64{100, 500}, []float64{100, 500}, 10, []int{0, 1}},
		{"nearest within tolerance", []float64{100}, []float64{104, 108}, 10, []int{0}},
		{"beyond tolerance", []float64{100}, []float64{150}, 10, []int{-1}},
		{"one to one consumption", []float64{100, 102}, []float64{101}, 10, []int{0, -1}},
		{"disappeared and shifted", []float64{100, 500, 900}, []float64{503, 950}, 10, []int{-1, 0, -1}},
		{"order follows previous events", []float64{900, 100}, []float64{98, 905}, 10, []int{1, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MatchByDistance(tc.previous, tc.detected, tc.tolerance)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.previous) {
				t.Fatalf("expected one result per previous event, got %v", got)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestMatchByDistanceRejectsInvalidTolerance(t *testing.T) {
	for _, tolerance := range []float64{0, -5} {
		if _, err := MatchByDistance([]float64{100}, []float64{100}, tolerance); err == nil {
			t.Fatalf("tolerance %v must be rejected", tolerance)
		}
	}
}
