package hx

import "testing"

func TestFullNodeEpochAggregateMatchesFold(t *testing.T) {
	check := func(bits uint32) {
		n := &fullNode{EpochAgg: bits}
		want, seen := uint8(0), false
		for i := 0; i < 16; i++ {
			want, seen = combineAgg(want, seen, n.getAgg(i))
		}
		if !seen {
			want = aggEmpty
		}
		if got := subtreeAggOf(n); got != want {
			t.Fatalf("EpochAgg=%08x: got %d, want %d", bits, got, want)
		}
	}
	for _, bits := range []uint32{0, 1, 2, 3, 0x55555555, 0xaaaaaaaa, 0xffffffff} {
		check(bits)
	}
	// Exercise combinations of empty, uniform, and mixed child aggregates.
	bits := uint32(0x9e3779b9)
	for i := 0; i < 100000; i++ {
		bits = bits*1664525 + 1013904223
		check(bits)
	}
}
