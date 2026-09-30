package cuckoo

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

// Insert the same ~112 keys in two different orders; compare Encode() bytes.
func TestFilterPlacementOrderDependence(t *testing.T) {
	keys := make([][]byte, 112)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}
	build := func(order []int) []byte {
		f := New(32, 4)
		for _, idx := range order {
			if err := f.Insert(keys[idx]); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		return f.Encode()
	}
	forward := make([]int, 112)
	for i := range forward {
		forward[i] = i
	}
	reverse := make([]int, 112)
	for i := range reverse {
		reverse[i] = 111 - i
	}
	shuffled := make([]int, 112)
	copy(shuffled, forward)
	rand.New(rand.NewSource(7)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	a, b, c := build(forward), build(reverse), build(shuffled)
	t.Logf("forward==reverse: %v, forward==shuffled: %v", bytes.Equal(a, b), bytes.Equal(a, c))
	if !bytes.Equal(a, b) || !bytes.Equal(a, c) {
		t.Logf("PLACEMENT LEAKS INTO BYTES: root is nondeterministic under map-order rebuilds")
	} else {
		t.Logf("placement canonical: insertion order does not leak")
	}
}
