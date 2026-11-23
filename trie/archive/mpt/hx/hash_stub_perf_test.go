package hx

import (
	"fmt"
	"testing"
)

var hashStubPerfSink node

// BenchmarkHashFullNodeWithStubs measures the cost of hashing a dirty branch
// that carries immutable archive references. The input node stays unhashed so
// each iteration traverses the same real hash path.
func BenchmarkHashFullNodeWithStubs(b *testing.B) {
	for _, count := range []int{1, 16, 64} {
		stubs := make([]*Stub, count)
		for i := range stubs {
			stubs[i] = &Stub{Path: []byte{byte(i >> 4), byte(i & 15)}}
			stubs[i].ContentHash[0] = byte(i)
		}
		n := &fullNode{Stubs: listOf(stubs)}
		b.Run(fmt.Sprintf("stubs=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h := newHasher(false)
				_, hashStubPerfSink = h.hash(n, true)
				returnHasherToPool(h)
			}
		})
	}
}
