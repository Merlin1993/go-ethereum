package hx

import (
	"fmt"
	"testing"
)

var copyPerfSink *fullNode

func copyFullNodeOriginal(n *fullNode) *fullNode {
	c := *n
	if n.Stubs != nil {
		c.Stubs = make([]*Stub, len(n.Stubs))
		for i, s := range n.Stubs {
			c.Stubs[i] = s.copy()
		}
	}
	return &c
}

func BenchmarkFullNodeCopy(b *testing.B) {
	for _, count := range []int{0, 1, 16, 128} {
		n := &fullNode{Stubs: make([]*Stub, count)}
		for i := range n.Stubs {
			n.Stubs[i] = &Stub{Path: []byte{byte(i)}}
		}
		for _, variant := range []struct {
			name string
			copy func(*fullNode) *fullNode
		}{{"original", copyFullNodeOriginal}, {"shared_descriptors", (*fullNode).copy}} {
			b.Run(fmt.Sprintf("%s/stubs=%d", variant.name, count), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					copyPerfSink = variant.copy(n)
				}
			})
		}
	}
}
