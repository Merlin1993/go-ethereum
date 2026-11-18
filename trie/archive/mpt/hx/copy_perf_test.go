package hx

import (
	"fmt"
	"testing"
)

var copyPerfSink *fullNode

func copyFullNodeOriginal(n *fullNode) *fullNode {
	c := *n
	if n.Stubs != nil {
		c.Stubs = listOf(make([]*Stub, len(n.Stubs.entries)))
		for i, s := range n.Stubs.entries {
			c.Stubs.entries[i] = s.copy()
		}
	}
	return &c
}

func BenchmarkFullNodeCopy(b *testing.B) {
	for _, count := range []int{0, 1, 16, 128} {
		n := &fullNode{Stubs: listOf(make([]*Stub, count))}
		for i := range stubsOf(n.Stubs) {
			n.Stubs.entries[i] = &Stub{Path: []byte{byte(i)}}
		}
		for _, variant := range []struct {
			name string
			copy func(*fullNode) *fullNode
		}{{"original", copyFullNodeOriginal}, {"shared_descriptors", (*fullNode).copy}, {"path_only", (*fullNode).copyPath}} {
			b.Run(fmt.Sprintf("%s/stubs=%d", variant.name, count), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					copyPerfSink = variant.copy(n)
				}
			})
		}
	}
}
