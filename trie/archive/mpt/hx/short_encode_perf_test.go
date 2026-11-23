package hx

import (
	"bytes"
	"testing"
)

var shortEncodePerfSink []byte

func BenchmarkEncodeShortNodeMeta(b *testing.B) {
	leafHex := append(bytes.Repeat([]byte{7}, 64), 16)
	leaf := &shortNode{Key: hexToCompact(leafHex), Val: valueNode(make([]byte, 32)), Epoch: 1}
	ext := &shortNode{Key: hexToCompact([]byte{1, 2, 3, 4}), Val: hashNode(make([]byte, 32)), Agg: 3}
	for _, variant := range []struct {
		name string
		n    *shortNode
	}{
		{"leaf", leaf},
		{"extension", ext},
	} {
		b.Run(variant.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				shortEncodePerfSink = nodeToBytes(variant.n)
			}
		})
	}
}
