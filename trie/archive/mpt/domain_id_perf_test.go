package mpt

import (
	"math/rand"
	"testing"
)

var domainIDSink int

func BenchmarkDomainID19(b *testing.B) {
	key := make([]byte, 32)
	rand.New(rand.NewSource(19)).Read(key)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		domainIDSink = domainIDOf(key, 19)
	}
}

func TestDomainIDOfShortAndFullKeys(t *testing.T) {
	for _, key := range [][]byte{nil, {0xab}, {0xab, 0xcd}, {0xab, 0xcd, 0xef}, make([]byte, 32)} {
		for depth := 1; depth <= 32; depth++ {
			want := 0
			for bit := 0; bit < depth; bit++ {
				want <<= 1
				if bit/8 < len(key) {
					want |= int(key[bit/8]>>(7-uint(bit%8))) & 1
				}
			}
			if got := domainIDOf(key, depth); got != want {
				t.Fatalf("key=%x depth=%d: got %x, want %x", key, depth, got, want)
			}
		}
	}
}
