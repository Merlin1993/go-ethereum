package mpt

import (
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
)

// BenchmarkMount100Entries measures a prune batch with one logical domain.
// It exercises both new-bucket creation and top-up of an existing bucket.
func BenchmarkMount100Entries(b *testing.B) {
	entries := make([]gethtrie.ExtractedEntry, 100)
	for i := range entries {
		key := make([]byte, 32)
		key[0] = 0x10
		binary.BigEndian.PutUint64(key[24:], uint64(i+1))
		value := make([]byte, 32)
		binary.BigEndian.PutUint64(value[24:], uint64(i+1))
		entries[i] = gethtrie.ExtractedEntry{Key: key, Value: value}
	}
	for _, tc := range []struct {
		name string
		seed int
	}{
		{name: "new100", seed: 0},
		{name: "topup50", seed: 50},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store := &testStore{memorydb.New()}
				tr, err := New(nil, store, &Config{ShardDepthBits: 4, BucketCapacity: 100, CuckooBuckets: 64, CuckooSlots: 4})
				if err != nil {
					b.Fatal(err)
				}
				if err := tr.ensureHotLocked(); err != nil {
					b.Fatal(err)
				}
				if tc.seed > 0 {
					if err := tr.mountEntriesLocked([]byte{1}, entries[:tc.seed]); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if err := tr.mountEntriesLocked([]byte{1}, entries[tc.seed:]); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
			}
		})
	}
}
