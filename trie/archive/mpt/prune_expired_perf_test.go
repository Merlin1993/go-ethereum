package mpt

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

// BenchmarkPruneExpiredDomain times a prune that actually archives entries.
// BenchmarkPruneCycle's first rotation does not evict recently written data.
func BenchmarkPruneExpiredDomain(b *testing.B) {
	for _, count := range []int{20, 200} {
		b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
			b.StopTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				store := &testStore{memorydb.New()}
				tr, err := New(nil, store, &Config{
					ShardDepthBits: 4,
					CuckooBuckets:  32,
					CuckooSlots:    4,
					Backend:        BackendPath,
				})
				if err != nil {
					b.Fatal(err)
				}
				for i := 0; i < count; i++ {
					var seed [8]byte
					binary.BigEndian.PutUint64(seed[:], uint64(i))
					key := sha256.Sum256(seed[:])
					key[0] &= 0x0f // all keys belong to domain zero
					value := sha256.Sum256(key[:])
					if err := tr.Put(key[:], value[:]); err != nil {
						b.Fatal(err)
					}
				}
				// The first full rotation flips the base bit; only the next
				// visit to domain zero evicts these leaves.
				for i := 0; i < tr.domainCount(); i++ {
					if err := tr.PruneNextShard(); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if err := tr.PruneNextShard(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if len(tr.buckets) == 0 {
					b.Fatal("prune did not create any archive buckets")
				}
			}
		})
	}
}
