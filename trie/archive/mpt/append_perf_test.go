package mpt

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/trie/archive/mpt/ecmh"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
)

func TestBucketAppendBatchPreservesCommitment(t *testing.T) {
	store := &testStore{memorydb.New()}
	tr, err := New(nil, store, &Config{ShardDepthBits: 4, BucketCapacity: 100, CuckooBuckets: 64, CuckooSlots: 4, Backend: BackendPath})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.ensureHotLocked(); err != nil {
		t.Fatal(err)
	}
	entry := func(key, value byte) gethtrie.ExtractedEntry {
		k := make([]byte, 32)
		k[31] = key
		return gethtrie.ExtractedEntry{Key: k, Value: []byte{value}}
	}
	initial := []gethtrie.ExtractedEntry{entry(1, 1), entry(2, 2)}
	if n, err := tr.createBucketLocked(nil, initial); err != nil || n != len(initial) {
		t.Fatalf("create consumed %d: %v", n, err)
	}
	var b *bucket
	for _, b = range tr.buckets {
	}
	updates := []gethtrie.ExtractedEntry{entry(3, 3), entry(4, 4), entry(3, 5), entry(1, 6), entry(5, 7)}
	if n, err := tr.bucketAppendLocked(b, updates); err != nil || n != len(updates) {
		t.Fatalf("append consumed %d: %v", n, err)
	}
	if b.count != 5 || len(b.entries) != 5 {
		t.Fatalf("bucket has count %d and %d entries, want 5", b.count, len(b.entries))
	}
	items := make([][]byte, 0, len(b.entries))
	for key, value := range b.entries {
		items = append(items, bucketItem([]byte(key), value))
	}
	if !ecmh.Verify(b.commitment, items) {
		t.Fatal("batch append commitment differs from final payload")
	}
}

// BenchmarkAppendExistingBucket isolates the work of topping up a resident
// bucket after filtering and before the next trie commit.
func BenchmarkAppendExistingBucket(b *testing.B) {
	entries := make([]gethtrie.ExtractedEntry, 100)
	for i := range entries {
		var seed [8]byte
		binary.BigEndian.PutUint64(seed[:], uint64(i))
		key := sha256.Sum256(seed[:])
		value := sha256.Sum256(key[:])
		entries[i] = gethtrie.ExtractedEntry{Key: key[:], Value: value[:]}
	}
	for _, added := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("added=%d", added), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store := &testStore{memorydb.New()}
				tr, err := New(nil, store, &Config{ShardDepthBits: 4, BucketCapacity: 100, CuckooBuckets: 64, CuckooSlots: 4, Backend: BackendPath})
				if err != nil {
					b.Fatal(err)
				}
				if err := tr.ensureHotLocked(); err != nil {
					b.Fatal(err)
				}
				initial := 100 - added
				if n, err := tr.createBucketLocked(nil, entries[:initial]); err != nil || n != initial {
					b.Fatalf("create consumed %d: %v", n, err)
				}
				var bucket *bucket
				for _, bucket = range tr.buckets {
				}
				b.StartTimer()
				if n, err := tr.bucketAppendLocked(bucket, entries[initial:]); err != nil || n != added {
					b.Fatalf("append consumed %d: %v", n, err)
				}
				b.StopTimer()
			}
		})
	}
}
