package mpt

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
)

func TestE21TwoFullBucketsBeforeSplit(t *testing.T) {
	db := &testStore{memorydb.New()}
	config := &Config{
		ShardDepthBits:         4,
		BucketCapacity:         2,
		CuckooBuckets:          32,
		CuckooSlots:            4,
		FullBucketsBeforeSplit: 2,
	}
	tr, err := New(nil, db, config)
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{
		e21Key(0x10, 0x00), e21Key(0x10, 0x01), // first logical 1 bucket fills
		e21Key(0x11, 0x00), e21Key(0x11, 0x01), // second logical 1 bucket fills
		e21Key(0x1a, 0x00), e21Key(0x1a, 0x01), // first logical 1a bucket fills
		e21Key(0x1a, 0x10), e21Key(0x1a, 0x11), // second logical 1a bucket fills
		e21Key(0x1a, 0x20), // logical 1a2 begins only now
	}
	tr.mu.Lock()
	if err := tr.ensureHotLocked(); err != nil {
		tr.mu.Unlock()
		t.Fatal(err)
	}
	for i, key := range keys {
		entry := gethtrie.ExtractedEntry{Key: key, Value: []byte{byte(i + 1)}}
		if err := tr.mountEntriesLocked([]byte{1}, []gethtrie.ExtractedEntry{entry}); err != nil {
			tr.mu.Unlock()
			t.Fatal(err)
		}
	}
	counts := map[string]int{}
	for _, b := range tr.buckets {
		if len(b.mount) != 0 {
			tr.mu.Unlock()
			t.Fatalf("bucket %x mounted at physical path %x, want root", b.path, b.mount)
		}
		counts[string(bucketLogicalPath(b.path))]++
	}
	if counts[string([]byte{1})] != 2 || counts[string([]byte{1, 0xa})] != 2 || counts[string([]byte{1, 0xa, 2})] != 1 || len(tr.buckets) != 5 {
		tr.mu.Unlock()
		t.Fatalf("logical bucket distribution: %v", counts)
	}
	tr.mu.Unlock()
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, db, config)
	if err != nil {
		t.Fatal(err)
	}
	beforeHotMisses := archivetrie.LastUpdateDiagnostics().MPTHotMissQueries
	for i, key := range keys {
		value, err := reloaded.Get(key)
		if err != nil || len(value) != 1 || value[0] != byte(i+1) {
			t.Fatalf("cold lookup %d key=%x: value=%x err=%v", i, key, value, err)
		}
	}
	if got := archivetrie.LastUpdateDiagnostics().MPTHotMissQueries - beforeHotMisses; got != int64(len(keys)) {
		t.Fatalf("hot-miss query count = %d, want %d", got, len(keys))
	}
}
