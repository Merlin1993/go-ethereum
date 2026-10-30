package mpt

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
)

func e21Key(first, second byte) []byte {
	key := make([]byte, 32)
	key[0], key[1] = first, second
	return key
}

func TestE21LogicalPrefixProgressionAndFallback(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{ShardDepthBits: 4, BucketCapacity: 2, CuckooBuckets: 32, CuckooSlots: 4, FullBucketsBeforeSplit: 1})
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{
		e21Key(0x10, 0x00), e21Key(0x10, 0x01), // logical 1 fills
		e21Key(0x1a, 0x00), e21Key(0x1a, 0x10), // logical 1a fills
		e21Key(0x1a, 0x20), // logical 1a2 starts before reaching M
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
	var parent, deepest *bucket
	for _, b := range tr.buckets {
		if len(b.mount) != 0 { // no hot branch exists: every bucket is physical-root mounted
			tr.mu.Unlock()
			t.Fatalf("bucket %x mounted at physical path %x, want root", b.path, b.mount)
		}
		logical := string(bucketLogicalPath(b.path))
		counts[logical]++
		switch logical {
		case string([]byte{1}):
			parent = b
		case string([]byte{1, 0xa, 2}):
			deepest = b
		}
	}
	if len(tr.buckets) != 3 || counts[string([]byte{1})] != 1 || counts[string([]byte{1, 0xa})] != 1 || counts[string([]byte{1, 0xa, 2})] != 1 {
		tr.mu.Unlock()
		t.Fatalf("logical bucket distribution: %v", counts)
	}
	if parent == nil || deepest == nil || deepest.count != 1 {
		tr.mu.Unlock()
		t.Fatalf("missing progressive bucket: parent=%v deepest=%v", parent, deepest)
	}
	// Resurrection leaves room in the shallower bucket. The matching deeper
	// bucket still receives the next expiry first.
	if err := tr.bucketDeleteLocked(parent, keys[0], []byte{1}); err != nil {
		tr.mu.Unlock()
		t.Fatal(err)
	}
	deepKey := e21Key(0x1a, 0x21)
	if err := tr.mountEntriesLocked([]byte{1}, []gethtrie.ExtractedEntry{{Key: deepKey, Value: []byte{6}}}); err != nil {
		tr.mu.Unlock()
		t.Fatal(err)
	}
	if deepest.count != 2 || parent.count != 1 {
		tr.mu.Unlock()
		t.Fatalf("deep-first append failed: deep=%d parent=%d", deepest.count, parent.count)
	}
	// The deep 1a bucket is full for this sibling key, so the parent with
	// newly freed capacity is the fallback; no fourth bucket is created.
	fallbackKey := e21Key(0x1a, 0x30)
	if err := tr.mountEntriesLocked([]byte{1}, []gethtrie.ExtractedEntry{{Key: fallbackKey, Value: []byte{7}}}); err != nil {
		tr.mu.Unlock()
		t.Fatal(err)
	}
	if parent.count != 2 || len(tr.buckets) != 3 {
		tr.mu.Unlock()
		t.Fatalf("shallow fallback failed: parent=%d buckets=%d", parent.count, len(tr.buckets))
	}
	tr.mu.Unlock()
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, db, &Config{ShardDepthBits: 4, BucketCapacity: 2, CuckooBuckets: 32, CuckooSlots: 4, FullBucketsBeforeSplit: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range append(keys[1:], deepKey, fallbackKey) {
		value, err := reloaded.Get(key)
		if err != nil || len(value) != 1 {
			t.Fatalf("cold lookup %d key=%x: value=%x err=%v", i, key, value, err)
		}
	}
	if value, err := reloaded.Get(keys[0]); err == nil || value != nil {
		t.Fatalf("deleted archive key returned %x, err=%v", value, err)
	}
}

func TestE21NonNibbleDomainUsesDistinctLogicalPaths(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{ShardDepthBits: 5, BucketCapacity: 3})
	if err != nil {
		t.Fatal(err)
	}
	a, b := e21Key(0x10, 0), e21Key(0x18, 0)
	tr.mu.Lock()
	if err := tr.ensureHotLocked(); err != nil {
		tr.mu.Unlock()
		t.Fatal(err)
	}
	for _, e := range []gethtrie.ExtractedEntry{{Key: a, Value: []byte("a")}, {Key: b, Value: []byte("b")}} {
		if err := tr.mountEntriesLocked([]byte{1}, []gethtrie.ExtractedEntry{e}); err != nil {
			tr.mu.Unlock()
			t.Fatal(err)
		}
	}
	var paths [][]byte
	for _, bucket := range tr.buckets {
		paths = append(paths, bucketLogicalPath(bucket.path))
	}
	tr.mu.Unlock()
	if len(paths) != 2 || bytes.Equal(paths[0], paths[1]) || len(paths[0]) != 2 || len(paths[1]) != 2 {
		t.Fatalf("D=5 logical paths = %x", paths)
	}
}
