package mpt

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
)

func bitwiseTestKey(first, second byte) []byte {
	key := make([]byte, 32)
	key[0], key[1] = first, second
	return key
}

func TestBitwiseLogicalPath(t *testing.T) {
	key := bitwiseTestKey(0x15, 0xa3)
	for _, depth := range []int{4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 255, 256} {
		path := keyLogicalPath(key, depth)
		if got := logicalPathBits(path); got != depth {
			t.Fatalf("depth %d decoded as %d", depth, got)
		}
		if !keyHasLogicalPath(key, path) {
			t.Fatalf("key does not match its %d-bit path", depth)
		}
		if want := keyNibblesPrefix(key, depth/4); !bytes.Equal(logicalPathMount(path), want) {
			t.Fatalf("depth %d physical mount %x, want %x", depth, logicalPathMount(path), want)
		}
		if depth < len(key)*8 {
			other := bytes.Clone(key)
			other[(depth-1)/8] ^= 1 << (7 - (depth-1)%8)
			if keyHasLogicalPath(other, path) {
				t.Fatalf("flipped key matches %d-bit path", depth)
			}
		}
	}
}

func TestBitwiseSplitRoutesSiblingsAfterReload(t *testing.T) {
	db := &testStore{memorydb.New()}
	cfg := &Config{ShardDepthBits: 4, BucketCapacity: 1, CuckooBuckets: 32, CuckooSlots: 4, FullBucketsBeforeSplit: 1}
	tr, err := New(nil, db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{bitwiseTestKey(0x10, 0), bitwiseTestKey(0x18, 0), bitwiseTestKey(0x14, 0)}
	tr.mu.Lock()
	if err := tr.ensureHotLocked(); err != nil {
		tr.mu.Unlock()
		t.Fatal(err)
	}
	for i, key := range keys {
		if err := tr.mountEntriesLocked([]byte{1}, []gethtrie.ExtractedEntry{{Key: key, Value: []byte{byte(i + 1)}}}); err != nil {
			tr.mu.Unlock()
			t.Fatal(err)
		}
	}
	paths := make(map[string]bool)
	for _, b := range tr.buckets {
		if len(b.mount) != 0 {
			tr.mu.Unlock()
			t.Fatalf("physical mount = %x, want root", b.mount)
		}
		paths[string(bucketLogicalPath(b.path))] = true
	}
	tr.mu.Unlock()
	if len(paths) != 3 || !paths[string(keyLogicalPath(keys[0], 4))] || !paths[string(keyLogicalPath(keys[1], 5))] || !paths[string(keyLogicalPath(keys[2], 5))] {
		t.Fatalf("unexpected logical paths: %v", paths)
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range keys {
		value, err := reopened.Get(key)
		if err != nil || !bytes.Equal(value, []byte{byte(i + 1)}) {
			t.Fatalf("Get %x = %x, %v", key, value, err)
		}
	}
	var found int
	reopened.ForEachPrefix([]byte{0x10}, 8, func(key, value []byte) bool {
		found++
		if !bytes.Equal(key, keys[0]) || !bytes.Equal(value, []byte{1}) {
			t.Fatalf("prefix scan found %x: %x", key, value)
		}
		return true
	})
	if found != 1 {
		t.Fatalf("prefix scan found %d entries, want 1", found)
	}
}

func TestBitwiseBatchRoutingMatchesSingleEntryRouting(t *testing.T) {
	entries := make([]gethtrie.ExtractedEntry, 0, 210)
	for i := 0; i < 200; i++ {
		key := make([]byte, 32)
		key[0] = 0x10 | byte(i%16)
		key[1] = byte(i / 16)
		binary.BigEndian.PutUint16(key[30:], uint16(i+1))
		entries = append(entries, gethtrie.ExtractedEntry{Key: key, Value: []byte{byte(i)}})
	}
	for i := 0; i < 10; i++ {
		entries = append(entries, gethtrie.ExtractedEntry{Key: bytes.Clone(entries[i].Key), Value: []byte{0xf0 + byte(i)}})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})
	for _, backend := range []string{BackendHash, BackendPath} {
		t.Run(backend, func(t *testing.T) {
			var roots [2][]byte
			var counts [2]int
			for run := 0; run < 2; run++ {
				store := &testStore{memorydb.New()}
				tr, err := New(nil, store, &Config{
					ShardDepthBits: 4, BucketCapacity: 5, CuckooBuckets: 64,
					CuckooSlots: 4, FullBucketsBeforeSplit: 2, Backend: backend,
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := tr.ensureHotLocked(); err != nil {
					t.Fatal(err)
				}
				if run == 0 {
					if err := tr.mountEntriesLocked([]byte{1}, entries); err != nil {
						t.Fatal(err)
					}
				} else {
					for _, entry := range entries {
						if err := tr.mountEntriesLocked([]byte{1}, []gethtrie.ExtractedEntry{entry}); err != nil {
							t.Fatal(err)
						}
					}
				}
				for _, b := range tr.buckets {
					counts[run] += b.count
				}
				roots[run], err = tr.Commit()
				if err != nil {
					t.Fatal(err)
				}
			}
			if counts[0] != counts[1] || !bytes.Equal(roots[0], roots[1]) {
				t.Fatalf("batch and singleton routing diverged: counts %v roots %x %x", counts, roots[0], roots[1])
			}
		})
	}
}
