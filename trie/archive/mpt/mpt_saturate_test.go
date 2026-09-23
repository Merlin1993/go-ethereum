package mpt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
)

// saturateTestKey builds a 32-byte key in domain 1 (leading nibble 0x1),
// distinct per index.
func saturateTestKey(i int) []byte {
	key := make([]byte, 32)
	key[0] = 0x10
	binary.BigEndian.PutUint64(key[24:], uint64(i))
	return key
}

// TestMPTCuckooSaturationRotatesSiblingBuckets is the deterministic regression
// for the production crash "cuckoo overflow appending to bucket ...: cuckoo
// filter is full" (mainnet trace, domain depth 5, batch 276386). A tiny
// 1x2 filter saturates after exactly two inserts, so every mount forces the
// rotation path: partial buckets plus fresh siblings, no batch failure.
func TestMPTCuckooSaturationRotatesSiblingBuckets(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{
		ShardDepthBits: 4,
		CuckooBuckets:  1,
		CuckooSlots:    2,
		BucketCapacity: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	const n = 11
	entries := make([]gethtrie.ExtractedEntry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, gethtrie.ExtractedEntry{
			Key:   saturateTestKey(i),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
	}
	tr.mu.Lock()
	if err := tr.ensureHotLocked(); err != nil {
		tr.mu.Unlock()
		t.Fatal(err)
	}
	if err := tr.mountEntriesLocked([]byte{1}, entries); err != nil {
		tr.mu.Unlock()
		t.Fatalf("mountEntriesLocked failed on saturated filter: %v", err)
	}
	tr.mu.Unlock()

	// Every entry probes back through the public read path.
	for i, e := range entries {
		value, err := tr.Get(e.Key)
		if err != nil {
			t.Fatalf("Get(%d) after saturation rotation: %v", i, err)
		}
		if !bytes.Equal(value, e.Value) {
			t.Fatalf("Get(%d) = %q, want %q", i, value, e.Value)
		}
	}

	// Stub bookkeeping: counts match payloads, the resident tally matches,
	// and every ECMH commitment verifies over its bucket's entries.
	tr.mu.Lock()
	defer tr.mu.Unlock()
	total := 0
	for path, b := range tr.buckets {
		if b.count != len(b.entries) {
			t.Fatalf("bucket %x: stub count %d, payload %d entries", path, b.count, len(b.entries))
		}
		if b.count > 2 {
			t.Fatalf("bucket %x holds %d entries, exceeds the 1x2 filter", path, b.count)
		}
		if err := tr.verifyBucketLocked(b); err != nil {
			t.Fatalf("bucket %x: %v", path, err)
		}
		// The filter must actually know every resident key: a failed insert
		// drops a fingerprint, which would probe-miss an existing entry.
		for key := range b.entries {
			if !b.filter.Lookup([]byte(key)) {
				t.Fatalf("bucket %x: filter lost fingerprint of resident key %x", path, []byte(key))
			}
		}
		total += b.count
	}
	if total != n {
		t.Fatalf("buckets hold %d entries, mounted %d", total, n)
	}
	if tr.residentEntries != n {
		t.Fatalf("residentEntries = %d, want %d", tr.residentEntries, n)
	}
	if len(tr.buckets) < 2 {
		t.Fatalf("saturation should force sibling buckets, got %d", len(tr.buckets))
	}
}

// TestMPTBucketAppendSaturationKeepsPrefix pins the top-up contract: a
// saturated filter stops the append at the absorbed prefix, returns
// errBucketSaturated, and leaves a probe-visible, commitment-consistent
// bucket behind.
func TestMPTBucketAppendSaturationKeepsPrefix(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{
		ShardDepthBits: 4,
		CuckooBuckets:  1,
		CuckooSlots:    3,
		BucketCapacity: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if err := tr.ensureHotLocked(); err != nil {
		t.Fatal(err)
	}
	// Mount two entries so one filter slot stays free.
	seed := []gethtrie.ExtractedEntry{
		{Key: saturateTestKey(0), Value: []byte("v0")},
		{Key: saturateTestKey(1), Value: []byte("v1")},
	}
	if err := tr.mountEntriesLocked([]byte{1}, seed); err != nil {
		t.Fatal(err)
	}
	var b *bucket
	for _, cand := range tr.buckets {
		b = cand
	}
	if b == nil || b.count != 2 {
		t.Fatalf("seed bucket: %+v", b)
	}

	// Appending two more entries saturates the third slot: the first is
	// absorbed, the second must not kill the batch.
	appended := []gethtrie.ExtractedEntry{
		{Key: saturateTestKey(2), Value: []byte("v2")},
		{Key: saturateTestKey(3), Value: []byte("v3")},
	}
	consumed, err := tr.bucketAppendLocked(b, appended)
	if !errors.Is(err, errBucketSaturated) {
		t.Fatalf("append err = %v, want errBucketSaturated", err)
	}
	if consumed != 1 {
		t.Fatalf("consumed = %d, want 1", consumed)
	}
	if b.count != 3 {
		t.Fatalf("bucket count = %d, want 3", b.count)
	}
	if err := tr.verifyBucketLocked(b); err != nil {
		t.Fatal(err)
	}
	for _, e := range appended[:1] {
		if _, ok := b.entries[string(e.Key)]; !ok {
			t.Fatalf("absorbed entry %x missing from bucket", e.Key)
		}
	}
	if _, ok := b.entries[string(appended[1].Key)]; ok {
		t.Fatalf("rejected entry %x leaked into the bucket", appended[1].Key)
	}
	// The rejected insert permuted the filter; the rebuild must keep every
	// resident key probe-visible.
	for key := range b.entries {
		if !b.filter.Lookup([]byte(key)) {
			t.Fatalf("filter lost fingerprint of resident key %x", []byte(key))
		}
	}

	// A fully saturated bucket reports saturation with zero consumption and
	// mutates nothing.
	consumed, err = tr.bucketAppendLocked(b, appended[1:])
	if !errors.Is(err, errBucketSaturated) || consumed != 0 {
		t.Fatalf("full-bucket append = (%d, %v), want (0, errBucketSaturated)", consumed, err)
	}
	if b.count != 3 {
		t.Fatalf("full-bucket append mutated count: %d", b.count)
	}
}
