package mpt

import (
	"fmt"
	"testing"
)

// TestLiveHotLeavesGauge verifies the live-leaf accounting against an
// independent full-tree walk: after puts, deletes, a commit, and a prune,
// LiveHotLeaves() must equal the number of leaves ForEachPrefix enumerates.
// This is the correctness gate for the Live_Hot_Leaves CSV column (the
// logical account separating live growth from storage-engine dead bytes).
func TestLiveHotLeavesGauge(t *testing.T) {
	db := newTestKVStore()
	tr := newTestTrie(t, db, false)
	const N = 4000
	keys := make([][]byte, N)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("leafverifkey%08x", i))
		if err := tr.Put(keys[i], []byte{byte(i), byte(i >> 8)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range keys[:1200] { // delete a third
		if err := tr.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	walk := 0
	tr.ForEachPrefix(nil, 0, func(key, value []byte) bool { walk++; return true })
	if got := tr.LiveHotLeaves(); got != int64(walk) {
		t.Fatalf("after puts/deletes: gauge=%d walk=%d (expect both %d)", got, walk, N-1200)
	}

	// Prune one domain, commit, and re-check. ExtractDomain does not flow
	// through insert/delete, so this exercises the manual subtraction.
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	walk = 0
	tr.ForEachPrefix(nil, 0, func(key, value []byte) bool { walk++; return true })
	if got := tr.LiveHotLeaves(); got != int64(walk) {
		t.Fatalf("after prune: gauge=%d walk=%d", got, walk)
	}

	// Resurrect-style reinsert of a deleted key keeps the account honest.
	if err := tr.Put(keys[0], []byte{0xaa, 0xbb}); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	walk = 0
	tr.ForEachPrefix(nil, 0, func(key, value []byte) bool { walk++; return true })
	if got := tr.LiveHotLeaves(); got != int64(walk) {
		t.Fatalf("after reinsert: gauge=%d walk=%d", got, walk)
	}
	if got := tr.LiveBuckets(); got < 0 {
		t.Fatalf("LiveBuckets negative: %d", got)
	}
}
