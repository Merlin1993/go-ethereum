package mpt

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func TestWorkingTrieOpenStatsCountsOnlyHandleCreation(t *testing.T) {
	tr := newTestTrie(t, &testStore{memorydb.New()}, false)
	count0, nanos0 := tr.WorkingTrieOpenStats()
	if err := tr.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	count1, nanos1 := tr.WorkingTrieOpenStats()
	if count1 != count0+1 || nanos1 < nanos0 {
		t.Fatalf("first open: count %d -> %d, nanos %d -> %d", count0, count1, nanos0, nanos1)
	}
	if err := tr.Put([]byte("key"), []byte("next")); err != nil {
		t.Fatal(err)
	}
	count2, _ := tr.WorkingTrieOpenStats()
	if count2 != count1 {
		t.Fatalf("reused handle counted twice: %d -> %d", count1, count2)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Put([]byte("key"), []byte("again")); err != nil {
		t.Fatal(err)
	}
	count3, nanos3 := tr.WorkingTrieOpenStats()
	if count3 != count2+1 || nanos3 < nanos1 {
		t.Fatalf("post-commit open: count %d -> %d, nanos %d -> %d", count2, count3, nanos1, nanos3)
	}
}
