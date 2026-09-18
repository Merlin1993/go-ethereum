package mpt

import (
	"bytes"
	crand "crypto/rand"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	gethtrie "github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

func newTestKVStore() *testStore { return &testStore{memorydb.New()} }

// TestKVStoreAdapterKeyValueRoundTrip checks the four strict pass-throughs plus
// Has, including the missing-key case that has to map "not found" onto false
// rather than surfacing the underlying store error.
func TestKVStoreAdapterKeyValueRoundTrip(t *testing.T) {
	store := newTestKVStore()
	adapter := newKVStoreAdapter(store)

	if has, err := adapter.Has([]byte("k")); err != nil || has {
		t.Fatalf("Has(missing) = %v, %v; want false, nil", has, err)
	}
	if err := adapter.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	has, err := adapter.Has([]byte("k"))
	if err != nil || !has {
		t.Fatalf("Has(existing) = %v, %v; want true, nil", has, err)
	}
	value, err := adapter.Get([]byte("k"))
	if err != nil || !bytes.Equal(value, []byte("v")) {
		t.Fatalf("Get = %q, %v; want %q", value, err, "v")
	}
	if err := adapter.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if remaining, err := store.Get([]byte("k")); err == nil {
		t.Fatalf("store still holds %q after adapter Delete (%q)", "k", remaining)
	}
}

// TestAdapterNamespacingIsolation guards the reason openPathDatabase wraps the
// adapter in a table prefix: triedb node keys must never land on top of this
// package's own MPTN/MPTA/MPTI keys inside the one shared LevelDB.
func TestAdapterNamespacingIsolation(t *testing.T) {
	store := newTestKVStore()
	table := rawdb.NewTable(rawdb.NewDatabase(newKVStoreAdapter(store)), pathTablePrefix)

	if err := table.Put([]byte("shared"), []byte("payload")); err != nil {
		t.Fatalf("namespaced Put: %v", err)
	}
	if raw, err := store.Get([]byte("shared")); err == nil {
		t.Fatalf("namespaced key leaked into the raw store: %q", raw)
	}
	got, err := table.Get([]byte("shared"))
	if err != nil || !bytes.Equal(got, []byte("payload")) {
		t.Fatalf("namespaced Get = %q, %v", got, err)
	}
}

// TestKVStoreAdapterBatchWrite proves the batch contract the triedb write path
// depends on: staged entries stay invisible until Write, then all of them go
// through. This is pathdb's only flush path (pathdb/buffer.go).
func TestKVStoreAdapterBatchWrite(t *testing.T) {
	store := newTestKVStore()
	adapter := newKVStoreAdapter(store)

	batch := adapter.NewBatch()
	if err := batch.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("batch Put: %v", err)
	}
	if err := batch.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("batch Put: %v", err)
	}
	if size := batch.ValueSize(); size <= 0 {
		t.Fatalf("ValueSize = %d; want the queued byte count", size)
	}
	if _, err := store.Get([]byte("a")); err == nil {
		t.Fatal("batch leaked into the store before Write")
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("batch Write: %v", err)
	}
	for key, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := store.Get([]byte(key))
		if err != nil || !bytes.Equal(got, []byte(want)) {
			t.Fatalf("Get(%q) = %q, %v; want %q", key, got, err, want)
		}
	}

	// Reset must clear the queue; otherwise a reused batch replays stale writes.
	batch.Reset()
	if size := batch.ValueSize(); size != 0 {
		t.Fatalf("ValueSize after Reset = %d; want 0", size)
	}

	// The size-hint variant has to behave identically: archive.Batcher has no
	// preallocation knob, so pathdb's pre-sized request degrades to this.
	sized := adapter.NewBatchWithSize(1 << 20)
	if err := sized.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("sized batch Put: %v", err)
	}
	if err := sized.Write(); err != nil {
		t.Fatalf("sized batch Write: %v", err)
	}
	if got, err := store.Get([]byte("c")); err != nil || !bytes.Equal(got, []byte("3")) {
		t.Fatalf("Get(c) = %q, %v; want %q", got, err, "3")
	}
}

// TestKVStoreAdapterUnsupportedCallsIsExplicit pins the stubbed methods to errors
// rather than silent successes. They are unreachable from pathdb today; if that
// ever changes, the failure must be loud instead of quietly wrong.
func TestKVStoreAdapterUnsupportedCallsIsExplicit(t *testing.T) {
	adapter := newKVStoreAdapter(newTestKVStore())

	if _, err := adapter.Stat(); err == nil {
		t.Error("Stat succeeded; want an explicit unsupported error")
	}
	if err := adapter.DeleteRange(nil, nil); err == nil {
		t.Error("DeleteRange succeeded; want an explicit unsupported error")
	}
	if err := adapter.Compact(nil, nil); err == nil {
		t.Error("Compact succeeded; want an explicit unsupported error")
	}
	if err := adapter.Close(); err != nil {
		t.Errorf("Close = %v; want nil (the driver owns the store)", err)
	}

	it := adapter.NewIterator(nil, nil)
	defer it.Release()
	if it.Next() {
		t.Error("unsupported iterator yielded a row")
	}
	if err := it.Error(); err == nil {
		t.Error("unsupported iterator reported success")
	}

	batch := adapter.NewBatch()
	if err := batch.Replay(nil); err == nil {
		t.Error("Replay succeeded; want an explicit unsupported error")
	}
}

// TestPathDatabaseRoundTrip is the real acceptance gate for the pathDB backend:
// a native 16-way geth trie must be updateable, committable and reopenable on
// top of the shared archive KVStore. Reading back through a reopened trie
// exercises NodeReader over pathdb's layer tree, which is exactly the
// addressing a hash-keyed reader cannot provide.
func TestPathDatabaseRoundTrip(t *testing.T) {
	store := newTestKVStore()
	tdb, _, err := openPathDatabase(store, 1024, 1024)
	if err != nil {
		t.Fatalf("openPathDatabase: %v", err)
	}
	defer tdb.Close()

	// pathdb bottoms out on the canonical empty-tree root, not the zero hash:
	// asking either reader or Update for common.Hash{} reports a missing layer.
	if _, err := tdb.NodeReader(types.EmptyRootHash); err != nil {
		t.Fatalf("NodeReader on the empty root: %v", err)
	}

	// 32-byte keys and values match the shape the trace driver feeds in.
	type entry struct{ key, value []byte }
	var (
		first  []entry
		second []entry
	)
	for i := 0; i < 256; i++ {
		key, value := make([]byte, 32), make([]byte, 32)
		if _, err := crand.Read(key); err != nil {
			t.Fatal(err)
		}
		if _, err := crand.Read(value); err != nil {
			t.Fatal(err)
		}
		if i < 128 {
			first = append(first, entry{key, value})
		} else {
			second = append(second, entry{key, value})
		}
	}

	var root = types.EmptyRootHash
	for round, batch := range [][]entry{first, second} {
		tr, err := gethtrie.New(gethtrie.TrieID(root), tdb)
		if err != nil {
			t.Fatalf("round %d: open trie: %v", round, err)
		}
		for _, e := range batch {
			if err := tr.Update(e.key, e.value); err != nil {
				t.Fatalf("round %d: Update: %v", round, err)
			}
		}
		nextRoot, nodes := tr.Commit(false)
		// Update requires a NON-nil state set: pathdb dereferences it unguarded
		// when constructing each diff layer (pathdb/difflayer.go states.size).
		if err := tdb.Update(nextRoot, root, uint64(round+1), trienode.NewWithNodeSet(nodes), triedb.NewStateSet()); err != nil {
			t.Fatalf("round %d: trie db Update: %v", round, err)
		}
		root = nextRoot
	}
	if err := tdb.Commit(root, false); err != nil {
		t.Fatalf("trie db Commit: %v", err)
	}

	reopened, err := gethtrie.New(gethtrie.TrieID(root), tdb)
	if err != nil {
		t.Fatalf("reopen trie: %v", err)
	}
	for _, e := range append(append([]entry{}, first...), second...) {
		got, err := reopened.Get(e.key)
		if err != nil {
			t.Fatalf("reopened Get(%x): %v", e.key, err)
		}
		if !bytes.Equal(got, e.value) {
			t.Fatalf("reopened Get(%x) = %x; want %x", e.key, got, e.value)
		}
	}

	// The adapter must not take ownership of the store: after everything the
	// trie db did, the driver's handle has to remain usable.
	if _, err := store.Get([]byte("absent")); err == nil {
		t.Fatal("expected a missing-key error from the shared store")
	}
}

// TestPathDatabaseSecondRootWinsGuards against the single head-root rule: once a
// new layer is registered, the trie must be reopened from that new root, not
// from the superseded one. This is the constraint that ruled out keeping one
// pathDB instance per shard.
func TestPathDatabaseSecondRootWins(t *testing.T) {
	store := newTestKVStore()
	tdb, _, err := openPathDatabase(store, 1024, 1024)
	if err != nil {
		t.Fatalf("openPathDatabase: %v", err)
	}
	defer tdb.Close()

	key, first, second := []byte("the-key"), []byte("first"), []byte("second")

	tr, err := gethtrie.New(gethtrie.TrieID(types.EmptyRootHash), tdb)
	if err != nil {
		t.Fatalf("open trie: %v", err)
	}
	if err := tr.Update(key, first); err != nil {
		t.Fatal(err)
	}
	rootA, nodesA := tr.Commit(false)
	if err := tdb.Update(rootA, types.EmptyRootHash, 1, trienode.NewWithNodeSet(nodesA), triedb.NewStateSet()); err != nil {
		t.Fatalf("first Update: %v", err)
	}

	tr, err = gethtrie.New(gethtrie.TrieID(rootA), tdb)
	if err != nil {
		t.Fatalf("reopen from rootA: %v", err)
	}
	if err := tr.Update(key, second); err != nil {
		t.Fatal(err)
	}
	rootB, nodesB := tr.Commit(false)
	if err := tdb.Update(rootB, rootA, 2, trienode.NewWithNodeSet(nodesB), triedb.NewStateSet()); err != nil {
		t.Fatalf("second Update: %v", err)
	}

	// Reading through the head root yields the newer value.
	head, err := gethtrie.New(gethtrie.TrieID(rootB), tdb)
	if err != nil {
		t.Fatalf("open head: %v", err)
	}
	if got, err := head.Get(key); err != nil || !bytes.Equal(got, second) {
		t.Fatalf("head Get = %q, %v; want %q", got, err, second)
	}
}
