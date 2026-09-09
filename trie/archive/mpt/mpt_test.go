package mpt

import (
	"bytes"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
)

type testStore struct{ *memorydb.Database }

func (db *testStore) NewBatch() archivetrie.Batcher { return db.Database.NewBatch() }

func newTestTrie(t *testing.T, db *testStore, activate bool) *Trie {
	t.Helper()
	tr, err := New(nil, db, &Config{
		ShardDepth:                2,
		CuckooBuckets:             32,
		CuckooSlots:               4,
		ActivateArchivedKeyOnRead: activate,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestMPTArchiveRoundTripAndForEach(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	entries := map[string]string{
		"alpha": "one",
		"beta":  "two",
		"gamma": "three",
	}
	batch := make([]KeyValue, 0, len(entries))
	for key, value := range entries {
		batch = append(batch, KeyValue{Key: []byte(key), Value: []byte(value)})
	}
	if err := tr.PutBatch(batch); err != nil {
		t.Fatal(err)
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(root) != 32 {
		t.Fatalf("commit returned root of length %d", len(root))
	}
	for key, want := range entries {
		got, err := tr.Get([]byte(key))
		if err != nil || !bytes.Equal(got, []byte(want)) {
			t.Fatalf("Get(%q) = %q, %v; want %q", key, got, err, want)
		}
	}

	reloaded, err := New(root, db, &Config{ShardDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	seen := make([]string, 0, len(entries))
	reloaded.ForEach(func(key, value []byte) bool {
		if !bytes.Equal(value, []byte(entries[string(key)])) {
			t.Fatalf("ForEach(%q) = %q", key, value)
		}
		seen = append(seen, string(key))
		return true
	})
	sort.Strings(seen)
	wantKeys := []string{"alpha", "beta", "gamma"}
	if len(seen) != len(wantKeys) {
		t.Fatalf("ForEach keys = %v, want %v", seen, wantKeys)
	}
	for i := range wantKeys {
		if seen[i] != wantKeys[i] {
			t.Fatalf("ForEach keys = %v, want %v", seen, wantKeys)
		}
	}
}

func TestMPTArchivePruneReloadAndReadActivation(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	key := []byte{0x00, 'k'}
	value := []byte("archived")
	if err := tr.Put(key, value); err != nil {
		t.Fatal(err)
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	root, err = tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	ref, fromArchive, err := tr.GetValueRef(key)
	if err != nil || !fromArchive || len(ref) != 32 {
		t.Fatalf("GetValueRef after prune = %x, archive=%v, err=%v", ref, fromArchive, err)
	}

	active, err := New(root, db, &Config{ShardDepth: 2, ActivateArchivedKeyOnRead: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := active.Get(key)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("activated Get = %q, %v; want %q", got, err, value)
	}
	if _, fromArchive, err := active.GetValueRef(key); err != nil || fromArchive {
		t.Fatalf("key remained archived after activation: archive=%v err=%v", fromArchive, err)
	}
	root, err = active.Commit()
	if err != nil {
		t.Fatal(err)
	}
	final, err := New(root, db, &Config{ShardDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := final.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("reloaded activated Get = %q, %v; want %q", got, err, value)
	}
}

func TestMPTArchiveDeleteArchivedKey(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	key := []byte{0x00, 'd'}
	if err := tr.Put(key, []byte("delete me")); err != nil {
		t.Fatal(err)
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	root, err = tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, db, &Config{ShardDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := reloaded.Delete(key); err != nil {
		t.Fatal(err)
	}
	root, err = reloaded.Commit()
	if err != nil {
		t.Fatal(err)
	}
	final, err := New(root, db, &Config{ShardDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := final.Get(key); err != ErrNotFound {
		t.Fatalf("deleted key error = %v, want %v", err, ErrNotFound)
	}
}
