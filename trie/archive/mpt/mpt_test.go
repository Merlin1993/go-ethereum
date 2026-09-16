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

func TestMPTCommitToBatchStagesCallerBatch(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	entries := map[string][]byte{
		"a": []byte("alpha-value"),
		"b": []byte("beta-value"),
		"c": []byte("gamma-value"),
	}
	for key, value := range entries {
		if err := tr.Put([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}
	batch := db.NewBatch()
	root, err := tr.CommitToBatch(batch, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(root) != 32 {
		t.Fatalf("CommitToBatch returned root of length %d", len(root))
	}
	if batch.ValueSize() == 0 {
		t.Fatal("CommitToBatch staged nothing into the caller batch")
	}
	// The writes must live in the batch, not the store: a flat record read
	// before the caller's flush must still be missing (T1 commit-merge —
	// the trie may no longer open a private batch and self-flush).
	if _, err := db.Get(flatKey([]byte("a"))); err == nil {
		t.Fatal("flat record visible before batch flush")
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, db, &Config{ShardDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range entries {
		got, err := reloaded.Get([]byte(key))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reloaded Get(%q) = %q, %v; want %q", key, got, err, want)
		}
	}
}

func TestMPTCommitToBatchMatchesCommit(t *testing.T) {
	// The shared-batch path and the self-managed Commit path must stay
	// bit-identical, including across a prune round (nodes, archive
	// records, index and aggregate all ride the caller batch).
	seed := map[string][]byte{
		"a": []byte("one"),
		"b": []byte("two"),
		"c": []byte("three"),
		"d": []byte("four"),
	}
	run := func(t *testing.T, shared bool) []byte {
		t.Helper()
		db := &testStore{memorydb.New()}
		tr := newTestTrie(t, db, false)
		for key, value := range seed {
			if err := tr.Put([]byte(key), value); err != nil {
				t.Fatal(err)
			}
		}
		var root []byte
		var err error
		if shared {
			var batch archivetrie.Batcher = db.NewBatch()
			root, err = tr.CommitToBatch(batch, false)
			if err == nil {
				err = batch.Write()
			}
		} else {
			root, err = tr.Commit()
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := tr.PruneNextShard(); err != nil {
			t.Fatal(err)
		}
		if err := tr.Delete([]byte("b")); err != nil {
			t.Fatal(err)
		}
		if shared {
			batch := db.NewBatch()
			root, err = tr.CommitToBatch(batch, true)
			if err == nil {
				err = batch.Write()
			}
		} else {
			root, err = tr.Commit()
		}
		if err != nil {
			t.Fatal(err)
		}
		return root
	}
	viaCommit := run(t, false)
	viaBatch := run(t, true)
	if !bytes.Equal(viaCommit, viaBatch) {
		t.Fatalf("root divergence: Commit=%x CommitToBatch=%x", viaCommit, viaBatch)
	}
}

func TestMPTIncrementalAggregateMatchesRebuild(t *testing.T) {
	// T2: Commit stages only this batch's shard roots into the persistent
	// aggregate trie. Hash() still rebuilds the aggregate from scratch over
	// all shard roots, so Root==Hash after every round proves the
	// incremental path cannot diverge from the full rebuild — across writes,
	// deletes, prunes (aggregate entries removed via the delete branch) and
	// reloads from the durable root.
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	key := func(round, i int) []byte {
		return []byte{byte('a' + round%26), byte('a' + i%26), byte(round*7 + i*13), byte(round)}
	}
	check := func(round int) {
		t.Helper()
		root, err := tr.Hash()
		if err != nil {
			t.Fatalf("round %d Hash: %v", round, err)
		}
		got := tr.Root()
		if (got == nil) != (root == nil) || !bytes.Equal(got, root) {
			t.Fatalf("round %d: incremental root %x != rebuild root %x", round, got, root)
		}
	}
	for round := 1; round <= 6; round++ {
		for i := 0; i < 6; i++ {
			value := []byte{byte(round), byte(i), 'v'}
			if err := tr.Put(key(round, i), value); err != nil {
				t.Fatalf("round %d Put: %v", round, err)
			}
		}
		batch := db.NewBatch()
		if _, err := tr.CommitToBatch(batch, false); err != nil {
			t.Fatalf("round %d CommitToBatch: %v", round, err)
		}
		if err := batch.Write(); err != nil {
			t.Fatalf("round %d Write: %v", round, err)
		}
		check(round)
	}
	// Prune drains shard roots from the aggregate via the delete branch.
	for round := 7; round <= 10; round++ {
		if err := tr.PruneNextShard(); err != nil {
			t.Fatalf("round %d Prune: %v", round, err)
		}
		if _, err := tr.Commit(); err != nil {
			t.Fatalf("round %d Commit: %v", round, err)
		}
		check(round)
	}
	// Reload from the durable root and keep committing on the reopened
	// aggregate: exercises ensureAggregateLocked's open-from-root path.
	reloaded, err := New(tr.Root(), db, &Config{ShardDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	tr = reloaded
	if err := tr.Put(key(11, 0), []byte("after-reload")); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	check(11)
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
