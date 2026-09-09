package archive

import (
	"bytes"
	"testing"
)

func commitAMTTestTrie(t *testing.T, trie *Trie) []byte {
	t.Helper()
	batch := trie.db.NewBatch()
	defer batch.Reset()
	root, err := trie.CommitToBatch(batch, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestArchiveTrieReadActivation(t *testing.T) {
	key := bytes.Repeat([]byte{0x2a}, 32)
	value := []byte("archived-read-value")

	config := DefaultConfig()
	config.ShardDepth = 8
	config.ActivateArchivedKeyOnRead = true
	config.PhysicalDelete = false
	db := NewMemoryDBAdapter()
	trie := NewTrie(nil, db, NewPooledKeccakHasher(), config, true)
	if err := trie.Put(key, value); err != nil {
		t.Fatal(err)
	}
	commitAMTTestTrie(t, trie)
	shardID := trie.GetShardID(key)
	trie.SetGlobalEpoch(0)
	trie.pruneShardIdx = shardID
	if err := trie.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	if _, fromArchive, err := trie.GetValueRef(key); err != nil || !fromArchive {
		t.Fatalf("expected key to be archived: fromArchive=%v err=%v", fromArchive, err)
	}

	beforeUpdate := LastUpdateDiagnostics()
	beforeCommit := LastCommitDiagnostics()
	got, err := trie.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("read value = %q, want %q", got, value)
	}
	if _, fromArchive, err := trie.GetValueRef(key); err != nil || fromArchive {
		t.Fatalf("expected read activation to restore hot membership: fromArchive=%v err=%v", fromArchive, err)
	}
	afterUpdate := LastUpdateDiagnostics()
	afterCommit := LastCommitDiagnostics()
	if afterUpdate.ArchiveReadPromotionCalls-beforeUpdate.ArchiveReadPromotionCalls != 1 ||
		afterUpdate.ArchiveReadPromotionHits-beforeUpdate.ArchiveReadPromotionHits != 1 {
		t.Fatalf("unexpected read promotion diagnostics: before=%+v after=%+v", beforeUpdate, afterUpdate)
	}
	if afterUpdate.ArchivePromotionChecks != beforeUpdate.ArchivePromotionChecks ||
		afterUpdate.ArchivePromotionHits != beforeUpdate.ArchivePromotionHits ||
		afterCommit.ArchivePromotionChecks != beforeCommit.ArchivePromotionChecks ||
		afterCommit.ArchivePromotionHits != beforeCommit.ArchivePromotionHits {
		t.Fatalf("read activation changed write promotion diagnostics: update before=%+v after=%+v commit before=%+v after=%+v", beforeUpdate, afterUpdate, beforeCommit, afterCommit)
	}
	activatedRoot := commitAMTTestTrie(t, trie)

	// A read promotion must produce the same hot-tree root as an immediate
	// write of the same value into an otherwise identical trie.
	compareConfig := DefaultConfig()
	compareConfig.ShardDepth = config.ShardDepth
	compareConfig.PhysicalDelete = false
	compareTrie := NewTrie(nil, NewMemoryDBAdapter(), NewPooledKeccakHasher(), compareConfig, true)
	if err := compareTrie.Put(key, value); err != nil {
		t.Fatal(err)
	}
	commitAMTTestTrie(t, compareTrie)
	compareTrie.SetGlobalEpoch(0)
	compareTrie.pruneShardIdx = compareTrie.GetShardID(key)
	if err := compareTrie.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	if err := compareTrie.Put(key, value); err != nil {
		t.Fatal(err)
	}
	putRoot := commitAMTTestTrie(t, compareTrie)
	if !bytes.Equal(activatedRoot, putRoot) {
		t.Fatalf("activated root %x differs from immediate-put root %x", activatedRoot, putRoot)
	}
}

func TestArchiveTrieAMTForEachMatchesGet(t *testing.T) {
	config := DefaultConfig()
	config.ShardDepth = 0
	db := NewMemoryDBAdapter()
	trie := NewTrie(nil, db, NewPooledKeccakHasher(), config, true)
	keys := [][]byte{bytes.Repeat([]byte{0x01}, 32), bytes.Repeat([]byte{0x81}, 32), bytes.Repeat([]byte{0xf1}, 32)}
	values := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	for i, key := range keys {
		if err := trie.Put(key, values[i]); err != nil {
			t.Fatal(err)
		}
	}
	commitAMTTestTrie(t, trie)
	trie.SetGlobalEpoch(0)
	trie.pruneShardIdx = 0
	if err := trie.PruneNextShard(); err != nil {
		t.Fatal(err)
	}

	want := make(map[string]string, len(keys))
	for i, key := range keys {
		got, err := trie.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		want[string(key)] = string(got)
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("Get(%x) = %q, want %q", key, got, values[i])
		}
	}
	seen := make(map[string]string, len(keys))
	trie.ForEach(func(key, value []byte) bool {
		seen[string(key)] = string(value)
		return true
	})
	if len(seen) != len(want) {
		t.Fatalf("ForEach returned %d keys, want %d", len(seen), len(want))
	}
	for key, value := range want {
		if seen[key] != value {
			t.Fatalf("ForEach(%x) = %q, want %q", []byte(key), seen[key], value)
		}
	}
}
