package binary

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

// MemoryDBAdapter adapts memorydb.Database to KVStore interface
type MemoryDBAdapter struct {
	*memorydb.Database
}

func (db *MemoryDBAdapter) NewBatch() Batcher {
	return db.Database.NewBatch()
}

func setupTrie() (*Trie, KVStore) {
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()
	return NewTrie(db, hasher, true), db
}

func TestBasicOperations(t *testing.T) {
	trie, _ := setupTrie()

	key := make([]byte, 32)
	rand.Read(key)
	value := []byte("hello world")

	// Put
	if err := trie.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get
	got, err := trie.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	// Note: Trie stores value hash, but Shard.Get returns value hash.
	// Wait, Shard.Get returns value hash? Let's check trie.go again.
	// Line 70: func (s *Shard) Get(key []byte) ([]byte, error) { return s.get(s.root, key, 16) }
	// Line 88: return n.ValueHash, nil
	// Yes, Shard.Get returns the value hash.
	hasher := NewPooledKeccakHasher()
	expectedHash := hasher.Hash(value)
	if !bytes.Equal(got, expectedHash) {
		t.Errorf("Get returned %x, want %x", got, expectedHash)
	}

	// Update
	newValue := []byte("new value")
	if err := trie.Put(key, newValue); err != nil {
		t.Fatalf("Update Put failed: %v", err)
	}
	got, _ = trie.Get(key)
	newExpectedHash := hasher.Hash(newValue)
	if !bytes.Equal(got, newExpectedHash) {
		t.Errorf("Updated Get returned %x, want %x", got, newExpectedHash)
	}

	// Delete
	if err := trie.BatchDelete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err = trie.Get(key)
	if err != ErrNodeNotFound {
		t.Errorf("Get after delete returned error %v, want ErrNodeNotFound", err)
	}
}

func TestEmptyTrie(t *testing.T) {
	trie, _ := setupTrie()
	key := make([]byte, 32)
	rand.Read(key)

	_, err := trie.Get(key)
	if err != ErrNodeNotFound {
		t.Errorf("Get on empty trie returned error %v, want ErrNodeNotFound", err)
	}

	if err := trie.BatchDelete(key); err != nil {
		t.Errorf("Delete on empty trie failed: %v", err)
	}
}

func TestLongCommonPrefix(t *testing.T) {
	trie, _ := setupTrie()
	hasher := NewPooledKeccakHasher()

	// Keys sharing 24 bytes (192 bits) of prefix
	prefix := make([]byte, 24)
	rand.Read(prefix)

	key1 := append(append([]byte{}, prefix...), make([]byte, 8)...)
	key1[24] = 0x00 // Bit 192 is 0
	val1 := []byte("val1")

	key2 := append(append([]byte{}, prefix...), make([]byte, 8)...)
	key2[24] = 0x80 // Bit 192 is 1
	val2 := []byte("val2")

	trie.Put(key1, val1)
	trie.Put(key2, val2)

	got1, _ := trie.Get(key1)
	if !bytes.Equal(got1, hasher.Hash(val1)) {
		t.Errorf("Key1 mismatch")
	}

	got2, _ := trie.Get(key2)
	if !bytes.Equal(got2, hasher.Hash(val2)) {
		t.Errorf("Key2 mismatch")
	}

	// Check if they are in the same shard (first 2 bytes)
	if trie.getShardID(key1) != trie.getShardID(key2) {
		t.Errorf("Keys should be in the same shard for this test to be effective")
	}
}

func TestNodeSerialization(t *testing.T) {
	// Test LeafNode
	leaf := NewLeafNode([]byte{0xAA, 0xBB}, 16, []byte("valuehash"))
	leaf.epoch = 0x05
	data, err := leaf.Serialize()
	if err != nil {
		t.Fatalf("Leaf Serialize failed: %v", err)
	}
	back, err := DeserializeNode(data)
	if err != nil {
		t.Fatalf("Leaf Deserialize failed: %v", err)
	}
	leafBack := back.(*LeafNode)
	if !bytes.Equal(leaf.Path, leafBack.Path) || leaf.PathBits != leafBack.PathBits || !bytes.Equal(leaf.ValueHash, leafBack.ValueHash) || leaf.epoch != leafBack.epoch {
		t.Errorf("Leaf mismatch after serialization")
	}

	// Test InternalNode
	internal := NewInternalNode(nil, nil)
	internal.Path = []byte{0xCC}
	internal.PathBits = 4
	internal.LeftHash = []byte("lefthash")
	internal.RightHash = []byte("righthash")
	internal.epoch = 0x0A
	data, err = internal.Serialize()
	if err != nil {
		t.Fatalf("Internal Serialize failed: %v", err)
	}
	back, err = DeserializeNode(data)
	if err != nil {
		t.Fatalf("Internal Deserialize failed: %v", err)
	}
	intBack := back.(*InternalNode)
	if !bytes.Equal(internal.Path, intBack.Path) || internal.PathBits != intBack.PathBits || !bytes.Equal(internal.LeftHash, intBack.LeftHash) || !bytes.Equal(internal.RightHash, intBack.RightHash) || internal.epoch != intBack.epoch {
		t.Errorf("Internal mismatch after serialization")
	}
}

func TestPersistence(t *testing.T) {
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()
	trie := NewTrie(db, hasher, true)

	key := make([]byte, 32)
	rand.Read(key)
	val := []byte("persistent")

	trie.Put(key, val)
	root, err := trie.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if len(root) == 0 {
		t.Fatalf("Root is empty")
	}

	// New trie instance
	_ = NewTrie(db, hasher, true)
	// Shards in our implementation are lazy. They don't load from DB until Get/Put.
	// But our NewTrie doesn't take root hashes, so shard.root will be nil.
	// Wait, the current NewTrie implementation:
	/*
		for i := 0; i < 65536; i++ {
			s, _ := NewShard(i, db, hasher, nil, pruning, func() byte { return t.globalEpochBit })
			t.shards[i] = s
		}
	*/
	// It doesn't load roots. In a real implementation it should.
	// Let's verify if we can manually set a root hash or if we need to mock it.
	// Shard.root is private.

	// If I can't load roots, I can't test persistence across instances easily without modifying Trie.
	// But I can test if Commit actually writes to DB.
	valHash := hasher.Hash(val)
	if data, _ := db.Get(valHash); !bytes.Equal(data, val) {
		t.Errorf("Value not written to DB")
	}

	// We can't easily test reloading the trie without a Way to set the root hash for shards.
	// However, Shard has a rootHash parameter in NewShard.
	// Trie could be improved to handle root hashes.
}

func TestPruningBasic(t *testing.T) {
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()
	trie := NewTrie(db, hasher, true)

	trie.SetGlobalEpoch(0)
	key := make([]byte, 32)
	rand.Read(key)
	val := []byte("prunable")

	trie.Put(key, val)
	trie.Commit()

	// Verify it exists
	valHash := hasher.Hash(val)
	if data, _ := db.Get(valHash); len(data) == 0 {
		t.Fatalf("Data missing before prune")
	}

	// Change epoch
	trie.SetGlobalEpoch(1)

	// Prune the shard containing this key
	shardID := int(key[0])<<8 | int(key[1])
	trie.pruneShardIdx = shardID
	deleted, err := trie.PruneNextShard()
	if err != nil {
		t.Fatalf("Prune failed: %v", err)
	}

	found := false
	for _, item := range deleted {
		if bytes.Equal(item.Value, val) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Deleted item not returned in PruneNextShard")
	}

	// Commit to finish pruning (deleting from DB)
	trie.Commit()

	// Verify it's gone from trie
	_, err = trie.Get(key)
	if err != ErrNodeNotFound {
		t.Errorf("Key still exists in trie after prune")
	}

	// Note: Our implementation might only delete the NODE, not the VALUE hash entry
	// because multiple keys might point to the same value (though unlikely in this test).
	// Let's check Shard.Prune:
	// n.staleSet[string(n.OriginalHash())] = struct{}{}
	// It only marks the node hash as stale.
}

func TestDeleteCollapsing(t *testing.T) {
	trie, _ := setupTrie()
	hasher := NewPooledKeccakHasher()

	// Keys that will cause a split
	key1 := make([]byte, 32)
	key1[2] = 0x00 // Part of shard 0x0000. Key bit 16 is 0.
	val1 := []byte("val1")

	key2 := make([]byte, 32)
	key2[2] = 0x80 // Key bit 16 is 1.
	val2 := []byte("val2")

	trie.Put(key1, val1)
	trie.Put(key2, val2)

	// Now we should have an InternalNode at depth 16 branching to two LeafNodes.
	// Delete key2. The InternalNode should be removed and the remaining leaf should be moved up (with path adjustment).
	trie.BatchDelete(key2)

	got1, err := trie.Get(key1)
	if err != nil {
		t.Fatalf("Key1 missing after sibling delete: %v", err)
	}
	if !bytes.Equal(got1, hasher.Hash(val1)) {
		t.Errorf("Value mismatch")
	}

	_, err = trie.Get(key2)
	if err != ErrNodeNotFound {
		t.Errorf("Key2 still exists")
	}
}

func TestLazyLoading(t *testing.T) {
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()
	trie := NewTrie(db, hasher, true)

	key := make([]byte, 32)
	rand.Read(key)
	val := []byte("lazy")

	trie.Put(key, val)
	trie.Commit()

	// Clear memory state by finding the shard and setting root to nil
	// (Simulating a fresh instance where roots aren't loaded yet)
	shardID := int(key[0])<<8 | int(key[1])
	shard := trie.shards[shardID]
	rootHash := shard.root.Hash()
	shard.root = nil

	// This is a bit of a hack since root is nil, but trie.Get will call shard.Get.
	// Shard.Get checks if root is nil (returns ErrNodeNotFound).
	// So we need to simulate the "loading" part.

	// Let's manually trigger loading if possible.
	// NewShard can take a rootHash.
	s2, _ := NewShard(shardID, db, hasher, rootHash, true, func() byte { return 0 })
	trie.shards[shardID] = s2

	got, err := trie.Get(key)
	if err != nil {
		t.Fatalf("Get failed after manual reload: %v", err)
	}
	if !bytes.Equal(got, hasher.Hash(val)) {
		t.Errorf("Value mismatch")
	}
}

func TestSubtreePruning(t *testing.T) {
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()
	trie := NewTrie(db, hasher, true)

	trie.SetGlobalEpoch(0)

	// Create two keys that share a common internal node (split at bit 17)
	// Shard starts at bit 16.
	key1 := make([]byte, 32)
	key1[2] = 0x00 // Bit 16=0, 17=0
	val1 := []byte("val1")

	key2 := make([]byte, 32)
	key2[2] = 0x40 // Bit 16=0, 17=1
	val2 := []byte("val2")

	trie.Put(key1, val1)
	trie.Put(key2, val2)
	trie.Commit()

	// Both should be in epoch 1 (since global=0, leaf epoch = 1-global = 1)
	// Ancestor InternalNode should have epoch bit0=1 (OR of children) and bit1=1 (same bit0).
	// So InternalNode epoch should be (1<<1)|1 = 3.

	// Change global to 1.
	// Now children (bit0=1) match global (1).
	// Subtree pruning optimization should trigger if parent epoch bit0 == global and bit1 == 1.
	trie.SetGlobalEpoch(1)

	shardID := int(key1[0])<<8 | int(key1[1])
	trie.pruneShardIdx = shardID
	deleted, err := trie.PruneNextShard()
	if err != nil {
		t.Fatalf("Prune failed: %v", err)
	}

	// Should have deleted both items
	if len(deleted) != 2 {
		t.Errorf("Expected 2 deleted items, got %d", len(deleted))
	}

	trie.Commit()

	// Verify both are gone
	if _, err := trie.Get(key1); err != ErrNodeNotFound {
		t.Errorf("Key1 not pruned")
	}
	if _, err := trie.Get(key2); err != ErrNodeNotFound {
		t.Errorf("Key2 not pruned")
	}
}
