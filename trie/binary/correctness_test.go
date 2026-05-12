package binary

import (
	"bytes"
	"crypto/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// MemoryDBAdapter adapts a simple map to KVStore interface for testing
type MemoryDBAdapter struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func NewMemoryDBAdapter() *MemoryDBAdapter {
	return &MemoryDBAdapter{data: make(map[string][]byte)}
}

func (db *MemoryDBAdapter) NewBatch() Batcher {
	return &MemoryBatchAdapter{db: db}
}

func (db *MemoryDBAdapter) PutBucket(hash []byte, data []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[string(hash)] = append([]byte{}, data...)
	return nil
}

func (db *MemoryDBAdapter) GetBucket(hash []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if val, ok := db.data[string(hash)]; ok {
		return append([]byte{}, val...), nil
	}
	return nil, nil
}

func (db *MemoryDBAdapter) DeleteBucket(hash []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.data, string(hash))
	return nil
}

func (db *MemoryDBAdapter) Get(key []byte) ([]byte, error) { return db.GetBucket(key) }
func (db *MemoryDBAdapter) Put(key, value []byte) error    { return db.PutBucket(key, value) }
func (db *MemoryDBAdapter) Delete(key []byte) error        { return db.DeleteBucket(key) }
func (db *MemoryDBAdapter) Has(key []byte) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, ok := db.data[string(key)]
	return ok, nil
}

type MemoryBatchAdapter struct {
	db  *MemoryDBAdapter
	ops []batchOp
}
type batchOp struct {
	isDel bool
	key   []byte
	val   []byte
}

func (b *MemoryBatchAdapter) Put(key, value []byte) error {
	b.ops = append(b.ops, batchOp{key: append([]byte{}, key...), val: append([]byte{}, value...)})
	return nil
}
func (b *MemoryBatchAdapter) Delete(key []byte) error {
	b.ops = append(b.ops, batchOp{isDel: true, key: append([]byte{}, key...)})
	return nil
}
func (b *MemoryBatchAdapter) Write() error {
	for _, op := range b.ops {
		if op.isDel {
			b.db.Delete(op.key)
		} else {
			b.db.Put(op.key, op.val)
		}
	}
	return nil
}
func (b *MemoryBatchAdapter) Reset()         { b.ops = nil }
func (b *MemoryBatchAdapter) ValueSize() int { return 0 }

func setupTrie() (*Trie, Hasher) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ArchiveDB = db // Use the same memory DB for archive for testing
	return NewTrie(nil, db, hasher, config, true), hasher
}

func TestCommitToBatchPropagatesStaleDeletesNonDestructive(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ShardDepth = 4
	config.ArchiveDB = db
	trie := NewTrie(nil, db, hasher, config, true)

	key := bytes.Repeat([]byte{0x42}, 32)
	batch := &MemoryBatchAdapter{db: db}

	if err := trie.Put(key, []byte("value-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := trie.CommitToBatch(batch, false); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	batch.Reset()

	if err := trie.Put(key, []byte("value-2")); err != nil {
		t.Fatal(err)
	}
	if _, err := trie.CommitToBatch(batch, false); err != nil {
		t.Fatal(err)
	}

	deletes := 0
	for _, op := range batch.ops {
		if op.isDel {
			deletes++
		}
	}
	if deletes == 0 {
		t.Fatalf("expected non-destructive Trie.CommitToBatch to forward stale deletes")
	}
}

func TestFlushArchivesAfterNonDestructiveCommitReload(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ShardDepth = 8
	config.ArchiveDB = db
	trie := NewTrie(nil, db, hasher, config, true)

	key := make([]byte, 32)
	key[0] = 0x11
	val := []byte("archived-after-commit")

	if err := trie.Put(key, val); err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	if _, err := trie.CommitToBatch(batch, false); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	trie.SetGlobalEpoch(0)
	if err := archiveShardForTest(trie, trie.GetShardID(key)); err != nil {
		t.Fatal(err)
	}
	batch = db.NewBatch()
	root, err := trie.CommitToBatch(batch, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := trie.FlushArchives(); err != nil {
		t.Fatal(err)
	}

	reloaded := NewTrie(root, db, hasher, config, true)
	got, err := reloaded.Get(key)
	if err != nil {
		t.Fatalf("archived key missing after reload: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value mismatch after reload: got %x, want %x", got, val)
	}
}

func TestShrinkPromotesStubList(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ArchiveDB = db
	shard, err := NewShard(0, db, hasher, config, nil, true, func() byte { return 0 })
	if err != nil {
		t.Fatal(err)
	}

	bucket := &ArchiveBucketNode{
		Path:     []byte{0x00},
		PathBits: 8,
		Count:    1,
		dirty:    true,
	}
	leaf := &LeafNode{
		Path:      []byte{0x80},
		PathBits:  1,
		ValueHash: bytes.Repeat([]byte{0x01}, 32),
		dirty:     true,
	}
	parent := &InternalNode{
		Path:     []byte{0x40}, // high bits 01
		PathBits: 2,
		Left:     leaf,
		StubList: []*ArchiveBucketNode{bucket},
		dirty:    true,
	}

	node, promoted := shard.shrinkPromote(parent)
	if len(promoted) != 1 || promoted[0] != bucket {
		t.Fatalf("expected bucket to be promoted, got %d", len(promoted))
	}
	if len(parent.StubList) != 0 {
		t.Fatalf("expected source StubList to be cleared")
	}
	shrunkLeaf, ok := node.(*LeafNode)
	if !ok {
		t.Fatalf("expected middle node to shrink away, got %T", node)
	}
	if shrunkLeaf.PathBits != 4 {
		t.Fatalf("expected leaf path to absorb parent path and branch bit, got %d bits", shrunkLeaf.PathBits)
	}
}

func archiveShardForTest(trie *Trie, shardID int) error {
	// Fresh writes start on epoch bit 1 under the rolling-epoch policy. Pruning
	// shard 0 flips the global bit inside PruneNextShard; other shards do not.
	if shardID == 0 {
		trie.SetGlobalEpoch(1)
	} else {
		trie.SetGlobalEpoch(0)
	}
	trie.pruneShardIdx = shardID
	return trie.PruneNextShard()
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
	if !bytes.Equal(got, value) {
		t.Errorf("Get returned %x, want %x", got, value)
	}

	// Update
	newValue := []byte("new value")
	if err := trie.Put(key, newValue); err != nil {
		t.Fatalf("Update Put failed: %v", err)
	}
	got, _ = trie.Get(key)
	if !bytes.Equal(got, newValue) {
		t.Errorf("Updated Get returned %x, want %x", got, newValue)
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
	if !bytes.Equal(got1, val1) {
		t.Errorf("Key1 mismatch")
	}

	got2, _ := trie.Get(key2)
	if !bytes.Equal(got2, val2) {
		t.Errorf("Key2 mismatch")
	}

	// Check if they are in the same shard (first 2 bytes)
	if trie.GetShardID(key1) != trie.GetShardID(key2) {
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
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	trie := NewTrie(nil, db, hasher, nil, true)

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
	_ = NewTrie(nil, db, hasher, nil, true)
	// Shards in our implementation are lazy. They don't load from DB until Get/Put.
	// But our NewTrie doesn't take root hashes, so shard.root will be nil.
	// Wait, the current NewTrie implementation:
	/*
		/*
			for i := 0; i < (1 << trie.config.ShardDepth); i++ {
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
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	trie := NewTrie(nil, db, hasher, nil, true)

	trie.SetGlobalEpoch(0)
	key := make([]byte, 32)
	rand.Read(key)
	val := []byte("prunable")

	trie.Put(key, val)
	trie.Commit()

	// Verify it exists in DB
	valHash := hasher.Hash(val)
	if data, _ := db.Get(valHash); len(data) == 0 {
		t.Fatalf("Data missing before prune")
	}

	// Perform shard pruning
	shardID := trie.GetShardID(key)
	err := archiveShardForTest(trie, shardID)
	if err != nil {
		t.Fatalf("Prune failed: %v", err)
	}

	// Verify data is still accessible via Get (penetrate to bucket)
	got, err := trie.Get(key)
	if err != nil {
		t.Errorf("Item lost after archiving: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Value mismatch after archiving: got %x, want %x", got, val)
	}

	// Commit to finish archiving
	trie.Commit()

	// Verify it's still accessible after commit
	gotAfter, err := trie.Get(key)
	if err != nil {
		t.Errorf("Key lost after commit in pure archiving mode: %v", err)
	}
	if !bytes.Equal(gotAfter, val) {
		t.Errorf("Value mismatch after commit: got %x, want %x", gotAfter, val)
	}
}

func TestDeleteCollapsing(t *testing.T) {
	trie, _ := setupTrie()

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
	// Delete key2. The InternalNode should be removed and the remaining leaf should be moved up.
	trie.BatchDelete(key2)

	got1, err := trie.Get(key1)
	if err != nil {
		t.Fatalf("Key1 missing after sibling delete: %v", err)
	}
	if !bytes.Equal(got1, val1) {
		t.Errorf("Value mismatch")
	}

	_, err = trie.Get(key2)
	if err != ErrNodeNotFound {
		t.Errorf("Key2 still exists")
	}
}

func TestLazyLoading(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	trie := NewTrie(nil, db, hasher, nil, true)

	key := make([]byte, 32)
	rand.Read(key)
	val := []byte("lazy")

	trie.Put(key, val)
	trie.Commit()

	shardID := trie.GetShardID(key)
	shard := trie.shards[shardID]
	rootHash, _ := shard.Hash()
	shard.root = nil

	// Manually trigger loading via NewShard and re-assigning
	s2, _ := NewShard(shardID, db, hasher, trie.config, rootHash, true, func() byte { return 0 })
	trie.shards[shardID] = s2

	got, err := trie.Get(key)
	if err != nil {
		t.Fatalf("Get failed after manual reload: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Value mismatch: got %x, want %x", got, val)
	}
}

func TestSubtreePruning(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	trie := NewTrie(nil, db, hasher, nil, true)

	trie.SetGlobalEpoch(0)

	// Create two keys that share a common internal node (split at bit 17)
	key1 := make([]byte, 32)
	key1[2] = 0x00 // Bit 16=0, 17=0
	val1 := []byte("val1")

	key2 := make([]byte, 32)
	key2[2] = 0x40 // Bit 16=0, 17=1
	val2 := []byte("val2")

	trie.Put(key1, val1)
	trie.Put(key2, val2)
	trie.Commit()

	shardID := int(key1[0])<<8 | int(key1[1])
	err := archiveShardForTest(trie, shardID)
	if err != nil {
		t.Fatalf("Prune failed: %v", err)
	}

	trie.Commit()

	// Verify data is still accessible
	if v, _ := trie.Get(key1); !bytes.Equal(v, val1) {
		t.Errorf("Key1 mismatch: got %x, want %x", v, val1)
	}
	if v, _ := trie.Get(key2); !bytes.Equal(v, val2) {
		t.Errorf("Key2 mismatch: got %x, want %x", v, val2)
	}
}

// TestCrossShardOperations verifies multi-shard writes and global Commit
func TestCrossShardOperations(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	trie := NewTrie(nil, db, hasher, nil, true)

	key1 := make([]byte, 32)
	key1[0], key1[1] = 0x00, 0x00
	val1 := []byte("shard-0")

	key2 := make([]byte, 32)
	key2[0], key2[1] = 0xFF, 0xFF
	val2 := []byte("shard-65535")

	trie.Put(key1, val1)
	trie.Put(key2, val2)

	root, err := trie.Commit()
	if err != nil {
		t.Fatalf("Global commit failed: %v", err)
	}
	if len(root) == 0 {
		t.Fatal("Root hash is empty")
	}

	got1, _ := trie.Get(key1)
	if !bytes.Equal(got1, val1) {
		t.Errorf("Value 1 mismatch")
	}

	got2, _ := trie.Get(key2)
	if !bytes.Equal(got2, val2) {
		t.Errorf("Value 2 mismatch")
	}
}

// TestCustomConfig verifies routing logic under non-default shard depth
func TestCustomConfig(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()

	// Shard depth 8 (256 shards)
	config := &Config{
		ShardDepth:        8,
		ArchiveBucketSize: 10,
	}
	trie := NewTrie(nil, db, hasher, config, true)

	key := make([]byte, 32)
	key[0] = 0xAA
	val := []byte("custom-shard-depth")

	trie.Put(key, val)

	shardID := int(key[0])
	if trie.shards[shardID] == nil {
		t.Errorf("Shard %d should be initialized", shardID)
	}

	got, _ := trie.Get(key)
	if !bytes.Equal(got, val) {
		t.Errorf("Value mismatch with custom config")
	}
}

// TestArchiveBucketSplitAndMovement verifies archive bucket split and depth movement
func TestArchiveBucketSplitAndMovement(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()

	// Very small bucket size to trigger split
	config := &Config{
		ShardDepth:        16,
		ArchiveBucketSize: 2,
		ArchiveDB:         db,
	}
	trie := NewTrie(nil, db, hasher, config, true)

	keyBase := make([]byte, 32)
	keyBase[0], keyBase[1] = 0x00, 0x00 // Shard 0

	keys := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		keys[i] = append([]byte{}, keyBase...)
		keys[i][2] = 0x00 // common bits
		keys[i][3] = byte(i)
		trie.Put(keys[i], []byte(string(rune('a'+i))))
	}

	trie.SetGlobalEpoch(0)
	trie.Commit()
	trie.FlushArchives()

	// Switch epoch and trigger archiving
	if err := archiveShardForTest(trie, 0); err != nil {
		t.Fatalf("Prune failed: %v", err)
	}
	trie.Commit()
	trie.FlushArchives()

	// Verify stats before reads, since Get automatically activates archived data.
	stats := trie.Stats()
	if stats.BucketCount < 2 {
		t.Errorf("Expected at least 2 buckets, got %d", stats.BucketCount)
	}
	if stats.ArchivedDataSize != 3 {
		t.Errorf("Expected 3 archived items, got %d", stats.ArchivedDataSize)
	}

	// Verify data is still accessible via Get (penetrate bucket search)
	for i := 0; i < 3; i++ {
		val := []byte(string(rune('a' + i)))
		got, err := trie.Get(keys[i])
		if err != nil {
			t.Errorf("Key %d lost after archiving: %v", i, err)
		}
		if !bytes.Equal(got, val) {
			t.Errorf("Value %d mismatch: got %x, want %x", i, got, val)
		}
	}
}

// TestDataActivation verifies Activate moves archived data back to hot
func TestDataActivation(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ArchiveDB = db
	trie := NewTrie(nil, db, hasher, config, true)

	key := make([]byte, 32)
	copy(key, []byte{0x00, 0x00, 0x01})
	val := []byte("to-be-archived")

	// 1. Write and archive
	trie.Put(key, val)
	trie.SetGlobalEpoch(0)
	trie.Commit()
	trie.FlushArchives()
	if err := archiveShardForTest(trie, 0); err != nil {
		t.Fatalf("Prune failed: %v", err)
	}
	trie.Commit()
	trie.FlushArchives()

	// Verify it's archived
	statsBefore := trie.Stats()
	if statsBefore.ArchivedDataSize != 1 {
		t.Fatalf("Expected 1 archived item, got %d", statsBefore.ArchivedDataSize)
	}

	// 2. Perform activation
	newVal := []byte("activated-and-updated")
	shardID := trie.GetShardID(key)
	err := trie.shards[shardID].Activate(key, newVal)
	if err != nil {
		t.Fatalf("Activate failed: %v", err)
	}
	trie.Commit()
	trie.FlushArchives()

	// 3. Verify archived count dropped
	statsAfter := trie.Stats()
	if statsAfter.ArchivedDataSize != 0 {
		t.Errorf("Expected 0 archived items after activation, got %d", statsAfter.ArchivedDataSize)
	}

	// Verify value updated and reachable
	got, _ := trie.Get(key)
	if !bytes.Equal(got, newVal) {
		t.Errorf("Value mismatch after activation: got %x, want %x", got, newVal)
	}
}

func TestPutRemovesArchivedVersion(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ShardDepth = 8
	config.ArchiveDB = db
	trie := NewTrie(nil, db, hasher, config, true)

	key := make([]byte, 32)
	key[0] = 0x00
	key[1] = 0x11
	oldVal := []byte("archived-value")
	newVal := []byte("rewritten-hot")

	if err := trie.Put(key, oldVal); err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	if _, err := trie.CommitToBatch(batch, false); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	if err := archiveShardForTest(trie, trie.GetShardID(key)); err != nil {
		t.Fatal(err)
	}
	batch = db.NewBatch()
	root, err := trie.CommitToBatch(batch, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := trie.FlushArchives(); err != nil {
		t.Fatal(err)
	}

	archived := NewTrie(root, db, hasher, config, true)
	stats := archived.Stats()
	if stats.ArchivedDataSize != 1 {
		t.Fatalf("expected archived item before rewrite, got %d", stats.ArchivedDataSize)
	}

	if err := trie.Put(key, newVal); err != nil {
		t.Fatal(err)
	}
	batch = db.NewBatch()
	root, err = trie.CommitToBatch(batch, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := trie.FlushArchives(); err != nil {
		t.Fatal(err)
	}

	reloaded := NewTrie(root, db, hasher, config, true)
	stats = reloaded.Stats()
	if stats.ArchivedDataSize != 0 {
		t.Fatalf("expected archived item to be removed after Put, got %d", stats.ArchivedDataSize)
	}
	if stats.BucketCount != 0 {
		t.Fatalf("expected no archive buckets after Put rewrite, got %d", stats.BucketCount)
	}

	got, err := reloaded.Get(key)
	if err != nil {
		t.Fatalf("reloaded get failed: %v", err)
	}
	if !bytes.Equal(got, newVal) {
		t.Fatalf("value mismatch after rewrite: got %x, want %x", got, newVal)
	}
}

// TestTrieStatistics verifies accuracy of stats
func TestTrieStatistics(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ShardDepth = 16
	config.ArchiveDB = db
	trie := NewTrie(nil, db, hasher, config, true)

	// Write some hot data
	for i := 0; i < 5; i++ {
		key := make([]byte, 32)
		key[0], key[1] = 0x01, 0x02
		key[2] = byte(i)
		trie.Put(key, []byte("data"))
	}

	stats := trie.Stats()
	if stats.ArchivedDataSize != 0 {
		t.Errorf("Expected 0 archived data, got %d", stats.ArchivedDataSize)
	}

	// Archive the data
	trie.SetGlobalEpoch(0)
	trie.Commit()
	trie.FlushArchives()
	if err := archiveShardForTest(trie, 0x0102); err != nil {
		t.Fatalf("Prune failed: %v", err)
	}
	trie.Commit()
	trie.FlushArchives()

	stats = trie.Stats()
	if stats.ArchivedDataSize != 5 {
		t.Errorf("Expected 5 archived items, got %d", stats.ArchivedDataSize)
	}
	if stats.BucketCount == 0 {
		t.Error("Expected at least one bucket")
	}
}

func TestFullTrieReload(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	trie := NewTrie(nil, db, hasher, nil, true)

	key := make([]byte, 32)
	rand.Read(key)
	val := []byte("reloadable")

	trie.Put(key, val)
	root, err := trie.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Fresh instance load
	trie2 := NewTrie(root, db, hasher, nil, true)

	got, err := trie2.Get(key)
	if err != nil {
		t.Fatalf("Get failed after reload: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Value mismatch after reload: got %x, want %x", got, val)
	}
}

func TestAutomaticRedemption(t *testing.T) {
	// 1. 设置 Trie 并插入数据
	trie, _ := setupTrie()

	key := make([]byte, 32)
	rand.Read(key)
	// 强制落在 Shard 0，方便测试 Prune
	key[0], key[1] = 0x00, 0x00
	val := []byte("automatic-redemption-test")

	trie.Put(key, val)
	trie.SetGlobalEpoch(0)
	trie.Commit()
	trie.FlushArchives()

	// 2. 增加 Epoch 并执行剪枝，将数据转入归档桶
	if err := archiveShardForTest(trie, 0); err != nil {
		t.Fatalf("Prune failed: %v", err)
	}
	trie.Commit()
	trie.FlushArchives()

	// 验证数据已在归档中
	stats := trie.Stats()
	if stats.ArchivedDataSize != 1 {
		t.Fatalf("Expected 1 archived item, got %d", stats.ArchivedDataSize)
	}

	// 记录初始计数器
	initialMissExistent := atomic.LoadInt64(&common.BinaryMissExistentCount)

	// 3. 执行 Get 操作，应该触发自动赎回
	got, err := trie.Get(key)
	if err != nil {
		t.Fatalf("First Get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Value mismatch: got %x, want %x", got, val)
	}

	// 验证第一次 Get 记录为 MissExistent (因为是从归档中找回的)
	if atomic.LoadInt64(&common.BinaryMissExistentCount) != initialMissExistent+1 {
		t.Errorf("Expected BinaryMissExistentCount to increment on first Get from archive")
	}

	// 4. 验证数据已回热路径
	statsAfter := trie.Stats()
	if statsAfter.ArchivedDataSize != 0 {
		t.Errorf("Expected 0 archived items after automatic activation, got %d", statsAfter.ArchivedDataSize)
	}

	// 5. 验证第二次 Get 是热路径命中 (Hit)
	atomic.StoreInt64(&common.BinaryHitCount, 0)
	got2, err := trie.Get(key)
	if err != nil {
		t.Fatalf("Second Get failed: %v", err)
	}
	if !bytes.Equal(got2, val) {
		t.Errorf("Value mismatch on second Get")
	}

	if atomic.LoadInt64(&common.BinaryHitCount) != 1 {
		t.Errorf("Expected BinaryHitCount to be 1 on second Get, got %d", atomic.LoadInt64(&common.BinaryHitCount))
	}
}
