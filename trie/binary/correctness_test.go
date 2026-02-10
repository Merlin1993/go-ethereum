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

func (db *MemoryDBAdapter) PutBucket(hash []byte, data []byte) error {
	return db.Put(hash, data)
}

func (db *MemoryDBAdapter) GetBucket(hash []byte) ([]byte, error) {
	return db.Get(hash)
}

func (db *MemoryDBAdapter) DeleteBucket(hash []byte) error {
	return db.Delete(hash)
}

func setupTrie() (*Trie, Hasher) {
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ArchiveDB = db // Use the same memory DB for archive for testing
	return NewTrie(db, hasher, config, true), hasher
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
	trie := NewTrie(db, hasher, nil, true)

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
	_ = NewTrie(db, hasher, nil, true)
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
	trie := NewTrie(db, hasher, nil, true)

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

	// 执行分片归档
	shardID := int(key[0])<<8 | int(key[1])
	trie.pruneShardIdx = shardID
	err := trie.PruneNextShard()
	if err != nil {
		t.Fatalf("Prune failed: %v", err)
	}

	// [验证点升级]：归档后数据依然可由 Get 获取 (穿透桶)
	got, err := trie.Get(key)
	if err != nil {
		t.Errorf("Item lost after archiving: %v", err)
	}
	if !bytes.Equal(got, valHash) {
		t.Errorf("Value mismatch after archiving")
	}

	// Commit to finish archiving
	trie.Commit()

	// [验证点升级]：即便 Commit 后，由于是归档模式，数据也绝不会“消失”
	_, err = trie.Get(key)
	if err != nil {
		t.Errorf("Key lost after commit in pure archiving mode: %v", err)
	}
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
	trie := NewTrie(db, hasher, nil, true)

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
	s2, _ := NewShard(shardID, db, hasher, trie.config, rootHash, true, func() byte { return 0 })
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
	trie := NewTrie(db, hasher, nil, true)

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
	err := trie.PruneNextShard()
	if err != nil {
		t.Fatalf("Prune failed: %v", err)
	}

	trie.Commit()

	// [验证点升级]：子树归档后，原数据依然可达
	if _, err := trie.Get(key1); err != nil {
		t.Errorf("Key1 lost after subtree archiving: %v", err)
	}
	if _, err := trie.Get(key2); err != nil {
		t.Errorf("Key2 lost after subtree archiving: %v", err)
	}
}

// TestCrossShardOperations 验证跨多个分片的数据写入和全局 Commit
func TestCrossShardOperations(t *testing.T) {
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()
	// 使用默认配置 (16位分片深度)
	trie := NewTrie(db, hasher, nil, true)

	// 构造分片 0x0000 和 0xFFFF 的 Key
	key1 := make([]byte, 32)
	key1[0], key1[1] = 0x00, 0x00
	val1 := []byte("shard-0")

	key2 := make([]byte, 32)
	key2[0], key2[1] = 0xFF, 0xFF
	val2 := []byte("shard-65535")

	trie.Put(key1, val1)
	trie.Put(key2, val2)

	// 执行全局提交
	root, err := trie.Commit()
	if err != nil {
		t.Fatalf("Global commit failed: %v", err)
	}
	if len(root) == 0 {
		t.Fatal("Root hash is empty")
	}

	// 验证数据在分片中正确存储
	got1, _ := trie.Get(key1)
	if !bytes.Equal(got1, hasher.Hash(val1)) {
		t.Errorf("Value 1 mismatch")
	}

	got2, _ := trie.Get(key2)
	if !bytes.Equal(got2, hasher.Hash(val2)) {
		t.Errorf("Value 2 mismatch")
	}
}

// TestCustomConfig 验证非默认分片深度下的路由逻辑
func TestCustomConfig(t *testing.T) {
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()

	// 设置分片深度为 8 (256个分片)
	config := &Config{
		ShardDepth:        8,
		ArchiveBucketSize: 10,
	}
	trie := NewTrie(db, hasher, config, true)

	// 构造分片 0xAA 的 Key
	key := make([]byte, 32)
	key[0] = 0xAA
	val := []byte("custom-shard-depth")

	trie.Put(key, val)

	// 检查内部路由是否正确
	shardID := int(key[0])
	if trie.shards[shardID] == nil {
		t.Errorf("Shard %d should be initialized", shardID)
	}

	got, _ := trie.Get(key)
	if !bytes.Equal(got, hasher.Hash(val)) {
		t.Errorf("Value mismatch with custom config")
	}
}

// TestArchiveBucketSplitAndMovement 验证归档桶分裂与深度移动
func TestArchiveBucketSplitAndMovement(t *testing.T) {
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()

	// 设置非常小的桶大小，以便触发分裂
	config := &Config{
		ShardDepth:        16,
		ArchiveBucketSize: 2,
		ArchiveDB:         db,
	}
	trie := NewTrie(db, hasher, config, true)

	// 构造 3 个共享长前缀的 Key (位 16 之后的前缀相同)
	// 这样它们在归档时会根据 ArchiveBucketSize=2 触发分裂
	keyBase := make([]byte, 32)
	keyBase[0], keyBase[1] = 0x00, 0x00 // Shard 0

	keys := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		keys[i] = append([]byte{}, keyBase...)
		// 让他们在 bit 16, 17 相同，但在后面不同
		keys[i][2] = 0x00 // bit 16=0, 17=0
		keys[i][3] = byte(i)
		trie.Put(keys[i], []byte(string(rune('a'+i))))
	}

	trie.SetGlobalEpoch(0)
	trie.Commit() // 确保 epoch 正确设定
	trie.FlushArchives()

	// 切换 Epoch 并触发归档
	trie.SetGlobalEpoch(1)
	trie.pruneShardIdx = 0
	trie.PruneNextShard()
	trie.Commit()
	trie.FlushArchives()

	// 验证数据依然可通过 Get 获取（穿透桶查找）
	for i := 0; i < 3; i++ {
		valHash := hasher.Hash([]byte(string(rune('a' + i))))
		got, err := trie.Get(keys[i])
		if err != nil {
			t.Errorf("Key %d lost after archiving: %v", i, err)
		}
		if !bytes.Equal(got, valHash) {
			t.Errorf("Value %d mismatch", i)
		}
	}

	// 验证统计信息，确保产生了多个桶（因为总数3 > 限制2）
	stats := trie.Stats()
	if stats.BucketCount < 2 {
		t.Errorf("Expected at least 2 buckets, got %d", stats.BucketCount)
	}
	if stats.ArchivedDataSize != 3 {
		t.Errorf("Expected 3 archived items, got %d", stats.ArchivedDataSize)
	}
}

// TestDataActivation 验证 Activate 将归档数据转为热数据
func TestDataActivation(t *testing.T) {
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ArchiveDB = db
	trie := NewTrie(db, hasher, config, true)

	key := make([]byte, 32)
	copy(key, []byte{0x00, 0x00, 0x01})
	val := []byte("to-be-archived")

	// 1. 正常写入并归档
	trie.Put(key, val)
	trie.SetGlobalEpoch(0)
	trie.Commit()
	trie.FlushArchives()
	trie.SetGlobalEpoch(1)
	trie.pruneShardIdx = 0
	trie.PruneNextShard()
	trie.Commit()
	trie.FlushArchives()

	// 确认它现在在归档桶中 (通过调查 Stats)
	statsBefore := trie.Stats()
	if statsBefore.ArchivedDataSize != 1 {
		t.Fatalf("Expected 1 archived item, got %d", statsBefore.ArchivedDataSize)
	}

	// 2. 执行激活
	newVal := []byte("activated-and-updated")
	shardID := int(key[0])<<8 | int(key[1])
	err := trie.shards[shardID].Activate(key, newVal)
	if err != nil {
		t.Fatalf("Activate failed: %v", err)
	}
	trie.Commit()
	trie.FlushArchives()

	// 3. 验证统计信息，归档项应移除
	statsAfter := trie.Stats()
	if statsAfter.ArchivedDataSize != 0 {
		t.Errorf("Expected 0 archived items after activation, got %d", statsAfter.ArchivedDataSize)
	}

	// 验证值已更新且可正常读取
	got, _ := trie.Get(key)
	if !bytes.Equal(got, hasher.Hash(newVal)) {
		t.Errorf("Value mismatch after activation")
	}
}

// TestTrieStatistics 验证统计信息的准确性
func TestTrieStatistics(t *testing.T) {
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ArchiveDB = db
	trie := NewTrie(db, hasher, config, true)

	// 写入一些热数据
	for i := 0; i < 5; i++ {
		key := make([]byte, 32)
		key[0], key[1] = 0x01, 0x02 // 全部在分片 0x0102
		key[2] = byte(i)
		trie.Put(key, []byte("data"))
	}

	stats := trie.Stats()
	if stats.ArchivedDataSize != 0 {
		t.Errorf("Expected 0 archived data, got %d", stats.ArchivedDataSize)
	}

	// 归档这些数据
	trie.SetGlobalEpoch(0)
	trie.Commit()
	trie.FlushArchives()
	trie.SetGlobalEpoch(1)
	trie.pruneShardIdx = 0x0102
	trie.PruneNextShard()
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
