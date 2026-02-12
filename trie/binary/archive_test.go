package binary

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestArchiveViaPrune(t *testing.T) {
	trie, _ := setupTrie()

	// 插入 60 条处于 Epoch 0 的数据
	keys := make([][]byte, 60)
	for i := 0; i < 60; i++ {
		keys[i] = make([]byte, 32)
		rand.Read(keys[i])
		keys[i][0] = 0x00
		keys[i][1] = 0x00
		trie.Put(keys[i], []byte("val"))
	}
	trie.Commit()

	// 增加 Epoch
	// 原逻辑：global 初始 0，叶子 epoch 1。
	// 现在：Prune(global) 会删除/归档 bit0 == global 的项。
	// global 此时为 0。
	trie.SetGlobalEpoch(1)
	trie.Commit()
	trie.FlushArchives()

	// 执行分片归档
	trie.pruneShardIdx = 0
	err := trie.PruneNextShard()
	if err != nil {
		t.Fatalf("Prune failed: %v", err)
	}
	trie.Commit()
	trie.FlushArchives()

	// 验证 Get 依然能搜到数据 (穿透 StubList)
	for i := 0; i < 60; i++ {
		val, err := trie.Get(keys[i])
		if err != nil {
			t.Errorf("Failed to get archived key %d: %v", i, err)
		}
		if len(val) == 0 {
			t.Errorf("Empty value for archived key %d", i)
		}
	}
}

func TestExplicitActivate(t *testing.T) {
	trie, _ := setupTrie()

	// 1. 创建归档桶
	keys := make([][]byte, 60)
	for i := 0; i < 60; i++ {
		keys[i] = make([]byte, 32)
		rand.Read(keys[i])
		keys[i][0] = 0x00
		keys[i][1] = 0x00
		trie.Put(keys[i], []byte("val"))
	}
	trie.Commit()
	trie.FlushArchives()
	trie.SetGlobalEpoch(1)
	trie.pruneShardIdx = 0
	trie.PruneNextShard()
	trie.Commit()
	trie.FlushArchives()

	// 2. 显式激活其中一条数据
	targetKey := keys[30]
	newValue := []byte("activated_value")

	if err := trie.Activate(targetKey, newValue); err != nil {
		t.Fatalf("Activate failed: %v", err)
	}

	// 3. 验证数据已变回热路径且值更新
	got, _ := trie.Get(targetKey)
	hasher := NewPooledKeccakHasher()
	if !bytes.Equal(got, hasher.Hash(newValue)) {
		t.Errorf("Value not updated after activation")
	}

	// 4. 验证同一路径下的其余归档数据依然可查 (并未消失)
	otherKey := keys[0]
	gotOther, err := trie.Get(otherKey)
	if err != nil {
		t.Errorf("Other archived key lost: %v", err)
	}
	if len(gotOther) == 0 {
		t.Errorf("Empty other value")
	}
}

func TestAggressiveShrinkage(t *testing.T) {
	trie, _ := setupTrie()

	key1 := make([]byte, 32)
	key1[2] = 0x00 // Bit 16=0
	key1[3] = 0x00 // Bit 24=0
	trie.Put(key1, []byte("val1"))

	key2 := make([]byte, 32)
	key2[2] = 0x00 // Bit 16=0
	key2[3] = 0x80 // Bit 24=1
	trie.Put(key2, []byte("val2"))

	trie.Commit()

	// 删除 key2，触发收缩
	if err := trie.BatchDelete(key2); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 验证 key1 依然可达
	val, err := trie.Get(key1)
	if err != nil {
		t.Fatalf("Key1 missing after shrinkage: %v", err)
	}
	if len(val) == 0 {
		t.Errorf("Empty val1")
	}
}

func TestArchiveDelete(t *testing.T) {
	trie, _ := setupTrie()
	hasher := NewPooledKeccakHasher()

	// 1. 正常写入并归档
	key := make([]byte, 32)
	copy(key, []byte{0x00, 0x00, 0x01})
	val := []byte("to-be-deleted-from-archive")
	trie.Put(key, val)
	trie.SetGlobalEpoch(0)
	trie.Commit()
	trie.FlushArchives()
	trie.SetGlobalEpoch(1)
	shardID := int(key[0])<<8 | int(key[1])
	trie.pruneShardIdx = shardID
	trie.PruneNextShard()
	trie.Commit()
	trie.FlushArchives()

	// 确认它现在在归档中
	got, err := trie.Get(key)
	if err != nil {
		t.Fatalf("Data missing in archive before delete: %v", err)
	}
	if !bytes.Equal(got, hasher.Hash(val)) {
		t.Fatalf("Incorrect data in archive")
	}

	// 2. 执行删除
	if err := trie.BatchDelete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	trie.Commit()
	trie.FlushArchives()

	// 3. 验证数据已消失  c
	_, err = trie.Get(key)
	if err != ErrNodeNotFound {
		t.Errorf("Data still exists in archive after delete: %v", err)
	}

	// 验证统计信息
	stats := trie.Stats()
	if stats.ArchivedDataSize != 0 {
		t.Errorf("Expected 0 archived data, got %d", stats.ArchivedDataSize)
	}
}

func TestBlindDelete(t *testing.T) {
	trie, _ := setupTrie()

	// 1. 正常写入并归档
	key := make([]byte, 32)
	copy(key, []byte{0x00, 0x00, 0x01})
	val := []byte("blind-delete-test")
	trie.Put(key, val)
	trie.SetGlobalEpoch(0)
	trie.Commit()
	trie.FlushArchives()
	trie.SetGlobalEpoch(1)
	shardID := int(key[0])<<8 | int(key[1])
	trie.pruneShardIdx = shardID
	trie.PruneNextShard()
	trie.Commit()
	trie.FlushArchives()

	// 2. 在不进行 Flush 的情况下执行删除（触发盲删除）
	if err := trie.BatchDelete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 验证：此时 Get 应当已经搜不到数据 (通过对 pendingDeletes 的内存过滤)
	_, err := trie.Get(key)
	if err != ErrNodeNotFound {
		t.Errorf("Data should be invisible after blind delete even before flush")
	}

	// 验证统计信息应该是 0 (因为盲删除会增量扣减计数)
	stats := trie.Stats()
	if stats.ArchivedDataSize != 0 {
		t.Errorf("Expected 0 archived data (blind), got %d", stats.ArchivedDataSize)
	}

	// 3. 执行 Flush，观察是否真正从 DB 移除
	trie.Commit()
	trie.FlushArchives()

	// 最终验证
	_, err = trie.Get(key)
	if err != ErrNodeNotFound {
		t.Errorf("Data still exists after final flush")
	}
}
