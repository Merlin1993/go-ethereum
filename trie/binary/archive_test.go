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
	trie.SetGlobalEpoch(0)

	// 执行分片归档
	trie.pruneShardIdx = 0
	err := trie.PruneNextShard()
	if err != nil {
		t.Fatalf("Prune failed: %v", err)
	}

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
		keys[i][2] = 0x00
		trie.Put(keys[i], []byte("val"))
	}
	trie.Commit()
	trie.SetGlobalEpoch(0)
	trie.pruneShardIdx = 0
	trie.PruneNextShard()

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
