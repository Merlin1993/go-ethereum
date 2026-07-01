package archive

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"testing"
)

func TestArchiveViaPrune(t *testing.T) {
	trie, _ := setupTrie()
	t.Logf("Trie pruning enabled: %v", trie.pruning)

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

	// 执行分片归档
	if err := archiveShardForTest(trie, 0); err != nil {
		t.Fatalf("Prune failed: %v", err)
	}

	trie.Commit()

	trie.Commit()

	// 4. 验证数据仍然可读（自动触发赎回或直接读取归档）
	for i := 0; i < 60; i++ {
		val, err := trie.Get(keys[i])
		if err != nil {
			t.Errorf("Failed to get archived key %d: %v", i, err)
		}
		expected := []byte("val")
		if !bytes.Equal(val, expected) {
			t.Errorf("Value mismatch for archived key %d: got %x, want %x", i, val, expected)
		}
	}
}

func TestArchiveItemKeyUsesVarintSuffixBits(t *testing.T) {
	suffix := []byte{0xab, 0xcd}

	key0 := archiveItemKey(0, suffix)
	key256 := archiveItemKey(256, suffix)
	if bytes.Equal(key0, key256) {
		t.Fatalf("archive item keys collided for suffix bit lengths 0 and 256")
	}

	bits, n := binary.Uvarint(key256)
	if n <= 0 {
		t.Fatalf("failed to decode suffix bit length from key")
	}
	if bits != 256 {
		t.Fatalf("decoded suffix bits mismatch: got %d, want 256", bits)
	}
	if !bytes.Equal(key256[n:], suffix) {
		t.Fatalf("suffix payload mismatch: got %x, want %x", key256[n:], suffix)
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
	if err := archiveShardForTest(trie, 0); err != nil {
		t.Fatalf("Prune failed: %v", err)
	}
	trie.Commit()

	// 2. 显式激活其中一条数据
	targetKey := keys[30]
	newValue := []byte("activated_value")

	if err := trie.Activate(targetKey, newValue); err != nil {
		t.Fatalf("Activate failed: %v", err)
	}

	// 3. 验证数据已变回热路径且值更新
	got, _ := trie.Get(targetKey)
	//hasher := NewPooledKeccakHasher()
	// 注意：Trie.Get 返回的是原始值还是哈希？
	// 在我们的实现中，Shard.Get 返回 db.Get(hash)，所以是原始值。
	if !bytes.Equal(got, newValue) {
		t.Errorf("Value not updated after activation: got %x, want %x", got, newValue)
	}

	// 4. 验证同一路径下的其余归档数据依然可查 (并未消失)
	otherKey := keys[0]
	gotOther, err := trie.Get(otherKey)
	if err != nil {
		t.Errorf("Other archived key lost: %v", err)
	}
	expectedOther := []byte("val")
	if !bytes.Equal(gotOther, expectedOther) {
		t.Errorf("Other archived key mismatch: got %x, want %x", gotOther, expectedOther)
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
	expected := []byte("val1")
	if !bytes.Equal(val, expected) {
		t.Errorf("Key1 value mismatch: got %x, want %x", val, expected)
	}
}
