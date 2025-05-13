// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

// Debugging helper function to dump trie structure
func dumpTrie(t *testing.T, trie *CacheTrie) {
	if trie.root == nil {
		t.Log("Empty trie")
		return
	}
	t.Logf("Trie Root: %v", trie.root)
}

// Tests that an empty trie is not nil.
func TestCacheTrieEmptyTrie(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie, err := NewCacheTrie(TrieID(common.Hash{}), db)
	if err != nil {
		t.Fatalf("failed to create empty cache trie: %v", err)
	}
	if trie == nil {
		t.Error("NewCacheTrie returned nil")
	}
}

// Tests that a new empty cache trie can be created.
func TestNewEmptyCacheTrie(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)
	if trie == nil {
		t.Error("NewEmptyCacheTrie returned nil")
	}
}

// Tests that a CacheTrie can be copied.
func TestCacheTrieCopy(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)

	copy := trie.Copy()
	if copy == nil {
		t.Error("Copy returned nil")
	}

	if copy == trie {
		t.Error("Copy returned the same instance")
	}
}

// Helper to convert key to hex and print for debugging
func debugKey(key []byte) string {
	hexKey := keybytesToHex(key)
	return fmt.Sprintf("Key: %q, Hex: %v", key, hexKey)
}

// Tests basic operations with CacheTrie
func TestCacheTrieOperations(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)

	// 设置缓存参数以便测试
	trie.SetCacheParams(0, 1)
	trie.SetBlockNum(1)

	// Insert some values and debug
	t.Log("Inserting key1")
	t.Log(debugKey([]byte("key1")))
	err := trie.Update([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Update key1 failed: %v", err)
	}
	dumpTrie(t, trie)

	// 测试Get方法
	t.Log("Testing Get method for key1")
	val1, err := trie.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	t.Logf("Get key1 result: %q", val1)
	if string(val1) != "value1" {
		t.Errorf("Wrong value for key1: got %q, want %q", string(val1), "value1")
	}

	t.Log("Inserting key2")
	t.Log(debugKey([]byte("key2")))
	err = trie.Update([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Update key2 failed: %v", err)
	}
	dumpTrie(t, trie)

	// 测试Get方法
	t.Log("Testing Get method for key2")
	val2, err := trie.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	t.Logf("Get key2 result: %q", val2)
	if string(val2) != "value2" {
		t.Errorf("Wrong value for key2: got %q, want %q", string(val2), "value2")
	}

	t.Log("Inserting key3")
	t.Log(debugKey([]byte("key3")))
	err = trie.Update([]byte("key3"), []byte("value3"))
	if err != nil {
		t.Fatalf("Update key3 failed: %v", err)
	}
	dumpTrie(t, trie)

	// 测试Get方法
	t.Log("Testing Get method for key3")
	val3, err := trie.Get([]byte("key3"))
	if err != nil {
		t.Fatalf("Get key3 failed: %v", err)
	}
	t.Logf("Get key3 result: %q", val3)
	if string(val3) != "value3" {
		t.Errorf("Wrong value for key3: got %q, want %q", string(val3), "value3")
	}

	// Delete a key
	t.Log("Deleting key2")
	err = trie.Delete([]byte("key2"))
	if err != nil {
		t.Fatalf("Delete key2 failed: %v", err)
	}
	dumpTrie(t, trie)

	// 检查删除是否成功
	t.Log("Testing if key2 was properly deleted")
	deletedVal, err := trie.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 after delete failed: %v", err)
	}
	t.Logf("Get key2 result after delete: %v", deletedVal)
	if deletedVal != nil {
		t.Errorf("Expected nil after delete, got: %q", string(deletedVal))
	}

	// Update key1
	t.Log("Updating key1 with new-value1")
	err = trie.Update([]byte("key1"), []byte("new-value1"))
	if err != nil {
		t.Fatalf("Update key1 with new value failed: %v", err)
	}
	dumpTrie(t, trie)

	// 检查更新后的值
	t.Log("Testing if key1 was properly updated")
	updatedVal, err := trie.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 after update failed: %v", err)
	}
	t.Logf("Get key1 result after update: %q", updatedVal)
	if string(updatedVal) != "new-value1" {
		t.Errorf("Wrong value after update: got %q, want %q", string(updatedVal), "new-value1")
	}

	// Hash should not be empty
	root := trie.Hash()
	t.Logf("Trie hash: %x", root)
	if root == (common.Hash{}) {
		t.Error("Hash returned empty hash")
	}

	// Commit changes
	hash, nodes := trie.Commit(false)
	t.Logf("Commit hash: %x, nodes: %v", hash, nodes)
	if hash == (common.Hash{}) {
		t.Error("Commit returned empty hash")
	}
	if nodes == nil {
		t.Error("Commit returned nil nodes")
	}

	// After commit, trie should be unusable
	_, err = trie.Get([]byte("key1"))
	if err != ErrCommitted {
		t.Errorf("expected ErrCommitted, got %v", err)
	}
}

// TestCacheTrieWithBlocks 测试CacheTrie在多个区块高度下的缓存功能
func TestCacheTrieWithBlocks(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)

	// 设置缓存参数：从区块10开始，每个位存储1个区块
	trie.SetCacheParams(10, 1)

	// 在区块11更新key1
	trie.SetBlockNum(11)
	err := trie.Update([]byte("key1"), []byte("block11-value1"))
	if err != nil {
		t.Fatalf("Update at block 11 failed: %v", err)
	}

	// 手动检查内部节点的window位
	node, err := trie.getNodeForPath([]byte("key1"))
	if err != nil {
		t.Fatalf("getNodeForPath key1 failed: %v", err)
	}
	if node == nil {
		t.Fatal("node for key1 is nil")
	}
	window := trie.getNodeWindow(node)
	expected := 1 << ((11 - 10) / 1) // 应该是第1位

	t.Logf("key1 window at block 11: %032b, expected: %032b", window, expected)
	if window != expected {
		t.Errorf("Wrong window for key1: got %032b, expected %032b", window, expected)
	}

	// 在区块15更新key1
	trie.SetBlockNum(15)
	err = trie.Update([]byte("key1"), []byte("block15-value1"))
	if err != nil {
		t.Fatalf("Update at block 15 failed: %v", err)
	}

	// 获取区块15的值
	value, err := trie.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get at block 15 failed: %v", err)
	}
	if string(value) != "block15-value1" {
		t.Errorf("Expected 'block15-value1', got '%s'", string(value))
	}

	// 检查window位
	node, _ = trie.getNodeForPath([]byte("key1"))
	window = trie.getNodeWindow(node)
	expected = 1 << ((15 - 10) / 1) // 应该是第5位

	t.Logf("key1 window at block 15: %032b, expected: %032b", window, expected)
	if window != expected {
		t.Errorf("Wrong window for key1: got %032b, expected %032b", window, expected)
	}

	// 提交变更
	hash, nodes := trie.Commit(false)
	if hash == (common.Hash{}) {
		t.Error("Commit returned empty hash")
	}
	if nodes == nil {
		t.Error("Commit returned nil nodes")
	}
}

// TestCacheTrieWindowUpdate 测试节点window字段的更新
func TestCacheTrieWindowUpdate(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)

	// 设置缓存参数：从区块100开始，每个位存储2个区块
	trie.SetCacheParams(100, 2)

	// 在区块102插入第一个键值对
	trie.SetBlockNum(102)
	err := trie.Update([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Update key1 failed: %v", err)
	}

	// 在区块104插入第二个键值对
	trie.SetBlockNum(104)
	err = trie.Update([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Update key2 failed: %v", err)
	}

	// 检查window位的设置
	node1, err := trie.getNodeForPath([]byte("key1"))
	if err != nil {
		t.Fatalf("getNodeForPath key1 failed: %v", err)
	}
	window1 := trie.getNodeWindow(node1)

	node2, err := trie.getNodeForPath([]byte("key2"))
	if err != nil {
		t.Fatalf("getNodeForPath key2 failed: %v", err)
	}
	window2 := trie.getNodeWindow(node2)

	// 计算预期的window值
	expectedWindow1 := 1 << ((102 - 100) / 2) // 区块102对应第1位
	expectedWindow2 := 1 << ((104 - 100) / 2) // 区块104对应第2位

	t.Logf("key1 window: %032b, expected: %032b", window1, expectedWindow1)
	t.Logf("key2 window: %032b, expected: %032b", window2, expectedWindow2)

	if window1 != expectedWindow1 {
		t.Errorf("Wrong window for key1: got %032b, expected %032b", window1, expectedWindow1)
	}

	if window2 != expectedWindow2 {
		t.Errorf("Wrong window for key2: got %032b, expected %032b", window2, expectedWindow2)
	}

	// 提交更改，并验证根节点window是两个子节点window的XOR
	hash, _ := trie.Commit(false)
	t.Logf("Commit hash: %x", hash)
}

// TestCacheTrieTombstone 测试删除操作创建墓碑标记而不是真正删除
func TestCacheTrieTombstone(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)

	// 设置缓存参数
	trie.SetCacheParams(50, 1)
	trie.SetBlockNum(55)

	// 先插入一个键值对
	err := trie.Update([]byte("deleteMe"), []byte("originalValue"))
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// 获取初始值
	value, err := trie.Get([]byte("deleteMe"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	t.Logf("Initial value: %s", string(value))

	// 检查window位
	node, err := trie.getNodeForPath([]byte("deleteMe"))
	if err != nil {
		t.Fatalf("getNodeForPath failed: %v", err)
	}
	if node == nil {
		t.Fatal("node is nil")
	}
	window55 := trie.getNodeWindow(node)
	expected55 := 1 << ((55 - 50) / 1) // 区块55对应第5位

	t.Logf("window at block 55: %032b, expected: %032b", window55, expected55)
	if window55 != expected55 {
		t.Errorf("Wrong window: got %032b, expected %032b", window55, expected55)
	}

	// "删除"这个键
	trie.SetBlockNum(60)
	err = trie.Delete([]byte("deleteMe"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 再次获取，应该返回nil（因为是墓碑标记）
	value, err = trie.Get([]byte("deleteMe"))
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	t.Logf("Value after delete: %v", value)
	if value != nil {
		t.Errorf("Expected nil after delete, got: %s", string(value))
	}

	// 检查删除后的window位
	node, err = trie.getNodeForPath([]byte("deleteMe"))
	if err != nil {
		t.Fatalf("getNodeForPath after delete failed: %v", err)
	}
	if node == nil {
		t.Fatal("node after delete is nil")
	}
	window60 := trie.getNodeWindow(node)
	expected60 := 1 << ((60 - 50) / 1) // 区块60对应第10位

	t.Logf("window at block 60: %032b, expected: %032b", window60, expected60)
	if window60 != expected60 {
		t.Errorf("Wrong window after delete: got %032b, expected %032b", window60, expected60)
	}
}

// TestCacheTrieMultipleBlocks 测试在更大范围内的区块缓存
func TestCacheTrieMultipleBlocks(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)

	// 设置缓存参数：从区块1000开始，每10个区块占用1位
	trie.SetCacheParams(1000, 10)

	// 创建一组测试区块和键
	testBlocks := []uint64{1005, 1015, 1025, 1035, 1045}

	// 在每个区块更新key
	for i, block := range testBlocks {
		trie.SetBlockNum(block)
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("block%d-value%d", block, i)

		err := trie.Update([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Update at block %d failed: %v", block, err)
		}

		// 检查缓存状态
		cached, err := trie.IsCachedAtBlock([]byte(key), block)
		if err != nil {
			t.Fatalf("IsCachedAtBlock failed: %v", err)
		}
		if !cached {
			t.Errorf("Expected key-%d to be cached at block %d, but it's not", i, block)
		}
	}

	// 更新最后一个区块的key-0
	trie.SetBlockNum(1055)
	err := trie.Update([]byte("key-0"), []byte("updated-value"))
	if err != nil {
		t.Fatalf("Update at block 1055 failed: %v", err)
	}

	// 获取更新后的值
	value, err := trie.Get([]byte("key-0"))
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if string(value) != "updated-value" {
		t.Errorf("Expected 'updated-value', got '%s'", string(value))
	}

	// 提交树并打印root hash
	hash, _ := trie.Commit(false)
	t.Logf("Final trie root: %x", hash)
}

// TestCacheTrieGetUpdateWindow 测试使用Get方法读取节点时更新window字段的功能
func TestCacheTrieGetUpdateWindow(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)

	// 设置缓存参数：从区块100开始，每个位存储1个区块
	trie.SetCacheParams(100, 1)

	// 步骤1：在区块101设置一个键
	trie.SetBlockNum(101)
	err := trie.Update([]byte("testKey"), []byte("testValue"))
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// 验证window在区块101被正确设置
	node, err := trie.getNodeForPath([]byte("testKey"))
	if err != nil {
		t.Fatalf("getNodeForPath failed: %v", err)
	}
	window := trie.getNodeWindow(node)
	expected101 := 1 << ((101 - 100) / 1) // 区块101对应第1位
	t.Logf("Window at block 101: %032b, Expected: %032b", window, expected101)
	if window != expected101 {
		t.Errorf("Window not set correctly at block 101. Got %032b, Expected %032b", window, expected101)
	}

	// 步骤2：在区块105读取同一个key，检查window更新
	trie.SetBlockNum(105)
	value, err := trie.Get([]byte("testKey"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "testValue" {
		t.Errorf("Expected 'testValue', got '%s'", string(value))
	}

	// 检查节点的window是否正确更新
	node, err = trie.getNodeForPath([]byte("testKey"))
	if err != nil {
		t.Fatalf("getNodeForPath failed: %v", err)
	}

	window = trie.getNodeWindow(node)
	// 现在window应该设置了第1位(区块101)和第5位(区块105)
	expected105 := (1 << ((101 - 100) / 1)) | (1 << ((105 - 100) / 1))

	t.Logf("Window after Get at block 105: %032b, Expected: %032b", window, expected105)
	if window != expected105 {
		t.Errorf("Window not updated correctly. Got: %032b, Expected: %032b", window, expected105)
	}

	// 步骤3：在区块110再次读取，验证window被更新
	trie.SetBlockNum(110)
	_, err = trie.Get([]byte("testKey"))
	if err != nil {
		t.Fatalf("Get at block 110 failed: %v", err)
	}

	// 再次检查window
	node, err = trie.getNodeForPath([]byte("testKey"))
	if err != nil {
		t.Fatalf("getNodeForPath after second Get failed: %v", err)
	}

	window = trie.getNodeWindow(node)
	// 应该设置了第1位(区块101)、第5位(区块105)和第10位(区块110)
	expected := expected105 | (1 << ((110 - 100) / 1))

	t.Logf("Window after Get at block 110: %032b, Expected: %032b", window, expected)
	if window != expected {
		t.Errorf("Window not updated correctly after second Get. Got: %032b, Expected: %032b",
			window, expected)
	}
}

// TestCacheTrieIsCachedAt 测试IsCachedAtBlock方法
func TestCacheTrieIsCachedAt(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)

	// 设置缓存参数：从区块200开始，每1个区块占用1位
	trie.SetCacheParams(200, 1) // 修改为每1个区块占用1位，以避免潜在的混淆

	// 先在区块200写入一个key
	trie.SetBlockNum(200)
	err := trie.Update([]byte("testKey"), []byte("testValue-200"))
	if err != nil {
		t.Fatalf("Update at block 200 failed: %v", err)
	}

	// 验证testKey在区块200被正确缓存
	node, err := trie.getNodeForPath([]byte("testKey"))
	if err != nil {
		t.Fatalf("getNodeForPath failed: %v", err)
	}
	window := trie.getNodeWindow(node)
	expected200 := 1 << (200 - 200) // 区块200对应第0位
	t.Logf("testKey at block 200 window: %032b, expected: %032b", window, expected200)

	// 验证通过IsCachedAtBlock方法
	cached, err := trie.IsCachedAtBlock([]byte("testKey"), 200)
	if err != nil {
		t.Fatalf("IsCachedAtBlock check failed: %v", err)
	}
	if !cached {
		t.Errorf("testKey should be cached at block 200")
	}

	// 在区块210进行Get操作
	trie.SetBlockNum(210)
	t.Logf("Getting testKey at block 210")
	value, err := trie.Get([]byte("testKey"))
	if err != nil {
		t.Fatalf("Get testKey at block 210 failed: %v", err)
	}
	t.Logf("Got value: %s", string(value))

	// 检查window位是否更新
	node, err = trie.getNodeForPath([]byte("testKey"))
	if err != nil {
		t.Fatalf("getNodeForPath failed after Get: %v", err)
	}
	window = trie.getNodeWindow(node)
	expected210 := expected200 | (1 << (210 - 200)) // 同时设置区块200和210的位
	t.Logf("testKey window after Get: %032b, expected: %032b", window, expected210)

	// 检查是否被IsCachedAtBlock正确识别
	cached, err = trie.IsCachedAtBlock([]byte("testKey"), 210)
	if err != nil {
		t.Fatalf("IsCachedAtBlock check failed: %v", err)
	}
	if !cached {
		t.Errorf("testKey should be cached at block 210 after Get")
	}

	// 测试边界条件
	// 1. 区块小于startNum
	cached, err = trie.IsCachedAtBlock([]byte("testKey"), 199)
	if err != nil || cached {
		t.Errorf("Expected false for block < startNum, got cached: %v, err: %v", cached, err)
	}

	// 2. 区块远大于startNum（超出窗口位宽）
	cached, err = trie.IsCachedAtBlock([]byte("testKey"), 200+32)
	if err != nil || cached {
		t.Errorf("Expected false for block beyond window range, got cached: %v, err: %v", cached, err)
	}

	// 3. 不存在的key
	cached, err = trie.IsCachedAtBlock([]byte("nonexistent"), 210)
	if err != nil || cached {
		t.Errorf("Expected false for nonexistent key, got cached: %v, err: %v", cached, err)
	}
}

// TestCacheTrieSize 测试CacheTrie的size计算和maxSize限制功能
func TestCacheTrieSize(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)

	// 设置缓存参数
	trie.SetCacheParams(100, 1) // 设置缓存参数
	trie.SetBlockNum(101)       // 设置当前区块号

	// 设置最大缓存大小为5个键值对
	trie.SetMaxSize(5)

	// 简单测试：插入5个单字符键
	keys := []string{"a", "b", "c", "d", "e"}
	for i, key := range keys {
		value := fmt.Sprintf("value-%d", i)
		t.Logf("Inserting key '%s' with value '%s'", key, value)

		// 十六进制转储键，便于调试
		hexKey := keybytesToHex([]byte(key))
		t.Logf("Key '%s' hex: %v", key, hexKey)

		err := trie.Update([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to insert key %s: %v", key, err)
		}

		// 验证插入成功
		val, err := trie.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", key, err)
		}
		if string(val) != value {
			t.Errorf("Key %s: expected '%s', got '%s'", key, value, string(val))
		} else {
			t.Logf("Inserted %s='%s'", key, string(val))
		}
	}

	// 打印trie结构以辅助调试
	dumpTrie(t, trie)

	// 验证size
	size := trie.GetSize()
	t.Logf("Size after insertion: %d", size)
	if size != 5 {
		t.Errorf("Wrong size, expected 5, got %d", size)
	}

	// 测试删除操作对size的影响
	err := trie.Delete([]byte("a"))
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// 验证删除成功
	val, err := trie.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Error getting deleted key: %v", err)
	}
	if val != nil {
		t.Errorf("Expected nil after delete, got %s", string(val))
	} else {
		t.Log("Delete successful, got nil value")
	}

	// 检查删除后的size
	sizeAfterDelete := trie.GetSize()
	t.Logf("Size after deletion: %d", sizeAfterDelete)
	if sizeAfterDelete != 4 {
		t.Errorf("Wrong size after delete, expected 4, got %d", sizeAfterDelete)
		dumpTrie(t, trie) // 打印trie结构以辅助调试
	}

	// 验证所有键的值
	t.Log("Verifying all keys after deletion:")
	for _, key := range keys {
		val, err := trie.Get([]byte(key))
		if err != nil {
			t.Fatalf("Error getting key %s: %v", key, err)
		}
		t.Logf("Key '%s' = '%s'", key, string(val))
	}

	// 测试maxSize限制
	t.Log("Testing maxSize limit")
	// 再插入两个键，超过maxSize
	err = trie.Update([]byte("f"), []byte("value-f"))
	if err != nil {
		t.Fatalf("Failed to insert key f: %v", err)
	}

	// 再次验证所有键的值
	t.Log("After inserting key 'f':")
	for _, key := range append(keys, "f") {
		val, err := trie.Get([]byte(key))
		if err != nil {
			t.Fatalf("Error getting key %s: %v", key, err)
		}
		t.Logf("Key '%s' = '%s'", key, string(val))
	}

	// 验证插入成功
	sizeFinal := trie.GetSize()
	t.Logf("Final size: %d (adding 'f', deleting 'a')", sizeFinal)
	if sizeFinal != 5 {
		t.Errorf("Wrong final size, expected 5, got %d", sizeFinal)
		dumpTrie(t, trie)
	}

	// 打印警告
	hash, _ := trie.Commit(false)
	t.Logf("Commit hash: %x", hash)
}

// TestCacheTrieTombstoneSize 测试墓碑标记在size计算中的正确处理
func TestCacheTrieTombstoneSize(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)

	// 设置maxSize和缓存参数
	trie.SetMaxSize(10)
	trie.SetCacheParams(100, 1)
	trie.SetBlockNum(101)

	// 使用单字符键，避免十六进制转换问题
	keys := []string{"a", "b"}
	for i, key := range keys {
		value := fmt.Sprintf("value-%d", i)
		err := trie.Update([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to insert key %s: %v", key, err)
		}

		// 验证插入成功
		val, err := trie.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after insert: %v", key, err)
		}
		if string(val) != value {
			t.Errorf("Wrong value for key %s: expected '%s', got '%s'", key, value, string(val))
		} else {
			t.Logf("Inserted key %s = '%s'", key, string(val))
		}
	}

	// 验证size
	size := trie.GetSize()
	t.Logf("Initial size: %d", size)
	if size != 2 {
		t.Errorf("Wrong size, expected 2, got %d", size)
	}

	// 删除第一个键
	err := trie.Delete([]byte("a"))
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// 验证删除成功
	val1, err := trie.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Error getting deleted key: %v", err)
	}
	if val1 != nil {
		t.Errorf("Expected nil for deleted key, got: %s", string(val1))
	} else {
		t.Log("Delete successful, key 'a' returns nil")
	}

	// 验证另一个键仍存在
	val2, err := trie.Get([]byte("b"))
	if err != nil {
		t.Fatalf("Error getting key 'b': %v", err)
	}
	if string(val2) != "value-1" {
		t.Errorf("Expected 'value-1' for key 'b', got: '%s'", string(val2))
	} else {
		t.Logf("Key 'b' still accessible with value: '%s'", string(val2))
	}

	// 检查size是否正确减少
	sizeAfterDelete := trie.GetSize()
	t.Logf("Size after deletion: %d", sizeAfterDelete)
	if sizeAfterDelete != 1 {
		t.Errorf("Wrong size after delete, expected 1, got %d", sizeAfterDelete)
		dumpTrie(t, trie)
	}
}

// TestCacheTrieSimpleTombstone 最简版测试，验证Delete和墓碑标记的行为
func TestCacheTrieSimpleTombstone(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme)
	trie := NewEmptyCacheTrie(db)

	// 设置maxSize
	trie.SetMaxSize(10)

	// 插入一个键值对
	key := "key"
	err := trie.Update([]byte(key), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 验证初始size为1
	size := trie.GetSize()
	t.Logf("Initial size: %d", size)
	if size != 1 {
		t.Errorf("Wrong initial size, expected 1, got %d", size)
	}

	// 删除键
	err = trie.Delete([]byte(key))
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// 验证删除后size为0
	sizeAfterDelete := trie.GetSize()
	t.Logf("Size after delete: %d", sizeAfterDelete)
	if sizeAfterDelete != 0 {
		t.Errorf("Wrong size after delete, expected 0, got %d", sizeAfterDelete)
	}

	// 验证墓碑标记生效（Get返回nil）
	val, err := trie.Get([]byte(key))
	if err != nil {
		t.Fatalf("Error getting deleted key: %v", err)
	}
	if val != nil {
		t.Errorf("Expected nil for deleted key, got: %s", string(val))
	} else {
		t.Log("Delete successful, returns nil")
	}
}
