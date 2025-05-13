// Copyright 2023 The go-ethereum Authors
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

package cacheTrie

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestCacheTrieCopy(t *testing.T) {
	trie := NewEmptyCacheTrie()

	copy := trie.Copy()
	if copy == nil {
		t.Error("Copy returned nil")
	}

	if copy == trie {
		t.Error("Copy returned the same instance")
	}
}

// 测试基本操作
func TestCacheTrieOperations(t *testing.T) {
	trie := NewEmptyCacheTrie()

	// 设置缓存参数以便测试
	trie.SetCacheParams(0, 1)
	trie.SetBlockNum(1)

	// 插入一些值
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

	// 测试哈希计算
	hash := trie.Hash()
	t.Logf("Trie root hash: %x", hash)
	if hash == (common.Hash{}) {
		t.Error("Hash returned empty hash")
	}
}

// 测试带有区块高度的缓存功能
func TestCacheTrieWithBlocks(t *testing.T) {
	trie := NewEmptyCacheTrie()

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
}

// 测试墓碑标记
func TestCacheTrieTombstone(t *testing.T) {
	trie := NewEmptyCacheTrie()

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
}

// 测试IsCachedAtBlock方法
func TestCacheTrieIsCachedAt(t *testing.T) {
	trie := NewEmptyCacheTrie()

	// 设置缓存参数：从区块200开始，每1个区块占用1位
	trie.SetCacheParams(200, 1)

	// 先在区块200写入一个key
	trie.SetBlockNum(200)
	err := trie.Update([]byte("testKey"), []byte("testValue-200"))
	if err != nil {
		t.Fatalf("Update at block 200 failed: %v", err)
	}

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
}

// 测试size计算
func TestCacheTrieSize(t *testing.T) {
	trie := NewEmptyCacheTrie()

	// 设置缓存参数
	trie.SetCacheParams(100, 1)
	trie.SetBlockNum(101)
	trie.SetMaxSize(5)

	// 测试：插入5个单字符键
	keys := []string{"a", "b", "c", "d", "e"}
	for i, key := range keys {
		value := fmt.Sprintf("value-%d", i)
		t.Logf("Inserting key '%s' with value '%s'", key, value)

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
		}
	}

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
	}

	// 检查删除后的size
	sizeAfterDelete := trie.GetSize()
	t.Logf("Size after deletion: %d", sizeAfterDelete)
	if sizeAfterDelete != 4 {
		t.Errorf("Wrong size after delete, expected 4, got %d", sizeAfterDelete)
	}
}
