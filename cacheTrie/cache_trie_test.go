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

// 测试基本操作: 插入、查询、删除
func TestBasicOperations(t *testing.T) {
	trie := NewCacheTrie(0, 1, 0)
	trie.SetBlockNum(1)

	// 测试插入
	err := trie.Update([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Update key1 failed: %v", err)
	}

	// 测试Get方法
	node, err := trie.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}

	valueNode, ok := node.(ValueNode)
	if !ok {
		t.Fatalf("Expected ValueNode, got %T", node)
	}

	if string(valueNode) != "value1" {
		t.Errorf("Wrong value for key1: got %q, want %q", string(valueNode), "value1")
	}

	// 插入第二个键值对
	err = trie.Update([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Update key2 failed: %v", err)
	}

	// 测试第二个键的Get方法
	node, err = trie.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}

	valueNode, ok = node.(ValueNode)
	if !ok {
		t.Fatalf("Expected ValueNode, got %T", node)
	}

	if string(valueNode) != "value2" {
		t.Errorf("Wrong value for key2: got %q, want %q", string(valueNode), "value2")
	}

	// 测试删除
	err = trie.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Delete key1 failed: %v", err)
	}

	// 验证删除后，key1不存在或者是空值
	node, err = trie.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}

	// 删除后该节点应该是nil或者是空的ValueNode
	if node != nil {
		valueNode, ok = node.(ValueNode)
		if !ok || len(valueNode) != 0 {
			t.Errorf("Expected nil or empty ValueNode after delete, got: %v", node)
		}
	}

	// 但是key2应该仍然存在
	node, err = trie.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 after deleting key1 failed: %v", err)
	}

	valueNode, ok = node.(ValueNode)
	if !ok {
		t.Fatalf("Expected ValueNode for key2, got %T", node)
	}

	if string(valueNode) != "value2" {
		t.Errorf("Wrong value for key2 after deleting key1: got %q, want %q", string(valueNode), "value2")
	}

	// 测试哈希计算
	hash, _ := trie.Hash()
	t.Logf("Trie root hash: %x", hash)
	if hash == (common.Hash{}) {
		t.Error("Hash returned empty hash")
	}
}

// 测试window的计算是否正确
func TestWindowCalculation(t *testing.T) {
	// 创建一个缓存树，startNum=100, multiple=5
	trie := NewCacheTrie(100, 5, 0)

	// 在区块110插入key1
	trie.SetBlockNum(110)
	err := trie.Update([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Update at block 110 failed: %v", err)
	}

	// 打印调试信息
	t.Logf("BlockNum: %d, StartNum: %d, Multiple: %d", trie.blockNum, trie.startNum, trie.multiple)
	t.Logf("Actual bitPos calculation: %d / %d %% 32 = %d", trie.blockNum, trie.multiple, trie.blockNum/trie.multiple%32)
	t.Logf("Expected window calculation: 1 << %d = %032b", trie.blockNum/trie.multiple%32, 1<<(trie.blockNum/trie.multiple%32))
	t.Logf("Actual window value: %032b", trie.root.window())

	// 检查window位
	// 计算期望的bit位置: 110 / 5 = 22，然后对32取模得到22
	expected := 1 << (110 / 5 % 32) // 应该是第22位

	// 验证window是否正确
	if trie.root.window() != expected {
		t.Errorf("Wrong window for key1 at block 110: got %032b, expected %032b",
			trie.root.window(), expected)
	}

	// 在区块120更新另一个key
	trie.SetBlockNum(120)
	err = trie.Update([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Update at block 120 failed: %v", err)
	}

	// 打印调试信息
	t.Logf("After key2 update - BlockNum: %d, StartNum: %d, Multiple: %d", trie.blockNum, trie.startNum, trie.multiple)
	t.Logf("After key2 update - Current bitPos: %d", trie.blockNum/trie.multiple%32)
	t.Logf("After key2 update - Expected window: %032b | %032b = %032b",
		1<<(110/5%32), 1<<(120/5%32), (1<<(110/5%32))|(1<<(120/5%32)))
	t.Logf("After key2 update - Actual window: %032b", trie.root.window())

	// 使用fstring方法打印结构
	if shortNode, ok := trie.root.(*ShortNode); ok {
		t.Logf("After key2 update - Structure: %s", shortNode.fstring(""))
	} else if fullNode, ok := trie.root.(*FullNode); ok {
		t.Logf("After key2 update - Structure: %s", fullNode.fstring(""))
	} else {
		t.Logf("After key2 update - Structure type: %T", trie.root)
	}

	// 检查window位，应该同时设置了第22位和第24位
	// 计算期望的bit位置: 120 / 5 = 24，然后对32取模得到24
	expected = (1 << (110 / 5 % 32)) | (1 << (120 / 5 % 32))

	// 验证window是否正确
	if trie.root.window() != expected {
		t.Errorf("Wrong window for key2 at block 120: got %032b, expected %032b",
			trie.root.window(), expected)
	}

	//更新同一个key,消除第22位的值
	err = trie.Update([]byte("key1"), []byte("value1-updated"))
	if err != nil {
		t.Fatalf("Update at block 120 failed: %v", err)
	}
	expected = 1 << (120 / 5 % 32)

	// 验证window是否正确
	if trie.root.window() != expected {
		t.Errorf("Wrong window for key1 at block 120: got %032b, expected %032b",
			trie.root.window(), expected)
	}

	// 使用IsCachedAtBlock验证
	// 因为IsCachedAtBlock方法中使用的是t.blockNum来计算位置
	// 所以我们需要先设置为110，然后再验证
	trie.SetBlockNum(110)
	isCached, err := trie.IsCachedAtBlock([]byte("key1"), 110)
	if err != nil {
		t.Fatalf("IsCachedAtBlock for block 110 failed: %v", err)
	}
	if !isCached {
		t.Error("key1 should be cached at block 110")
	}

	// 设置回120继续测试
	trie.SetBlockNum(120)
	isCached, err = trie.IsCachedAtBlock([]byte("key1"), 120)
	if err != nil {
		t.Fatalf("IsCachedAtBlock for block 120 failed: %v", err)
	}
	if !isCached {
		t.Error("key1 should be cached at block 120")
	}

	// 测试不应该缓存的区块
	// 设置为115
	trie.SetBlockNum(115)
	isCached, err = trie.IsCachedAtBlock([]byte("key1"), 115)
	if err != nil {
		t.Fatalf("IsCachedAtBlock for block 115 failed: %v", err)
	}
	if isCached {
		t.Error("key1 should NOT be cached at block 115")
	}
}

// 测试size的计算是否正确
func TestSizeCalculation(t *testing.T) {
	// 创建一个缓存树
	trie := NewCacheTrie(0, 1, 0)

	// 初始size应该是0
	if size := trie.GetSize(); size != 0 {
		t.Errorf("Initial size expected 0, got %d", size)
	}

	// 插入10个键值对
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// 验证size是否为10
	if size := trie.GetSize(); size != 10 {
		t.Errorf("Size after 10 insertions: expected 10, got %d", size)
	}

	// 删除2个键值对
	err := trie.Delete([]byte("key3"))
	if err != nil {
		t.Fatalf("Delete key3 failed: %v", err)
	}
	err = trie.Delete([]byte("key7"))
	if err != nil {
		t.Fatalf("Delete key7 failed: %v", err)
	}

	// 因为delete操作是伪删除，所以size还为10
	if size := trie.GetSize(); size != 10 {
		t.Errorf("Size after deleting 2 keys: expected 8, got %d", size)
	}

	// 更新一个已存在的键
	err = trie.Update([]byte("key1"), []byte("updated-value"))
	if err != nil {
		t.Fatalf("Update existing key failed: %v", err)
	}

	// 验证size是否仍为10(更新不应该改变size)
	if size := trie.GetSize(); size != 10 {
		t.Errorf("Size after updating existing key: expected 8, got %d", size)
	}
}

// 测试基于size的pruneCache功能
func TestPruneCacheBySize(t *testing.T) {
	// 创建一个缓存树，maxSize设置为10
	maxSize := 10
	trie := NewCacheTrie(100, 1, maxSize)
	trie.SetBlockNum(101)

	// 插入11个键值对
	for i := 0; i < 11; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	//需要跨越多个区块，才可以触发裁剪
	trie.SetBlockNum(105)

	// 插入4个键值对，超过maxSize限制
	for i := 11; i < 15; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// 验证size(此时应该是15)
	if size := trie.GetSize(); size != 15 {
		t.Errorf("Expected size 15 before Hash, got %d", size)
	}

	// 调用Hash方法触发清理
	trie.Hash()

	// 验证size是否减少(应该小于等于targetSize，即maxSize*2/3)
	targetSize := maxSize * 2 / 3
	if size := trie.GetSize(); size > targetSize {
		t.Errorf("Expected size <= %d after pruning, got %d", targetSize, size)
	} else {
		t.Logf("Size after pruning: %d (target: %d)", size, targetSize)
	}

	// 验证是否仍然可以插入新键
	err := trie.Update([]byte("newKey"), []byte("newValue"))
	if err != nil {
		t.Fatalf("Failed to insert after pruning: %v", err)
	}

	// 验证新键是否可以正确获取
	node, err := trie.Get([]byte("newKey"))
	if err != nil {
		t.Fatalf("Failed to get new key: %v", err)
	}

	valueNode, ok := node.(ValueNode)
	if !ok {
		t.Fatalf("Expected ValueNode, got %T", node)
	}

	if string(valueNode) != "newValue" {
		t.Errorf("Wrong value for newKey: got %q, want %q", string(valueNode), "newValue")
	}
}

// 测试基于window的pruneCache功能
func TestPruneCacheByWindow(t *testing.T) {
	// 创建一个缓存树，startNum从100开始，multiple=1
	startNum := uint64(100)
	multiple := uint64(1)
	trie := NewCacheTrie(startNum, multiple, 100) // 设置较大的maxSize确保不会因size触发

	// 在不同区块高度插入键值对
	keys := []string{"keyA", "keyB", "keyC"}

	// 在区块101插入keyA
	trie.SetBlockNum(101)
	err := trie.Update([]byte(keys[0]), []byte("valueA"))
	if err != nil {
		t.Fatalf("Update keyA failed: %v", err)
	}

	// 在区块110插入keyB
	trie.SetBlockNum(110)
	err = trie.Update([]byte(keys[1]), []byte("valueB"))
	if err != nil {
		t.Fatalf("Update keyB failed: %v", err)
	}

	// 在区块120插入keyC
	trie.SetBlockNum(120)
	err = trie.Update([]byte(keys[2]), []byte("valueC"))
	if err != nil {
		t.Fatalf("Update keyC failed: %v", err)
	}

	// 在区块125处理哈希，不应触发清理(因为还有足够的窗口位)
	trie.SetBlockNum(125)
	trie.Hash()

	// 验证startNum未变
	if trie.startNum != startNum {
		t.Errorf("startNum changed unexpectedly: expected %d, got %d", startNum, trie.startNum)
	}

	// 验证所有键值对都可以获取
	for _, key := range keys {
		node, err := trie.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s should be available but got error: %v", key, err)
		}
		if node == nil {
			t.Errorf("Key %s should be available but got nil", key)
		}
	}

	// 在区块125触发window位数只剩8位的情况(跳转到127会用满24位)
	// 区块100开始，multiple=1，到127就是已使用了27位，只剩5位
	trie.SetBlockNum(127)

	// 手动添加几个区块，填满余下的窗口位
	for i := 0; i < 5; i++ {
		blockNum := uint64(128 + i)
		trie.SetBlockNum(blockNum)
		err := trie.Update([]byte(fmt.Sprintf("fill%d", i)), []byte("filler"))
		if err != nil {
			t.Fatalf("Failed to insert filler at block %d: %v", blockNum, err)
		}
	}

	// 现在窗口应该快满了，计算哈希触发清理
	trie.Hash()

	// 验证startNum已经更新(应该向前移动)
	if trie.startNum <= startNum {
		t.Errorf("startNum didn't increase after pruning, still at %d", trie.startNum)
	} else {
		t.Logf("startNum updated to %d after pruning", trie.startNum)
	}

	// 验证最旧的键可能已被清理(keyA可能被清理掉了)
	node, err := trie.Get([]byte(keys[0]))
	t.Logf("Trying to get keyA after pruning: %v, err: %v", node, err)

	// 但是较新的键应该仍然可用
	for _, key := range keys[1:] {
		node, err := trie.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s should still be available but got error: %v", key, err)
		}
		if node == nil {
			t.Errorf("Key %s should still be available but got nil", key)
		}
	}

	// 验证可以在清理后继续插入新键
	trie.SetBlockNum(140)
	err = trie.Update([]byte("newKey"), []byte("newValue"))
	if err != nil {
		t.Fatalf("Failed to insert after window pruning: %v", err)
	}

	// 验证新键是否可以正确获取
	node, err = trie.Get([]byte("newKey"))
	if err != nil {
		t.Fatalf("Failed to get new key after pruning: %v", err)
	}

	valueNode, ok := node.(ValueNode)
	if !ok {
		t.Fatalf("Expected ValueNode, got %T", node)
	}

	if string(valueNode) != "newValue" {
		t.Errorf("Wrong value for newKey: got %q, want %q", string(valueNode), "newValue")
	}
}
