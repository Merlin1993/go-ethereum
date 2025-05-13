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
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// 测试基本操作
func TestCacheTrieOperations(t *testing.T) {
	trie := NewCacheTrie(0, 1, 0)
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
	trie := NewCacheTrie(10, 1, 0)

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
	trie := NewCacheTrie(50, 1, 0)
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
	trie := NewCacheTrie(200, 1, 0)

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
	// 使用带参数的构造函数替代SetCacheParams
	trie := NewCacheTrie(100, 1, 100)

	// 插入20个键值对，确保不会触发自动清理
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// 检查size计数
	size := trie.GetSize()
	t.Logf("Size after 20 insertions: %d", size)
	if size != 20 {
		t.Errorf("Expected size 20, got %d", size)
	}

	// 删除2个键值对
	err := trie.Delete([]byte("key5"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	err = trie.Delete([]byte("key10"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 重新检查size
	size = trie.GetSize()
	t.Logf("Size after deleting 2 keys: %d", size)
	if size != 18 {
		t.Errorf("Expected size 18, got %d", size)
	}
}

// 测试UpdateRoot功能
func TestCacheTrieUpdateRoot(t *testing.T) {
	// 使用带参数的构造函数替代默认空构造函数
	trie := NewCacheTrie(0, 1, 0)

	// 插入一些值
	trie.SetBlockNum(1)
	err := trie.Update([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Update key1 failed: %v", err)
	}

	err = trie.Update([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Update key2 failed: %v", err)
	}

	// 计算第一次哈希
	firstHash := trie.Hash()
	t.Logf("First hash: %x", firstHash)

	// 更新值
	err = trie.Update([]byte("key1"), []byte("newvalue1"))
	if err != nil {
		t.Fatalf("Update key1 with new value failed: %v", err)
	}

	// 更新根并获取新的哈希
	newHash := trie.UpdateRoot()
	t.Logf("New hash after UpdateRoot: %x", newHash)

	// 验证哈希已经更改
	if newHash == firstHash {
		t.Errorf("Root hash didn't change after updating key1: %x", newHash)
	}

	// 验证更新后的值能被正确获取
	val, err := trie.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 after UpdateRoot failed: %v", err)
	}
	if string(val) != "newvalue1" {
		t.Errorf("Wrong value for key1 after UpdateRoot: got %q, want %q", string(val), "newvalue1")
	}
}

// 测试单个字符键插入与检索
func TestCacheTrieSingleCharKey(t *testing.T) {
	// 使用带参数的构造函数替代SetCacheParams
	trie := NewCacheTrie(100, 1, 0)
	trie.SetBlockNum(101)

	// 插入单个字符键
	key := []byte("b")
	value := []byte("test-value")

	t.Logf("Inserting key: %q, value: %q", key, value)
	err := trie.Update(key, value)
	if err != nil {
		t.Fatalf("Failed to insert key: %v", err)
	}

	// 直接获取
	result, err := trie.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key after insert: %v", err)
	}

	t.Logf("Retrieved value: %q", result)
	if !bytes.Equal(result, value) {
		t.Errorf("Value mismatch after insert: got %q, expected %q", result, value)
	}

	// 计算哈希
	trie.Hash()

	// 哈希后再次获取
	result, err = trie.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key after hash: %v", err)
	}

	t.Logf("Retrieved value after hash: %q", result)
	if !bytes.Equal(result, value) {
		t.Errorf("Value mismatch after hash: got %q, expected %q", result, value)
	}
}

// 测试pruneCache通过Size触发清理的功能
func TestPruneCacheBySize(t *testing.T) {
	// 创建一个缓存树，maxSize设置为10（只允许存储10个键值对）
	maxSize := 10
	trie := NewCacheTrie(100, 1, maxSize)
	trie.SetBlockNum(101)

	// 插入15个键值对，超过maxSize限制
	for i := 0; i < 15; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// 验证size（此时应该是15）
	if size := trie.GetSize(); size != 15 {
		t.Errorf("Expected size 15 before Hash, got %d", size)
	}

	// 调用Hash方法触发清理
	trie.Hash()

	// 验证size是否减少（应该小于等于targetSize，即maxSize*2/3）
	targetSize := maxSize * 2 / 3
	if size := trie.GetSize(); size > targetSize {
		t.Errorf("Expected size <= %d after pruning, got %d", targetSize, size)
	} else {
		t.Logf("Size after pruning: %d (target: %d)", size, targetSize)
	}

	// 验证是否可以继续插入新键
	err := trie.Update([]byte("newKey"), []byte("newValue"))
	if err != nil {
		t.Fatalf("Failed to insert after pruning: %v", err)
	}

	// 验证新键是否可以正确获取
	val, err := trie.Get([]byte("newKey"))
	if err != nil {
		t.Fatalf("Failed to get new key: %v", err)
	}
	if string(val) != "newValue" {
		t.Errorf("Wrong value for newKey: got %q, want %q", string(val), "newValue")
	}
}

// 测试pruneCache通过window位数触发清理的功能
func TestPruneCacheByWindow(t *testing.T) {
	// 创建一个缓存树，startNum从0开始
	startNum := uint64(0)
	multiple := uint64(1)
	trie := NewCacheTrie(startNum, multiple, 1000) // 设置较大的maxSize确保不会因size触发

	// 在不同区块高度插入键值对
	keys := []string{"keyA", "keyB", "keyC", "keyD"}

	// 1. 在区块1插入keyA
	trie.SetBlockNum(1)
	err := trie.Update([]byte(keys[0]), []byte("valueA-1"))
	if err != nil {
		t.Fatalf("Update keyA failed: %v", err)
	}

	// 2. 在区块10插入keyB
	trie.SetBlockNum(10)
	err = trie.Update([]byte(keys[1]), []byte("valueB-10"))
	if err != nil {
		t.Fatalf("Update keyB failed: %v", err)
	}

	// 3. 在区块20插入keyC
	trie.SetBlockNum(20)
	err = trie.Update([]byte(keys[2]), []byte("valueC-20"))
	if err != nil {
		t.Fatalf("Update keyC failed: %v", err)
	}

	// 4. 在区块25处理哈希，不应触发清理
	t.Logf("Calculating hash at block 25...")
	trie.SetBlockNum(25)
	trie.Hash()

	// 验证startNum未变
	if trie.startNum != startNum {
		t.Errorf("startNum changed unexpectedly: expected %d, got %d", startNum, trie.startNum)
	}

	// 验证所有键值对都可以获取
	for _, key := range keys[:3] {
		_, err := trie.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s should be available but got error: %v", key, err)
		}
	}

	// 5. 在区块30触发window位数只剩8位的情况（已用24位）
	trie.SetBlockNum(25 + 24)
	t.Logf("Calculating hash at block %d to trigger window pruning...", trie.blockNum)
	trie.Hash()

	// 验证startNum已经更新
	if trie.startNum <= startNum {
		t.Errorf("startNum didn't increase after pruning, still at %d", trie.startNum)
	} else {
		t.Logf("startNum updated to %d after pruning", trie.startNum)
	}

	// 验证最旧的键可能已被清理
	val, err := trie.Get([]byte(keys[0]))
	t.Logf("Trying to get keyA after pruning: %v, err: %v", val, err)

	// 再次插入一个键
	trie.SetBlockNum(50)
	err = trie.Update([]byte(keys[3]), []byte("valueD-50"))
	if err != nil {
		t.Fatalf("Failed to insert after window pruning: %v", err)
	}

	// 验证可以正确获取
	val, err = trie.Get([]byte(keys[3]))
	if err != nil {
		t.Fatalf("Failed to get keyD: %v", err)
	}
	if string(val) != "valueD-50" {
		t.Errorf("Wrong value for keyD: got %q, want %q", string(val), "valueD-50")
	}
}

// 测试基本功能的综合测试
func TestCacheTrieFunctionality(t *testing.T) {
	// 创建一个小型缓存树用于全面测试
	trie := NewCacheTrie(100, 1, 5)

	// 1. 测试插入
	t.Log("==== 测试插入 ====")
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for i, key := range keys {
		trie.SetBlockNum(uint64(100 + i))
		value := fmt.Sprintf("value-%d", i)
		err := trie.Update([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("插入失败 %s: %v", key, err)
		}

		// 立即验证
		val, err := trie.Get([]byte(key))
		if err != nil {
			t.Fatalf("插入后获取失败 %s: %v", key, err)
		}
		if string(val) != value {
			t.Errorf("插入后值不匹配 %s: 期望 %q, 得到 %q", key, value, string(val))
		}
	}

	// 2. 测试size计算
	t.Log("==== 测试size计算 ====")
	size := trie.GetSize()
	if size != 5 {
		t.Errorf("Size计算错误: 期望 5, 得到 %d", size)
	}

	// 3. 测试删除
	t.Log("==== 测试删除 ====")
	keyToDelete := "key3"
	trie.SetBlockNum(110)
	err := trie.Delete([]byte(keyToDelete))
	if err != nil {
		t.Fatalf("删除失败 %s: %v", keyToDelete, err)
	}

	// 验证删除后无法获取
	val, err := trie.Get([]byte(keyToDelete))
	if err != nil {
		t.Logf("删除后预期的错误: %v", err)
	}
	if val != nil {
		t.Errorf("删除后仍能获取值: %q", string(val))
	}

	// 验证size已减少
	size = trie.GetSize()
	if size != 4 {
		t.Errorf("删除后Size计算错误: 期望 4, 得到 %d", size)
	}

	// 4. 测试window计算
	t.Log("==== 测试window计算 ====")
	for i, key := range keys {
		if key == keyToDelete {
			continue
		}

		node, err := trie.getNodeForPath([]byte(key))
		if err != nil {
			t.Fatalf("获取节点失败 %s: %v", key, err)
		}
		if node == nil {
			t.Fatalf("节点为空 %s", key)
		}

		window := trie.getNodeWindow(node)
		expectedBit := 1 << (i % 32)
		t.Logf("key %s window: %032b, expected bit: %032b", key, window, expectedBit)

		if window&expectedBit == 0 {
			t.Errorf("window计算错误 %s: 期望位 %d 被设置", key, i%32)
		}
	}

	// 5. 测试pruneCache (通过hash触发)
	t.Log("==== 测试清理缓存 ====")
	initialSize := trie.GetSize()

	// 添加更多键以超出限制
	for i := 0; i < 5; i++ {
		newKey := fmt.Sprintf("extraKey%d", i)
		trie.SetBlockNum(uint64(115 + i))
		err := trie.Update([]byte(newKey), []byte(fmt.Sprintf("extraValue%d", i)))
		if err != nil {
			t.Fatalf("添加额外键失败 %s: %v", newKey, err)
		}
	}

	// 验证size增加
	sizeBeforeHash := trie.GetSize()
	if sizeBeforeHash <= initialSize {
		t.Errorf("添加额外键后size未增加: %d", sizeBeforeHash)
	}

	// 计算hash触发清理
	trie.Hash()

	// 验证size减少
	sizeAfterHash := trie.GetSize()
	targetSize := trie.maxSize * 2 / 3
	if sizeAfterHash > targetSize {
		t.Errorf("清理后size仍超过目标值: 得到 %d, 目标 <= %d", sizeAfterHash, targetSize)
	}

	t.Logf("清理前size: %d, 清理后: %d, 目标: <= %d", sizeBeforeHash, sizeAfterHash, targetSize)
}

// 测试空缓存树的创建和哈希计算
func TestEmptyCacheTrie(t *testing.T) {
	trie := NewCacheTrie(0, 1, 100)
	if trie.Hash() != EmptyRoot {
		t.Errorf("空树哈希计算错误，期望为EmptyRoot，实际为%s", trie.Hash().Hex())
	}
}

// 测试基本的键值操作
func TestBasicOperations(t *testing.T) {
	trie := NewCacheTrie(0, 1, 100)

	// 插入测试数据
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	// 执行插入操作
	for _, data := range testData {
		err := trie.Update([]byte(data.key), []byte(data.value))
		if err != nil {
			t.Fatalf("Update失败: %v", err)
		}
	}

	// 验证插入后的size
	if trie.GetSize() != len(testData) {
		t.Errorf("树大小不匹配，期望为%d，实际为%d", len(testData), trie.GetSize())
	}

	// 测试查询
	for _, data := range testData {
		value, err := trie.Get([]byte(data.key))
		if err != nil {
			t.Fatalf("Get失败: %v", err)
		}
		if !bytes.Equal(value, []byte(data.value)) {
			t.Errorf("键'%s'的值不匹配，期望为'%s'，实际为'%s'", data.key, data.value, string(value))
		}
	}

	// 测试删除
	err := trie.Delete([]byte("key2"))
	if err != nil {
		t.Fatalf("Delete失败: %v", err)
	}

	// 验证删除后的size
	if trie.GetSize() != len(testData)-1 {
		t.Errorf("删除后树大小不匹配，期望为%d，实际为%d", len(testData)-1, trie.GetSize())
	}

	// 确认key2已被删除
	value, err := trie.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get失败: %v", err)
	}
	if value != nil {
		t.Errorf("键'key2'应该已被删除，但仍返回值'%s'", string(value))
	}
}

// 测试window功能
func TestWindowFunctionality(t *testing.T) {
	// 创建起始区块为10，倍数为5的缓存树
	trie := NewCacheTrie(10, 5, 100)

	// 设置当前区块为15
	trie.SetBlockNum(15)

	// 插入key1
	err := trie.Update([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Update失败: %v", err)
	}

	// 当前区块位置应该是(15-10)/5=1
	// 验证key1是否在区块15有缓存
	isCached, err := trie.IsCachedAtBlock([]byte("key1"), 15)
	if err != nil {
		t.Fatalf("IsCachedAtBlock失败: %v", err)
	}
	if !isCached {
		t.Error("键'key1'在区块15应有缓存，但检查结果为否")
	}

	// 验证key1不在区块20有缓存
	isCached, err = trie.IsCachedAtBlock([]byte("key1"), 20)
	if err != nil {
		t.Fatalf("IsCachedAtBlock失败: %v", err)
	}
	if isCached {
		t.Error("键'key1'在区块20不应有缓存，但检查结果为是")
	}

	// 设置当前区块为20
	trie.SetBlockNum(20)

	// 更新同一个key
	err = trie.Update([]byte("key1"), []byte("value1-updated"))
	if err != nil {
		t.Fatalf("Update失败: %v", err)
	}

	// 验证key1在区块15和20都有缓存
	for _, blockNum := range []uint64{15, 20} {
		isCached, err = trie.IsCachedAtBlock([]byte("key1"), blockNum)
		if err != nil {
			t.Fatalf("IsCachedAtBlock失败: %v", err)
		}
		if !isCached {
			t.Errorf("键'key1'在区块%d应有缓存，但检查结果为否", blockNum)
		}
	}

	// 获取key1以设置窗口位
	_, err = trie.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get失败: %v", err)
	}

	// 测试缓存清理
	// 为了触发清理，创建一个小容量的树并填充
	smallTrie := NewCacheTrie(10, 5, 2)
	smallTrie.SetBlockNum(15)

	// 插入3个键，超过maxSize=2，应该触发清理
	for i := 0; i < 3; i++ {
		key := []byte{byte(i)}
		err := smallTrie.Update(key, []byte("value"))
		if err != nil {
			t.Fatalf("Update失败: %v", err)
		}
	}

	// 计算一次哈希以触发清理
	smallTrie.Hash()

	// 验证大小是否已减少
	if smallTrie.GetSize() > 2 {
		t.Errorf("清理后树大小应小于等于2，实际为%d", smallTrie.GetSize())
	}
}

// 测试缓存树的复制功能
func TestCacheTrieCopy(t *testing.T) {
	trie := NewCacheTrie(0, 1, 100)

	// 插入测试数据
	err := trie.Update([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Update失败: %v", err)
	}

	// 复制树
	copy := trie.Copy()

	// 修改原树
	err = trie.Update([]byte("key"), []byte("new-value"))
	if err != nil {
		t.Fatalf("Update失败: %v", err)
	}

	// 验证副本未受影响
	value, err := copy.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get失败: %v", err)
	}
	if !bytes.Equal(value, []byte("value")) {
		t.Errorf("副本数据不匹配，期望为'value'，实际为'%s'", string(value))
	}
}

// 测试复杂的键路径
func TestComplexKeyPaths(t *testing.T) {
	trie := NewCacheTrie(0, 1, 100)

	// 使用有共同前缀的键
	testData := []struct {
		key   string
		value string
	}{
		{"abc", "value1"},
		{"abcd", "value2"},
		{"abce", "value3"},
		{"abcde", "value4"},
	}

	// 执行插入操作
	for _, data := range testData {
		err := trie.Update([]byte(data.key), []byte(data.value))
		if err != nil {
			t.Fatalf("Update失败: %v", err)
		}
	}

	// 验证所有键都可以正确获取
	for _, data := range testData {
		value, err := trie.Get([]byte(data.key))
		if err != nil {
			t.Fatalf("Get失败: %v", err)
		}
		if !bytes.Equal(value, []byte(data.value)) {
			t.Errorf("键'%s'的值不匹配，期望为'%s'，实际为'%s'", data.key, data.value, string(value))
		}
	}

	// 测试部分匹配的键
	value, err := trie.Get([]byte("ab"))
	if err != nil {
		t.Fatalf("Get失败: %v", err)
	}
	if value != nil {
		t.Errorf("不存在的键'ab'应返回nil，实际为'%s'", string(value))
	}

	// 删除一个键并验证树的完整性
	err = trie.Delete([]byte("abcd"))
	if err != nil {
		t.Fatalf("Delete失败: %v", err)
	}

	for _, data := range testData {
		expected := []byte(data.value)
		if data.key == "abcd" {
			expected = nil
		}

		value, err := trie.Get([]byte(data.key))
		if err != nil {
			t.Fatalf("Get失败: %v", err)
		}

		if !bytes.Equal(value, expected) {
			if expected == nil {
				t.Errorf("键'%s'应被删除，实际为'%s'", data.key, string(value))
			} else {
				t.Errorf("键'%s'的值不匹配，期望为'%s'，实际为'%s'", data.key, string(expected), string(value))
			}
		}
	}
}
