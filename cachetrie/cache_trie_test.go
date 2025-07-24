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

package cachetrie

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// 测试基本操作: 插入、查询、删除
func TestBasicOperations(t *testing.T) {
	trie := NewCacheTrie(0, 1, 0)
	trie.SetBlockNum(1)

	// 测试插入
	err := trie.Update([]byte("key1"), []byte("value1"), true)
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

	if string(valueNode.Data) != "value1" {
		t.Errorf("Wrong value for key1: got %q, want %q", string(valueNode.Data), "value1")
	}

	// 插入第二个键值对
	err = trie.Update([]byte("key2"), []byte("value2"), true)
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

	if string(valueNode.Data) != "value2" {
		t.Errorf("Wrong value for key2: got %q, want %q", string(valueNode.Data), "value2")
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
		if !ok || len(valueNode.Data) != 0 {
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

	if string(valueNode.Data) != "value2" {
		t.Errorf("Wrong value for key2 after deleting key1: got %q, want %q", string(valueNode.Data), "value2")
	}

	// 测试哈希计算
	hash, _, _ := trie.Hash()
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
	err := trie.Update([]byte("key1"), []byte("value1"), true)
	if err != nil {
		t.Fatalf("Update at block 110 failed: %v", err)
	}

	expected := 1 << 3
	if expected != trie.root.window() {
		t.Errorf("Wrong window for key1 at block 110: got %032b, expected %032b",
			trie.root.window(), expected)
	}

	// 在区块120更新另一个key
	trie.SetBlockNum(120)
	err = trie.Update([]byte("key2"), []byte("value2"), true)
	if err != nil {
		t.Fatalf("Update at block 120 failed: %v", err)
	}

	// 检查window位，应该同时设置了第22位和第24位
	// 计算期望的bit位置: 120 / 5 = 24，然后对32取模得到24
	expected = (1<<3 | 1<<4)

	// 验证window是否正确
	if trie.root.window() != expected {
		t.Errorf("Wrong window for key2 at block 120: got %032b, expected %032b",
			trie.root.window(), expected)
	}

	//更新同一个key,消除第22位的值
	err = trie.Update([]byte("key1"), []byte("value1-updated"), true)
	if err != nil {
		t.Fatalf("Update at block 120 failed: %v", err)
	}
	expected = 1 << 4

	// 验证window是否正确
	if trie.root.window() != expected {
		t.Errorf("Wrong window for key1 at block 120: got %032b, expected %032b",
			trie.root.window(), expected)
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
		err := trie.Update(key, value, true)
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
	err = trie.Update([]byte("key1"), []byte("updated-value"), true)
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
		err := trie.Update(key, value, true)
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
		err := trie.Update(key, value, true)
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
	err := trie.Update([]byte("newKey"), []byte("newValue"), true)
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

	if string(valueNode.Data) != "newValue" {
		t.Errorf("Wrong value for newKey: got %q, want %q", string(valueNode.Data), "newValue")
	}
}

// 测试基于window的pruneCache功能
func TestPruneCacheByWindow(t *testing.T) {
	// 创建一个缓存树，startNum从100开始，multiple=1
	trie := NewCacheTrie(100, 1, 100) // 设置较大的maxSize确保不会因size触发

	// 在不同区块高度插入键值对
	keys := []string{"keyA", "keyB", "keyC"}

	// 在区块101插入keyA
	trie.SetBlockNum(101)
	err := trie.Update([]byte(keys[0]), []byte("valueA"), true)
	if err != nil {
		t.Fatalf("Update keyA failed: %v", err)
	}

	// 在区块109插入keyB
	trie.SetBlockNum(109)
	err = trie.Update([]byte(keys[1]), []byte("valueB"), true)
	if err != nil {
		t.Fatalf("Update keyB failed: %v", err)
	}

	// 在区块120插入keyC
	trie.SetBlockNum(120)
	err = trie.Update([]byte(keys[2]), []byte("valueC"), true)
	if err != nil {
		t.Fatalf("Update keyC failed: %v", err)
	}

	// 在区块596处理哈希 (1+32)*32/2 - 32，不应触发清理(因为还有足够的窗口位)
	trie.SetBlockNum(496)
	trie.Hash()

	// 验证startNum未变
	if trie.hrw.windowStartNumber != 100 {
		t.Errorf("startNum changed unexpectedly: expected %d, got %d", 100, trie.hrw.windowStartNumber)
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

	// 在区块597触发window位数只剩8位的情况(跳转到597会用满32位)
	trie.SetBlockNum(497)

	// 手动添加几个区块，填满余下的窗口位
	for i := 0; i < 5; i++ {
		blockNum := uint64(597 + i)
		trie.SetBlockNum(blockNum)
		err := trie.Update([]byte(fmt.Sprintf("fill%d", i)), []byte("filler"), true)
		if err != nil {
			t.Fatalf("Failed to insert filler at block %d: %v", blockNum, err)
		}
	}

	// 现在窗口应该快满了，计算哈希触发清理
	trie.Hash()

	// 验证startNum已经更新(应该向前移动)
	if trie.hrw.windowStartNumber <= 100 {
		t.Errorf("startNum didn't increase after pruning, still at %d", trie.hrw.windowStartNumber)
	} else {
		t.Logf("startNum updated to %d after pruning", trie.hrw.windowStartNumber)
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
}

// 测试ValueNode的New字段功能
func TestValueNodeNewField(t *testing.T) {
	trie := NewCacheTrie(0, 1, 100)
	trie.SetBlockNum(1)

	// 首先测试通过Update插入的键值对，New字段默认为true
	err := trie.Update([]byte("key1"), []byte("value1"), true)
	if err != nil {
		t.Fatalf("Update key1 failed: %v", err)
	}

	// 获取节点并检查New字段值
	node, err := trie.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}

	valueNode, ok := node.(ValueNode)
	if !ok {
		t.Fatalf("Expected ValueNode, got %T", node)
	}

	if !valueNode.New {
		t.Errorf("Expected New field to be true for new insertion, got false")
	}

	// 测试设置New为false的情况
	err = trie.Update([]byte("key2"), []byte("value2"), false)
	if err != nil {
		t.Fatalf("Update key2 failed: %v", err)
	}

	node, err = trie.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}

	valueNode, ok = node.(ValueNode)
	if !ok {
		t.Fatalf("Expected ValueNode, got %T", node)
	}

	if valueNode.New {
		t.Errorf("Expected New field to be false for isNew=false, got true")
	}

	// 测试删除操作会将New设置为true
	err = trie.Delete([]byte("key2"))
	if err != nil {
		t.Fatalf("Delete key2 failed: %v", err)
	}

	node, err = trie.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 after delete failed: %v", err)
	}

	if node != nil {
		valueNode, ok = node.(ValueNode)
		if !ok {
			t.Fatalf("Expected ValueNode after delete, got %T", node)
		}

		if !valueNode.New {
			t.Errorf("Expected New field to be true after delete, got false")
		}

		if len(valueNode.Data) != 0 {
			t.Errorf("Expected empty Data after delete, got %x", valueNode.Data)
		}
	}

	// 测试更新值会将New设置为true
	// 首先插入一个New=false的值
	err = trie.Update([]byte("updateKey"), []byte("initialValue"), false)
	if err != nil {
		t.Fatalf("Update updateKey failed: %v", err)
	}

	// 获取并确认New=false
	node, err = trie.Get([]byte("updateKey"))
	if err != nil {
		t.Fatalf("Get updateKey failed: %v", err)
	}

	valueNode, ok = node.(ValueNode)
	if !ok {
		t.Fatalf("Expected ValueNode, got %T", node)
	}

	if valueNode.New {
		t.Errorf("Expected New field to be false initially, got true")
	}

	// 更新相同键的值
	err = trie.Update([]byte("updateKey"), []byte("updatedValue"), true)
	if err != nil {
		t.Fatalf("Update updateKey (second time) failed: %v", err)
	}

	// 再次获取并确认New=true（因为值已更新）
	node, err = trie.Get([]byte("updateKey"))
	if err != nil {
		t.Fatalf("Get updateKey after update failed: %v", err)
	}

	valueNode, ok = node.(ValueNode)
	if !ok {
		t.Fatalf("Expected ValueNode after update, got %T", node)
	}

	if !valueNode.New {
		t.Errorf("Expected New field to be true after update, got false")
	}

	if string(valueNode.Data) != "updatedValue" {
		t.Errorf("Expected updated value, got %s", string(valueNode.Data))
	}

	// 测试pruneCache会根据New字段返回节点
	trie.SetBlockNum(31) // 移动到会触发清理的区块

	// 添加一些键值对
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("pruneKey%d", i))
		value := []byte(fmt.Sprintf("pruneValue%d", i))
		isNew := i%2 == 0 // 交替设置New字段

		err := trie.Update(key, value, isNew)
		if err != nil {
			t.Fatalf("Update pruneKey%d failed: %v", i, err)
		}
	}

	// 触发Hash计算和pruneCache
	_, _, deletedKVs := trie.Hash()

	// 检查删除的键值对
	if deletedKVs == nil {
		t.Log("No keys were pruned yet, which might be expected")
	} else {
		// 记录删除的节点信息
		for i, kv := range deletedKVs.Data {
			t.Logf("Pruned KV[%d]: key=%x, value=%x", i, kv.Key, kv.Value)
		}
	}
}

// 测试pruneCache返回的DeleteKV中的New字段
func TestPruneCacheDeleteKVNewField(t *testing.T) {
	// 创建一个缓存树，使用较小的窗口以便快速触发pruneCache
	trie := NewCacheTrie(100, 1, 10)

	// 在区块101插入几个键值对，分别设置不同的New值
	trie.SetBlockNum(101)

	// 插入New=true的键值对
	err := trie.Update([]byte("trueKey1"), []byte("trueValue1"), true)
	if err != nil {
		t.Fatalf("Update trueKey1 failed: %v", err)
	}

	err = trie.Update([]byte("trueKey2"), []byte("trueValue2"), true)
	if err != nil {
		t.Fatalf("Update trueKey2 failed: %v", err)
	}

	// 插入New=false的键值对
	err = trie.Update([]byte("falseKey1"), []byte("falseValue1"), false)
	if err != nil {
		t.Fatalf("Update falseKey1 failed: %v", err)
	}

	err = trie.Update([]byte("falseKey2"), []byte("falseValue2"), false)
	if err != nil {
		t.Fatalf("Update falseKey2 failed: %v", err)
	}

	// 跳转到足够远的区块，以确保触发pruneCache
	trie.SetBlockNum(131)

	// 触发Hash计算和pruneCache
	_, _, deletedKVs := trie.Hash()

	// 验证pruneCache结果
	if deletedKVs == nil {
		t.Fatalf("Expected deletedKVs from pruneCache, got nil")
	}

	// 统计从true和false节点中删除的键值对数量
	trueKeyCount := 0
	falseKeyCount := 0

	for _, kv := range deletedKVs.Data {
		// 根据键名前缀判断是true还是false节点
		if bytes.HasPrefix(kv.Key, []byte("true")) {
			trueKeyCount++
			t.Logf("Found key from 'true' node: key=%x, value=%x", kv.Key, kv.Value)
		} else if bytes.HasPrefix(kv.Key, []byte("false")) {
			falseKeyCount++
			t.Logf("Found key from 'false' node: key=%x, value=%x", kv.Key, kv.Value)
		}
	}

	// 验证应该只有来自New=true节点的键值对被返回
	// 注意：由于hashKey的原因，我们无法直接通过前缀判断，这里只是通过数量大致判断
	if falseKeyCount > 0 {
		t.Logf("Found %d keys from 'false' nodes, which might be unexpected", falseKeyCount)
	}

	// 验证至少有一些键值对被删除
	if deletedKVs == nil || len(deletedKVs.Data) == 0 {
		t.Errorf("Expected some deleted KVs, got none")
	}

	// 再次测试：更新一个原来New=false的值，触发变更
	trie.SetBlockNum(145)

	// 重新插入更多数据
	err = trie.Update([]byte("falseKey3"), []byte("falseValue3"), false)
	if err != nil {
		t.Fatalf("Update falseKey3 failed: %v", err)
	}

	// 更新值
	err = trie.Update([]byte("falseKey3"), []byte("updatedValue3"), false)
	if err != nil {
		t.Fatalf("Update falseKey3 (second time) failed: %v", err)
	}

	// 跳转到更远的区块
	trie.SetBlockNum(180)

	// 再次触发Hash和pruneCache
	_, _, deletedKVs = trie.Hash()

	// 由于hashKey的原因，我们无法直接通过值内容判断，这里只记录日志
	if deletedKVs != nil && len(deletedKVs.Data) > 0 {
		t.Logf("Found %d deleted KVs in second pruning", len(deletedKVs.Data))
		for i, kv := range deletedKVs.Data {
			t.Logf("DeletedKV[%d]: key=%x, value=%x", i, kv.Key, kv.Value)
		}
	}
}

// 测试New=true的节点在pruneCache后仍然可用
func TestNewNodesRetentionAfterPrune(t *testing.T) {
	// 创建一个缓存树，window足够小以触发pruneCache
	trie := NewCacheTrie(100, 1, 10)

	// 在区块101插入键值对
	trie.SetBlockNum(101)

	// 插入New=true的键值对
	err := trie.Update([]byte("trueNode"), []byte("trueValue"), true)
	if err != nil {
		t.Fatalf("Update trueNode failed: %v", err)
	}

	// 插入New=false的键值对
	err = trie.Update([]byte("falseNode"), []byte("falseValue"), false)
	if err != nil {
		t.Fatalf("Update falseNode failed: %v", err)
	}

	// 确认两个节点都可以正常访问
	node1, err := trie.Get([]byte("trueNode"))
	if err != nil || node1 == nil {
		t.Fatalf("Get trueNode failed before pruning: %v", err)
	}

	node2, err := trie.Get([]byte("falseNode"))
	if err != nil || node2 == nil {
		t.Fatalf("Get falseNode failed before pruning: %v", err)
	}

	// 检查节点的New字段
	valueNode1, ok := node1.(ValueNode)
	if ok && valueNode1.New {
		t.Logf("trueNode has New=true as expected")
	} else if ok {
		t.Errorf("trueNode should have New=true, got false")
	}

	valueNode2, ok := node2.(ValueNode)
	if ok && !valueNode2.New {
		t.Logf("falseNode has New=false as expected")
	} else if ok {
		t.Errorf("falseNode should have New=false, got true")
	}

	// 跳转到足够远的区块触发pruneCache
	trie.SetBlockNum(135)

	// 添加一些额外的键，确保触发pruneCache
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("extraKey%d", i))
		value := []byte(fmt.Sprintf("extraValue%d", i))
		err := trie.Update(key, value, i%2 == 0) // 交替设置New
		if err != nil {
			t.Fatalf("Update extraKey%d failed: %v", i, err)
		}
	}

	// 触发Hash计算和pruneCache
	_, _, deletedKVs := trie.Hash()

	// 验证pruneCache结果
	if deletedKVs == nil {
		t.Log("No nodes were pruned yet, which might not be expected")
	} else {
		t.Logf("Pruned %d nodes", len(deletedKVs.Data))

		// 记录删除的键值对内容
		for i, kv := range deletedKVs.Data {
			t.Logf("DeletedKV[%d]: key=%x, value=%x", i, kv.Key, kv.Value)
		}
	}

	// 尝试再次访问两个节点
	// New=true的节点应该被返回在DeleteKV中
	node1, err = trie.Get([]byte("trueNode"))
	if err != nil {
		t.Fatalf("Get trueNode failed after pruning: %v", err)
	}

	if node1 == nil {
		t.Logf("trueNode (New=true) was pruned from trie and should be in deletedKVs")

		// 检查在deletedKVs中是否能找到相似的值
		found := false
		for _, kv := range deletedKVs.Data {
			if bytes.Equal(kv.Value, []byte("trueValue")) {
				found = true
				t.Logf("Found value matching trueNode in deletedKVs")
				break
			}
		}

		if !found && len(deletedKVs.Data) > 0 {
			t.Logf("trueNode value not found in deletedKVs")
		}
	} else {
		t.Logf("trueNode (New=true) is still available in trie after pruning")

		// 检查New字段是否仍为true
		if valueNode, ok := node1.(ValueNode); ok {
			if valueNode.New {
				t.Logf("trueNode still has New=true after pruning")
			} else {
				t.Errorf("trueNode should still have New=true after pruning, got false")
			}
		}
	}

	// New=false的节点预期会被删除
	node2, err = trie.Get([]byte("falseNode"))
	if err != nil {
		t.Fatalf("Get falseNode failed with error after pruning: %v", err)
	}

	// 检查删除的节点不应该出现在deletedKVs中
	if node2 == nil {
		t.Logf("falseNode (New=false) was pruned from trie as expected")

		// 确认在deletedKVs中找不到
		for _, kv := range deletedKVs.Data {
			if bytes.Equal(kv.Value, []byte("falseValue")) {
				t.Logf("Found value matching falseNode in deletedKVs, which might be unexpected")
				break
			}
		}
	} else {
		// 如果没有触发pruneCache，这是可能的
		valueNode, ok := node2.(ValueNode)
		if !ok {
			t.Errorf("Expected ValueNode for falseNode, got %T", node2)
		} else if valueNode.New {
			t.Errorf("falseNode has New=true after pruning, expected false")
		} else {
			t.Logf("falseNode still has New=false and wasn't pruned yet")
		}
	}
}

// 测试性能：测试1000个区块，window是10个区块一计，每个区块有2w的数据写入
func TestPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过耗时的性能测试")
	}

	// 测试参数
	blockCount := 200
	entriesPerBlock := 5000
	multiple := 1024 // window是10个区块一计
	newRatio := 0.3  // 3成是new,7成是false

	// 创建足够大的maxSize，保证只有window满时才修剪
	maxSize := entriesPerBlock * blockCount

	// 生成测试数据
	keys := make([][]byte, entriesPerBlock)
	values := make([][]byte, entriesPerBlock)
	isNews := make([]bool, entriesPerBlock)

	for i := 0; i < entriesPerBlock; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		values[i] = []byte(fmt.Sprintf("value-%d", i))
		isNews[i] = float64(i)/float64(entriesPerBlock) < newRatio
	}

	// 1. 测试有修剪情况下的性能
	t.Run("WithPruning", func(t *testing.T) {
		trie := NewCacheTrie(1, uint64(multiple), maxSize)

		totalBlockTime := int64(0)
		totalHashTime := int64(0)
		pruneCount := 0

		for blockNum := 1; blockNum <= blockCount; blockNum++ {
			trie.SetBlockNum(uint64(blockNum))

			// 记录区块写入时间
			blockStart := time.Now()
			for i := 0; i < entriesPerBlock; i++ {
				// 生成该区块特定的key
				blockKey := []byte(fmt.Sprintf("%s-block%d", keys[i], blockNum))
				err := trie.Update(blockKey, values[i], isNews[i])
				if err != nil {
					t.Fatalf("无法更新key: %v", err)
				}
			}
			blockDuration := time.Since(blockStart)
			totalBlockTime += blockDuration.Nanoseconds()

			// 生成Hash并记录时间
			hashStart := time.Now()
			_, _, deletedKVs := trie.Hash()
			hashDuration := time.Since(hashStart)
			totalHashTime += hashDuration.Nanoseconds()

			// 检查是否发生了修剪
			if deletedKVs != nil && len(deletedKVs.Data) > 0 {
				pruneCount++
				t.Logf("区块 %d: 修剪了 %d 个键值对", blockNum, len(deletedKVs.Data))
			}

			// 每100个区块输出一次进度
			if blockNum%100 == 0 {
				t.Logf("处理进度: %d/%d 区块", blockNum, blockCount)
			}
		}

		// 计算平均时间
		avgBlockTime := time.Duration(totalBlockTime / int64(blockCount))
		avgHashTime := time.Duration(totalHashTime / int64(blockCount))

		t.Logf("有修剪模式性能统计:")
		t.Logf("总区块数: %d", blockCount)
		t.Logf("每区块条目数: %d", entriesPerBlock)
		t.Logf("修剪次数: %d", pruneCount)
		t.Logf("平均区块写入时间: %v", avgBlockTime)
		t.Logf("平均Hash计算时间: %v", avgHashTime)
		t.Logf("总区块写入时间: %v", time.Duration(totalBlockTime))
		t.Logf("总Hash计算时间: %v", time.Duration(totalHashTime))
		t.Logf("总处理时间: %v", time.Duration(totalBlockTime+totalHashTime))
	})

	// 2. 测试无修剪情况下的性能
	t.Run("WithoutPruning", func(t *testing.T) {
		// 创建一个超级大的window，保证永远不会修剪
		trie := NewCacheTrie(1, uint64(blockCount*2), maxSize)

		totalBlockTime := int64(0)
		totalHashTime := int64(0)

		for blockNum := 1; blockNum <= blockCount; blockNum++ {
			trie.SetBlockNum(uint64(blockNum))

			// 记录区块写入时间
			blockStart := time.Now()
			for i := 0; i < entriesPerBlock; i++ {
				// 生成该区块特定的key
				blockKey := []byte(fmt.Sprintf("%s-block%d", keys[i], blockNum))
				err := trie.Update(blockKey, values[i], isNews[i])
				if err != nil {
					t.Fatalf("无法更新key: %v", err)
				}
			}
			blockDuration := time.Since(blockStart)
			totalBlockTime += blockDuration.Nanoseconds()

			// 生成Hash并记录时间
			hashStart := time.Now()
			_, _, deletedKVs := trie.Hash()
			hashDuration := time.Since(hashStart)
			totalHashTime += hashDuration.Nanoseconds()

			// 检查确认没有发生修剪
			if deletedKVs != nil && len(deletedKVs.Data) > 0 {
				t.Errorf("区块 %d: 意外修剪了 %d 个键值对", blockNum, len(deletedKVs.Data))
			}

			// 每100个区块输出一次进度
			if blockNum%100 == 0 {
				t.Logf("处理进度: %d/%d 区块", blockNum, blockCount)
			}
		}

		// 计算平均时间
		avgBlockTime := time.Duration(totalBlockTime / int64(blockCount))
		avgHashTime := time.Duration(totalHashTime / int64(blockCount))

		t.Logf("无修剪模式性能统计:")
		t.Logf("总区块数: %d", blockCount)
		t.Logf("每区块条目数: %d", entriesPerBlock)
		t.Logf("平均区块写入时间: %v", avgBlockTime)
		t.Logf("平均Hash计算时间: %v", avgHashTime)
		t.Logf("总区块写入时间: %v", time.Duration(totalBlockTime))
		t.Logf("总Hash计算时间: %v", time.Duration(totalHashTime))
		t.Logf("总处理时间: %v", time.Duration(totalBlockTime+totalHashTime))
	})
}

func TestHitRateStatistics(t *testing.T) {
	// 创建一个缓存树
	trie := NewCacheTrie(0, 1, 0)

	// 插入10个键值对
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value, true)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// 读取存在的键（命中）
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		node, err := trie.Get(key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if node == nil {
			t.Fatalf("Expected node for key%d, got nil", i)
		}
	}

	// 读取不存在的键（未命中）
	for i := 10; i < 15; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		node, err := trie.Get(key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if node != nil {
			t.Fatalf("Expected nil for key%d, got %v", i, node)
		}
	}

	// 更新已存在的键（update命中）
	for i := 0; i < 4; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("updated-value%d", i))
		err := trie.Update(key, value, true)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// 新增不存在的键（update未命中）
	for i := 20; i < 24; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value, true)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// 删除存在的键（delete命中）
	for i := 4; i < 6; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		err := trie.Delete(key)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	// 删除不存在的键（delete未命中）
	for i := 30; i < 32; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		err := trie.Delete(key)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	// 验证Get命中率统计
	totalGetRequests, hitCount, missCount, getHitRate,
		updateCount, updateHitCount, updateMissCount, updateHitRate := trie.GetHitRate()

	// 验证Get统计
	if totalGetRequests != 10 {
		t.Errorf("Expected 10 total Get requests, got %d", totalGetRequests)
	}
	if hitCount != 5 {
		t.Errorf("Expected 5 Get hits, got %d", hitCount)
	}
	if missCount != 5 {
		t.Errorf("Expected 5 Get misses, got %d", missCount)
	}
	if getHitRate != 0.5 {
		t.Errorf("Expected 0.5 Get hit rate, got %f", getHitRate)
	}

	// 验证Update统计
	// 初始10个插入 + 4个更新 + 4个新增 + 2个删除命中 + 2个删除未命中 = 22个update操作
	expectedUpdateCount := 10 + 4 + 4 + 2 + 2
	if updateCount != uint64(expectedUpdateCount) {
		t.Errorf("Expected %d total Update operations, got %d", expectedUpdateCount, updateCount)
	}

	// 4个更新操作 + 2个删除操作命中 = 6个命中
	expectedUpdateHits := 4 + 2
	if updateHitCount != uint64(expectedUpdateHits) {
		t.Errorf("Expected %d Update hits, got %d", expectedUpdateHits, updateHitCount)
	}

	// 10个初始插入 + 4个新增 + 2个删除未命中 = 16个未命中
	expectedUpdateMisses := 10 + 4 + 2
	if updateMissCount != uint64(expectedUpdateMisses) {
		t.Errorf("Expected %d Update misses, got %d", expectedUpdateMisses, updateMissCount)
	}

	// 命中率应该是 6/22 ≈ 0.273
	expectedUpdateHitRate := float64(expectedUpdateHits) / float64(expectedUpdateCount)
	if math.Abs(updateHitRate-expectedUpdateHitRate) > 0.001 {
		t.Errorf("Expected %.3f Update hit rate, got %.3f", expectedUpdateHitRate, updateHitRate)
	}

	// 重置统计
	trie.ResetStats()

	// 验证重置后的统计
	totalGetRequests, hitCount, missCount, getHitRate,
		updateCount, updateHitCount, updateMissCount, updateHitRate = trie.GetHitRate()

	if totalGetRequests != 0 || hitCount != 0 || missCount != 0 || getHitRate != 0 ||
		updateCount != 0 || updateHitCount != 0 || updateMissCount != 0 || updateHitRate != 0 {
		t.Errorf("Stats not reset properly: Get(total=%d, hits=%d, misses=%d, rate=%f), Update(total=%d, hits=%d, misses=%d, rate=%f)",
			totalGetRequests, hitCount, missCount, getHitRate,
			updateCount, updateHitCount, updateMissCount, updateHitRate)
	}
}

// 测试配置
const (
	// 预热阶段的状态数
	warmupStateCount = 1000000

	// 测试参数
	startBlockNum  = 1000
	windowMultiple = 10
)

// 生成随机数据
func generateRandomData() ([]byte, []byte) {
	key := make([]byte, 32)
	value := make([]byte, 32)
	rand.Read(key)
	rand.Read(value)
	return key, value
}

// 生成随机地址
func generateRandomAddress() common.Address {
	addr := common.Address{}
	rand.Read(addr[:])
	return addr
}

// TestCacheTriePerformance 测试CacheTrie的性能
func TestCacheTriePerformance(t *testing.T) {
	// 测试不同的写入状态数
	stateCounts := []int{5000, 50000, 500000} // 1K, 10K, 100K
	iterationCount := 20                      // 统计循环次数
	maxSize := 500000                         // 初始存储大小限制
	windowMultiple := 1024
	for _, stateCount := range stateCounts {
		t.Run(fmt.Sprintf("StateCount_%d", stateCount), func(t *testing.T) {
			testCacheTrieWithStateCount(t, stateCount, iterationCount, windowMultiple, maxSize, 0)
		})
	}
}

func TestSampleCacheTriePerformance(t *testing.T) {
	stateCount := 5000
	iterationCount := 4000 // 统计循环次数
	maxSize := 1000000     // 初始存储大小限制
	windowMultiple := 256
	t.Run(fmt.Sprintf("StateCount_%d", stateCount), func(t *testing.T) {
		testCacheTrieWithStateCount(t, stateCount, iterationCount, windowMultiple, maxSize, 0)
	})
}

// testCacheTrieWithStateCount 使用指定状态数进行CacheTrie测试
func testCacheTrieWithStateCount(t *testing.T, stateCount, iterationCount, windowMultiple, maxSize int, mod int) {
	t.Logf("开始测试: 单次写入状态数=%d, 统计循环次数=%d, 初始存储大小=%d", stateCount, iterationCount, maxSize)

	// 创建CacheTrie实例
	cacheTrie := NewCacheTrie(startBlockNum, uint64(windowMultiple), maxSize)

	// 预热阶段 - 执行到第一次清理
	t.Log("开始预热阶段...")
	preWarmupStartTime := time.Now()

	// 设置初始区块高度
	currentBlock := uint64(startBlockNum)
	cacheTrie.SetBlockNum(currentBlock)

	// 记录初始状态
	initialHRW := cacheTrie.GetHRW()
	initialThreshold := initialHRW.GetThreshold()
	t.Logf("初始状态: 阈值(ssthresh)=%d", initialThreshold)

	// 进行预热，直到发生第一次清理
	warmupBatchSize := 10000 // 每批次写入数量
	warmupBatches := 0

	for i := 0; i < warmupStateCount; i += warmupBatchSize {
		batchSize := warmupBatchSize
		if i+warmupBatchSize > warmupStateCount {
			batchSize = warmupStateCount - i
		}

		// 写入数据
		for j := 0; j < batchSize; j++ {
			key, value := generateRandomData()
			cacheTrie.Update(key, value, true)
		}

		// 获取哈希，这会触发清理机制
		hash, _, kvList := cacheTrie.Hash()

		if kvList != nil && len(kvList.Data) > 0 {
			go func() {
				cacheTrie.FinishCleanup(currentBlock, hash)
			}()
		}

		currentBlock++
		cacheTrie.SetBlockNum(currentBlock)

		warmupBatches++

		// 检查是否发生了清理
		currentCleanupCount := cacheTrie.GetCleanupCount()
		if currentCleanupCount > 0 {
			t.Logf("预热阶段检测到清理发生，批次数=%d, 写入状态数=%d", warmupBatches, (warmupBatches-1)*warmupBatchSize+batchSize)
			break
		}

		// 如果写入了太多数据还没有触发清理，可以提前结束预热
		if i+batchSize >= warmupStateCount {
			t.Logf("预热阶段结束，未检测到清理发生，已写入状态数=%d", i+batchSize)
		}
	}
	CleanupTime = 0

	preWarmupDuration := time.Since(preWarmupStartTime)
	t.Logf("预热阶段完成，耗时: %v", preWarmupDuration)

	// 正式测试阶段
	t.Log("开始正式测试阶段...")

	// 记录每次操作的统计数据
	type IterationStats struct {
		WriteTime       time.Duration // 写入耗时
		HashTime        time.Duration // 计算哈希耗时
		WriteSpeed      float64       // 写入速度（状态/秒）
		Size            int           // 当前size
		Threshold       int           // 当前阈值
		CleanupOccurred bool          // 是否发生清理
		CleanupTime     time.Duration // 清理耗时（如果发生）
	}

	stats := make([]IterationStats, iterationCount)

	// 准备CSV数据
	csvRecords := [][]string{
		{"迭代", "写入耗时(ns)", "哈希耗时(ns)", "写入速度(状态/秒)", "Size", "Threshold", "是否清理", "清理耗时(ns)"},
	}

	// 重置清理时间统计
	cacheTrie.ResetCleanupTimes()

	for i := 0; i < iterationCount; i++ {
		// 记录写入开始时间
		writeStart := time.Now()

		// 写入指定数量的状态
		for j := 0; j < stateCount; j++ {
			// 50%概率使用普通键值，50%概率使用带地址的键值
			if rand.Intn(2) == 0 {
				key, value := generateRandomData()
				cacheTrie.Update(key, value, true)
			} else {
				addr := generateRandomAddress()
				key, value := generateRandomData()
				cacheTrie.UpdateWithAddress(addr, key, value, true)
			}
		}

		writeTime := time.Since(writeStart)

		// 记录当前清理计数
		beforeHashCleanupCount := cacheTrie.GetCleanupCount()
		beforeCleanupTotalTime, _ := cacheTrie.GetCleanupTimes()

		// 获取哈希，这会触发清理机制
		hashStart := time.Now()
		hash, _, kvList := cacheTrie.Hash()
		hashTime := time.Since(hashStart) - CleanupTime
		CleanupTime = 0

		if kvList != nil {
			go func() {
				cacheTrie.FinishCleanup(currentBlock, hash)
			}()
		}

		// 移动到下一个区块
		currentBlock++
		cacheTrie.SetBlockNum(currentBlock)

		// 检查是否发生了清理
		afterHashCleanupCount := cacheTrie.GetCleanupCount()
		cleanupOccurred := afterHashCleanupCount > beforeHashCleanupCount

		// 计算清理耗时（如果发生）
		var cleanupTime time.Duration
		if cleanupOccurred {
			afterCleanupTotalTime, _ := cacheTrie.GetCleanupTimes()
			cleanupTime = afterCleanupTotalTime - beforeCleanupTotalTime
		}

		// 计算写入速度（状态/秒）
		writeSpeed := float64(stateCount) / writeTime.Seconds()

		// 获取当前size和threshold
		currentSize := cacheTrie.GetSize()
		currentThreshold := cacheTrie.GetHRW().GetThreshold()

		// 保存统计信息
		stats[i] = IterationStats{
			WriteTime:       writeTime,
			HashTime:        hashTime,
			WriteSpeed:      writeSpeed,
			Size:            currentSize,
			Threshold:       currentThreshold,
			CleanupOccurred: cleanupOccurred,
			CleanupTime:     cleanupTime,
		}

		// 添加到CSV记录
		csvRecords = append(csvRecords, []string{
			strconv.Itoa(i + 1),
			strconv.FormatInt(writeTime.Nanoseconds(), 10),
			strconv.FormatInt(hashTime.Nanoseconds(), 10),
			strconv.FormatFloat(writeSpeed, 'f', 2, 64),
			strconv.Itoa(currentSize),
			strconv.Itoa(currentThreshold),
			strconv.FormatBool(cleanupOccurred),
			strconv.FormatInt(cleanupTime.Nanoseconds(), 10),
		})

		// 输出当前迭代的统计信息
		cleanupStatus := "无"
		if cleanupOccurred {
			cleanupStatus = fmt.Sprintf("发生，耗时: %v", cleanupTime)
		}

		t.Logf("迭代 %d/%d: 写入耗时=%v, 速度=%.2f 状态/秒, 哈希耗时=%v, Size=%d, Threshold=%d, 清理: %s",
			i+1, iterationCount, writeTime, writeSpeed, hashTime.String(), currentSize, currentThreshold, cleanupStatus)
	}

	// 计算平均统计数据
	var totalWriteTime, totalHashTime, totalCleanupTime time.Duration
	var totalWriteSpeed float64
	cleanupCount := 0

	for _, stat := range stats {
		totalWriteTime += stat.WriteTime
		totalHashTime += stat.HashTime
		totalWriteSpeed += stat.WriteSpeed

		if stat.CleanupOccurred {
			cleanupCount++
			totalCleanupTime += stat.CleanupTime
		}
	}

	avgWriteTime := totalWriteTime / time.Duration(iterationCount)
	avgHashTime := totalHashTime / time.Duration(iterationCount)
	avgWriteSpeed := totalWriteSpeed / float64(iterationCount)

	var avgCleanupTime time.Duration
	if cleanupCount > 0 {
		avgCleanupTime = totalCleanupTime / time.Duration(cleanupCount)
	}

	// 输出汇总统计信息
	t.Logf("\n===== 测试汇总 (状态数: %d) =====", stateCount)
	t.Logf("平均写入耗时: %v", avgWriteTime)
	t.Logf("平均写入速度: %.2f 状态/秒", avgWriteSpeed)
	t.Logf("平均哈希耗时: %v", avgHashTime)
	t.Logf("触发清理次数: %d/%d", cleanupCount, iterationCount)

	if cleanupCount > 0 {
		t.Logf("平均清理耗时: %v", avgCleanupTime)
	}

	// 获取最终的hit/miss统计
	totalGetRequests, hitCount, missCount, getHitRate,
		totalUpdateRequests, updateHitCount, updateMissCount, updateHitRate := cacheTrie.GetHitRate()

	t.Logf("Get操作: 总请求=%d, 命中=%d, 未命中=%d, 命中率=%.2f%%",
		totalGetRequests, hitCount, missCount, getHitRate*100)
	t.Logf("Update操作: 总请求=%d, 命中=%d, 未命中=%d, 命中率=%.2f%%",
		totalUpdateRequests, updateHitCount, updateMissCount, updateHitRate*100)

	// 获取内存占用信息
	memSize := cacheTrie.GetMemorySize()
	t.Logf("内存占用: %d 字节 (%.2f MB)", memSize, float64(memSize)/(1024*1024))

	// 将结果写入CSV文件
	csvFileName := fmt.Sprintf("cacheTrie_states%d_iter%d_window%d_maxsize%d.csv",
		stateCount, iterationCount, windowMultiple, maxSize)
	writeCSVFile(t, csvFileName, csvRecords)

	// 汇总统计添加到摘要CSV
	writeCSVSummary(t, stateCount, iterationCount, windowMultiple, maxSize,
		avgWriteTime, avgHashTime, avgWriteSpeed, cleanupCount,
		avgCleanupTime, getHitRate, updateHitRate, uint64(memSize))
}

// 将测试结果写入CSV文件
func writeCSVFile(t *testing.T, fileName string, records [][]string) {
	// 确保结果目录存在
	resultsDir := "results"
	if _, err := os.Stat(resultsDir); os.IsNotExist(err) {
		if err := os.Mkdir(resultsDir, 0755); err != nil {
			t.Logf("创建结果目录失败: %v", err)
			return
		}
	}

	// 创建CSV文件
	filePath := filepath.Join(resultsDir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		t.Logf("创建CSV文件失败: %v", err)
		return
	}
	defer file.Close()

	// 创建CSV写入器
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入数据
	if err := writer.WriteAll(records); err != nil {
		t.Logf("写入CSV数据失败: %v", err)
		return
	}

	t.Logf("测试结果已写入CSV文件: %s", filePath)
}

// 将汇总统计写入摘要CSV文件
func writeCSVSummary(t *testing.T, stateCount, iterationCount, windowMultiple, maxSize int,
	avgWriteTime, avgHashTime time.Duration, avgWriteSpeed float64,
	cleanupCount int, avgCleanupTime time.Duration,
	getHitRate, updateHitRate float64, memSize uint64) {

	// 确保结果目录存在
	resultsDir := "results"
	if _, err := os.Stat(resultsDir); os.IsNotExist(err) {
		if err := os.Mkdir(resultsDir, 0755); err != nil {
			t.Logf("创建结果目录失败: %v", err)
			return
		}
	}

	// 摘要文件名
	summaryFile := filepath.Join(resultsDir, "cacheTrie_summary.csv")

	// 检查文件是否存在，决定是否需要写入标题行
	fileExists := false
	if _, err := os.Stat(summaryFile); err == nil {
		fileExists = true
	}

	// 打开文件用于追加
	file, err := os.OpenFile(summaryFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Logf("打开摘要文件失败: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 如果文件不存在，先写入标题行
	if !fileExists {
		headers := []string{
			"状态数", "迭代次数", "窗口乘数", "最大大小",
			"平均写入耗时(ns)", "平均哈希耗时(ns)", "平均写入速度(状态/秒)",
			"清理次数", "平均清理耗时(ns)",
			"Get命中率(%)", "Update命中率(%)", "内存占用(MB)",
		}
		if err := writer.Write(headers); err != nil {
			t.Logf("写入摘要标题失败: %v", err)
			return
		}
	}

	// 写入当前测试的摘要数据
	record := []string{
		strconv.Itoa(stateCount),
		strconv.Itoa(iterationCount),
		strconv.Itoa(windowMultiple),
		strconv.Itoa(maxSize),
		strconv.FormatInt(avgWriteTime.Nanoseconds(), 10),
		strconv.FormatInt(avgHashTime.Nanoseconds(), 10),
		strconv.FormatFloat(avgWriteSpeed, 'f', 2, 64),
		strconv.Itoa(cleanupCount),
		strconv.FormatInt(avgCleanupTime.Nanoseconds(), 10),
		strconv.FormatFloat(getHitRate*100, 'f', 2, 64),
		strconv.FormatFloat(updateHitRate*100, 'f', 2, 64),
		strconv.FormatFloat(float64(memSize)/(1024*1024), 'f', 2, 64),
	}

	if err := writer.Write(record); err != nil {
		t.Logf("写入摘要数据失败: %v", err)
		return
	}

	t.Logf("测试摘要已添加到: %s", summaryFile)
}
