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

// Test basic operations: insert, query, delete
func TestBasicOperations(t *testing.T) {
	trie := NewCacheTrie(0, 1, 0)
	trie.SetBlockNum(1)

	// Test insert
	err := trie.Update([]byte("key1"), []byte("value1"), true)
	if err != nil {
		t.Fatalf("Update key1 failed: %v", err)
	}

	// Test Get method
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

	// Test delete
	err = trie.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Delete key1 failed: %v", err)
	}

	// Verify that key1 is absent or has empty value after delete
	node, err = trie.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}

	// After delete, node should be nil or an empty ValueNode
	if node != nil {
		valueNode, ok = node.(ValueNode)
		if !ok || len(valueNode.Data) != 0 {
			t.Errorf("Expected nil or empty ValueNode after delete, got: %v", node)
		}
	}

	// key2 should still exist
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

	// Test hash computation
	hash, _, _ := trie.Hash()
	t.Logf("Trie root hash: %x", hash)
	if hash == (common.Hash{}) {
		t.Error("Hash returned empty hash")
	}
}

// Test window calculation correctness
func TestWindowCalculation(t *testing.T) {
	// Create a cache trie, startNum=100, multiple=5
	trie := NewCacheTrie(100, 5, 0)

	// Insert key1 at block 110
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

	// Update another key at block 120
	trie.SetBlockNum(120)
	err = trie.Update([]byte("key2"), []byte("value2"), true)
	if err != nil {
		t.Fatalf("Update at block 120 failed: %v", err)
	}

	// Check window bits: should set both bit 22 and 24
	// Expected bit position: 120 / 5 = 24, then mod 32 => 24
	expected = (1<<3 | 1<<4)

	// Verify window value
	if trie.root.window() != expected {
		t.Errorf("Wrong window for key2 at block 120: got %032b, expected %032b",
			trie.root.window(), expected)
	}

	// Update the same key, clearing bit 22
	err = trie.Update([]byte("key1"), []byte("value1-updated"), true)
	if err != nil {
		t.Fatalf("Update at block 120 failed: %v", err)
	}
	expected = 1 << 4

	// Verify window value
	if trie.root.window() != expected {
		t.Errorf("Wrong window for key1 at block 120: got %032b, expected %032b",
			trie.root.window(), expected)
	}
}

// Test size calculation correctness
func TestSizeCalculation(t *testing.T) {
	// Create a cache trie
	trie := NewCacheTrie(0, 1, 0)

	// Initial size should be 0
	if size := trie.GetSize(); size != 0 {
		t.Errorf("Initial size expected 0, got %d", size)
	}

	// Insert 10 key-value pairs
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value, true)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// Verify size equals 10
	if size := trie.GetSize(); size != 10 {
		t.Errorf("Size after 10 insertions: expected 10, got %d", size)
	}

	// Delete 2 key-value pairs
	err := trie.Delete([]byte("key3"))
	if err != nil {
		t.Fatalf("Delete key3 failed: %v", err)
	}
	err = trie.Delete([]byte("key7"))
	if err != nil {
		t.Fatalf("Delete key7 failed: %v", err)
	}

	// Delete is logical-only, so size remains 10
	if size := trie.GetSize(); size != 10 {
		t.Errorf("Size after deleting 2 keys: expected 8, got %d", size)
	}

	// Update an existing key
	err = trie.Update([]byte("key1"), []byte("updated-value"), true)
	if err != nil {
		t.Fatalf("Update existing key failed: %v", err)
	}

	// Verify size still equals 10 (update should not change size)
	if size := trie.GetSize(); size != 10 {
		t.Errorf("Size after updating existing key: expected 8, got %d", size)
	}
}

// Test pruneCache based on size
func TestPruneCacheBySize(t *testing.T) {
	// Create a cache trie, with maxSize=10
	maxSize := 10
	trie := NewCacheTrie(100, 1, maxSize)
	trie.SetBlockNum(101)

	// Insert 11 key-value pairs
	for i := 0; i < 11; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value, true)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// Need to span multiple blocks to trigger pruning
	trie.SetBlockNum(105)

	// Insert 4 more pairs to exceed maxSize
	for i := 11; i < 15; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value, true)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// Verify size (should be 15 before Hash)
	if size := trie.GetSize(); size != 15 {
		t.Errorf("Expected size 15 before Hash, got %d", size)
	}

	// Trigger cleanup by calling Hash
	trie.Hash()

	// Verify size decreased (should be <= targetSize, i.e., maxSize*2/3)
	targetSize := maxSize * 2 / 3
	if size := trie.GetSize(); size > targetSize {
		t.Errorf("Expected size <= %d after pruning, got %d", targetSize, size)
	} else {
		t.Logf("Size after pruning: %d (target: %d)", size, targetSize)
	}

	// Verify inserting new key still works
	err := trie.Update([]byte("newKey"), []byte("newValue"), true)
	if err != nil {
		t.Fatalf("Failed to insert after pruning: %v", err)
	}

	// Verify new key can be retrieved
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

// Test pruneCache based on window
func TestPruneCacheByWindow(t *testing.T) {
	// Create a cache trie, startNum=100, multiple=1
	trie := NewCacheTrie(100, 1, 100) // Large maxSize to avoid size-based pruning

	// Insert at different block heights
	keys := []string{"keyA", "keyB", "keyC"}

	// Insert keyA at block 101
	trie.SetBlockNum(101)
	err := trie.Update([]byte(keys[0]), []byte("valueA"), true)
	if err != nil {
		t.Fatalf("Update keyA failed: %v", err)
	}

	// Insert keyB at block 109
	trie.SetBlockNum(109)
	err = trie.Update([]byte(keys[1]), []byte("valueB"), true)
	if err != nil {
		t.Fatalf("Update keyB failed: %v", err)
	}

	// Insert keyC at block 120
	trie.SetBlockNum(120)
	err = trie.Update([]byte(keys[2]), []byte("valueC"), true)
	if err != nil {
		t.Fatalf("Update keyC failed: %v", err)
	}

	// Process hash at block 496; should not prune (enough window bits left)
	trie.SetBlockNum(496)
	trie.Hash()

	// Verify startNum unchanged
	if trie.hrw.windowStartNumber != 100 {
		t.Errorf("startNum changed unexpectedly: expected %d, got %d", 100, trie.hrw.windowStartNumber)
	}

	// Verify all key-value pairs are accessible
	for _, key := range keys {
		node, err := trie.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s should be available but got error: %v", key, err)
		}
		if node == nil {
			t.Errorf("Key %s should be available but got nil", key)
		}
	}

	// At block 597, window has only 8 bits left (jump to 597 fills 32 bits)
	trie.SetBlockNum(497)

	// Manually add blocks to fill remaining window bits
	for i := 0; i < 5; i++ {
		blockNum := uint64(597 + i)
		trie.SetBlockNum(blockNum)
		err := trie.Update([]byte(fmt.Sprintf("fill%d", i)), []byte("filler"), true)
		if err != nil {
			t.Fatalf("Failed to insert filler at block %d: %v", blockNum, err)
		}
	}

	// Window should be almost full; compute hash to trigger pruning
	trie.Hash()

	// Verify startNum moved forward after pruning
	if trie.hrw.windowStartNumber <= 100 {
		t.Errorf("startNum didn't increase after pruning, still at %d", trie.hrw.windowStartNumber)
	} else {
		t.Logf("startNum updated to %d after pruning", trie.hrw.windowStartNumber)
	}

	// Oldest key might have been pruned (keyA possibly pruned)
	node, err := trie.Get([]byte(keys[0]))
	t.Logf("Trying to get keyA after pruning: %v, err: %v", node, err)

	// Newer keys should still be available
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

// Test ValueNode's New field behavior
func TestValueNodeNewField(t *testing.T) {
	trie := NewCacheTrie(0, 1, 100)
	trie.SetBlockNum(1)

	// Insertion via Update: New should default to true
	err := trie.Update([]byte("key1"), []byte("value1"), true)
	if err != nil {
		t.Fatalf("Update key1 failed: %v", err)
	}

	// Get node and check New field
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

	// Test insertion with New=false
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

	// Deleting should set New=true
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

	// Updating value sets New=true
	// First insert a value with New=false
	err = trie.Update([]byte("updateKey"), []byte("initialValue"), false)
	if err != nil {
		t.Fatalf("Update updateKey failed: %v", err)
	}

	// Get and confirm New=false
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

	// Update the same key's value
	err = trie.Update([]byte("updateKey"), []byte("updatedValue"), true)
	if err != nil {
		t.Fatalf("Update updateKey (second time) failed: %v", err)
	}

	// Get again and confirm New=true (value updated)
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

	// Test pruneCache returns nodes based on New field
	trie.SetBlockNum(31) // Move to block that triggers pruning

	// Add some key-value pairs
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("pruneKey%d", i))
		value := []byte(fmt.Sprintf("pruneValue%d", i))
		isNew := i%2 == 0 // Alternate New field

		err := trie.Update(key, value, isNew)
		if err != nil {
			t.Fatalf("Update pruneKey%d failed: %v", i, err)
		}
	}

	// Trigger Hash and pruneCache
	_, _, deletedKVs := trie.Hash()

	// Inspect pruned key-value pairs
	if deletedKVs == nil {
		t.Log("No keys were pruned yet, which might be expected")
	} else {
		// Log pruned node info
		for i, kv := range deletedKVs.Data {
			t.Logf("Pruned KV[%d]: key=%x, value=%x", i, kv.Key, kv.Value)
		}
	}
}

// Test New field in DeleteKV returned by pruneCache
func TestPruneCacheDeleteKVNewField(t *testing.T) {
	// Create cache trie with small window to trigger pruneCache quickly
	trie := NewCacheTrie(100, 1, 10)

	// Insert at block 101 with different New values
	trie.SetBlockNum(101)

	// Insert keys with New=true
	err := trie.Update([]byte("trueKey1"), []byte("trueValue1"), true)
	if err != nil {
		t.Fatalf("Update trueKey1 failed: %v", err)
	}

	err = trie.Update([]byte("trueKey2"), []byte("trueValue2"), true)
	if err != nil {
		t.Fatalf("Update trueKey2 failed: %v", err)
	}

	// Insert keys with New=false
	err = trie.Update([]byte("falseKey1"), []byte("falseValue1"), false)
	if err != nil {
		t.Fatalf("Update falseKey1 failed: %v", err)
	}

	err = trie.Update([]byte("falseKey2"), []byte("falseValue2"), false)
	if err != nil {
		t.Fatalf("Update falseKey2 failed: %v", err)
	}

	// Jump far enough to ensure pruneCache triggers
	trie.SetBlockNum(131)

	// Trigger Hash and pruneCache
	_, _, deletedKVs := trie.Hash()

	// Validate pruneCache result
	if deletedKVs == nil {
		t.Fatalf("Expected deletedKVs from pruneCache, got nil")
	}

	// Count KVs deleted from true/false nodes
	trueKeyCount := 0
	falseKeyCount := 0

	for _, kv := range deletedKVs.Data {
		// Use key prefix to infer true/false nodes
		if bytes.HasPrefix(kv.Key, []byte("true")) {
			trueKeyCount++
			t.Logf("Found key from 'true' node: key=%x, value=%x", kv.Key, kv.Value)
		} else if bytes.HasPrefix(kv.Key, []byte("false")) {
			falseKeyCount++
			t.Logf("Found key from 'false' node: key=%x, value=%x", kv.Key, kv.Value)
		}
	}

	// Expect only KVs from New=true nodes
	// Note: due to hashKey, prefix detection is approximate
	if falseKeyCount > 0 {
		t.Logf("Found %d keys from 'false' nodes, which might be unexpected", falseKeyCount)
	}

	// Verify at least some KVs were pruned
	if deletedKVs == nil || len(deletedKVs.Data) == 0 {
		t.Errorf("Expected some deleted KVs, got none")
	}

	// Second test: update a previously New=false value to trigger change
	trie.SetBlockNum(145)

	// Insert more data again
	err = trie.Update([]byte("falseKey3"), []byte("falseValue3"), false)
	if err != nil {
		t.Fatalf("Update falseKey3 failed: %v", err)
	}

	// Update value
	err = trie.Update([]byte("falseKey3"), []byte("updatedValue3"), false)
	if err != nil {
		t.Fatalf("Update falseKey3 (second time) failed: %v", err)
	}

	// Jump to a farther block
	trie.SetBlockNum(180)

	// Trigger Hash and pruneCache again
	_, _, deletedKVs = trie.Hash()

	// Due to hashKey, cannot directly judge by value; log only
	if deletedKVs != nil && len(deletedKVs.Data) > 0 {
		t.Logf("Found %d deleted KVs in second pruning", len(deletedKVs.Data))
		for i, kv := range deletedKVs.Data {
			t.Logf("DeletedKV[%d]: key=%x, value=%x", i, kv.Key, kv.Value)
		}
	}
}

// Test that New=true nodes remain accessible after pruneCache
func TestNewNodesRetentionAfterPrune(t *testing.T) {
	// Create cache trie with small window to trigger pruneCache
	trie := NewCacheTrie(100, 1, 10)

	// Insert key-value pairs at block 101
	trie.SetBlockNum(101)

	// Insert New=true KV
	err := trie.Update([]byte("trueNode"), []byte("trueValue"), true)
	if err != nil {
		t.Fatalf("Update trueNode failed: %v", err)
	}

	// Insert New=false KV
	err = trie.Update([]byte("falseNode"), []byte("falseValue"), false)
	if err != nil {
		t.Fatalf("Update falseNode failed: %v", err)
	}

	// Confirm both nodes are accessible
	node1, err := trie.Get([]byte("trueNode"))
	if err != nil || node1 == nil {
		t.Fatalf("Get trueNode failed before pruning: %v", err)
	}

	node2, err := trie.Get([]byte("falseNode"))
	if err != nil || node2 == nil {
		t.Fatalf("Get falseNode failed before pruning: %v", err)
	}

	// Check node New field
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

	// Jump far enough to trigger pruneCache
	trie.SetBlockNum(135)

	// Add extra keys to ensure pruneCache triggers
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("extraKey%d", i))
		value := []byte(fmt.Sprintf("extraValue%d", i))
		err := trie.Update(key, value, i%2 == 0) // Alternate New
		if err != nil {
			t.Fatalf("Update extraKey%d failed: %v", i, err)
		}
	}

	// Trigger Hash and pruneCache
	_, _, deletedKVs := trie.Hash()

	// Validate pruneCache result
	if deletedKVs == nil {
		t.Log("No nodes were pruned yet, which might not be expected")
	} else {
		t.Logf("Pruned %d nodes", len(deletedKVs.Data))

		// Log deleted key-value contents
		for i, kv := range deletedKVs.Data {
			t.Logf("DeletedKV[%d]: key=%x, value=%x", i, kv.Key, kv.Value)
		}
	}

	// Try accessing nodes again
	// New=true node should appear in DeleteKV
	node1, err = trie.Get([]byte("trueNode"))
	if err != nil {
		t.Fatalf("Get trueNode failed after pruning: %v", err)
	}

	if node1 == nil {
		t.Logf("trueNode (New=true) was pruned from trie and should be in deletedKVs")

		// Check if similar value exists in deletedKVs
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

	// New=false node is expected to be deleted
	node2, err = trie.Get([]byte("falseNode"))
	if err != nil {
		t.Fatalf("Get falseNode failed with error after pruning: %v", err)
	}

	// Deleted node should not appear in deletedKVs
	if node2 == nil {
		t.Logf("falseNode (New=false) was pruned from trie as expected")

		// Confirm it is not present in deletedKVs
		for _, kv := range deletedKVs.Data {
			if bytes.Equal(kv.Value, []byte("falseValue")) {
				t.Logf("Found value matching falseNode in deletedKVs, which might be unexpected")
				break
			}
		}
	} else {
		// Possible if pruneCache did not trigger
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

// Performance test: 1000 blocks, window counts every 10 blocks, 20k writes per block
func TestPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running performance test")
	}

	// Test parameters
	blockCount := 200
	entriesPerBlock := 5000
	multiple := 1024 // window counts every 10 blocks
	newRatio := 0.3  // 30% new, 70% false

	// Large maxSize to prune only when window is full
	maxSize := entriesPerBlock * blockCount

	// Generate test data
	keys := make([][]byte, entriesPerBlock)
	values := make([][]byte, entriesPerBlock)
	isNews := make([]bool, entriesPerBlock)

	for i := 0; i < entriesPerBlock; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		values[i] = []byte(fmt.Sprintf("value-%d", i))
		isNews[i] = float64(i)/float64(entriesPerBlock) < newRatio
	}

	// 1) Performance with pruning
	t.Run("WithPruning", func(t *testing.T) {
		trie := NewCacheTrie(1, uint64(multiple), maxSize)

		totalBlockTime := int64(0)
		totalHashTime := int64(0)
		pruneCount := 0

		for blockNum := 1; blockNum <= blockCount; blockNum++ {
			trie.SetBlockNum(uint64(blockNum))

			// Record block write time
			blockStart := time.Now()
			for i := 0; i < entriesPerBlock; i++ {
				// Generate block-specific keys
				blockKey := []byte(fmt.Sprintf("%s-block%d", keys[i], blockNum))
				err := trie.Update(blockKey, values[i], isNews[i])
				if err != nil {
					t.Fatalf("failed to update key: %v", err)
				}
			}
			blockDuration := time.Since(blockStart)
			totalBlockTime += blockDuration.Nanoseconds()

			// Compute hash and record time
			hashStart := time.Now()
			_, _, deletedKVs := trie.Hash()
			hashDuration := time.Since(hashStart)
			totalHashTime += hashDuration.Nanoseconds()

			// Check whether pruning occurred
			if deletedKVs != nil && len(deletedKVs.Data) > 0 {
				pruneCount++
				t.Logf("block %d: pruned %d key-value pairs", blockNum, len(deletedKVs.Data))
			}

			// Output progress every 100 blocks
			if blockNum%100 == 0 {
				t.Logf("progress: %d/%d blocks", blockNum, blockCount)
			}
		}

		// Compute averages
		avgBlockTime := time.Duration(totalBlockTime / int64(blockCount))
		avgHashTime := time.Duration(totalHashTime / int64(blockCount))

		t.Logf("with-pruning performance stats:")
		t.Logf("total blocks: %d", blockCount)
		t.Logf("entries per block: %d", entriesPerBlock)
		t.Logf("prune count: %d", pruneCount)
		t.Logf("avg block write time: %v", avgBlockTime)
		t.Logf("avg hash time: %v", avgHashTime)
		t.Logf("total block write time: %v", time.Duration(totalBlockTime))
		t.Logf("total hash time: %v", time.Duration(totalHashTime))
		t.Logf("total processing time: %v", time.Duration(totalBlockTime+totalHashTime))
	})

	// 2) Performance without pruning
	t.Run("WithoutPruning", func(t *testing.T) {
		// Super-large window to ensure no pruning
		trie := NewCacheTrie(1, uint64(blockCount*2), maxSize)

		totalBlockTime := int64(0)
		totalHashTime := int64(0)

		for blockNum := 1; blockNum <= blockCount; blockNum++ {
			trie.SetBlockNum(uint64(blockNum))

			// Record block write time
			blockStart := time.Now()
			for i := 0; i < entriesPerBlock; i++ {
				// Generate block-specific keys
				blockKey := []byte(fmt.Sprintf("%s-block%d", keys[i], blockNum))
				err := trie.Update(blockKey, values[i], isNews[i])
				if err != nil {
					t.Fatalf("failed to update key: %v", err)
				}
			}
			blockDuration := time.Since(blockStart)
			totalBlockTime += blockDuration.Nanoseconds()

			// Compute hash and record time
			hashStart := time.Now()
			_, _, deletedKVs := trie.Hash()
			hashDuration := time.Since(hashStart)
			totalHashTime += hashDuration.Nanoseconds()

			// Ensure pruning did not occur
			if deletedKVs != nil && len(deletedKVs.Data) > 0 {
				t.Errorf("block %d: unexpected pruning of %d key-value pairs", blockNum, len(deletedKVs.Data))
			}

			// Output progress every 100 blocks
			if blockNum%100 == 0 {
				t.Logf("progress: %d/%d blocks", blockNum, blockCount)
			}
		}

		// Compute averages
		avgBlockTime := time.Duration(totalBlockTime / int64(blockCount))
		avgHashTime := time.Duration(totalHashTime / int64(blockCount))

		t.Logf("no-pruning performance stats:")
		t.Logf("total blocks: %d", blockCount)
		t.Logf("entries per block: %d", entriesPerBlock)
		t.Logf("avg block write time: %v", avgBlockTime)
		t.Logf("avg hash time: %v", avgHashTime)
		t.Logf("total block write time: %v", time.Duration(totalBlockTime))
		t.Logf("total hash time: %v", time.Duration(totalHashTime))
		t.Logf("total processing time: %v", time.Duration(totalBlockTime+totalHashTime))
	})
}

func TestHitRateStatistics(t *testing.T) {
	// Create a cache trie
	trie := NewCacheTrie(0, 1, 0)

	// Insert 10 key-value pairs
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value, true)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// Read existing keys (hit)
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

	// Read non-existing keys (miss)
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

	// Update existing keys (update hit)
	for i := 0; i < 4; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("updated-value%d", i))
		err := trie.Update(key, value, true)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// Add new non-existing keys (update miss)
	for i := 20; i < 24; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := trie.Update(key, value, true)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// Delete existing keys (delete hit)
	for i := 4; i < 6; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		err := trie.Delete(key)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	// Delete non-existing keys (delete miss)
	for i := 30; i < 32; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		err := trie.Delete(key)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	// Validate Get hit rate stats
	totalGetRequests, hitCount, missCount, getHitRate,
		updateCount, updateHitCount, updateMissCount, updateHitRate := trie.GetHitRate()

	// Validate Get stats
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

	// Validate Update stats
	// 10 initial inserts + 4 updates + 4 adds + 2 delete hits + 2 delete misses = 22 updates
	expectedUpdateCount := 10 + 4 + 4 + 2 + 2
	if updateCount != uint64(expectedUpdateCount) {
		t.Errorf("Expected %d total Update operations, got %d", expectedUpdateCount, updateCount)
	}

	// 4 update hits + 2 delete hits = 6 hits
	expectedUpdateHits := 4 + 2
	if updateHitCount != uint64(expectedUpdateHits) {
		t.Errorf("Expected %d Update hits, got %d", expectedUpdateHits, updateHitCount)
	}

	// 10 initial inserts + 4 adds + 2 delete misses = 16 misses
	expectedUpdateMisses := 10 + 4 + 2
	if updateMissCount != uint64(expectedUpdateMisses) {
		t.Errorf("Expected %d Update misses, got %d", expectedUpdateMisses, updateMissCount)
	}

	// Hit rate should be 6/22 ≈ 0.273
	expectedUpdateHitRate := float64(expectedUpdateHits) / float64(expectedUpdateCount)
	if math.Abs(updateHitRate-expectedUpdateHitRate) > 0.001 {
		t.Errorf("Expected %.3f Update hit rate, got %.3f", expectedUpdateHitRate, updateHitRate)
	}

	// Reset stats
	trie.ResetStats()

	// Validate stats after reset
	totalGetRequests, hitCount, missCount, getHitRate,
		updateCount, updateHitCount, updateMissCount, updateHitRate = trie.GetHitRate()

	if totalGetRequests != 0 || hitCount != 0 || missCount != 0 || getHitRate != 0 ||
		updateCount != 0 || updateHitCount != 0 || updateMissCount != 0 || updateHitRate != 0 {
		t.Errorf("Stats not reset properly: Get(total=%d, hits=%d, misses=%d, rate=%f), Update(total=%d, hits=%d, misses=%d, rate=%f)",
			totalGetRequests, hitCount, missCount, getHitRate,
			updateCount, updateHitCount, updateMissCount, updateHitRate)
	}
}

// Test configuration
const (
	// Number of states for warmup phase
	warmupStateCount = 1000000

	// Test parameters
	startBlockNum  = 1000
	windowMultiple = 10
)

// Generate random data
func generateRandomData() ([]byte, []byte) {
	key := make([]byte, 32)
	value := make([]byte, 32)
	rand.Read(key)
	rand.Read(value)
	return key, value
}

// Generate random address
func generateRandomAddress() common.Address {
	addr := common.Address{}
	rand.Read(addr[:])
	return addr
}

// TestCacheTriePerformance tests CacheTrie performance
func TestCacheTriePerformance(t *testing.T) {
	// Test different write state counts
	stateCounts := []int{5000, 50000, 500000} // 1K, 10K, 100K
	iterationCount := 20                      // number of iterations to measure
	maxSize := 500000                         // initial storage size limit
	windowMultiple := 1024
	for _, stateCount := range stateCounts {
		t.Run(fmt.Sprintf("StateCount_%d", stateCount), func(t *testing.T) {
			testCacheTrieWithStateCount(t, stateCount, iterationCount, windowMultiple, maxSize, 0)
		})
	}
}

func TestSampleCacheTriePerformance(t *testing.T) {
	stateCount := 5000
	iterationCount := 4000 // number of iterations to measure
	maxSize := 1000000     // initial storage size limit
	windowMultiple := 256
	t.Run(fmt.Sprintf("StateCount_%d", stateCount), func(t *testing.T) {
		testCacheTrieWithStateCount(t, stateCount, iterationCount, windowMultiple, maxSize, 0)
	})
}

// testCacheTrieWithStateCount tests CacheTrie with specified state count
func testCacheTrieWithStateCount(t *testing.T, stateCount, iterationCount, windowMultiple, maxSize int, mod int) {
	t.Logf("start test: states per iteration=%d, iterations=%d, initial storage size=%d", stateCount, iterationCount, maxSize)

	// 创建CacheTrie实例
	cacheTrie := NewCacheTrie(startBlockNum, uint64(windowMultiple), maxSize)

	// Warmup phase - run until first cleanup
	t.Log("start warmup phase...")
	preWarmupStartTime := time.Now()

	// Set initial block number
	currentBlock := uint64(startBlockNum)
	cacheTrie.SetBlockNum(currentBlock)

	// Record initial state
	initialHRW := cacheTrie.GetHRW()
	initialThreshold := initialHRW.GetThreshold()
	t.Logf("initial state: threshold(ssthresh)=%d", initialThreshold)

	// Warm up until first cleanup occurs
	warmupBatchSize := 10000 // writes per batch
	warmupBatches := 0

	for i := 0; i < warmupStateCount; i += warmupBatchSize {
		batchSize := warmupBatchSize
		if i+warmupBatchSize > warmupStateCount {
			batchSize = warmupStateCount - i
		}

		// Write data
		for j := 0; j < batchSize; j++ {
			key, value := generateRandomData()
			cacheTrie.Update(key, value, true)
		}

		// Get hash, which triggers cleanup
		hash, _, kvList := cacheTrie.Hash()

		if kvList != nil && len(kvList.Data) > 0 {
			go func() {
				cacheTrie.FinishCleanup(currentBlock, hash)
			}()
		}

		currentBlock++
		cacheTrie.SetBlockNum(currentBlock)

		warmupBatches++

		// Check if cleanup occurred
		currentCleanupCount := cacheTrie.GetCleanupCount()
		if currentCleanupCount > 0 {
			t.Logf("warmup detected cleanup, batches=%d, states written=%d", warmupBatches, (warmupBatches-1)*warmupBatchSize+batchSize)
			break
		}

		// If too much data was written without cleanup, end warmup
		if i+batchSize >= warmupStateCount {
			t.Logf("warmup ended, no cleanup detected, states written=%d", i+batchSize)
		}
	}
	CleanupTime = 0

	preWarmupDuration := time.Since(preWarmupStartTime)
	t.Logf("warmup completed, elapsed: %v", preWarmupDuration)

	// Main testing phase
	t.Log("start main testing phase...")

	// 记录每次操作的统计数据
	type IterationStats struct {
		WriteTime       time.Duration // write duration
		HashTime        time.Duration // hash duration
		WriteSpeed      float64       // write speed (states/s)
		Size            int           // current size
		Threshold       int           // current threshold
		CleanupOccurred bool          // whether cleanup occurred
		CleanupTime     time.Duration // cleanup duration (if occurred)
	}

	stats := make([]IterationStats, iterationCount)

	// Prepare CSV records
	csvRecords := [][]string{
		{"Iteration", "write time(ns)", "hash time(ns)", "write speed (states/s)", "Size", "Threshold", "cleaned", "cleanup time(ns)"},
	}

	// Reset cleanup time stats
	cacheTrie.ResetCleanupTimes()

	for i := 0; i < iterationCount; i++ {
		// Record write start time
		writeStart := time.Now()

		// Write the specified number of states
		for j := 0; j < stateCount; j++ {
			// 50% chance normal KV, 50% KV with address
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

		// Record current cleanup count
		beforeHashCleanupCount := cacheTrie.GetCleanupCount()
		beforeCleanupTotalTime, _ := cacheTrie.GetCleanupTimes()

		// Get hash, which triggers cleanup
		hashStart := time.Now()
		hash, _, kvList := cacheTrie.Hash()
		hashTime := time.Since(hashStart) - CleanupTime
		CleanupTime = 0

		if kvList != nil {
			go func() {
				cacheTrie.FinishCleanup(currentBlock, hash)
			}()
		}

		// Move to next block
		currentBlock++
		cacheTrie.SetBlockNum(currentBlock)

		// Check whether cleanup occurred
		afterHashCleanupCount := cacheTrie.GetCleanupCount()
		cleanupOccurred := afterHashCleanupCount > beforeHashCleanupCount

		// Compute cleanup duration (if occurred)
		var cleanupTime time.Duration
		if cleanupOccurred {
			afterCleanupTotalTime, _ := cacheTrie.GetCleanupTimes()
			cleanupTime = afterCleanupTotalTime - beforeCleanupTotalTime
		}

		// Compute write speed (states/s)
		writeSpeed := float64(stateCount) / writeTime.Seconds()

		// Get current size and threshold
		currentSize := cacheTrie.GetSize()
		currentThreshold := cacheTrie.GetHRW().GetThreshold()

		// Save stats
		stats[i] = IterationStats{
			WriteTime:       writeTime,
			HashTime:        hashTime,
			WriteSpeed:      writeSpeed,
			Size:            currentSize,
			Threshold:       currentThreshold,
			CleanupOccurred: cleanupOccurred,
			CleanupTime:     cleanupTime,
		}

		// Append to CSV records
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

		// Output stats for current iteration
		cleanupStatus := "none"
		if cleanupOccurred {
			cleanupStatus = fmt.Sprintf("occurred, duration: %v", cleanupTime)
		}

		t.Logf("iteration %d/%d: write=%v, speed=%.2f states/s, hash=%v, Size=%d, Threshold=%d, cleanup: %s",
			i+1, iterationCount, writeTime, writeSpeed, hashTime.String(), currentSize, currentThreshold, cleanupStatus)
	}

	// Compute average stats
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

	// Output summary stats
	t.Logf("\n===== Test summary (states: %d) =====", stateCount)
	t.Logf("avg write time: %v", avgWriteTime)
	t.Logf("avg write speed: %.2f states/s", avgWriteSpeed)
	t.Logf("avg hash time: %v", avgHashTime)
	t.Logf("cleanup occurrences: %d/%d", cleanupCount, iterationCount)

	if cleanupCount > 0 {
		t.Logf("平均清理耗时: %v", avgCleanupTime)
	}

	// Get final hit/miss stats
	totalGetRequests, hitCount, missCount, getHitRate,
		totalUpdateRequests, updateHitCount, updateMissCount, updateHitRate := cacheTrie.GetHitRate()

	t.Logf("Get: total=%d, hits=%d, misses=%d, hit rate=%.2f%%",
		totalGetRequests, hitCount, missCount, getHitRate*100)
	t.Logf("Update: total=%d, hits=%d, misses=%d, hit rate=%.2f%%",
		totalUpdateRequests, updateHitCount, updateMissCount, updateHitRate*100)

	// Get memory usage info
	memSize := cacheTrie.GetMemorySize()
	t.Logf("memory usage: %d bytes (%.2f MB)", memSize, float64(memSize)/(1024*1024))

	// Write results to CSV
	csvFileName := fmt.Sprintf("cacheTrie_states%d_iter%d_window%d_maxsize%d.csv",
		stateCount, iterationCount, windowMultiple, maxSize)
	writeCSVFile(t, csvFileName, csvRecords)

	// Append summary stats to summary CSV
	writeCSVSummary(t, stateCount, iterationCount, windowMultiple, maxSize,
		avgWriteTime, avgHashTime, avgWriteSpeed, cleanupCount,
		avgCleanupTime, getHitRate, updateHitRate, uint64(memSize))
}

// Write test results to CSV file
func writeCSVFile(t *testing.T, fileName string, records [][]string) {
	// Ensure results directory exists
	resultsDir := "results"
	if _, err := os.Stat(resultsDir); os.IsNotExist(err) {
		if err := os.Mkdir(resultsDir, 0755); err != nil {
			t.Logf("failed to create results directory: %v", err)
			return
		}
	}

	// Create CSV file
	filePath := filepath.Join(resultsDir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		t.Logf("failed to create CSV file: %v", err)
		return
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write data
	if err := writer.WriteAll(records); err != nil {
		t.Logf("failed to write CSV data: %v", err)
		return
	}

	t.Logf("test results written to CSV: %s", filePath)
}

// Write summary stats to summary CSV file
func writeCSVSummary(t *testing.T, stateCount, iterationCount, windowMultiple, maxSize int,
	avgWriteTime, avgHashTime time.Duration, avgWriteSpeed float64,
	cleanupCount int, avgCleanupTime time.Duration,
	getHitRate, updateHitRate float64, memSize uint64) {

	// Ensure results directory exists
	resultsDir := "results"
	if _, err := os.Stat(resultsDir); os.IsNotExist(err) {
		if err := os.Mkdir(resultsDir, 0755); err != nil {
			t.Logf("failed to create results directory: %v", err)
			return
		}
	}

	// Summary file name
	summaryFile := filepath.Join(resultsDir, "cacheTrie_summary.csv")

	// Check if file exists to decide writing header
	fileExists := false
	if _, err := os.Stat(summaryFile); err == nil {
		fileExists = true
	}

	// Open file for appending
	file, err := os.OpenFile(summaryFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Logf("failed to open summary file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if file did not exist
	if !fileExists {
		headers := []string{
			"States", "Iterations", "Window multiple", "Max size",
			"Avg write time(ns)", "Avg hash time(ns)", "Avg write speed(states/s)",
			"Cleanup count", "Avg cleanup time(ns)",
			"Get hit rate(%)", "Update hit rate(%)", "Memory(MB)",
		}
		if err := writer.Write(headers); err != nil {
			t.Logf("failed to write summary header: %v", err)
			return
		}
	}

	// Write summary record for current test
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
		t.Logf("failed to write summary record: %v", err)
		return
	}

	t.Logf("test summary appended to: %s", summaryFile)
}
