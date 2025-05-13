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
)

// 测试辅助函数，打印树结构
func dumpTrie(t *testing.T, trie *CacheTrie) {
	t.Logf("Trie dump: %v", trie.root)
}

// 测试比较两个字节数组
func compareBytesSlice(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// 测试比较查找结果
func compareGetResult(t *testing.T, key []byte, expected, actual []byte) {
	if expected == nil && actual == nil {
		t.Logf("Key %q: both results are nil, test passed", key)
		return
	}

	if expected == nil {
		t.Errorf("Key %q: expected nil, got %q", key, actual)
		return
	}

	if actual == nil {
		t.Errorf("Key %q: expected %q, got nil", key, expected)
		return
	}

	if !compareBytesSlice(expected, actual) {
		t.Errorf("Key %q: expected %q, got %q", key, expected, actual)
		return
	}

	t.Logf("Key %q: got expected %q", key, actual)
}

// 辅助函数，将原始key转换为十六进制格式并打印，用于调试
func debugKey(key []byte) string {
	hexKey := keybytesToHex(key)
	return fmt.Sprintf("Key: %q, Hex: %v", key, hexKey)
}
