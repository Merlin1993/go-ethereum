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

package state

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// TestCacheTreeVsTriePerformance 测试普通Trie和CacheTree在大量数据上的读写性能差异
func TestCacheTreeVsTriePerformance(t *testing.T) {
	// 跳过短测试
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	// 测试数据量
	const numEntries = 200000

	// 生成随机账户地址和值
	addresses := make([]common.Address, numEntries)
	values := make([]*types.StateAccount, numEntries)
	randomAddrs := make([]common.Address, numEntries)

	// 初始化随机数据
	for i := 0; i < numEntries; i++ {
		addrBytes := make([]byte, 20)
		rand.Read(addrBytes)
		addresses[i] = common.BytesToAddress(addrBytes)

		// 创建随机账户
		balance := uint256.NewInt(1)
		root := common.HexToHash(fmt.Sprintf("0x%x", i+1))
		codeHash := common.HexToHash(fmt.Sprintf("0x%x", i+2))
		values[i] = &types.StateAccount{
			Nonce:    uint64(i),
			Balance:  new(uint256.Int).Set(balance),
			Root:     root,
			CodeHash: codeHash[:],
		}

		// 用于读取测试的随机地址
		randBytes := make([]byte, 20)
		rand.Read(randBytes)
		randomAddrs[i] = common.BytesToAddress(randBytes)
	}

	// 测试普通Trie
	testTriePerformance(t, false, addresses, values, randomAddrs)

	// 测试CacheTree
	testTriePerformance(t, true, addresses, values, randomAddrs)
}

// testTriePerformance 测试指定Trie实现的性能
func testTriePerformance(t *testing.T, useCacheTree bool, addresses []common.Address, values []*types.StateAccount, randomAddrs []common.Address) {
	// 创建数据库
	memdb := rawdb.NewMemoryDatabase()

	// 配置
	config := &triedb.Config{
		Preimages: false,
		IsVerkle:  false,
		CacheTrie: useCacheTree,
		HashDB:    triedb.HashDefaults.HashDB,
	}

	db := triedb.NewDatabase(memdb, config)
	defer db.Close()

	// 创建state
	state, err := New(types.EmptyRootHash, NewDatabase(db, nil))

	if err != nil {
		t.Fatalf("failed to create trie: %v", err)
	}

	// 写入测试
	startWrite := time.Now()
	for i := 0; i < len(addresses); i++ {
		keyHash := common.BytesToHash(addresses[i].Bytes())
		valueHash := common.BytesToHash([]byte(fmt.Sprintf("value-%d", i)))
		state.SetState(addresses[i], keyHash, valueHash)
	}
	writeTime := time.Since(startWrite)
	t.Logf("Write operation (%s): %v for %d entries (%.2f entries/s)",
		trieTypeStr(useCacheTree), writeTime, len(addresses), float64(len(addresses))/writeTime.Seconds())

	// 计算根哈希
	startRootCalc := time.Now()
	root, _ := state.Commit(0, false, false)
	rootCalcTime := time.Since(startRootCalc)
	t.Logf("Root hash calculation (%s): %v (%.2f entries/s)",
		trieTypeStr(useCacheTree), rootCalcTime, float64(len(addresses))/rootCalcTime.Seconds())

	// 提交更改到数据库
	startDBWrite := time.Now()
	db.Commit(root, false)
	dbWriteTime := time.Since(startDBWrite)
	t.Logf("Database commit (%s): %v (%.2f entries/s)",
		trieTypeStr(useCacheTree), dbWriteTime, float64(len(addresses))/dbWriteTime.Seconds())

	totalTime := writeTime + rootCalcTime + dbWriteTime
	t.Logf("Total write+commit (%s): %v for %d entries (%.2f entries/s)",
		trieTypeStr(useCacheTree), totalTime, len(addresses), float64(len(addresses))/totalTime.Seconds())

	// 重新加载state
	state, err = New(root, NewDatabase(db, nil))

	if err != nil {
		t.Fatalf("failed to create trie with root %x: %v", root, err)
	}

	// 读取测试 - 顺序读取
	startRead := time.Now()
	for i := 0; i < len(addresses); i++ {
		_ = state.GetBalance(addresses[i])
	}
	readTime := time.Since(startRead)
	t.Logf("Sequential read performance (%s): %v for %d entries (%.2f entries/s)",
		trieTypeStr(useCacheTree), readTime, len(addresses), float64(len(addresses))/readTime.Seconds())

	// 读取测试 - 随机地址读取 (大部分会miss)
	startRandomRead := time.Now()
	for i := 0; i < len(randomAddrs); i++ {
		_ = state.GetBalance(randomAddrs[i])
	}
	randomReadTime := time.Since(startRandomRead)
	t.Logf("Random read performance (%s): %v for %d entries (%.2f entries/s)",
		trieTypeStr(useCacheTree), randomReadTime, len(randomAddrs), float64(len(randomAddrs))/randomReadTime.Seconds())
}

// trieTypeStr 返回Trie类型的字符串表示
func trieTypeStr(useCacheTree bool) string {
	if useCacheTree {
		return "CacheTree"
	}
	return "Standard Trie"
}
