// Copyright 2024 The go-ethereum Authors
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

package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/ethereum/go-verkle"

	"github.com/ethereum/go-ethereum/trie/trienode"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
	blake3 "github.com/zeebo/blake3"
)

// min returns the smaller of x or y.
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func TestBlake3(t *testing.T) {
	h := blake3.New()
	h.Write([]byte("some data"))
	t.Logf("Blake3 hash: %x", h.Sum(nil))
}

func BenchmarkBlake3(b *testing.B) {
	data := []byte("some data")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := blake3.New()
		h.Write(data)
		h.Sum(nil)
	}
}

func BenchmarkSha256(b *testing.B) {
	data := []byte("some data")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := sha256.New()
		h.Write(data)
		h.Sum(nil)
	}
}

// TestBasicVerkleTreeOperations 测试verkle树的基本操作，包括创建、插入、持久化和验证
func TestBasicVerkleTreeOperations(t *testing.T) {
	// 创建临时目录用于存储数据库文件
	tempDir := t.TempDir()

	// 创建持久化数据库，而不是内存数据库
	ldb, err := leveldb.New(tempDir, 128, 128, "verkle_test", false)
	if err != nil {
		t.Fatalf("创建磁盘数据库失败: %v", err)
	}
	defer ldb.Close()

	cacheConfig := DefaultCacheConfigWithScheme(rawdb.PathScheme)
	diskDB := rawdb.NewDatabase(ldb)
	db := triedb.NewDatabase(diskDB, cacheConfig.triedbConfig(true))

	// 创建一个新的verkle树
	tr, err := trie.NewVerkleTrie(types.EmptyVerkleHash, db, utils.NewPointCache(100))
	if err != nil {
		t.Fatalf("创建verkle树失败: %v", err)
	}

	// 测试账户数据
	testAccounts := map[common.Address]*types.StateAccount{
		common.HexToAddress("0x1111111111111111111111111111111111111111"): {
			Nonce:    1,
			Balance:  uint256.NewInt(1000),
			CodeHash: crypto.Keccak256Hash([]byte("code1")).Bytes(),
		},
		common.HexToAddress("0x2222222222222222222222222222222222222222"): {
			Nonce:    2,
			Balance:  uint256.NewInt(2000),
			CodeHash: crypto.Keccak256Hash([]byte("code2")).Bytes(),
		},
	}

	// 测试存储数据
	testStorages := map[common.Address]map[common.Hash][]byte{
		common.HexToAddress("0x1111111111111111111111111111111111111111"): {
			common.HexToHash("0x1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a"): []byte{0x1a},
			common.HexToHash("0x1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b"): []byte{0x1b},
		},
		common.HexToAddress("0x2222222222222222222222222222222222222222"): {
			common.HexToHash("0x2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a"): []byte{0x2a},
			common.HexToHash("0x2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b"): []byte{0x2b},
		},
	}

	// 测试代码数据
	testCodes := map[common.Address][]byte{
		common.HexToAddress("0x1111111111111111111111111111111111111111"): []byte("code1"),
		common.HexToAddress("0x2222222222222222222222222222222222222222"): []byte("code2"),
	}

	// 将账户数据插入到verkle树中
	for addr, acct := range testAccounts {
		if err := tr.UpdateAccount(addr, acct, len(testCodes[addr])); err != nil {
			t.Fatalf("更新账户失败: %v", err)
		}

		// 更新合约代码
		codeHash := crypto.Keccak256Hash(testCodes[addr])
		if err := tr.UpdateContractCode(addr, codeHash, testCodes[addr]); err != nil {
			t.Fatalf("更新合约代码失败: %v", err)
		}

		// 更新存储数据
		for key, val := range testStorages[addr] {
			if err := tr.UpdateStorage(addr, key.Bytes(), val); err != nil {
				t.Fatalf("更新存储数据失败: %v", err)
			}
		}
	}

	// 获取verkle树的根哈希
	root, _ := tr.Commit(false)

	t.Logf("Verkle树根哈希: %x", root)

	// 验证账户数据
	for addr, expectedAcct := range testAccounts {
		actualAcct, err := tr.GetAccount(addr)
		if err != nil {
			t.Fatalf("获取账户失败: %v", err)
		}
		if actualAcct.Nonce != expectedAcct.Nonce {
			t.Errorf("账户nonce不匹配: 期望 %d, 实际 %d", expectedAcct.Nonce, actualAcct.Nonce)
		}
		if !actualAcct.Balance.Eq(expectedAcct.Balance) {
			t.Errorf("账户余额不匹配: 期望 %s, 实际 %s", expectedAcct.Balance.Dec(), actualAcct.Balance.Dec())
		}
		if !bytes.Equal(actualAcct.CodeHash, expectedAcct.CodeHash) {
			t.Errorf("账户代码哈希不匹配: 期望 %x, 实际 %x", expectedAcct.CodeHash, actualAcct.CodeHash)
		}
	}

	// 验证存储数据
	for addr, storage := range testStorages {
		for key, expectedVal := range storage {
			actualVal, err := tr.GetStorage(addr, key.Bytes())
			if err != nil {
				t.Fatalf("获取存储数据失败: %v", err)
			}
			if !bytes.Equal(actualVal, expectedVal) {
				t.Errorf("存储数据不匹配: 期望 %x, 实际 %x", expectedVal, actualVal)
			}
		}
	}

	// 将数据持久化到磁盘
	rootHash, nodeset := tr.Commit(false)

	// 首先使用Update将节点集保存到数据库
	mergedNodeset := trienode.NewWithNodeSet(nodeset)

	// 使用空的StateSet（只含有Verkle树节点）
	stateSet := triedb.NewStateSet()

	if err := db.Update(rootHash, common.Hash{}, 0, mergedNodeset, stateSet); err != nil {
		t.Fatalf("更新数据库失败: %v", err)
	}

	// 然后使用Commit确保数据被刷新到磁盘
	if err := db.Commit(rootHash, false); err != nil {
		t.Fatalf("提交数据到磁盘失败: %v", err)
	}

	// 验证数据已正确持久化
	// 重新打开数据库和verkle树
	ldb.Close()

	ldb2, err := leveldb.New(tempDir, 128, 128, "verkle_test", false)
	if err != nil {
		t.Fatalf("重新打开磁盘数据库失败: %v", err)
	}
	defer ldb2.Close()

	diskDB2 := rawdb.NewDatabase(ldb2)
	db2 := triedb.NewDatabase(diskDB2, cacheConfig.triedbConfig(true))

	// 重新加载之前的verkle树
	tr2, err := trie.NewVerkleTrie(rootHash, db2, utils.NewPointCache(100))
	if err != nil {
		t.Fatalf("从持久化存储加载verkle树失败: %v", err)
	}

	// 验证持久化后的数据是否正确
	for addr, expectedAcct := range testAccounts {
		actualAcct, err := tr2.GetAccount(addr)
		if err != nil {
			t.Fatalf("从持久化存储获取账户失败: %v", err)
		}
		if actualAcct.Nonce != expectedAcct.Nonce {
			t.Errorf("持久化后账户nonce不匹配: 期望 %d, 实际 %d", expectedAcct.Nonce, actualAcct.Nonce)
		}
		if !actualAcct.Balance.Eq(expectedAcct.Balance) {
			t.Errorf("持久化后账户余额不匹配: 期望 %s, 实际 %s", expectedAcct.Balance.Dec(), actualAcct.Balance.Dec())
		}
		if !bytes.Equal(actualAcct.CodeHash, expectedAcct.CodeHash) {
			t.Errorf("持久化后账户代码哈希不匹配: 期望 %x, 实际 %x", expectedAcct.CodeHash, actualAcct.CodeHash)
		}
	}

	// 验证存储数据
	for addr, storage := range testStorages {
		for key, expectedVal := range storage {
			actualVal, err := tr2.GetStorage(addr, key.Bytes())
			if err != nil {
				t.Fatalf("从持久化存储获取存储数据失败: %v", err)
			}
			if !bytes.Equal(actualVal, expectedVal) {
				t.Errorf("持久化后存储数据不匹配: 期望 %x, 实际 %x", expectedVal, actualVal)
			}
		}
	}

	t.Logf("持久化数据验证成功!")
}

// TestVerkleTreeBenchmark 对Verkle树进行性能测试
// 可配置循环次数，读写操作数，以及新旧数据比例
func TestVerkleTreeBenchmark(t *testing.T) {
	// 测试配置
	config := struct {
		Cycles      int     // 测试循环次数
		ReadOps     int     // 每个循环中的读取操作数
		WriteOps    int     // 每个循环中的写入操作数
		UpdateRatio float64 // 更新旧数据的比例，0-1之间，剩余为插入新数据
		Proof       bool
	}{
		Cycles:      2,
		ReadOps:     0,
		WriteOps:    3900,
		UpdateRatio: 0.3, // 30%更新, 70%新增
		Proof:       true,
	}
	os.MkdirAll("E:\\ethdata\\ztree\\vt", os.ModePerm)
	// 创建临时目录
	tempDir := "E:\\ethdata\\ztree\\vt"

	// 创建数据库
	ldb, err := leveldb.New(tempDir, 128, 128, "verkle_benchmark", false)
	if err != nil {
		t.Fatalf("创建磁盘数据库失败: %v", err)
	}
	defer ldb.Close()

	cacheConfig := DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	diskDB := rawdb.NewDatabase(ldb)
	db := triedb.NewDatabase(diskDB, cacheConfig.triedbConfig(true))

	// 存储所有账户地址和对应的存储键
	allAccounts := make([]common.Address, 0)
	allStorageKeys := make(map[common.Address][]common.Hash)

	// 创建AccessEvents实例用于跟踪访问
	pointCache := utils.NewPointCache(1024)
	accessEvents := state.NewAccessEvents(pointCache)
	stateSet := triedb.NewStateSet()

	// 生成随机数据的帮助函数
	randAddr := func() common.Address {
		return common.BytesToAddress(crypto.Keccak256([]byte(fmt.Sprintf("addr-%d", rand.Int())))[:20])
	}
	randHash := func() common.Hash {
		return common.BytesToHash(crypto.Keccak256([]byte(fmt.Sprintf("key-%d", rand.Int()))))
	}
	randBytes := func(size int) []byte {
		data := make([]byte, size)
		rand.Read(data)
		return data
	}

	// 初始化根哈希为空和上一棵树
	//parentRoot := types.EmptyVerkleHash

	parentRoot := common.HexToHash("0x6aff8b7cdec5669c243315a784d7dcd9bc9a7b2f9cb8782446db9ddb510acf37")

	// 性能统计
	stats := struct {
		ReadTime        time.Duration
		WriteTime       time.Duration
		CommitTime      time.Duration
		ProofGenTime    time.Duration
		ProofVerifyTime time.Duration
		ProofSize       int
		MemoryUsage     uint64
	}{}

	// 运行测试循环
	for cycle := 0; cycle < config.Cycles; cycle++ {
		t.Logf("运行循环 %d/%d", cycle+1, config.Cycles)

		// 创建新的verkle树实例
		tr, err := trie.NewVerkleTrie(parentRoot, db, pointCache)
		if err != nil {
			t.Fatalf("创建verkle树失败: %v", err)
		}

		// 重置AccessEvents和StateSet
		accessEvents = state.NewAccessEvents(pointCache)
		stateSet = triedb.NewStateSet()

		// 1. 读取操作
		readStart := time.Now()
		for i := 0; i < config.ReadOps && len(allAccounts) > 0; i++ {
			// 随机选择一个已有账户
			addrIdx := rand.Intn(len(allAccounts))
			addr := allAccounts[addrIdx]

			// 读取账户数据
			_, err := tr.GetAccount(addr)
			if err != nil {
				t.Logf("读取账户失败: %v", err)
				continue
			}
			// 记录读取访问
			accessEvents.AddAccount(addr, false)

			// 如果有存储键，读取存储数据
			if storageKeys, ok := allStorageKeys[addr]; ok && len(storageKeys) > 0 {
				keyIdx := rand.Intn(len(storageKeys))
				key := storageKeys[keyIdx]

				_, err := tr.GetStorage(addr, key.Bytes())
				if err != nil {
					t.Logf("读取存储数据失败: %v", err)
				}
				// 记录存储读取访问
				accessEvents.SlotGas(addr, key, false)
			}
		}
		readTime := time.Since(readStart)
		stats.ReadTime += readTime

		// 2. 写入操作
		writeStart := time.Now()
		for i := 0; i < config.WriteOps; i++ {
			var addr common.Address
			var isUpdate bool

			// 根据更新比例决定是更新现有数据还是插入新数据
			if rand.Float64() < config.UpdateRatio && len(allAccounts) > 0 {
				// 更新现有账户
				addrIdx := rand.Intn(len(allAccounts))
				addr = allAccounts[addrIdx]
				isUpdate = true
			} else {
				// 创建新账户
				addr = randAddr()
				allAccounts = append(allAccounts, addr)
				allStorageKeys[addr] = make([]common.Hash, 0)
				isUpdate = false
			}

			// 记录账户写入访问
			accessEvents.AddAccount(addr, true)

			// 生成账户数据
			acct := &types.StateAccount{
				Nonce:   uint64(rand.Intn(1000)),
				Balance: uint256.NewInt(rand.Uint64()),
			}

			// 更新账户
			if err := tr.UpdateAccount(addr, acct, 10); err != nil {
				t.Fatalf("更新账户失败: %v", err)
			}

			for j := 0; j < 2; j++ {
				var key common.Hash

				// 如果是更新操作，有50%概率更新已有存储键
				if isUpdate && len(allStorageKeys[addr]) > 0 && rand.Float64() < 0.5 {
					keyIdx := rand.Intn(len(allStorageKeys[addr]))
					key = allStorageKeys[addr][keyIdx]
				} else {
					// 新增存储键
					key = randHash()
					allStorageKeys[addr] = append(allStorageKeys[addr], key)
				}

				// 记录存储写入访问
				accessEvents.SlotGas(addr, key, true)

				// 更新存储数据
				val := randBytes(32)
				if err := tr.UpdateStorage(addr, key.Bytes(), val); err != nil {
					t.Fatalf("更新存储数据失败: %v", err)
				}
			}
		}
		writeTime := time.Since(writeStart)
		stats.WriteTime += writeTime

		//证明需要提前证明，因为写入是覆盖式的，等写入再去获取旧的树，已经获取不到了
		if config.Proof {
			newRoot := tr.Hash()
			// 在修改之前创建树的副本
			preTr, err := trie.NewVerkleTrie(parentRoot, db, pointCache)
			if err != nil {
				t.Fatalf("生成父状态失败: %v, %v", err, parentRoot)
			}

			// 生成和验证证明
			proofStart := time.Now()

			// 使用AccessEvents收集的键来生成证明
			modifiedKeys := accessEvents.Keys()

			proof, stateDiff, err := preTr.Proof(tr, modifiedKeys)
			if err != nil {
				t.Fatalf("生成状态差异证明失败: %v", err)
			}

			proofGenTime := time.Since(proofStart)
			stats.ProofGenTime += proofGenTime

			// 记录证明大小
			jsonBytes, err := json.Marshal(proof)
			if err != nil {
				t.Fatalf("序列化证明失败: %v", err)
			}
			proofSize := len(jsonBytes)
			stats.ProofSize += proofSize

			// 5. 验证证明
			verifyStart := time.Now()

			// 验证从旧状态到新状态的证明
			err = verkle.Verify(proof, parentRoot.Bytes(), newRoot.Bytes(), stateDiff)
			if err != nil {
				t.Fatalf("验证状态差异证明失败: %v, %v， %v", err, newRoot, tr.Hash())
			}

			verifyTime := time.Since(verifyStart)
			stats.ProofVerifyTime += verifyTime

			//t.Logf("循环 %d 状态差异证明 - 生成时间: %v, 验证时间: %v, 大小: %d 字节",
			//	cycle, proofGenTime, verifyTime, proofSize)

		}

		// 3. 提交并持久化
		commitStart := time.Now()

		// 现在提交更改
		newRoot, nodeset := tr.Commit(false)

		// 将节点集保存到数据库
		mergedNodeset := trienode.NewWithNodeSet(nodeset)

		if err := db.Update(newRoot, parentRoot, uint64(cycle), mergedNodeset, stateSet); err != nil {
			t.Fatalf("更新数据库失败: %v", err)
		}

		// 提交数据到磁盘
		if err := db.Commit(newRoot, false); err != nil {
			t.Fatalf("提交数据到磁盘失败: %v", err)
		}
		commitTime := time.Since(commitStart)
		stats.CommitTime += commitTime

		// 更新根哈希
		parentRoot = newRoot

		// 获取内存使用情况
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		stats.MemoryUsage += m.Alloc

		//t.Logf("循环 %d 统计: 读取 %v, 写入 %v, 提交 %v",
		//	cycle+1, readTime, writeTime, commitTime)
	}

	// 计算平均值
	cycles := float64(config.Cycles)
	proofCycles := cycles - 1 // 第一轮没有证明
	t.Logf("=== 性能测试结果 ===")
	t.Logf("最终Root: %v", parentRoot.String())
	t.Logf("平均读取时间: %v", stats.ReadTime/time.Duration(cycles))
	t.Logf("平均写入时间: %v", stats.WriteTime/time.Duration(cycles))
	t.Logf("平均提交时间: %v", stats.CommitTime/time.Duration(cycles))
	if proofCycles > 0 {
		t.Logf("平均证明生成时间: %v", stats.ProofGenTime/time.Duration(proofCycles))
		t.Logf("平均证明验证时间: %v", stats.ProofVerifyTime/time.Duration(proofCycles))
		t.Logf("平均证明大小: %d 字节", stats.ProofSize/int(proofCycles))
	}
	t.Logf("平均内存使用: %d 字节", stats.MemoryUsage/uint64(cycles))
}
