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
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-verkle"
	"github.com/holiman/uint256"
)

// TestBasicVerkleTreeOperations 测试verkle树的基本操作，包括创建、插入、生成证明和验证
func TestBasicVerkleTreeOperations(t *testing.T) {
	// 创建内存数据库
	memDB := rawdb.NewMemoryDatabase()
	db := triedb.NewDatabase(memDB, nil)

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

	// 创建一个新的verkle树实例，用于生成状态差异
	tr2, err := trie.NewVerkleTrie(types.EmptyVerkleHash, db, utils.NewPointCache(100))
	if err != nil {
		t.Fatalf("创建第二个verkle树失败: %v", err)
	}

	// 修改第一个账户的余额
	modifiedAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	modifiedAcct := *testAccounts[modifiedAddr]
	modifiedAcct.Balance = uint256.NewInt(5000) // 修改余额

	// 将修改后的账户数据插入到第二个verkle树中
	if err := tr2.UpdateAccount(modifiedAddr, &modifiedAcct, len(testCodes[modifiedAddr])); err != nil {
		t.Fatalf("更新第二个verkle树的账户失败: %v", err)
	}

	// 更新其他数据（与第一个树相同）
	for addr, acct := range testAccounts {
		if addr != modifiedAddr { // 跳过已修改的账户
			if err := tr2.UpdateAccount(addr, acct, len(testCodes[addr])); err != nil {
				t.Fatalf("更新第二个verkle树的账户失败: %v", err)
			}
		}

		// 更新合约代码
		codeHash := crypto.Keccak256Hash(testCodes[addr])
		if err := tr2.UpdateContractCode(addr, codeHash, testCodes[addr]); err != nil {
			t.Fatalf("更新第二个verkle树的合约代码失败: %v", err)
		}

		// 更新存储数据
		for key, val := range testStorages[addr] {
			if err := tr2.UpdateStorage(addr, key.Bytes(), val); err != nil {
				t.Fatalf("更新第二个verkle树的存储数据失败: %v", err)
			}
		}
	}

	// 获取第二个verkle树的根哈希
	root2, _ := tr2.Commit(false)

	t.Logf("第二个Verkle树根哈希: %x", root2)

	// 准备验证需要的键列表
	var keyList [][]byte

	// 添加账户基本数据键
	for addr := range testAccounts {
		keyList = append(keyList, utils.BasicDataKey(addr.Bytes()))
	}

	// 添加存储键
	for addr, storage := range testStorages {
		for key := range storage {
			keyList = append(keyList, utils.StorageSlotKey(addr.Bytes(), key.Bytes()))
		}
	}

	// 生成验证所需的证明
	proof, stateDiff, err := tr.Proof(tr2, keyList)
	if err != nil {
		t.Fatalf("生成证明失败: %v", err)
	}

	// 验证证明
	err = verkle.Verify(proof, root.Bytes(), root2.Bytes(), stateDiff)
	if err != nil {
		t.Fatalf("验证证明失败: %v", err)
	}

	t.Logf("证明验证成功!")
}
