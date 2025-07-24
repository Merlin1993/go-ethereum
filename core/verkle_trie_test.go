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

// TestBasicVerkleTreeOperations tests basic operations of the verkle tree, including creation, insertion, persistence, and verification
func TestBasicVerkleTreeOperations(t *testing.T) {
	// Create a temporary directory for storing database files
	tempDir := t.TempDir()

	// Create a persistent database instead of an in-memory database
	ldb, err := leveldb.New(tempDir, 128, 128, "verkle_test", false)
	if err != nil {
		t.Fatalf("Failed to create disk database: %v", err)
	}
	defer ldb.Close()

	cacheConfig := DefaultCacheConfigWithScheme(rawdb.PathScheme)
	diskDB := rawdb.NewDatabase(ldb)
	db := triedb.NewDatabase(diskDB, cacheConfig.triedbConfig(true))

	// Create a new verkle tree
	tr, err := trie.NewVerkleTrie(types.EmptyVerkleHash, db, utils.NewPointCache(100))
	if err != nil {
		t.Fatalf("Failed to create verkle tree: %v", err)
	}

	// Test account data
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

	// Test storage data
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

	// Test code data
	testCodes := map[common.Address][]byte{
		common.HexToAddress("0x1111111111111111111111111111111111111111"): []byte("code1"),
		common.HexToAddress("0x2222222222222222222222222222222222222222"): []byte("code2"),
	}

	// Insert account data into the verkle tree
	for addr, acct := range testAccounts {
		if err := tr.UpdateAccount(addr, acct, len(testCodes[addr])); err != nil {
			t.Fatalf("Failed to update account: %v", err)
		}

		// Update contract code
		codeHash := crypto.Keccak256Hash(testCodes[addr])
		if err := tr.UpdateContractCode(addr, codeHash, testCodes[addr]); err != nil {
			t.Fatalf("Failed to update contract code: %v", err)
		}

		// Update storage data
		for key, val := range testStorages[addr] {
			if err := tr.UpdateStorage(addr, key.Bytes(), val); err != nil {
				t.Fatalf("Failed to update storage data: %v", err)
			}
		}
	}

	// Get the root hash of the verkle tree
	root, _ := tr.Commit(false)

	t.Logf("Verkle tree root hash: %x", root)

	// Verify account data
	for addr, expectedAcct := range testAccounts {
		actualAcct, err := tr.GetAccount(addr)
		if err != nil {
			t.Fatalf("Failed to get account: %v", err)
		}
		if actualAcct.Nonce != expectedAcct.Nonce {
			t.Errorf("Account nonce mismatch: expected %d, got %d", expectedAcct.Nonce, actualAcct.Nonce)
		}
		if !actualAcct.Balance.Eq(expectedAcct.Balance) {
			t.Errorf("Account balance mismatch: expected %s, got %s", expectedAcct.Balance.Dec(), actualAcct.Balance.Dec())
		}
		if !bytes.Equal(actualAcct.CodeHash, expectedAcct.CodeHash) {
			t.Errorf("Account code hash mismatch: expected %x, got %x", expectedAcct.CodeHash, actualAcct.CodeHash)
		}
	}

	// Verify storage data
	for addr, storage := range testStorages {
		for key, expectedVal := range storage {
			actualVal, err := tr.GetStorage(addr, key.Bytes())
			if err != nil {
				t.Fatalf("Failed to get storage data: %v", err)
			}
			if !bytes.Equal(actualVal, expectedVal) {
				t.Errorf("Storage data mismatch: expected %x, got %x", expectedVal, actualVal)
			}
		}
	}

	// Persist data to disk
	rootHash, nodeset := tr.Commit(false)

	// First, save the node set to the database using Update
	mergedNodeset := trienode.NewWithNodeSet(nodeset)

	// Use an empty StateSet (only containing Verkle tree nodes)
	stateSet := triedb.NewStateSet()

	if err := db.Update(rootHash, common.Hash{}, 0, mergedNodeset, stateSet); err != nil {
		t.Fatalf("Failed to update database: %v", err)
	}

	// Then, use Commit to ensure data is flushed to disk
	if err := db.Commit(rootHash, false); err != nil {
		t.Fatalf("Failed to commit data to disk: %v", err)
	}

	// Verify data has been correctly persisted
	// Reopen database and verkle tree
	ldb.Close()

	ldb2, err := leveldb.New(tempDir, 128, 128, "verkle_test", false)
	if err != nil {
		t.Fatalf("Failed to reopen disk database: %v", err)
	}
	defer ldb2.Close()

	diskDB2 := rawdb.NewDatabase(ldb2)
	db2 := triedb.NewDatabase(diskDB2, cacheConfig.triedbConfig(true))

	// Reload the previous verkle tree
	tr2, err := trie.NewVerkleTrie(rootHash, db2, utils.NewPointCache(100))
	if err != nil {
		t.Fatalf("Failed to load verkle tree from persistent storage: %v", err)
	}

	// Verify the persisted data is correct
	for addr, expectedAcct := range testAccounts {
		actualAcct, err := tr2.GetAccount(addr)
		if err != nil {
			t.Fatalf("Failed to get account from persistent storage: %v", err)
		}
		if actualAcct.Nonce != expectedAcct.Nonce {
			t.Errorf("Account nonce mismatch after persistence: expected %d, got %d", expectedAcct.Nonce, actualAcct.Nonce)
		}
		if !actualAcct.Balance.Eq(expectedAcct.Balance) {
			t.Errorf("Account balance mismatch after persistence: expected %s, got %s", expectedAcct.Balance.Dec(), actualAcct.Balance.Dec())
		}
		if !bytes.Equal(actualAcct.CodeHash, expectedAcct.CodeHash) {
			t.Errorf("Account code hash mismatch after persistence: expected %x, got %x", expectedAcct.CodeHash, actualAcct.CodeHash)
		}
	}

	// Verify storage data
	for addr, storage := range testStorages {
		for key, expectedVal := range storage {
			actualVal, err := tr2.GetStorage(addr, key.Bytes())
			if err != nil {
				t.Fatalf("Failed to get storage data from persistent storage: %v", err)
			}
			if !bytes.Equal(actualVal, expectedVal) {
				t.Errorf("Storage data mismatch after persistence: expected %x, got %x", expectedVal, actualVal)
			}
		}
	}

	t.Logf("Persistent data verification successful!")
}

// TestVerkleTreeBenchmark performs performance testing on the Verkle tree
// Configurable number of cycles, read/write operations, and new/old data ratio
func TestVerkleTreeBenchmark(t *testing.T) {
	// Test configuration
	config := struct {
		Cycles      int     // Number of test cycles
		ReadOps     int     // Number of read operations per cycle
		WriteOps    int     // Number of write operations per cycle
		UpdateRatio float64 // Ratio of updating old data, 0-1, remaining for new data
		Proof       bool
	}{
		Cycles:      2,
		ReadOps:     0,
		WriteOps:    3900,
		UpdateRatio: 0.3, // 30% update, 70% new
		Proof:       true,
	}
	os.MkdirAll("E:\\ethdata\\ztree\\vt", os.ModePerm)
	// Create a temporary directory
	tempDir := "E:\\ethdata\\ztree\\vt"

	// Create database
	ldb, err := leveldb.New(tempDir, 128, 128, "verkle_benchmark", false)
	if err != nil {
		t.Fatalf("Failed to create disk database: %v", err)
	}
	defer ldb.Close()

	cacheConfig := DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	diskDB := rawdb.NewDatabase(ldb)
	db := triedb.NewDatabase(diskDB, cacheConfig.triedbConfig(true))

	// Store all account addresses and their corresponding storage keys
	allAccounts := make([]common.Address, 0)
	allStorageKeys := make(map[common.Address][]common.Hash)

	// Create AccessEvents instance for tracking access
	pointCache := utils.NewPointCache(1024)
	accessEvents := state.NewAccessEvents(pointCache)
	stateSet := triedb.NewStateSet()

	// Helper function to generate random data
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

	// Initialize root hash to empty and previous tree
	//parentRoot := types.EmptyVerkleHash

	parentRoot := common.HexToHash("0x6aff8b7cdec5669c243315a784d7dcd9bc9a7b2f9cb8782446db9ddb510acf37")

	// Performance statistics
	stats := struct {
		ReadTime        time.Duration
		WriteTime       time.Duration
		CommitTime      time.Duration
		ProofGenTime    time.Duration
		ProofVerifyTime time.Duration
		ProofSize       int
		MemoryUsage     uint64
	}{}

	// Run test cycles
	for cycle := 0; cycle < config.Cycles; cycle++ {
		t.Logf("Running cycle %d/%d", cycle+1, config.Cycles)

		// Create a new verkle tree instance
		tr, err := trie.NewVerkleTrie(parentRoot, db, pointCache)
		if err != nil {
			t.Fatalf("Failed to create verkle tree: %v", err)
		}

		// Reset AccessEvents and StateSet
		accessEvents = state.NewAccessEvents(pointCache)
		stateSet = triedb.NewStateSet()

		// 1. Read operations
		readStart := time.Now()
		for i := 0; i < config.ReadOps && len(allAccounts) > 0; i++ {
			// Randomly select an existing account
			addrIdx := rand.Intn(len(allAccounts))
			addr := allAccounts[addrIdx]

			// Read account data
			_, err := tr.GetAccount(addr)
			if err != nil {
				t.Logf("Failed to read account: %v", err)
				continue
			}
			// Record read access
			accessEvents.AddAccount(addr, false)

			// If there are storage keys, read storage data
			if storageKeys, ok := allStorageKeys[addr]; ok && len(storageKeys) > 0 {
				keyIdx := rand.Intn(len(storageKeys))
				key := storageKeys[keyIdx]

				_, err := tr.GetStorage(addr, key.Bytes())
				if err != nil {
					t.Logf("Failed to read storage data: %v", err)
				}
				// Record storage read access
				accessEvents.SlotGas(addr, key, false)
			}
		}
		readTime := time.Since(readStart)
		stats.ReadTime += readTime

		// 2. Write operations
		writeStart := time.Now()
		for i := 0; i < config.WriteOps; i++ {
			var addr common.Address
			var isUpdate bool

			// Decide whether to update existing data or insert new data based on update ratio
			if rand.Float64() < config.UpdateRatio && len(allAccounts) > 0 {
				// Update existing account
				addrIdx := rand.Intn(len(allAccounts))
				addr = allAccounts[addrIdx]
				isUpdate = true
			} else {
				// Create new account
				addr = randAddr()
				allAccounts = append(allAccounts, addr)
				allStorageKeys[addr] = make([]common.Hash, 0)
				isUpdate = false
			}

			// Record account write access
			accessEvents.AddAccount(addr, true)

			// Generate account data
			acct := &types.StateAccount{
				Nonce:   uint64(rand.Intn(1000)),
				Balance: uint256.NewInt(rand.Uint64()),
			}

			// Update account
			if err := tr.UpdateAccount(addr, acct, 10); err != nil {
				t.Fatalf("Failed to update account: %v", err)
			}

			for j := 0; j < 2; j++ {
				var key common.Hash

				// If it's an update operation, there's a 50% chance to update an existing storage key
				if isUpdate && len(allStorageKeys[addr]) > 0 && rand.Float64() < 0.5 {
					keyIdx := rand.Intn(len(allStorageKeys[addr]))
					key = allStorageKeys[addr][keyIdx]
				} else {
					// New storage key
					key = randHash()
					allStorageKeys[addr] = append(allStorageKeys[addr], key)
				}

				// Record storage write access
				accessEvents.SlotGas(addr, key, true)

				// Update storage data
				val := randBytes(32)
				if err := tr.UpdateStorage(addr, key.Bytes(), val); err != nil {
					t.Fatalf("Failed to update storage data: %v", err)
				}
			}
		}
		writeTime := time.Since(writeStart)
		stats.WriteTime += writeTime

		// Proof needs to be generated in advance, as writing is overwriting,
		// so get the old tree before writing, which is no longer available.
		if config.Proof {
			newRoot := tr.Hash()
			// Create a copy of the tree before modification
			preTr, err := trie.NewVerkleTrie(parentRoot, db, pointCache)
			if err != nil {
				t.Fatalf("Failed to generate parent state: %v, %v", err, parentRoot)
			}

			// Generate and verify proof
			proofStart := time.Now()

			// Use keys collected by AccessEvents to generate proof
			modifiedKeys := accessEvents.Keys()

			proof, stateDiff, err := preTr.Proof(tr, modifiedKeys)
			if err != nil {
				t.Fatalf("Failed to generate state difference proof: %v", err)
			}

			proofGenTime := time.Since(proofStart)
			stats.ProofGenTime += proofGenTime

			// Record proof size
			jsonBytes, err := json.Marshal(proof)
			if err != nil {
				t.Fatalf("Failed to serialize proof: %v", err)
			}
			proofSize := len(jsonBytes)
			stats.ProofSize += proofSize

			// 5. Verify proof
			verifyStart := time.Now()

			// Verify proof from old state to new state
			err = verkle.Verify(proof, parentRoot.Bytes(), newRoot.Bytes(), stateDiff)
			if err != nil {
				t.Fatalf("Failed to verify state difference proof: %v, %v, %v", err, newRoot, tr.Hash())
			}

			verifyTime := time.Since(verifyStart)
			stats.ProofVerifyTime += verifyTime

			//t.Logf("Cycle %d state difference proof - Generation time: %v, Verification time: %v, Size: %d bytes",
			//	cycle, proofGenTime, verifyTime, proofSize)

		}

		// 3. Commit and persist
		commitStart := time.Now()

		// Now commit changes
		newRoot, nodeset := tr.Commit(false)

		// Save node set to database
		mergedNodeset := trienode.NewWithNodeSet(nodeset)

		if err := db.Update(newRoot, parentRoot, uint64(cycle), mergedNodeset, stateSet); err != nil {
			t.Fatalf("Failed to update database: %v", err)
		}

		// Commit data to disk
		if err := db.Commit(newRoot, false); err != nil {
			t.Fatalf("Failed to commit data to disk: %v", err)
		}
		commitTime := time.Since(commitStart)
		stats.CommitTime += commitTime

		// Update root hash
		parentRoot = newRoot

		// Get memory usage
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		stats.MemoryUsage += m.Alloc

		//t.Logf("Cycle %d stats: Read %v, Write %v, Commit %v",
		//	cycle+1, readTime, writeTime, commitTime)
	}

	// Calculate averages
	cycles := float64(config.Cycles)
	proofCycles := cycles - 1 // First round has no proof
	t.Logf("=== Performance Test Results ===")
	t.Logf("Final Root: %v", parentRoot.String())
	t.Logf("Average Read Time: %v", stats.ReadTime/time.Duration(cycles))
	t.Logf("Average Write Time: %v", stats.WriteTime/time.Duration(cycles))
	t.Logf("Average Commit Time: %v", stats.CommitTime/time.Duration(cycles))
	if proofCycles > 0 {
		t.Logf("Average Proof Generation Time: %v", stats.ProofGenTime/time.Duration(proofCycles))
		t.Logf("Average Proof Verification Time: %v", stats.ProofVerifyTime/time.Duration(proofCycles))
		t.Logf("Average Proof Size: %d bytes", stats.ProofSize/int(proofCycles))
	}
	t.Logf("Average Memory Usage: %d bytes", stats.MemoryUsage/uint64(cycles))
}
