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

package state

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/cachetrie"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// ContractCodeReader defines the interface for accessing contract code.
type ContractCodeReader interface {
	// Code retrieves a particular contract's code.
	//
	// - Returns nil code along with nil error if the requested contract code
	//   doesn't exist
	// - Returns an error only if an unexpected issue occurs
	Code(addr common.Address, codeHash common.Hash) ([]byte, error)

	// CodeSize retrieves a particular contracts code's size.
	//
	// - Returns zero code size along with nil error if the requested contract code
	//   doesn't exist
	// - Returns an error only if an unexpected issue occurs
	CodeSize(addr common.Address, codeHash common.Hash) (int, error)
}

// StateReader defines the interface for accessing accounts and storage slots
// associated with a specific state.
type StateReader interface {
	// Account retrieves the account associated with a particular address.
	//
	// - Returns a nil account if it does not exist
	// - Returns an error only if an unexpected issue occurs
	// - The returned account is safe to modify after the call
	Account(addr common.Address) (*types.StateAccount, error)

	// Storage retrieves the storage slot associated with a particular account
	// address and slot key.
	//
	// - Returns an empty slot if it does not exist
	// - Returns an error only if an unexpected issue occurs
	// - The returned storage slot is safe to modify after the call
	Storage(addr common.Address, slot common.Hash) (common.Hash, error)
}

// Reader defines the interface for accessing accounts, storage slots and contract
// code associated with a specific state.
type Reader interface {
	ContractCodeReader
	StateReader
}

// cachingCodeReader implements ContractCodeReader, accessing contract code either in
// local key-value store or the shared code cache.
type cachingCodeReader struct {
	db ethdb.KeyValueReader

	// These caches could be shared by multiple code reader instances,
	// they are natively thread-safe.
	codeCache     *lru.SizeConstrainedCache[common.Hash, []byte]
	codeSizeCache *lru.Cache[common.Hash, int]
}

// newCachingCodeReader constructs the code reader.
func newCachingCodeReader(db ethdb.KeyValueReader, codeCache *lru.SizeConstrainedCache[common.Hash, []byte], codeSizeCache *lru.Cache[common.Hash, int]) *cachingCodeReader {
	return &cachingCodeReader{
		db:            db,
		codeCache:     codeCache,
		codeSizeCache: codeSizeCache,
	}
}

// Code implements ContractCodeReader, retrieving a particular contract's code.
// If the contract code doesn't exist, no error will be returned.
func (r *cachingCodeReader) Code(addr common.Address, codeHash common.Hash) ([]byte, error) {
	code, _ := r.codeCache.Get(codeHash)
	if len(code) > 0 {
		return code, nil
	}
	code = rawdb.ReadCode(r.db, codeHash)
	if len(code) > 0 {
		r.codeCache.Add(codeHash, code)
		r.codeSizeCache.Add(codeHash, len(code))
	}
	return code, nil
}

// CodeSize implements ContractCodeReader, retrieving a particular contracts code's size.
// If the contract code doesn't exist, no error will be returned.
func (r *cachingCodeReader) CodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	if cached, ok := r.codeSizeCache.Get(codeHash); ok {
		return cached, nil
	}
	code, err := r.Code(addr, codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

type CacheTrieReader struct {
	ct *cachetrie.CacheTrie
}

var CacheNilErr = errors.New("cache trie not find")

// newFlatReader constructs a state reader with on the given state root.
func newCacheReader(ct *cachetrie.CacheTrie) *CacheTrieReader {
	return &CacheTrieReader{
		ct: ct,
	}
}

func (r *CacheTrieReader) Account(addr common.Address) (*types.StateAccount, error) {
	if r.ct != nil {
		// 从缓存中获取
		cacheNode, err := r.ct.Get(addr.Bytes())
		if err == nil && cacheNode != nil {
			if valueNode, ok := cacheNode.(cachetrie.ValueNode); ok {
				if len(valueNode.Data) > 0 {

					ret := new(types.StateAccount)
					if err := rlp.DecodeBytes(valueNode.Data, ret); err == nil {
						return ret, nil
					}
				} else {
					return nil, nil
				}
			}
		}
	}

	return nil, CacheNilErr
}

// Storage implements StateReader, retrieving the storage slot specified by the
// address and slot key.
//
// An error will be returned if the associated snapshot is already stale or
// the requested storage slot is not yet covered by the snapshot.
//
// The returned storage slot might be empty if it's not existent.
func (r *CacheTrieReader) Storage(addr common.Address, key common.Hash) (common.Hash, error) {
	if r.ct != nil {
		// 从缓存中获取
		cacheNode, err := r.ct.GetWithAddress(addr, key.Bytes())
		if err != nil || cacheNode == nil {
			return common.Hash{}, CacheNilErr
		}
		if valueNode, ok := cacheNode.(cachetrie.ValueNode); ok {
			if len(valueNode.Data) > 0 {
				content := valueNode.Data
				// 如果需要将RLP编码的数据提取出实际内容
				_, actualContent, _, err := rlp.Split(content)
				if err != nil {
					return common.Hash{}, err // 如果解码失败，直接返回原始内容
				}
				var value common.Hash
				value.SetBytes(actualContent)
				return value, nil
			} else {
				//已被删除了
				return common.Hash{}, nil
			}
		}
	}
	return common.Hash{}, CacheNilErr
}

// flatReader wraps a database state reader.
type flatReader struct {
	reader database.StateReader
	buff   crypto.KeccakState
}

// newFlatReader constructs a state reader with on the given state root.
func newFlatReader(reader database.StateReader) *flatReader {
	return &flatReader{
		reader: reader,
		buff:   crypto.NewKeccakState(),
	}
}

// Account implements StateReader, retrieving the account specified by the address.
//
// An error will be returned if the associated snapshot is already stale or
// the requested account is not yet covered by the snapshot.
//
// The returned account might be nil if it's not existent.
func (r *flatReader) Account(addr common.Address) (*types.StateAccount, error) {
	account, err := r.reader.Account(crypto.HashData(r.buff, addr.Bytes()))
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, nil
	}
	acct := &types.StateAccount{
		Nonce:    account.Nonce,
		Balance:  account.Balance,
		CodeHash: account.CodeHash,
		Root:     common.BytesToHash(account.Root),
	}
	if len(acct.CodeHash) == 0 {
		acct.CodeHash = types.EmptyCodeHash.Bytes()
	}
	if acct.Root == (common.Hash{}) {
		acct.Root = types.EmptyRootHash
	}
	return acct, nil
}

// Storage implements StateReader, retrieving the storage slot specified by the
// address and slot key.
//
// An error will be returned if the associated snapshot is already stale or
// the requested storage slot is not yet covered by the snapshot.
//
// The returned storage slot might be empty if it's not existent.
func (r *flatReader) Storage(addr common.Address, key common.Hash) (common.Hash, error) {
	addrHash := crypto.HashData(r.buff, addr.Bytes())
	slotHash := crypto.HashData(r.buff, key.Bytes())
	ret, err := r.reader.Storage(addrHash, slotHash)
	if err != nil {
		return common.Hash{}, err
	}
	if len(ret) == 0 {
		return common.Hash{}, nil
	}
	// Perform the rlp-decode as the slot value is RLP-encoded in the state
	// snapshot.
	_, content, _, err := rlp.Split(ret)
	if err != nil {
		return common.Hash{}, err
	}
	var value common.Hash
	value.SetBytes(content)
	return value, nil
}

// trieReader implements the StateReader interface, providing functions to access
// state from the referenced trie.
type trieReader struct {
	root     common.Hash                    // State root which uniquely represent a state
	db       *triedb.Database               // Database for loading trie
	buff     crypto.KeccakState             // Buffer for keccak256 hashing
	mainTrie Trie                           // Main trie, resolved in constructor
	subRoots map[common.Address]common.Hash // Set of storage roots, cached when the account is resolved
	subTries map[common.Address]Trie        // Group of storage tries, cached when it's resolved
}

// trieReader constructs a trie reader of the specific state. An error will be
// returned if the associated trie specified by root is not existent.
func newTrieReader(root common.Hash, db *triedb.Database, cache *utils.PointCache) (*trieReader, error) {
	var (
		tr  Trie
		err error
	)
	if db.IsVerkle() {
		tr, err = trie.NewVerkleTrie(root, db, cache)
	} else if db.IsBinary() {
		tr, err = trie.NewBinaryTrie(root, db, db.Archive())
	} else {
		tr, err = trie.NewStateTrie(trie.StateTrieID(root), db)
	}
	if err != nil {
		return nil, err
	}
	return &trieReader{
		root:     root,
		db:       db,
		buff:     crypto.NewKeccakState(),
		mainTrie: tr,
		subRoots: make(map[common.Address]common.Hash),
		subTries: make(map[common.Address]Trie),
	}, nil
}

// Account implements StateReader, retrieving the account specified by the address.
//
// An error will be returned if the trie state is corrupted. An nil account
// will be returned if it's not existent in the trie.
func (r *trieReader) Account(addr common.Address) (*types.StateAccount, error) {
	account, err := r.mainTrie.GetAccount(addr)
	if err != nil {
		return nil, err
	}
	if account == nil {
		r.subRoots[addr] = types.EmptyRootHash
	} else {
		r.subRoots[addr] = account.Root
	}
	return account, nil
}

// Storage implements StateReader, retrieving the storage slot specified by the
// address and slot key.
//
// An error will be returned if the trie state is corrupted. An empty storage
// slot will be returned if it's not existent in the trie.
func (r *trieReader) Storage(addr common.Address, key common.Hash) (common.Hash, error) {
	var (
		tr    Trie
		found bool
		value common.Hash
	)
	if r.db.IsVerkle() || r.db.IsBinary() {
		tr = r.mainTrie
	} else {
		tr, found = r.subTries[addr]
		if !found {
			root, ok := r.subRoots[addr]

			// The storage slot is accessed without account caching. It's unexpected
			// behavior but try to resolve the account first anyway.
			if !ok {
				_, err := r.Account(addr)
				if err != nil {
					return common.Hash{}, err
				}
				root = r.subRoots[addr]
			}
			var err error
			tr, err = trie.NewStateTrie(trie.StorageTrieID(r.root, crypto.HashData(r.buff, addr.Bytes()), root), r.db)
			if err != nil {
				return common.Hash{}, err
			}
			r.subTries[addr] = tr
		}
	}
	ret, err := tr.GetStorage(addr, key.Bytes())
	if err != nil {
		return common.Hash{}, err
	}
	value.SetBytes(ret)
	return value, nil
}

const Readers = 3

var (
	// 统计信息
	accountHitCounts  []int64         // 每个reader的Account方法调用成功次数
	storageHitCounts  []int64         // 每个reader的Storage方法调用成功次数
	accountAccessTime []time.Duration // 每个reader的Account方法累计访问时间
	storageAccessTime []time.Duration // 每个reader的Storage方法累计访问时间
)

// multiStateReader is the aggregation of a list of StateReader interface,
// providing state access by leveraging all readers. The checking priority
// is determined by the position in the reader list.
type multiStateReader struct {
	readers []StateReader // List of state readers, sorted by checking priority
}

// newMultiStateReader constructs a multiStateReader instance with the given
// readers. The priority among readers is assumed to be sorted. Note, it must
// contain at least one reader for constructing a multiStateReader.
func newMultiStateReader(readers ...StateReader) (*multiStateReader, error) {
	if len(readers) == 0 {
		return nil, errors.New("empty reader set")
	}
	if accountHitCounts == nil && len(readers) >= Readers {
		accountHitCounts = make([]int64, len(readers))          // 每个reader的Account方法调用成功次数
		storageHitCounts = make([]int64, len(readers))          // 每个reader的Storage方法调用成功次数
		accountAccessTime = make([]time.Duration, len(readers)) // 每个reader的Account方法累计访问时间
		storageAccessTime = make([]time.Duration, len(readers)) // 每个reader的Storage方法累计访问时间
	}
	return &multiStateReader{
		readers: readers,
	}, nil
}

// Account implementing StateReader interface, retrieving the account associated
// with a particular address.
//
// - Returns a nil account if it does not exist
// - Returns an error only if an unexpected issue occurs
// - The returned account is safe to modify after the call
func (r *multiStateReader) Account(addr common.Address) (*types.StateAccount, error) {
	var errs []error
	start := time.Now()
	var cacheMiss bool

	atomic.AddInt64(&common.TotalAccountReads, 1)
	atomic.AddInt64(&common.TotalReads, 1)
	for i, reader := range r.readers {
		acct, err := reader.Account(addr)
		elapsed := time.Since(start)

		// 更新统计信息
		if len(r.readers) == Readers {
			accountAccessTime[i] += elapsed
		}

		// 检查是否是 CacheTrieReader 且未命中
		if i == 0 {
			if _, ok := reader.(*CacheTrieReader); ok {
				if err == CacheNilErr {
					cacheMiss = true
				} else if err == nil {
					atomic.AddInt64(&common.CacheAccountHit, 1)
					if acct != nil {
						data, _ := rlp.EncodeToBytes(acct)
						atomic.AddInt64(&common.CacheAccountReadSize, int64(len(data)))
					}
				}
			}
		}

		if err == nil {
			if len(r.readers) == Readers {
				accountHitCounts[i]++
			}
			// 如果缓存未命中但在后续层找到
			if cacheMiss && i > 0 {
				if acct != nil {
					atomic.AddInt64(&common.CacheAccountMissExists, 1)
				} else {
					atomic.AddInt64(&common.CacheAccountMissNotExists, 1)
				}

				if common.ReadSet {
					// 如果设置了写回，则写回缓存
					if ctReader, ok := r.readers[0].(*CacheTrieReader); ok && ctReader.ct != nil {
						if acct != nil {
							data, _ := rlp.EncodeToBytes(acct)
							ctReader.ct.Update(addr.Bytes(), data, false)
							atomic.AddInt64(&common.CacheAccountWriteSize, int64(len(data)))
						}
					}
				}
			}
			return acct, nil
		}

		errs = append(errs, err)
	}
	// 如果所有 reader 都失败或未找到
	if cacheMiss {
		atomic.AddInt64(&common.CacheAccountMissNotExists, 1)
	}
	return nil, errors.Join(errs...)
}

// Storage implementing StateReader interface, retrieving the storage slot
// associated with a particular account address and slot key.
//
// - Returns an empty slot if it does not exist
// - Returns an error only if an unexpected issue occurs
// - The returned storage slot is safe to modify after the call
func (r *multiStateReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	var errs []error
	start := time.Now()
	var cacheMiss bool

	atomic.AddInt64(&common.TotalStorageReads, 1)
	atomic.AddInt64(&common.TotalReads, 1)
	for i, reader := range r.readers {
		slotValue, err := reader.Storage(addr, slot)
		elapsed := time.Since(start)

		// 更新统计信息
		if len(r.readers) == Readers {
			storageAccessTime[i] += elapsed
		}

		// 检查是否是 CacheTrieReader 且未命中
		if i == 0 {
			if _, ok := reader.(*CacheTrieReader); ok {
				if err == CacheNilErr {
					cacheMiss = true
				} else if err == nil {
					atomic.AddInt64(&common.CacheStorageHit, 1)
					data, _ := rlp.EncodeToBytes(slotValue)
					atomic.AddInt64(&common.CacheStorageReadSize, int64(len(data)))
				}
			}
		}

		if err == nil {
			if len(r.readers) == Readers {
				storageHitCounts[i]++
			}
			// 如果缓存未命中但在后续层找到
			if cacheMiss && i > 0 {
				if slotValue != (common.Hash{}) {
					atomic.AddInt64(&common.CacheStorageMissExists, 1)
				} else {
					atomic.AddInt64(&common.CacheStorageMissNotExists, 1)
				}

				if common.ReadSet {
					// 如果设置了写回，则写回缓存
					if ctReader, ok := r.readers[0].(*CacheTrieReader); ok && ctReader.ct != nil {
						if slotValue != (common.Hash{}) {
							data, _ := rlp.EncodeToBytes(slotValue)
							ctReader.ct.UpdateWithAddress(addr, slot.Bytes(), data, false)
							atomic.AddInt64(&common.CacheStorageWriteSize, int64(len(data)))
						}
					}
				}
			}
			return slotValue, nil
		}

		errs = append(errs, err)
	}
	// 如果所有 reader 都失败或未找到
	if cacheMiss {
		atomic.AddInt64(&common.CacheStorageMissNotExists, 1)
	}
	return common.Hash{}, errors.Join(errs...)
}

// PrintStat 打印multiStateReader的统计信息
func PrintStat() {

	fmt.Println("=== MultiStateReader 统计信息 ===")
	fmt.Printf("Reader 数量: %d\n", len(accountHitCounts))
	fmt.Println()

	// 计算总数
	var totalAccountHits, totalStorageHits int64
	var totalAccountTime, totalStorageTime time.Duration

	for i := 0; i < len(accountHitCounts); i++ {
		totalAccountHits += accountHitCounts[i]
		totalStorageHits += storageHitCounts[i]
		totalAccountTime += accountAccessTime[i]
		totalStorageTime += storageAccessTime[i]
	}

	fmt.Println("Account 方法统计:")
	for i := 0; i < len(accountHitCounts); i++ {
		percentage := float64(0)
		if totalAccountHits > 0 {
			percentage = float64(accountHitCounts[i]) * 100.0 / float64(totalAccountHits)
		}
		avgTime := time.Duration(0)
		if accountHitCounts[i] > 0 {
			avgTime = accountAccessTime[i] / time.Duration(accountHitCounts[i])
		}
		fmt.Printf("  Reader[%d]: 命中次数=%d (%.2f%%), 累计时间=%v, 平均时间=%v\n",
			i, accountHitCounts[i], percentage, accountAccessTime[i], avgTime)
	}
	fmt.Printf("  总计: 命中次数=%d, 累计时间=%v\n", totalAccountHits, totalAccountTime)
	fmt.Println()

	fmt.Println("Storage 方法统计:")
	for i := 0; i < len(accountHitCounts); i++ {
		percentage := float64(0)
		if totalStorageHits > 0 {
			percentage = float64(storageHitCounts[i]) * 100.0 / float64(totalStorageHits)
		}
		avgTime := time.Duration(0)
		if storageHitCounts[i] > 0 {
			avgTime = storageAccessTime[i] / time.Duration(storageHitCounts[i])
		}
		fmt.Printf("  Reader[%d]: 命中次数=%d (%.2f%%), 累计时间=%v, 平均时间=%v\n",
			i, storageHitCounts[i], percentage, storageAccessTime[i], avgTime)
	}
	fmt.Printf("  总计: 命中次数=%d, 累计时间=%v\n", totalStorageHits, totalStorageTime)
	fmt.Println()

	fmt.Println("CacheTrie 详细统计:")
	fmt.Printf("  Account: 命中=%d, 未命中但存在=%d (已写回), 未命中且不存在=%d, 读取数据量=%d bytes, 写入数据量=%d bytes, 总读取次数=%d, 总更新次数=%d\n",
		atomic.LoadInt64(&common.CacheAccountHit), atomic.LoadInt64(&common.CacheAccountMissExists), atomic.LoadInt64(&common.CacheAccountMissNotExists),
		atomic.LoadInt64(&common.CacheAccountReadSize), atomic.LoadInt64(&common.CacheAccountWriteSize),
		atomic.LoadInt64(&common.TotalAccountReads), atomic.LoadInt64(&common.TotalAccountUpdates))
	fmt.Printf("  Storage: 命中=%d, 未命中但存在=%d (已写回), 未命中且不存在=%d, 读取数据量=%d bytes, 写入数据量=%d bytes, 总读取次数=%d, 总更新次数=%d\n",
		atomic.LoadInt64(&common.CacheStorageHit), atomic.LoadInt64(&common.CacheStorageMissExists), atomic.LoadInt64(&common.CacheStorageMissNotExists),
		atomic.LoadInt64(&common.CacheStorageReadSize), atomic.LoadInt64(&common.CacheStorageWriteSize),
		atomic.LoadInt64(&common.TotalStorageReads), atomic.LoadInt64(&common.TotalStorageUpdates))

	fmt.Printf("  [汇总统计]: 总读取次数=%d, 总写入(更新)次数=%d, 读取命中在缓存=%d, 读取命中不在缓存=%d, 读取不中=%d\n",
		atomic.LoadInt64(&common.TotalReads), atomic.LoadInt64(&common.TotalUpdates),
		atomic.LoadInt64(&common.CacheAccountHit)+atomic.LoadInt64(&common.CacheStorageHit),
		atomic.LoadInt64(&common.CacheAccountMissExists)+atomic.LoadInt64(&common.CacheStorageMissExists),
		atomic.LoadInt64(&common.CacheAccountMissNotExists)+atomic.LoadInt64(&common.CacheStorageMissNotExists))
	fmt.Println("================================")
}

// GetCacheStats 返回 CacheTrie 的统计信息
func GetCacheStats() (hit, missExists, missNotExists, storageHit, storageMissExists, storageMissNotExists,
	acctReadSize, acctWriteSize, storReadSize, storWriteSize,
	totalAcctReads, totalStorReads, totalAcctUpdates, totalStorUpdates,
	totalReads, totalUpdates int64) {
	return atomic.LoadInt64(&common.CacheAccountHit),
		atomic.LoadInt64(&common.CacheAccountMissExists),
		atomic.LoadInt64(&common.CacheAccountMissNotExists),
		atomic.LoadInt64(&common.CacheStorageHit),
		atomic.LoadInt64(&common.CacheStorageMissExists),
		atomic.LoadInt64(&common.CacheStorageMissNotExists),
		atomic.LoadInt64(&common.CacheAccountReadSize),
		atomic.LoadInt64(&common.CacheAccountWriteSize),
		atomic.LoadInt64(&common.CacheStorageReadSize),
		atomic.LoadInt64(&common.CacheStorageWriteSize),
		atomic.LoadInt64(&common.TotalAccountReads),
		atomic.LoadInt64(&common.TotalStorageReads),
		atomic.LoadInt64(&common.TotalAccountUpdates),
		atomic.LoadInt64(&common.TotalStorageUpdates),
		atomic.LoadInt64(&common.TotalReads),
		atomic.LoadInt64(&common.TotalUpdates)
}

// ResetCacheStats 重置所有 CacheTrie 统计计数器为零
func ResetCacheStats() {
	atomic.StoreInt64(&common.CacheAccountHit, 0)
	atomic.StoreInt64(&common.CacheAccountMissExists, 0)
	atomic.StoreInt64(&common.CacheAccountMissNotExists, 0)
	atomic.StoreInt64(&common.CacheStorageHit, 0)
	atomic.StoreInt64(&common.CacheStorageMissExists, 0)
	atomic.StoreInt64(&common.CacheStorageMissNotExists, 0)
	atomic.StoreInt64(&common.CacheAccountReadSize, 0)
	atomic.StoreInt64(&common.CacheAccountWriteSize, 0)
	atomic.StoreInt64(&common.CacheStorageReadSize, 0)
	atomic.StoreInt64(&common.CacheStorageWriteSize, 0)

	atomic.StoreInt64(&common.TotalAccountReads, 0)
	atomic.StoreInt64(&common.TotalStorageReads, 0)
	atomic.StoreInt64(&common.TotalAccountUpdates, 0)
	atomic.StoreInt64(&common.TotalStorageUpdates, 0)

	atomic.StoreInt64(&common.TotalReads, 0)
	atomic.StoreInt64(&common.TotalUpdates, 0)

	atomic.StoreInt64(&common.BinaryHitCount, 0)
	atomic.StoreInt64(&common.BinaryMissNonExistentCount, 0)
	atomic.StoreInt64(&common.BinaryMissExistentCount, 0)
	atomic.StoreInt64(&common.BinaryCycleFPCount, 0)
	atomic.StoreInt64(&common.BinaryMaxFPInSingleBlock, 0)
	atomic.StoreInt64(&common.BinaryTrieFPInBlock, 0)
}

// reader is the wrapper of ContractCodeReader and StateReader interface.
type reader struct {
	ContractCodeReader
	StateReader
}

// newReader constructs a reader with the supplied code reader and state reader.
func newReader(codeReader ContractCodeReader, stateReader StateReader) *reader {
	return &reader{
		ContractCodeReader: codeReader,
		StateReader:        stateReader,
	}
}
