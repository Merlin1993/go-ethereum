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

package trie

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/binary"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// BinaryTrie is a wrapper around binary.Trie that implements the state.Trie interface.
type BinaryTrie struct {
	trie *binary.Trie
}

// NewBinaryTrie creates a new binary trie.
func NewBinaryTrie(root common.Hash, db database.NodeDatabase, archive ethdb.Database) (*BinaryTrie, error) {
	// Try to reuse the active trie from database if available
	type persistentDB interface {
		GetBinaryTrie() interface{}
		SetBinaryTrie(interface{})
	}
	if pdb, ok := db.(persistentDB); ok {
		if active := pdb.GetBinaryTrie(); active != nil {
			if t, ok := active.(*binary.Trie); ok {
				// We can reuse the trie if the root matches
				// Note: Load() will skip if root is same as cachedRoot
				if err := t.Load(root.Bytes()); err == nil {
					// Update adapter root if needed to ensure correct NodeReader is used
					if adapter, ok := t.Database().(*binaryDBAdapter); ok {
						adapter.root = root
						adapter.reader = nil
					}
					return &BinaryTrie{trie: t}, nil
				}
			}
		}
	}

	// For now, we use a simple adapter for the KVStore and ArchiveDB
	kvAdapter := &binaryDBAdapter{db: db, root: root}
	config := binary.DefaultConfig()
	if archive != nil {
		config.ArchiveDB = &binaryDBAdapterArchive{db: archive}
	} else {
		config.ArchiveDB = kvAdapter
	}

	// NewTrie(root []byte, db KVStore, hasher Hasher, config *Config, pruning bool)
	t := binary.NewTrie(root.Bytes(), kvAdapter, binary.NewPooledKeccakHasher(), config, false)

	// Register it as the active trie for reuse
	if pdb, ok := db.(persistentDB); ok {
		pdb.SetBinaryTrie(t)
	}

	return &BinaryTrie{trie: t}, nil
}

type binaryDBAdapter struct {
	db     database.NodeDatabase
	root   common.Hash
	reader database.NodeReader
	disk   ethdb.Database
	values map[common.Hash][]byte
	mu     sync.RWMutex
}

func (a *binaryDBAdapter) diskDB() ethdb.Database {
	if a.disk != nil {
		return a.disk
	}
	type diskDB interface {
		Disk() ethdb.Database
	}
	if ddb, ok := a.db.(diskDB); ok {
		a.disk = ddb.Disk()
	}
	return a.disk
}

func (a *binaryDBAdapter) Put(key, value []byte) error {
	if len(key) == 32 {
		a.mu.Lock()
		if a.values == nil {
			a.values = make(map[common.Hash][]byte)
		}
		a.values[common.BytesToHash(key)] = common.CopyBytes(value)
		a.mu.Unlock()
	}

	if db := a.diskDB(); db != nil {
		return db.Put(key, value)
	}
	return nil
}

func (a *binaryDBAdapter) Get(key []byte) ([]byte, error) {
	// Try Disk first for potentially uncommitted/standalone nodes (like TopTree container)
	if db := a.diskDB(); db != nil {
		data, err := db.Get(key)
		if err == nil {
			return data, nil
		}
	}
	// Fallback to NodeReader for trie nodes
	if a.reader == nil {
		r, err := a.db.NodeReader(a.root)
		if err != nil {
			return nil, err
		}
		a.reader = r
	}
	if a.reader == nil {
		return nil, nil
	}
	return a.reader.Node(common.Hash{}, nil, common.BytesToHash(key))
}
func (a *binaryDBAdapter) Delete(key []byte) error {
	if db := a.diskDB(); db != nil {
		return db.Delete(key)
	}
	return nil
}
func (a *binaryDBAdapter) NewBatch() binary.Batcher {
	if db := a.diskDB(); db != nil {
		return &binaryBatchAdapter{db: a.db, batch: db.NewBatch()}
	}
	return &binaryBatchAdapter{db: a.db}
}

// ArchiveStore implementation for binaryDBAdapter
func (a *binaryDBAdapter) PutBucket(hash, data []byte) error     { return a.Put(hash, data) }
func (a *binaryDBAdapter) GetBucket(hash []byte) ([]byte, error) { return a.Get(hash) }
func (a *binaryDBAdapter) DeleteBucket(hash []byte) error        { return a.Delete(hash) }

type binaryDBAdapterArchive struct {
	db ethdb.Database
}

func (a *binaryDBAdapterArchive) Put(key, value []byte) error    { return a.db.Put(key, value) }
func (a *binaryDBAdapterArchive) Get(key []byte) ([]byte, error) { return a.db.Get(key) }
func (a *binaryDBAdapterArchive) Delete(key []byte) error        { return a.db.Delete(key) }
func (a *binaryDBAdapterArchive) NewBatch() binary.Batcher {
	return &binaryBatchAdapterArchive{a.db.NewBatch()}
}
func (a *binaryDBAdapterArchive) PutBucket(hash, data []byte) error     { return a.db.Put(hash, data) }
func (a *binaryDBAdapterArchive) GetBucket(hash []byte) ([]byte, error) { return a.db.Get(hash) }
func (a *binaryDBAdapterArchive) DeleteBucket(hash []byte) error        { return a.db.Delete(hash) }

type binaryBatchAdapter struct {
	db    database.NodeDatabase
	batch ethdb.Batch
}

func (a *binaryBatchAdapter) Put(key, value []byte) error {
	if a.batch != nil {
		return a.batch.Put(key, value)
	}
	return nil
}
func (a *binaryBatchAdapter) Delete(key []byte) error {
	if a.batch != nil {
		return a.batch.Delete(key)
	}
	return nil
}
func (a *binaryBatchAdapter) Write() error {
	if a.batch != nil {
		return a.batch.Write()
	}
	return nil
}
func (a *binaryBatchAdapter) Reset() {
	if a.batch != nil {
		a.batch.Reset()
	}
}
func (a *binaryBatchAdapter) ValueSize() int {
	if a.batch != nil {
		return a.batch.ValueSize()
	}
	return 0
}

type binaryBatchAdapterArchive struct {
	ethdb.Batch
}

func (a *binaryBatchAdapterArchive) Put(key, value []byte) error { return a.Batch.Put(key, value) }
func (a *binaryBatchAdapterArchive) Delete(key []byte) error     { return a.Batch.Delete(key) }
func (a *binaryBatchAdapterArchive) Write() error                { return a.Batch.Write() }
func (a *binaryBatchAdapterArchive) Reset()                      { a.Batch.Reset() }
func (a *binaryBatchAdapterArchive) ValueSize() int              { return a.Batch.ValueSize() }

type nodeSetBatcher struct {
	db    database.NodeDatabase
	nodes *trienode.NodeSet
	mu    sync.Mutex
}

func (b *nodeSetBatcher) Put(key, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodes.AddNode(key, trienode.New(common.BytesToHash(key), value))
	return nil
}
func (b *nodeSetBatcher) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodes.AddNode(key, trienode.NewDeleted())
	return nil
}
func (b *nodeSetBatcher) Write() error   { return nil }
func (b *nodeSetBatcher) Reset()         {}
func (b *nodeSetBatcher) ValueSize() int { return 0 }

func (t *BinaryTrie) trieDB() database.NodeDatabase {
	if adapter, ok := t.trie.Database().(*binaryDBAdapter); ok {
		return adapter.db
	}
	return nil
}

func (t *BinaryTrie) GetKey(key []byte) []byte { return nil }

func (t *BinaryTrie) GetAccount(address common.Address) (*types.StateAccount, error) {
	key := address.Bytes()
	data, err := t.trie.Get(key)
	if err != nil {
		if err == binary.ErrNodeNotFound {
			return nil, nil
		}
		return nil, err
	}
	var acc types.StateAccount
	if err := rlp.DecodeBytes(data, &acc); err != nil {
		return nil, err
	}
	return &acc, nil
}

func (t *BinaryTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	compositeKey := make([]byte, 20+len(key))
	copy(compositeKey, addr.Bytes())
	compositeKey[0] ^= 0x01 // XOR domain 1 for storage
	copy(compositeKey[20:], key)
	val, err := t.trie.Get(compositeKey)
	if err != nil && err == binary.ErrNodeNotFound {
		return nil, nil
	}
	return val, err
}

func (t *BinaryTrie) UpdateAccount(address common.Address, acc *types.StateAccount, codeLen int) error {
	data, err := rlp.EncodeToBytes(acc)
	if err != nil {
		return err
	}
	return t.trie.Put(address.Bytes(), data)
}

func (t *BinaryTrie) UpdateAccountRLP(address common.Address, account []byte, codeLen int) error {
	return t.trie.Put(address.Bytes(), account)
}

func (t *BinaryTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	compositeKey := make([]byte, 20+len(key))
	copy(compositeKey, addr.Bytes())
	compositeKey[0] ^= 0x01
	copy(compositeKey[20:], key)
	if len(value) == 0 {
		return t.trie.BatchDelete(compositeKey)
	}
	return t.trie.Put(compositeKey, value)
}

func (t *BinaryTrie) DeleteAccount(address common.Address) error {
	return t.trie.BatchDelete(address.Bytes())
}

func (t *BinaryTrie) DeleteStorage(addr common.Address, key []byte) error {
	compositeKey := make([]byte, 20+len(key))
	copy(compositeKey, addr.Bytes())
	compositeKey[0] ^= 0x01
	copy(compositeKey[20:], key)
	return t.trie.BatchDelete(compositeKey)
}

func (t *BinaryTrie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	key := codeHash.Bytes()
	key[0] ^= 0x02 // XOR domain 2 for code
	return t.trie.Put(key, code)
}

func (t *BinaryTrie) Hash() common.Hash {
	h, _ := t.trie.Hash()
	return common.BytesToHash(h)
}

func (t *BinaryTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet) {
	isDirty := t.trie.IsDirty()
	if !isDirty {
		h, _ := t.trie.Commit()
		return common.BytesToHash(h), nil
	}

	if db := t.trieDB(); db != nil {
		// Prepare a batcher that feeds into a NodeSet for triedb.Update
		nodes := trienode.NewNodeSet(common.Hash{})
		batch := &nodeSetBatcher{db: db, nodes: nodes}

		h, err := t.trie.CommitToBatch(batch, true)
		if err != nil {
			return common.Hash{}, nil
		}

		// Flush buffered values into NodeSet
		if adapter, ok := t.trie.Database().(*binaryDBAdapter); ok {
			adapter.mu.Lock()
			for hash, blob := range adapter.values {
				nodes.AddNode(hash.Bytes(), trienode.New(hash, blob))
			}
			adapter.values = nil
			adapter.mu.Unlock()
		}

		return common.BytesToHash(h), nodes
	}
	// Fallback for non-triedb case
	h, _ := t.trie.CommitToBatch(nil, true)
	return common.BytesToHash(h), nil
}

func (t *BinaryTrie) Witness() map[string]struct{} { return nil }

func (t *BinaryTrie) NodeIterator(startKey []byte) (NodeIterator, error) {
	// For now, we only support a simple leaf-only iterator if startKey is nil.
	// This is primarily for debugging or tools that need to dump the trie.
	return newBinaryStorageIterator(common.Address{}, t.trie, true), nil
}

func (t *BinaryTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return errors.New("not implemented")
}

func (t *BinaryTrie) IsVerkle() bool { return false }

func (t *BinaryTrie) PruneNextShard() error {
	start := time.Now()
	err := t.trie.PruneNextShard()
	dur := time.Since(start).Nanoseconds()

	atomic.AddInt64(&common.BinaryPruneTime, dur)
	for {
		oldMax := atomic.LoadInt64(&common.BinaryPruneTimeMax)
		if dur <= oldMax || atomic.CompareAndSwapInt64(&common.BinaryPruneTimeMax, oldMax, dur) {
			break
		}
	}
	return err
}

// BinaryStorageTrie is a wrapper for a specific account's storage in the binary trie.
type BinaryStorageTrie struct {
	address common.Address
	bt      *BinaryTrie
}

func NewBinaryStorageTrie(address common.Address, bt *BinaryTrie) *BinaryStorageTrie {
	return &BinaryStorageTrie{address: address, bt: bt}
}

func (s *BinaryStorageTrie) GetKey(key []byte) []byte { return nil }

func (s *BinaryStorageTrie) GetAccount(address common.Address) (*types.StateAccount, error) {
	return nil, errors.New("not supported")
}

func (s *BinaryStorageTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	return s.bt.GetStorage(s.address, key)
}

func (s *BinaryStorageTrie) UpdateAccount(address common.Address, acc *types.StateAccount, codeLen int) error {
	return errors.New("not supported")
}

func (s *BinaryStorageTrie) UpdateAccountRLP(address common.Address, account []byte, codeLen int) error {
	return errors.New("not supported")
}

func (s *BinaryStorageTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	return s.bt.UpdateStorage(s.address, key, value)
}

func (s *BinaryStorageTrie) DeleteAccount(address common.Address) error {
	return errors.New("not supported")
}

func (s *BinaryStorageTrie) DeleteStorage(addr common.Address, key []byte) error {
	return s.bt.DeleteStorage(s.address, key)
}

func (s *BinaryStorageTrie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return errors.New("not supported")
}

func (s *BinaryStorageTrie) Hash() common.Hash {
	return common.Hash{}
}

func (s *BinaryStorageTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet) {
	return common.Hash{}, nil
}

func (s *BinaryStorageTrie) Witness() map[string]struct{} { return nil }

func (s *BinaryStorageTrie) NodeIterator(startKey []byte) (NodeIterator, error) {
	return newBinaryStorageIterator(s.address, s.bt.trie, false), nil
}

func (s *BinaryStorageTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return errors.New("not implemented")
}

func (s *BinaryStorageTrie) IsVerkle() bool { return false }

func (s *BinaryStorageTrie) PruneNextShard() error { return nil }

// binaryStorageIterator implements NodeIterator for storage wiping.
type binaryStorageIterator struct {
	prefix []byte
	leaves []leafKV
	index  int
	err    error
}

type leafKV struct {
	key   []byte
	value []byte
}

func newBinaryStorageIterator(address common.Address, bt *binary.Trie, isGlobal bool) *binaryStorageIterator {
	var prefix []byte
	if !isGlobal {
		prefix = make([]byte, 20)
		copy(prefix, address.Bytes())
		prefix[0] ^= 0x01
	}
	it := &binaryStorageIterator{
		prefix: prefix,
		index:  -1,
	}
	bt.ForEach(func(key, val []byte) bool {
		if len(it.prefix) == 0 || bytes.HasPrefix(key, it.prefix) {
			it.leaves = append(it.leaves, leafKV{
				key:   key[len(it.prefix):],
				value: val,
			})
		}
		return true
	})
	return it
}

func (it *binaryStorageIterator) Next(bool) bool {
	it.index++
	return it.index < len(it.leaves)
}

func (it *binaryStorageIterator) Error() error        { return it.err }
func (it *binaryStorageIterator) Hash() common.Hash   { return common.Hash{} }
func (it *binaryStorageIterator) Parent() common.Hash { return common.Hash{} }
func (it *binaryStorageIterator) Path() []byte        { return nil }

func (it *binaryStorageIterator) NodeBlob() []byte                  { return nil }
func (it *binaryStorageIterator) Leaf() bool                        { return true }
func (it *binaryStorageIterator) LeafKey() []byte                   { return it.leaves[it.index].key }
func (it *binaryStorageIterator) LeafBlob() []byte                  { return it.leaves[it.index].value }
func (it *binaryStorageIterator) LeafProof() [][]byte               { return nil }
func (it *binaryStorageIterator) AddResolver(resolver NodeResolver) {}
