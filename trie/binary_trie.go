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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/binary"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// BinaryTrie is a wrapper around binary.Trie that implements the state.Trie interface.
type BinaryTrie struct {
	trie       *binary.Trie
	db         database.NodeDatabase
	originRoot common.Hash
	block      uint64
}

var (
	// globalNodeCache stores nodes that are not yet committed to triedb's disk.
	// This is needed because NewBinaryTrie creates new adapters that wouldn't see
	// the previous blocks' uncommitted nodes otherwise.
	globalNodeCache      *lru.Cache[common.Hash, []byte]
	globalNodeCacheLimit int
	globalNodeCacheMu    sync.RWMutex

	globalTrieRegistry   = make(map[database.NodeDatabase]*binary.Trie)
	globalTrieRegistryMu sync.Mutex
)

const defaultBinaryNodeCacheLimit = 262144

func configureBinaryNodeCache(limit int) {
	globalNodeCacheMu.Lock()
	defer globalNodeCacheMu.Unlock()

	if limit == globalNodeCacheLimit {
		return
	}
	globalNodeCacheLimit = limit
	if limit <= 0 {
		globalNodeCache = nil
		return
	}
	globalNodeCache = lru.NewCache[common.Hash, []byte](limit)
}

func binaryNodeCacheAdd(hash common.Hash, value []byte) {
	globalNodeCacheMu.RLock()
	cache := globalNodeCache
	globalNodeCacheMu.RUnlock()
	if cache == nil {
		return
	}
	cache.Add(hash, common.CopyBytes(value))
}

func binaryNodeCacheGet(hash common.Hash) ([]byte, bool) {
	globalNodeCacheMu.RLock()
	cache := globalNodeCache
	globalNodeCacheMu.RUnlock()
	if cache == nil {
		return nil, false
	}
	val, ok := cache.Get(hash)
	if !ok {
		return nil, false
	}
	return common.CopyBytes(val), true
}

func binaryNodeCacheRemove(hash common.Hash) {
	globalNodeCacheMu.RLock()
	cache := globalNodeCache
	globalNodeCacheMu.RUnlock()
	if cache != nil {
		cache.Remove(hash)
	}
}

// NewBinaryTrie creates a new binary trie.
func NewBinaryTrie(root common.Hash, db database.NodeDatabase, archive ethdb.Database) (*BinaryTrie, error) {
	var active *binary.Trie

	// Try to reuse the active trie from database if possible
	type trieStore interface {
		GetBinaryTrie() interface{}
	}
	if ts, ok := db.(trieStore); ok {
		if bt := ts.GetBinaryTrie(); bt != nil {
			active = bt.(*binary.Trie)
		}
	}

	if active != nil {
		// Check if the current root already matches. If so, return immediately to preserve memory shards.
		h, _ := active.Hash()
		if bytes.Equal(h, root.Bytes()) {
			// Update adapter root just in case, but keep the trie as is
			if adapter, ok := active.Database().(*binaryDBAdapter); ok {
				adapter.root = root
			}
			return &BinaryTrie{trie: active, db: db, originRoot: root}, nil
		}

		if err := active.Load(root.Bytes()); err == nil {
			// Update adapter root if needed to ensure correct NodeReader is used
			if adapter, ok := active.Database().(*binaryDBAdapter); ok {
				adapter.root = root
				adapter.reader = nil // Clear stale reader
			}
			return &BinaryTrie{trie: active, db: db, originRoot: root}, nil
		}
	}

	config := binary.DefaultConfig()
	nodeCacheLimit := defaultBinaryNodeCacheLimit
	physicalDelete := false

	// Try to get config from DB
	type configDB interface {
		BinaryAblationConfig() *database.BinaryConfig
	}
	if cdb, ok := db.(configDB); ok {
		dbConf := cdb.BinaryAblationConfig()
		if dbConf != nil {
			if dbConf.ShardDepth > 0 {
				config.ShardDepth = dbConf.ShardDepth
			}
			if dbConf.ArchiveBucketSize > 0 {
				config.ArchiveBucketSize = dbConf.ArchiveBucketSize
			}
			config.ArchiveItemCacheLimit = dbConf.ArchiveItemCacheLimit
			if dbConf.CuckooBuckets > 0 {
				config.CuckooBuckets = dbConf.CuckooBuckets
			}
			if dbConf.CuckooSlots > 0 {
				config.CuckooSlots = dbConf.CuckooSlots
			}
			if dbConf.NodeCacheLimit != 0 {
				nodeCacheLimit = dbConf.NodeCacheLimit
			}
			physicalDelete = dbConf.PhysicalDelete
		}
	}

	configureBinaryNodeCache(nodeCacheLimit)

	// For now, we use a simple adapter for the KVStore and ArchiveDB
	kvAdapter := &binaryDBAdapter{db: db, root: root, archive: archive, physicalDelete: physicalDelete}

	if archive != nil {
		config.ArchiveDB = &binaryDBAdapterArchive{db: archive}
	} else {
		config.ArchiveDB = kvAdapter
	}

	// NewTrie(root []byte, db KVStore, hasher Hasher, config *Config, pruning bool)
	t := binary.NewTrie(root.Bytes(), kvAdapter, binary.NewPooledKeccakHasher(), config, true)
	bt := &BinaryTrie{
		trie:       t,
		db:         db,
		originRoot: root,
	}
	// Native persistence for reuse via interface assertion
	type trieSetter interface {
		SetBinaryTrie(interface{})
	}
	if ts, ok := db.(trieSetter); ok {
		ts.SetBinaryTrie(t)
	}
	return bt, nil
}

type binaryDBAdapter struct {
	db             database.NodeDatabase
	root           common.Hash
	reader         database.NodeReader
	disk           ethdb.Database
	archive        ethdb.Database
	values         map[common.Hash][]byte
	pendingNodes   *trienode.NodeSet // [FIX] Tracks all nodes written between commits
	physicalDelete bool
	mu             sync.RWMutex
}

func (a *binaryDBAdapter) archiveDB() ethdb.Database {
	return a.archive
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
	h := common.BytesToHash(key)
	if len(key) == 32 {
		binaryNodeCacheAdd(h, value)

		// [FIX] Record for current NodeSet in Commit
		a.mu.Lock()
		if a.pendingNodes != nil {
			a.pendingNodes.AddNode(key, trienode.New(h, value))
		}
		a.mu.Unlock()
	}

	if db := a.diskDB(); db != nil {
		return db.Put(key, value)
	}
	return nil
}

func (a *binaryDBAdapter) Get(key []byte) ([]byte, error) {
	if len(key) == 32 {
		h := common.BytesToHash(key)
		if val, ok := binaryNodeCacheGet(h); ok {
			return val, nil
		}
	}
	// ... rest of the code

	// Try Disk first for potentially uncommitted/standalone nodes (like TopTree container)
	if db := a.diskDB(); db != nil {
		data, err := db.Get(key)
		if err == nil && data != nil {
			return data, nil
		}
	}
	// Try Archive
	if db := a.archiveDB(); db != nil {
		data, err := db.Get(key)
		if err == nil && data != nil {
			return data, nil
		}
	}
	// Fallback to trie database node reader.
	// We might need to try multiple recent roots because asynchronous pruning or
	// pending commits might have nodes that are not yet perfectly visible via
	// the latest root's reader.
	if a.reader == nil {
		if a.root == (common.Hash{}) {
			return nil, nil // Empty root has no nodes
		}
		r, err := a.db.NodeReader(a.root)
		if err != nil {
			// If reader fails, it might be an unindexed root. Look in cache or return nil.
			return nil, nil
		}
		a.reader = r
	}

	h := common.BytesToHash(key)
	data, err := a.reader.Node(common.Hash{}, nil, h)
	if err == nil && data != nil {
		// [FIX] Warm the cache! Once a node is found, keep it in globalNodeCache
		// to ensure cross-block and cross-reset visibility.
		binaryNodeCacheAdd(h, data)
		return data, nil
	}
	return data, err
}
func (a *binaryDBAdapter) Delete(key []byte) error {
	if len(key) == common.HashLength {
		binaryNodeCacheRemove(common.BytesToHash(key))
	}
	if !a.physicalDelete {
		return nil
	}
	if db := a.diskDB(); db != nil {
		return db.Delete(key)
	}
	return nil
}
func (a *binaryDBAdapter) NewBatch() binary.Batcher {
	if db := a.diskDB(); db != nil {
		return &binaryBatchAdapter{db: a.db, batch: db.NewBatch(), physicalDelete: a.physicalDelete}
	}
	return &binaryBatchAdapter{db: a.db, physicalDelete: a.physicalDelete}
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
	db             database.NodeDatabase
	batch          ethdb.Batch
	physicalDelete bool
}

func (a *binaryBatchAdapter) Put(key, value []byte) error {
	if a.batch != nil {
		return a.batch.Put(key, value)
	}
	return nil
}
func (a *binaryBatchAdapter) Delete(key []byte) error {
	if len(key) == common.HashLength {
		binaryNodeCacheRemove(common.BytesToHash(key))
	}
	if !a.physicalDelete {
		return nil
	}
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
	adapter *binaryDBAdapter
	nodes   *trienode.NodeSet
	owner   common.Hash
}

func (b *nodeSetBatcher) Put(key, value []byte) error {
	h := common.BytesToHash(key)
	b.nodes.AddNode(key, trienode.New(h, value))

	// [FIX] Update global cache for immediate visibility in subsequent adapter.Get across blocks
	binaryNodeCacheAdd(h, value)

	return nil
}
func (b *nodeSetBatcher) Delete(key []byte) error {
	return b.adapter.Delete(key)
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
	commitStart := time.Now()
	// [SHARD-AWARE COMMIT via Anonymous Interface to break Import Cycle]
	if adapter, ok := t.trie.Database().(*binaryDBAdapter); ok {
		// 1. Prepare MergedNodeSet for atomic update across owners
		merged := trienode.NewMergedNodeSet()
		dirtyShards := t.trie.GetDirtyShards()
		shardRoots := make(map[int][]byte, len(dirtyShards))

		// 2. Iterate through dirty shards and populate MergedNodeSet
		shardCommitStart := time.Now()
		for _, id := range dirtyShards {
			nodes := trienode.NewNodeSet(common.Hash{})
			batch := &nodeSetBatcher{adapter: adapter, nodes: nodes}
			shardRootBytes, err := t.trie.CommitShardToBatch(id, batch, false)
			if err != nil {
				fmt.Printf("[DEBUG] BinaryTrie.Commit shard %d error: %v\n", id, err)
				return common.Hash{}, nil
			}
			if len(shardRootBytes) > 0 {
				shardRoots[id] = shardRootBytes
			} else {
				shardRoots[id] = make([]byte, common.HashLength)
			}

			shardRoot := common.BytesToHash(shardRoots[id])
			nodes.Owner = shardRoot
			if len(nodes.Nodes) > 0 {
				if err := merged.Merge(nodes); err != nil {
					fmt.Printf("[DEBUG] BinaryTrie.Commit merge shard %d error: %v\n", id, err)
					return common.Hash{}, nil
				}
			}
		}
		shardCommitDuration := time.Since(shardCommitStart)

		// 3. Commit the top tree container nodes under the zero owner.
		topNodes := trienode.NewNodeSet(common.Hash{})
		topBatch := &nodeSetBatcher{adapter: adapter, nodes: topNodes}
		topTreeStart := time.Now()
		h, err := t.trie.CommitTopTreeToBatch(shardRoots, dirtyShards, topBatch)
		if err != nil {
			fmt.Printf("[DEBUG] BinaryTrie.Commit top tree error: %v\n", err)
			return common.Hash{}, nil
		}
		topTreeDuration := time.Since(topTreeStart)
		root := common.BytesToHash(h)
		if len(topNodes.Nodes) > 0 {
			if err := merged.Merge(topNodes); err != nil {
				fmt.Printf("[DEBUG] BinaryTrie.Commit merge top tree error: %v\n", err)
				return common.Hash{}, nil
			}
		}

		// 4. Add pending nodes and manual values to MergedNodeSet under zero owner
		adapterMergeStart := time.Now()
		adapter.mu.Lock()
		if adapter.pendingNodes != nil && len(adapter.pendingNodes.Nodes) > 0 {
			if err := merged.Merge(adapter.pendingNodes); err != nil {
				adapter.mu.Unlock()
				fmt.Printf("[DEBUG] BinaryTrie.Commit merge pending nodes error: %v\n", err)
				return common.Hash{}, nil
			}
			adapter.pendingNodes = nil
		}
		if len(adapter.values) > 0 {
			vNodes := trienode.NewNodeSet(common.Hash{})
			for hash, blob := range adapter.values {
				vNodes.AddNode(hash.Bytes(), trienode.New(hash, blob))
			}
			if err := merged.Merge(vNodes); err != nil {
				adapter.mu.Unlock()
				fmt.Printf("[DEBUG] BinaryTrie.Commit merge values error: %v\n", err)
				return common.Hash{}, nil
			}
			adapter.values = nil
		}
		adapter.mu.Unlock()
		adapterMergeDuration := time.Since(adapterMergeStart)

		// 5. Update triedb atomically via anonymous interface assertion
		var updateDuration time.Duration
		type updater interface {
			Update(common.Hash, common.Hash, uint64, *trienode.MergedNodeSet, interface{}) error
		}
		if u, ok := t.db.(updater); ok {
			updateStart := time.Now()
			if err := u.Update(root, t.originRoot, t.block, merged, nil); err != nil {
				fmt.Printf("[DEBUG] BinaryTrie.Commit Update error: %v\n", err)
			}
			updateDuration = time.Since(updateStart)
		}

		// 6. Native persistence for reuse
		type trieSetter interface {
			SetBinaryTrie(interface{})
		}
		if ts, ok := t.db.(trieSetter); ok {
			ts.SetBinaryTrie(t.trie)
		}

		t.originRoot = root
		binary.RecordWrapperCommitDiagnostics(time.Since(commitStart), shardCommitDuration, topTreeDuration, adapterMergeDuration, updateDuration, len(dirtyShards))
		// Return nil NodeSet because the internal logic already handled the atomic update
		return root, nil
	}

	fallbackStart := time.Now()
	h, _ := t.trie.CommitToBatch(nil, true)
	binary.RecordWrapperCommitDiagnostics(time.Since(commitStart), time.Since(fallbackStart), 0, 0, 0, 0)
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

func (s *BinaryStorageTrie) PruneNextShard() error { return s.bt.PruneNextShard() }

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
	visit := func(key, val []byte) bool {
		if len(it.prefix) == 0 || bytes.HasPrefix(key, it.prefix) {
			it.leaves = append(it.leaves, leafKV{
				key:   key[len(it.prefix):],
				value: val,
			})
		}
		return true
	}
	if len(it.prefix) == 0 {
		bt.ForEach(visit)
	} else {
		bt.ForEachPrefix(it.prefix, len(it.prefix)*8, visit)
	}
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
