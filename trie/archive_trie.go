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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/trie/trienode"
	trieutils "github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// ArchiveTrie 是 ASCT 归档 trie 在 state.Trie 接口上的包装层。
// 它负责把账户、storage、code 三类以太坊状态映射到底层 archivetrie.Trie 的归档键空间。
type ArchiveTrie struct {
	trie       *archivetrie.Trie
	stem       *archivetrie.StemTrie
	db         database.NodeDatabase
	originRoot common.Hash
	block      uint64
	flat       ArchiveFlatSnapshotResolver
	indexMu    sync.Mutex
	indexOps   map[string]bool
}

// ArchiveFlatSnapshotResolver 按 state root 返回可读的 flat snapshot。
// wrapper 通过它在命中 snapshot 时绕过 trie 读取，降低历史状态回放的随机读压力。
type ArchiveFlatSnapshotResolver func(common.Hash) ArchiveFlatSnapshot

// ArchiveFlatSnapshot 暴露账户和 storage 的 flat 读取能力。
// 这里保持最小接口，避免 archive_trie 直接依赖 snapshot 的具体实现。
type ArchiveFlatSnapshot interface {
	AccountRLP(common.Hash) ([]byte, error)
	Storage(common.Hash, common.Hash) ([]byte, error)
}

// archiveSnapshotFlatReader 是下层 archivetrie.Trie 使用的 flat reader 适配器。
// archivetrie.Trie 只认识归档键，适配器负责把归档键还原成 snapshot 使用的 hash 键。
type archiveSnapshotFlatReader struct {
	resolver ArchiveFlatSnapshotResolver
	root     *common.Hash
}

// GetFlatValue 解析归档键并从 flat snapshot 取值。
// 账户键直接按地址 hash 读取；storage 键需要去掉地址域前缀并拆出 RLP 内容。
func (r *archiveSnapshotFlatReader) GetFlatValue(key []byte) ([]byte, error) {
	if r == nil || r.resolver == nil || r.root == nil {
		return nil, archivetrie.ErrNodeNotFound
	}
	snap := r.resolver(*r.root)
	if snap == nil {
		return nil, archivetrie.ErrNodeNotFound
	}
	switch {
	case len(key) == common.AddressLength:
		return snap.AccountRLP(crypto.Keccak256Hash(key))
	case len(key) == common.AddressLength+common.HashLength:
		addr := common.BytesToAddress(key[:common.AddressLength])
		addr[0] ^= 0x01
		accountHash := crypto.Keccak256Hash(addr.Bytes())
		storageHash := crypto.Keccak256Hash(key[common.AddressLength:])
		blob, err := snap.Storage(accountHash, storageHash)
		if err != nil || len(blob) == 0 {
			return nil, err
		}
		_, content, _, err := rlp.Split(blob)
		if err != nil {
			return nil, err
		}
		return content, nil
	default:
		return nil, archivetrie.ErrNodeNotFound
	}
}

// installFlatReader 将当前 root 的 flat snapshot 接到下层 trie。
// root 指针绑定到 ArchiveTrie.originRoot，commit 后会随 wrapper root 一起更新。
func (t *ArchiveTrie) installFlatReader() {
	if t == nil || t.trie == nil || t.flat == nil {
		return
	}
	t.trie.SetFlatReader(&archiveSnapshotFlatReader{
		resolver: t.flat,
		root:     &t.originRoot,
	})
}

func wrapArchiveTrie(active *archivetrie.Trie, db database.NodeDatabase, root common.Hash, flat ArchiveFlatSnapshotResolver) (*ArchiveTrie, error) {
	bt := &ArchiveTrie{trie: active, db: db, originRoot: root, flat: flat, indexOps: make(map[string]bool)}
	if active != nil && active.Config() != nil && active.Config().StemMode {
		stem, err := archivetrie.NewStemTrie(active)
		if err != nil {
			return nil, err
		}
		bt.stem = stem
		if err := bt.prepareArchiveStemIndex(); err != nil {
			return nil, err
		}
	}
	bt.installFlatReader()
	return bt, nil
}

var (
	// globalArchiveNodeCache 缓存 hash-mode 节点 blob，主要解决 wrapper 每个区块重建 adapter
	// 时看不到上一轮尚未完全落盘节点的问题。它是跨 block 的长期缓存，因此必须同时
	// 受 entry 数和字节数限制，否则少量超大节点会绕过 entry 上限把堆撑大。
	globalArchiveNodeCache           *archiveNodeBlobCache
	globalArchiveNodeCacheLimit      int
	globalArchiveNodeCacheBytesLimit int64
	globalArchiveNodeCacheMu         sync.RWMutex

	globalTrieRegistry   = make(map[database.NodeDatabase]*archivetrie.Trie)
	globalTrieRegistryMu sync.Mutex
)

const defaultBinaryNodeCacheLimit = archivetrie.DefaultNodeCacheLimit

// archiveNodeBlobCache 是 wrapper 层的全局节点缓存。
// 与 trie 内部的 nodeBlobCache 不同，它服务于跨 adapter 读取，所以生命周期更长；
// 这里显式维护 bytes，避免长回放时只按条目淘汰导致实际内存失控。
type archiveNodeBlobCache struct {
	mu         sync.Mutex
	cache      lru.BasicLRU[common.Hash, []byte]
	limit      int
	bytesLimit int64
	bytes      int64
	hits       int64
	misses     int64
	evictions  int64
}

// newArchiveNodeBlobCache 创建带 entry 上限和字节上限的全局节点缓存。
// limit 控制 LRU 条目数，bytesLimit 控制真实 blob 体积，二者任一触顶都会淘汰旧节点。
func newArchiveNodeBlobCache(limit int, bytesLimit int64) *archiveNodeBlobCache {
	if limit <= 0 {
		return nil
	}
	if bytesLimit < 0 {
		bytesLimit = 0
	}
	return &archiveNodeBlobCache{
		cache:      lru.NewBasicLRU[common.Hash, []byte](limit),
		limit:      limit,
		bytesLimit: bytesLimit,
	}
}

// add 写入一个节点 blob，并按 entry/bytes 上限执行 LRU 淘汰。
func (c *archiveNodeBlobCache) add(hash common.Hash, value []byte) {
	if c == nil || value == nil {
		return
	}
	// 入缓存前复制一份，避免调用方复用/修改切片导致缓存内容被污染。
	data := common.CopyBytes(value)
	size := int64(common.HashLength + len(data))
	c.mu.Lock()
	defer c.mu.Unlock()
	// 单个节点已经超过总字节上限时直接丢弃，防止大对象长期驻留。
	if c.bytesLimit > 0 && size > c.bytesLimit {
		c.removeLocked(hash)
		return
	}
	if old, ok := c.cache.Peek(hash); ok {
		c.bytes -= int64(common.HashLength + len(old))
		c.cache.Remove(hash)
	}
	for c.limit > 0 && c.cache.Len() >= c.limit {
		c.removeOldestLocked()
	}
	// byte 上限比 entry 上限更贴近真实内存压力。这里用 LRU 逐个淘汰到可容纳
	// 新节点为止，让实验里的 binaryNodeCacheBytesLimitMB 变成硬保护。
	for c.bytesLimit > 0 && c.bytes+size > c.bytesLimit && c.cache.Len() > 0 {
		c.removeOldestLocked()
	}
	c.cache.Add(hash, data)
	c.bytes += size
}

// get 返回缓存节点的副本，避免调用方修改缓存内的共享切片。
func (c *archiveNodeBlobCache) get(hash common.Hash) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.cache.Get(hash)
	if !ok {
		c.misses++
		return nil, false
	}
	c.hits++
	return common.CopyBytes(value), true
}

// remove 删除一个 hash-mode 节点，并同步维护缓存字节数。
func (c *archiveNodeBlobCache) remove(hash common.Hash) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(hash)
}

// removeLocked 假设调用方已经持有 c.mu。
func (c *archiveNodeBlobCache) removeLocked(hash common.Hash) {
	if old, ok := c.cache.Peek(hash); ok {
		c.bytes -= int64(common.HashLength + len(old))
		c.cache.Remove(hash)
	}
	if c.bytes < 0 {
		c.bytes = 0
	}
}

// removeOldestLocked 淘汰最旧节点，供 entry/bytes 两种限制复用。
func (c *archiveNodeBlobCache) removeOldestLocked() {
	_, value, ok := c.cache.RemoveOldest()
	if !ok {
		return
	}
	c.bytes -= int64(common.HashLength + len(value))
	c.evictions++
	if c.bytes < 0 {
		c.bytes = 0
	}
}

// stats 返回当前缓存体量，用于实验 CSV 的 NodeCache_* 统计。
func (c *archiveNodeBlobCache) stats() (entries int64, bytes int64) {
	diag := c.diagnostics()
	return diag.Entries, diag.Bytes
}

func (c *archiveNodeBlobCache) diagnostics() archivetrie.NodeCacheDiagnostics {
	if c == nil {
		return archivetrie.NodeCacheDiagnostics{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return archivetrie.NodeCacheDiagnostics{
		Entries:    int64(c.cache.Len()),
		Bytes:      c.bytes,
		EntryLimit: int64(c.limit),
		BytesLimit: c.bytesLimit,
		Shards:     1,
		Hits:       c.hits,
		Misses:     c.misses,
		Evictions:  c.evictions,
	}
}

// configureArchiveNodeCache 根据 triedb 配置重建进程级归档节点缓存。
// 配置变化时直接丢弃旧 LRU，可以避免上一轮实验的缓存体量污染下一轮结果。
func configureArchiveNodeCache(limit int, bytesLimit int64) {
	globalArchiveNodeCacheMu.Lock()
	defer globalArchiveNodeCacheMu.Unlock()

	if limit == globalArchiveNodeCacheLimit && bytesLimit == globalArchiveNodeCacheBytesLimit {
		return
	}
	globalArchiveNodeCacheLimit = limit
	globalArchiveNodeCacheBytesLimit = bytesLimit
	if limit <= 0 {
		globalArchiveNodeCache = nil
		return
	}
	// 配置变化时直接换一个新 LRU。旧缓存会被 GC 回收，避免旧上限下积累的
	// 大对象继续影响新一轮实验。
	globalArchiveNodeCache = newArchiveNodeBlobCache(limit, bytesLimit)
}

// archiveNodeCacheAdd 写入进程级节点缓存，供后续 wrapper/adapter 读路径复用。
func archiveNodeCacheAdd(hash common.Hash, value []byte) {
	globalArchiveNodeCacheMu.RLock()
	cache := globalArchiveNodeCache
	globalArchiveNodeCacheMu.RUnlock()
	if cache == nil {
		return
	}
	cache.add(hash, value)
}

// archiveNodeCacheGet 优先从进程级缓存读取 hash-mode 节点。
func archiveNodeCacheGet(hash common.Hash) ([]byte, bool) {
	globalArchiveNodeCacheMu.RLock()
	cache := globalArchiveNodeCache
	globalArchiveNodeCacheMu.RUnlock()
	if cache == nil {
		return nil, false
	}
	return cache.get(hash)
}

// archiveNodeCacheRemove 在物理删除或批量删除时清掉失效节点。
func archiveNodeCacheRemove(hash common.Hash) {
	globalArchiveNodeCacheMu.RLock()
	cache := globalArchiveNodeCache
	globalArchiveNodeCacheMu.RUnlock()
	if cache != nil {
		cache.remove(hash)
	}
}

// archiveNodeCacheStats 返回进程级缓存统计。
func archiveNodeCacheStats() (entries int64, bytes int64) {
	diag := archiveNodeCacheDiagnostics()
	return diag.Entries, diag.Bytes
}

func archiveNodeCacheDiagnostics() archivetrie.NodeCacheDiagnostics {
	globalArchiveNodeCacheMu.RLock()
	cache := globalArchiveNodeCache
	globalArchiveNodeCacheMu.RUnlock()
	if cache == nil {
		return archivetrie.NodeCacheDiagnostics{}
	}
	return cache.diagnostics()
}

// NewArchiveTrie 创建 ArchiveTrie，并尽量复用 triedb 中的 active archivetrie.Trie。
// 复用可以保留下层跨区块缓存；如果 active trie root 不匹配，则尝试 Load 到目标 root。
func NewArchiveTrie(root common.Hash, db database.NodeDatabase, archive ethdb.Database, flatResolvers ...ArchiveFlatSnapshotResolver) (*ArchiveTrie, error) {
	var flat ArchiveFlatSnapshotResolver
	if len(flatResolvers) > 0 {
		flat = flatResolvers[0]
	}
	var active *archivetrie.Trie

	// 优先复用 triedb 保存的 active trie，避免每个区块重新构造分片和缓存。
	type trieStore interface {
		GetArchiveTrie() interface{}
	}
	if ts, ok := db.(trieStore); ok {
		if bt := ts.GetArchiveTrie(); bt != nil {
			active = bt.(*archivetrie.Trie)
		}
	}

	if active != nil {
		// root 已经一致时直接返回 wrapper，保留 active trie 的分片状态和缓存。
		h, _ := active.Hash()
		if bytes.Equal(h, root.Bytes()) {
			// adapter 的 root 用于回退 NodeReader，复用 active trie 时仍需刷新。
			if adapter, ok := active.Database().(*archiveDBAdapter); ok {
				adapter.root = root
			}
			return wrapArchiveTrie(active, db, root, flat)
		}

		if err := active.Load(root.Bytes()); err == nil {
			// Load 成功说明 active trie 可以切到目标 root，此时清掉旧 NodeReader。
			if adapter, ok := active.Database().(*archiveDBAdapter); ok {
				adapter.root = root
				adapter.reader = nil // Clear stale reader
			}
			return wrapArchiveTrie(active, db, root, flat)
		}
	}

	config := archivetrie.DefaultConfig()
	nodeCacheLimit := defaultBinaryNodeCacheLimit
	nodeCacheBytesLimit := archivetrie.DefaultNodeCacheBytesLimit
	physicalDelete := false

	// 从 triedb 读取实验配置，再落到下层 archivetrie.Config。
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
			if dbConf.CuckooBuckets > 0 {
				config.CuckooBuckets = dbConf.CuckooBuckets
			}
			if dbConf.CuckooSlots > 0 {
				config.CuckooSlots = dbConf.CuckooSlots
			}
			if dbConf.NodeCacheLimit != 0 {
				nodeCacheLimit = dbConf.NodeCacheLimit
			}
			if dbConf.NodeCacheBytesLimit != 0 {
				nodeCacheBytesLimit = dbConf.NodeCacheBytesLimit
			}
			config.NodeCacheLimit = nodeCacheLimit
			config.NodeCacheBytesLimit = nodeCacheBytesLimit
			if dbConf.NodeCacheWarmPathBits != 0 {
				config.NodeCacheWarmPathBits = dbConf.NodeCacheWarmPathBits
			}
			if dbConf.CommitmentPointCacheLimit != 0 {
				config.CommitmentPointCacheLimit = dbConf.CommitmentPointCacheLimit
			}
			if dbConf.ArchiveStubMaxBucketsPath != 0 {
				config.ArchiveStubMaxBucketsPath = dbConf.ArchiveStubMaxBucketsPath
			}
			config.AsyncPrune = dbConf.AsyncPrune
			if dbConf.CommitWorkers > 0 {
				config.CommitWorkers = dbConf.CommitWorkers
			}
			if dbConf.CommitWatchdogSeconds > 0 {
				config.CommitWatchdogSeconds = dbConf.CommitWatchdogSeconds
			}
			config.EnablePathDiagnostics = dbConf.EnablePathDiagnostics
			config.PhysicalDelete = dbConf.PhysicalDelete
			config.StemMode = dbConf.StemMode
			if dbConf.NodeStorageScheme == archivetrie.NodeStorageHash || dbConf.NodeStorageScheme == archivetrie.NodeStoragePath {
				config.NodeStorageScheme = dbConf.NodeStorageScheme
			}
			physicalDelete = dbConf.PhysicalDelete
		}
	}

	// Path-mode nodes are cached by their physical path inside archivetrie.
	// The wrapper cache is keyed by 32-byte hashes and cannot serve those reads,
	// so keeping a second equally-sized LRU only adds lock and memory overhead.
	if config.UsePathStorage() {
		configureArchiveNodeCache(-1, 0)
	} else {
		configureArchiveNodeCache(nodeCacheLimit, nodeCacheBytesLimit)
	}
	config.NodeCacheLimit = nodeCacheLimit
	config.NodeCacheBytesLimit = nodeCacheBytesLimit
	config.PhysicalDelete = physicalDelete

	// kvAdapter 负责把 archivetrie.Trie 的 KV 读写接到 triedb/disk 两个后端。
	_ = archive
	kvAdapter := &archiveDBAdapter{db: db, root: root, physicalDelete: physicalDelete}

	// pruning=true 表示下层 trie 以归档裁剪模式运行。
	t := archivetrie.NewTrie(root.Bytes(), kvAdapter, archivetrie.NewPooledKeccakHasher(), config, true)
	bt, err := wrapArchiveTrie(t, db, root, flat)
	if err != nil {
		return nil, err
	}
	// 把 active trie 放回 triedb，下一次 NewArchiveTrie 可以继续复用。
	type trieSetter interface {
		SetArchiveTrie(interface{})
	}
	if ts, ok := db.(trieSetter); ok {
		ts.SetArchiveTrie(t)
	}
	return bt, nil
}

// SetBlockNum 写入当前区块号，主要用于 commit/prune 诊断统计。
func (t *ArchiveTrie) SetBlockNum(num uint64) {
	if t != nil {
		t.block = num
	}
}

// currentBlockNum 优先从 triedb 读取最新区块号，读不到时使用 wrapper 自己保存的值。
func (t *ArchiveTrie) currentBlockNum() uint64 {
	if t == nil {
		return 0
	}
	type blockReader interface {
		CurrentBlockNum() uint64
	}
	if br, ok := t.db.(blockReader); ok {
		t.block = br.CurrentBlockNum()
	}
	return t.block
}

// archiveDBAdapter 把下层 archivetrie.Trie 需要的 KVStore/ArchiveStore 接口适配到 triedb。
// hash-mode 节点走 triedb NodeSet，path-storage/raw flat 写入走 disk/archive 批处理。
type archiveDBAdapter struct {
	db             database.NodeDatabase
	root           common.Hash
	reader         database.NodeReader
	disk           ethdb.Database
	values         map[common.Hash][]byte
	pendingNodes   *trienode.NodeSet // 记录两次 commit 之间写出的所有节点。
	physicalDelete bool
	mu             sync.RWMutex
}

// diskDB 懒加载 triedb 暴露的底层 ethdb，避免构造时强依赖具体数据库类型。
func (a *archiveDBAdapter) diskDB() ethdb.Database {
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

// Put 写入下层 trie 产生的节点或 raw 数据。
// 32 字节 hash-mode 节点会进入进程级缓存，并在 commit 期间补进 pending NodeSet。
// ArchiveIndexLogicalSize reports reachable index key/value bytes. It excludes
// LevelDB table, WAL, obsolete-version and compaction overhead.
func (a *archiveDBAdapter) ArchiveIndexLogicalSize() (int64, int64, error) {
	db := a.diskDB()
	if db == nil {
		return 0, 0, errors.New("archive index store is unavailable")
	}
	var totalBytes, totalEntries int64
	for _, prefix := range [][]byte{archiveStemStorageIndexPrefix, archiveStemCodeIndexPrefix} {
		it := db.NewIterator(prefix, nil)
		for it.Next() {
			totalEntries++
			totalBytes += int64(len(it.Key()) + len(it.Value()))
		}
		err := it.Error()
		it.Release()
		if err != nil {
			return 0, 0, err
		}
	}
	if data, err := db.Get(archiveStemIndexSchemaKey); err == nil {
		totalEntries++
		totalBytes += int64(len(archiveStemIndexSchemaKey) + len(data))
	}
	return totalBytes, totalEntries, nil
}

// Put stores one lower-trie node or flat record.
func (a *archiveDBAdapter) Put(key, value []byte) error {
	h := common.BytesToHash(key)
	if len(key) == 32 && !archivetrie.IsPathStorageKey(key) {
		archiveNodeCacheAdd(h, value)

		// 记录到当前 Commit 的 NodeSet。
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

// Get 按缓存、disk、archive、triedb NodeReader 的顺序读取节点。
// path-storage key 不回退到 NodeReader，因为它们本身不是 hashdb 节点。
func (a *archiveDBAdapter) Get(key []byte) ([]byte, error) {
	pathKey := archivetrie.IsPathStorageKey(key)
	if len(key) == 32 && !pathKey {
		h := common.BytesToHash(key)
		if val, ok := archiveNodeCacheGet(h); ok {
			return val, nil
		}
	}
	// ... rest of the code

	// Try disk first for standalone binary root branches and uncommitted nodes.
	if db := a.diskDB(); db != nil {
		data, err := db.Get(key)
		if err == nil && data != nil {
			return data, nil
		}
	}
	if pathKey {
		return nil, nil
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
		// NodeReader 命中后写入进程缓存，保证跨 block / reset 的可见性稳定。
		archiveNodeCacheAdd(h, data)
		return data, nil
	}
	return data, err
}

// Delete 删除 raw/path 数据；hash-mode 节点默认只从缓存移除，是否物理删除由配置控制。
func (a *archiveDBAdapter) Delete(key []byte) error {
	pathKey := archivetrie.IsPathStorageKey(key)
	if len(key) == common.HashLength && !pathKey {
		archiveNodeCacheRemove(common.BytesToHash(key))
	}
	if !a.physicalDelete && !pathKey {
		return nil
	}
	if db := a.diskDB(); db != nil {
		return db.Delete(key)
	}
	return nil
}

// NewBatch 为下层 archivetrie.Trie 提供批量写接口。
func (a *archiveDBAdapter) NewBatch() archivetrie.Batcher {
	if db := a.diskDB(); db != nil {
		return &archiveBatchAdapter{db: a.db, batch: db.NewBatch(), physicalDelete: a.physicalDelete}
	}
	return &archiveBatchAdapter{db: a.db, physicalDelete: a.physicalDelete}
}

// archiveBatchAdapter 包装 ethdb.Batch，并在删除时同步维护进程级节点缓存。
type archiveBatchAdapter struct {
	db             database.NodeDatabase
	batch          ethdb.Batch
	physicalDelete bool
}

// Put 把写入转发到 ethdb.Batch；没有 batch 时表示该路径只做逻辑收集。
func (a *archiveBatchAdapter) Put(key, value []byte) error {
	if a.batch != nil {
		return a.batch.Put(key, value)
	}
	return nil
}

// Delete 删除 batch 中的 key，并按物理删除配置决定是否真正落到底层 DB。
func (a *archiveBatchAdapter) Delete(key []byte) error {
	pathKey := archivetrie.IsPathStorageKey(key)
	if len(key) == common.HashLength && !pathKey {
		archiveNodeCacheRemove(common.BytesToHash(key))
	}
	if !a.physicalDelete && !pathKey {
		return nil
	}
	if a.batch != nil {
		return a.batch.Delete(key)
	}
	return nil
}

// Write 提交底层 batch。
func (a *archiveBatchAdapter) Write() error {
	if a.batch != nil {
		return a.batch.Write()
	}
	return nil
}

// Reset 释放底层 batch 缓冲。
func (a *archiveBatchAdapter) Reset() {
	if a.batch != nil {
		a.batch.Reset()
	}
}

// ValueSize 返回底层 batch 当前累计 value 字节数。
func (a *archiveBatchAdapter) ValueSize() int {
	if a.batch != nil {
		return a.batch.ValueSize()
	}
	return 0
}

// nodeSetBatcher 把 commit 产出的 hash-mode 节点收集到 NodeSet。
// raw path/flat 写入则转发到 rawBatch，最终与 triedb update 保持同一个 commit 边界。
type nodeSetBatcher struct {
	adapter  *archiveDBAdapter
	nodes    *trienode.NodeSet
	owner    common.Hash
	rawBatch ethdb.KeyValueWriter
}

// Put 根据 key 类型区分 NodeSet 节点和 raw 数据。
func (b *nodeSetBatcher) Put(key, value []byte) error {
	if archivetrie.IsPathStorageKey(key) || len(key) != common.HashLength {
		if b.rawBatch != nil {
			return b.rawBatch.Put(key, value)
		}
		return b.adapter.Put(key, value)
	}
	h := common.BytesToHash(key)
	b.nodes.AddNode(key, trienode.New(h, value))

	// 更新进程缓存，保证后续 adapter.Get 立刻可见。
	archiveNodeCacheAdd(h, value)

	return nil
}

// Delete 对 raw 数据立即进入 rawBatch；hash-mode 节点交给 adapter 处理缓存和物理删除策略。
func (b *nodeSetBatcher) Delete(key []byte) error {
	if archivetrie.IsPathStorageKey(key) || len(key) != common.HashLength {
		if b.rawBatch != nil {
			return b.rawBatch.Delete(key)
		}
		if db := b.adapter.diskDB(); db != nil {
			return db.Delete(key)
		}
		return nil
	}
	return b.adapter.Delete(key)
}

// Write 对 nodeSetBatcher 是空操作，真正写入由 ArchiveTrie.Commit 统一完成。
func (b *nodeSetBatcher) Write() error { return nil }

// Reset 对 nodeSetBatcher 是空操作，worker 生命周期结束后直接丢弃。
func (b *nodeSetBatcher) Reset() {}

// ValueSize 对 nodeSetBatcher 没有实际意义，返回 0。
func (b *nodeSetBatcher) ValueSize() int { return 0 }

// rawBatchOp 是延迟回放到单一 ethdb.Batch 的 raw 写入操作。
type rawBatchOp struct {
	key    []byte
	value  []byte
	delete bool
}

// rawBatchBuffer 是每个 shard worker 私有的 raw 写入缓冲。
// worker 不共享 ethdb.Batch，避免并发写 batch 带来的锁竞争和顺序不确定。
type rawBatchBuffer struct {
	ops []rawBatchOp
}

// Put 记录一条 raw put 操作，并复制 key/value 避免调用方复用切片。
func (b *rawBatchBuffer) Put(key, value []byte) error {
	b.ops = append(b.ops, rawBatchOp{
		key:   common.CopyBytes(key),
		value: common.CopyBytes(value),
	})
	return nil
}

// Delete 记录一条 raw delete 操作，并复制 key 避免调用方复用切片。
func (b *rawBatchBuffer) Delete(key []byte) error {
	b.ops = append(b.ops, rawBatchOp{
		key:    common.CopyBytes(key),
		delete: true,
	})
	return nil
}

// replay 按 worker 内部原始顺序把 raw 操作回放到最终 batch。
func (b *rawBatchBuffer) replay(dst ethdb.KeyValueWriter) error {
	for _, op := range b.ops {
		if op.delete {
			if err := dst.Delete(op.key); err != nil {
				return err
			}
			continue
		}
		if err := dst.Put(op.key, op.value); err != nil {
			return err
		}
	}
	return nil
}

// rawBatchStats 读取 ethdb.Batch 的操作数和字节数，用于 commit 诊断。
func rawBatchStats(batch ethdb.Batch) (ops int, bytes int) {
	if batch == nil {
		return 0, 0
	}
	type lenner interface {
		Len() int
	}
	if l, ok := batch.(lenner); ok {
		ops = l.Len()
	}
	bytes = batch.ValueSize()
	return ops, bytes
}

// stats 返回 shard worker 私有 raw buffer 的体量。
func (b *rawBatchBuffer) stats() (ops int, bytes int) {
	if b == nil {
		return 0, 0
	}
	for _, op := range b.ops {
		ops++
		bytes += len(op.key) + len(op.value)
	}
	return ops, bytes
}

// trieDB 返回 wrapper 背后的 triedb，供调试或后续扩展使用。
func (t *ArchiveTrie) trieDB() database.NodeDatabase {
	if adapter, ok := t.trie.Database().(*archiveDBAdapter); ok {
		return adapter.db
	}
	return nil
}

// StemMode reports whether this wrapper groups binary-tree keys by stem.
func (t *ArchiveTrie) StemMode() bool {
	return t != nil && t.stem != nil
}

// GetKey 对 ArchiveTrie 没有实际意义；归档键已经是下层 trie 的原始键。
func (t *ArchiveTrie) GetKey(key []byte) []byte { return nil }

// flatSnapshot 返回当前 originRoot 对应的 flat snapshot。
func (t *ArchiveTrie) flatSnapshot() ArchiveFlatSnapshot {
	if t.flat == nil {
		return nil
	}
	return t.flat(t.originRoot)
}

var (
	archiveStemStorageMagic       = [5]byte{'A', 'S', 'S', 'V', 1}
	archiveStemStorageIndexPrefix = []byte{'A', 'S', 'I', 'S', 1}
	archiveStemCodeIndexPrefix    = []byte{'A', 'S', 'I', 'C', 1}
	archiveStemIndexSchemaKey     = []byte{'A', 'S', 'I', 'M', 1}
	archiveFlatValuePrefix        = []byte{'B', 'F', 'V', '1'}
)

type archiveIndexIteratorStore interface {
	Has(key []byte) (bool, error)
	NewIterator(prefix []byte, start []byte) ethdb.Iterator
}

type archiveIndexOp struct {
	key []byte
	put bool
}

func archiveStemStorageIndexAccountPrefix(addr common.Address) []byte {
	prefix := make([]byte, 0, len(archiveStemStorageIndexPrefix)+common.AddressLength)
	prefix = append(prefix, archiveStemStorageIndexPrefix...)
	prefix = append(prefix, addr[:]...)
	return prefix
}

func archiveStemStorageIndexKey(addr common.Address, slot []byte) []byte {
	slotHash := common.BytesToHash(slot)
	key := archiveStemStorageIndexAccountPrefix(addr)
	return append(key, slotHash[:]...)
}

func archiveStemCodeIndexAccountPrefix(addr common.Address) []byte {
	prefix := make([]byte, 0, len(archiveStemCodeIndexPrefix)+common.AddressLength)
	prefix = append(prefix, archiveStemCodeIndexPrefix...)
	prefix = append(prefix, addr[:]...)
	return prefix
}

func archiveStemCodeIndexKey(addr common.Address, chunk uint64) []byte {
	key := archiveStemCodeIndexAccountPrefix(addr)
	var suffix [8]byte
	binary.BigEndian.PutUint64(suffix[:], chunk)
	return append(key, suffix[:]...)
}

func (t *ArchiveTrie) archiveIndexStore() archiveIndexIteratorStore {
	if t == nil || t.trie == nil {
		return nil
	}
	if adapter, ok := t.trie.Database().(*archiveDBAdapter); ok {
		if disk := adapter.diskDB(); disk != nil {
			return disk
		}
	}
	store, _ := t.trie.Database().(archiveIndexIteratorStore)
	return store
}

func (t *ArchiveTrie) prepareArchiveStemIndex() error {
	store := t.archiveIndexStore()
	if store == nil {
		return errors.New("archive stem index store is unavailable")
	}
	ready, err := store.Has(archiveStemIndexSchemaKey)
	if err != nil {
		return fmt.Errorf("check archive stem index schema: %w", err)
	}
	if ready {
		return nil
	}
	it := store.NewIterator(archiveFlatValuePrefix, nil)
	hasStemData := it.Next()
	iterErr := it.Error()
	it.Release()
	if iterErr != nil {
		return fmt.Errorf("check existing archive stem data: %w", iterErr)
	}
	if hasStemData {
		return errors.New("archive stem account index is missing; start with a fresh database")
	}
	t.stageArchiveIndex(archiveStemIndexSchemaKey, true)
	return nil
}

func (t *ArchiveTrie) archiveStemIndexReady() (bool, error) {
	store := t.archiveIndexStore()
	if store == nil {
		return false, errors.New("archive stem index store is unavailable")
	}
	ready, err := store.Has(archiveStemIndexSchemaKey)
	if err != nil {
		return false, err
	}
	if ready {
		return true, nil
	}
	t.indexMu.Lock()
	pending := t.indexOps[string(archiveStemIndexSchemaKey)]
	t.indexMu.Unlock()
	return pending, nil
}

func (t *ArchiveTrie) listArchiveIndexKeys(prefix []byte) ([][]byte, error) {
	store := t.archiveIndexStore()
	if store == nil {
		return nil, errors.New("archive stem index store is unavailable")
	}
	// Snapshot only the uncommitted changes for this account. The database
	// iterator is already ordered, so avoid copying every persisted key into a
	// map and sorting the full (potentially 100K+) account index again.
	pending := make(map[string]bool)
	t.indexMu.Lock()
	for key, put := range t.indexOps {
		if bytes.HasPrefix([]byte(key), prefix) {
			pending[key] = put
		}
	}
	t.indexMu.Unlock()
	it := store.NewIterator(prefix, nil)
	defer it.Release()
	var keys [][]byte
	for it.Next() {
		key := common.CopyBytes(it.Key())
		keyString := string(key)
		if put, changed := pending[keyString]; changed {
			delete(pending, keyString)
			if !put {
				continue
			}
		}
		keys = append(keys, key)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	// New keys that do not yet exist in the database can be appended. Sort only
	// this usually tiny tail to keep repeated runs deterministic.
	added := make([]string, 0, len(pending))
	for key, put := range pending {
		if put {
			added = append(added, key)
		}
	}
	sort.Strings(added)
	for _, key := range added {
		keys = append(keys, []byte(key))
	}
	return keys, nil
}

func (t *ArchiveTrie) stageArchiveIndex(key []byte, put bool) {
	if t == nil || len(key) == 0 {
		return
	}
	t.indexMu.Lock()
	if t.indexOps == nil {
		t.indexOps = make(map[string]bool)
	}
	t.indexOps[string(key)] = put
	t.indexMu.Unlock()
}

func (t *ArchiveTrie) stageArchiveIndexes(keys [][]byte, put bool) {
	if t == nil || len(keys) == 0 {
		return
	}
	t.indexMu.Lock()
	if t.indexOps == nil {
		t.indexOps = make(map[string]bool)
	}
	for _, key := range keys {
		if len(key) > 0 {
			t.indexOps[string(key)] = put
		}
	}
	t.indexMu.Unlock()
}

func (t *ArchiveTrie) snapshotArchiveIndexOps() []archiveIndexOp {
	t.indexMu.Lock()
	defer t.indexMu.Unlock()
	ops := make([]archiveIndexOp, 0, len(t.indexOps))
	for key, put := range t.indexOps {
		ops = append(ops, archiveIndexOp{key: []byte(key), put: put})
	}
	return ops
}

func applyArchiveIndexOps(batch archivetrie.Batcher, ops []archiveIndexOp) error {
	for _, op := range ops {
		if op.put {
			if err := batch.Put(op.key, []byte{1}); err != nil {
				return err
			}
			continue
		}
		if err := batch.Delete(op.key); err != nil {
			return err
		}
	}
	return nil
}

func (t *ArchiveTrie) clearArchiveIndexOps(ops []archiveIndexOp) {
	t.indexMu.Lock()
	defer t.indexMu.Unlock()
	for _, op := range ops {
		if current, ok := t.indexOps[string(op.key)]; ok && current == op.put {
			delete(t.indexOps, string(op.key))
		}
	}
}

func encodeArchiveStemStorageValue(addr common.Address, slot, value []byte) []byte {
	slotHash := common.BytesToHash(slot)
	encoded := make([]byte, 0, len(archiveStemStorageMagic)+common.AddressLength+common.HashLength+len(value))
	encoded = append(encoded, archiveStemStorageMagic[:]...)
	encoded = append(encoded, addr[:]...)
	encoded = append(encoded, slotHash[:]...)
	encoded = append(encoded, value...)
	return encoded
}

func decodeArchiveStemStorageValue(addr common.Address, slot, encoded []byte) ([]byte, error) {
	header := len(archiveStemStorageMagic) + common.AddressLength + common.HashLength
	if len(encoded) < header || !bytes.Equal(encoded[:len(archiveStemStorageMagic)], archiveStemStorageMagic[:]) {
		return nil, errors.New("invalid ASCT stem storage value")
	}
	offset := len(archiveStemStorageMagic)
	if !bytes.Equal(encoded[offset:offset+common.AddressLength], addr[:]) {
		return nil, errors.New("ASCT stem storage address mismatch")
	}
	offset += common.AddressLength
	slotHash := common.BytesToHash(slot)
	if !bytes.Equal(encoded[offset:offset+common.HashLength], slotHash[:]) {
		return nil, errors.New("ASCT stem storage slot mismatch")
	}
	return common.CopyBytes(encoded[header:]), nil
}

func decodeArchiveStemStorageRecord(encoded []byte) (common.Address, common.Hash, []byte, bool) {
	header := len(archiveStemStorageMagic) + common.AddressLength + common.HashLength
	if len(encoded) < header || !bytes.Equal(encoded[:len(archiveStemStorageMagic)], archiveStemStorageMagic[:]) {
		return common.Address{}, common.Hash{}, nil, false
	}
	offset := len(archiveStemStorageMagic)
	addr := common.BytesToAddress(encoded[offset : offset+common.AddressLength])
	offset += common.AddressLength
	slot := common.BytesToHash(encoded[offset : offset+common.HashLength])
	return addr, slot, common.CopyBytes(encoded[header:]), true
}

// GetAccount 先查 flat snapshot，再回退到归档 trie。
// 下层保存的是 slim/full account RLP，这里兼容两种解码方式。
func (t *ArchiveTrie) GetAccount(address common.Address) (*types.StateAccount, error) {
	if snap := t.flatSnapshot(); snap != nil {
		if blob, err := snap.AccountRLP(crypto.Keccak256Hash(address.Bytes())); err == nil && len(blob) > 0 {
			return types.FullAccount(blob)
		}
	}
	var (
		data []byte
		err  error
	)
	if t.stem != nil {
		data, err = t.stem.Get(trieutils.BinaryTreeBasicDataKey(address))
	} else {
		data, err = t.trie.Get(address.Bytes())
	}
	if err != nil {
		if err == archivetrie.ErrNodeNotFound {
			return nil, nil
		}
		return nil, err
	}
	if acc, err := types.FullAccount(data); err == nil {
		return acc, nil
	}
	var acc types.StateAccount
	if err := rlp.DecodeBytes(data, &acc); err != nil {
		return nil, err
	}
	return &acc, nil
}

// GetStorage 先查 flat snapshot，再用“地址域 + storage key”的复合键访问归档 trie。
// 地址首字节 xor 0x01 是 storage 域分隔，避免与账户键冲突。
func (t *ArchiveTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	if snap := t.flatSnapshot(); snap != nil {
		accountHash := crypto.Keccak256Hash(addr.Bytes())
		storageHash := crypto.Keccak256Hash(key)
		if blob, err := snap.Storage(accountHash, storageHash); err == nil && len(blob) > 0 {
			_, content, _, err := rlp.Split(blob)
			if err != nil {
				return nil, err
			}
			return content, nil
		}
	}
	if t.stem != nil {
		val, err := t.stem.Get(trieutils.BinaryTreeStorageSlotKey(addr, key))
		if err == archivetrie.ErrNodeNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return decodeArchiveStemStorageValue(addr, key, val)
	}
	compositeKey := make([]byte, 20+len(key))
	copy(compositeKey, addr.Bytes())
	compositeKey[0] ^= 0x01 // XOR domain 1 for storage
	copy(compositeKey[20:], key)
	val, err := t.trie.Get(compositeKey)
	if err != nil && err == archivetrie.ErrNodeNotFound {
		return nil, nil
	}
	return val, err
}

// UpdateAccount 写入账户 RLP。codeLen 在 ArchiveTrie 中不参与键值编码。
func (t *ArchiveTrie) UpdateAccount(address common.Address, acc *types.StateAccount, codeLen int) error {
	value := types.SlimAccountRLP(*acc)
	if t.stem != nil {
		if accountUsesOnlyBasicStem(acc) {
			return t.stem.ReplaceStem(trieutils.BinaryTreeBasicDataKey(address), value)
		}
		return t.stem.Put(trieutils.BinaryTreeBasicDataKey(address), value)
	}
	return t.trie.Put(address.Bytes(), value)
}

// UpdateAccountRLP 直接写入上层已经编码好的账户 RLP。
func (t *ArchiveTrie) UpdateAccountRLP(address common.Address, account []byte, codeLen int) error {
	if t.stem != nil {
		if decoded, err := types.FullAccount(account); err == nil && accountUsesOnlyBasicStem(decoded) {
			return t.stem.ReplaceStem(trieutils.BinaryTreeBasicDataKey(address), account)
		}
		return t.stem.Put(trieutils.BinaryTreeBasicDataKey(address), account)
	}
	return t.trie.Put(address.Bytes(), account)
}

// UpdateAccountsBatch applies one block's account mutations together. Stem
// mode keeps the basic-account metadata-only load and lets independent shards
// resolve their paths in parallel.
func (t *ArchiveTrie) UpdateAccountsBatch(updates []AccountUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	if t.stem == nil {
		entries := make([]archivetrie.KeyValue, 0, len(updates))
		for _, update := range updates {
			if update.Account == nil {
				return errors.New("nil account in batch")
			}
			entries = append(entries, archivetrie.KeyValue{
				Key:   update.Address.Bytes(),
				Value: types.SlimAccountRLP(*update.Account),
			})
		}
		return t.trie.PutBatch(entries)
	}
	stemUpdates := make([]archivetrie.StemUpdate, 0, len(updates))
	for _, update := range updates {
		if update.Account == nil {
			return errors.New("nil account in batch")
		}
		stemUpdates = append(stemUpdates, archivetrie.StemUpdate{
			Key:     trieutils.BinaryTreeBasicDataKey(update.Address),
			Value:   types.SlimAccountRLP(*update.Account),
			Replace: accountUsesOnlyBasicStem(update.Account),
		})
	}
	return t.stem.ApplyBatch(stemUpdates)
}

func accountUsesOnlyBasicStem(account *types.StateAccount) bool {
	return account != nil &&
		account.Root == types.EmptyRootHash &&
		bytes.Equal(account.CodeHash, types.EmptyCodeHash[:])
}

// UpdateStorage 写入单个 storage slot；空值表示删除。
func (t *ArchiveTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	if t.stem != nil {
		return t.UpdateStorageBatch(addr, []StorageUpdate{{Key: common.CopyBytes(key), Value: common.CopyBytes(value), Delete: len(value) == 0}})
	}
	compositeKey := make([]byte, 20+len(key))
	copy(compositeKey, addr.Bytes())
	compositeKey[0] ^= 0x01
	copy(compositeKey[20:], key)
	if len(value) == 0 {
		return t.trie.BatchDelete(compositeKey)
	}
	return t.trie.Put(compositeKey, value)
}

// UpdateStorageBatch groups all mutations by stem so each 256-value payload is
// decoded, rehashed, and encoded only once.
func (t *ArchiveTrie) UpdateStorageBatch(addr common.Address, updates []StorageUpdate) error {
	if t.stem == nil {
		for _, update := range updates {
			if update.Delete || len(update.Value) == 0 {
				if err := t.DeleteStorage(addr, update.Key); err != nil {
					return err
				}
				continue
			}
			if err := t.UpdateStorage(addr, update.Key, update.Value); err != nil {
				return err
			}
		}
		return nil
	}
	stemUpdates := make([]archivetrie.StemUpdate, 0, len(updates))
	for _, update := range updates {
		if len(update.Key) != common.HashLength {
			return fmt.Errorf("invalid storage slot length %d", len(update.Key))
		}
		deleteValue := update.Delete || len(update.Value) == 0
		item := archivetrie.StemUpdate{
			Key:    trieutils.BinaryTreeStorageSlotKey(addr, update.Key),
			Delete: deleteValue,
		}
		if !deleteValue {
			item.Value = encodeArchiveStemStorageValue(addr, update.Key, update.Value)
		}
		stemUpdates = append(stemUpdates, item)
	}
	if err := t.stem.ApplyBatch(stemUpdates); err != nil {
		return err
	}
	for _, update := range updates {
		deleteValue := update.Delete || len(update.Value) == 0
		t.stageArchiveIndex(archiveStemStorageIndexKey(addr, update.Key), !deleteValue)
	}
	return nil
}

// DeleteAccount 删除账户键。
func (t *ArchiveTrie) DeleteAccount(address common.Address) error {
	if t.stem != nil {
		return t.stem.Delete(trieutils.BinaryTreeBasicDataKey(address))
	}
	return t.trie.BatchDelete(address.Bytes())
}

// DeleteStorage 删除指定账户的 storage slot。
func (t *ArchiveTrie) DeleteStorage(addr common.Address, key []byte) error {
	if t.stem != nil {
		return t.UpdateStorageBatch(addr, []StorageUpdate{{Key: common.CopyBytes(key), Delete: true}})
	}
	compositeKey := make([]byte, 20+len(key))
	copy(compositeKey, addr.Bytes())
	compositeKey[0] ^= 0x01
	copy(compositeKey[20:], key)
	return t.trie.BatchDelete(compositeKey)
}

func (t *ArchiveTrie) archiveStemCodeChunks(addr common.Address) ([]uint64, error) {
	prefix := archiveStemCodeIndexAccountPrefix(addr)
	keys, err := t.listArchiveIndexKeys(prefix)
	if err != nil {
		return nil, err
	}
	seen := make(map[uint64]struct{}, len(keys))
	for _, key := range keys {
		if len(key) != len(prefix)+8 || !bytes.HasPrefix(key, prefix) {
			return nil, errors.New("invalid archive stem code index key")
		}
		seen[binary.BigEndian.Uint64(key[len(prefix):])] = struct{}{}
	}
	chunks := make([]uint64, 0, len(seen))
	for chunk := range seen {
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

// WipeAccountState removes all storage slots and address-owned code chunks of
// one account from the unified ASCT. Stem mode uses the persisted address
// index, so the work is proportional to this account rather than the global
// trie. The returned storage values are used by StateDB's snapshot/history
// bookkeeping.
func (t *ArchiveTrie) WipeAccountState(address common.Address) (AccountStateWipeResult, error) {
	result := AccountStateWipeResult{Address: address}
	if t == nil || t.trie == nil {
		return result, errors.New("nil archive trie")
	}
	if t.stem == nil {
		prefix := make([]byte, common.AddressLength)
		copy(prefix, address[:])
		prefix[0] ^= 0x01
		var keys [][]byte
		t.trie.ForEachPrefix(prefix, len(prefix)*8, func(key, value []byte) bool {
			if len(key) == common.AddressLength+common.HashLength {
				keys = append(keys, common.CopyBytes(key))
				result.Storage = append(result.Storage, StorageWipeItem{
					Key:   common.CopyBytes(key[common.AddressLength:]),
					Value: common.CopyBytes(value),
				})
			}
			return true
		})
		for _, key := range keys {
			if err := t.trie.BatchDelete(key); err != nil {
				return AccountStateWipeResult{Address: address}, err
			}
		}
		return result, nil
	}
	ready, err := t.archiveStemIndexReady()
	if err != nil {
		return result, fmt.Errorf("check archive stem account index: %w", err)
	}
	if !ready {
		return result, errors.New("archive stem account index is not initialized; start with a fresh database")
	}
	indexScanStart := time.Now()
	storagePrefix := archiveStemStorageIndexAccountPrefix(address)
	storageKeys, err := t.listArchiveIndexKeys(storagePrefix)
	if err != nil {
		return result, err
	}
	codeChunks, err := t.archiveStemCodeChunks(address)
	if err != nil {
		return AccountStateWipeResult{Address: address}, err
	}
	result.IndexScanNanos = time.Since(indexScanStart).Nanoseconds()

	treeKeys := make([][]byte, 0, len(storageKeys)+len(codeChunks))
	slots := make([][]byte, 0, len(storageKeys))
	for _, indexKey := range storageKeys {
		if len(indexKey) != len(storagePrefix)+common.HashLength || !bytes.HasPrefix(indexKey, storagePrefix) {
			return AccountStateWipeResult{Address: address}, errors.New("invalid archive stem storage index key")
		}
		slot := common.CopyBytes(indexKey[len(storagePrefix):])
		slots = append(slots, slot)
		treeKeys = append(treeKeys, trieutils.BinaryTreeStorageSlotKey(address, slot))
	}
	for _, chunk := range codeChunks {
		treeKeys = append(treeKeys, trieutils.BinaryTreeCodeChunkKey(address, chunk))
	}
	stemDeleteStart := time.Now()
	deleted, err := t.stem.DeleteBatchWithValues(treeKeys)
	result.StemDeleteNanos = time.Since(stemDeleteStart).Nanoseconds()
	if err != nil {
		return AccountStateWipeResult{Address: address}, fmt.Errorf("delete indexed account state: %w", err)
	}
	result.Storage = make([]StorageWipeItem, 0, len(slots))
	for i, slot := range slots {
		value, err := decodeArchiveStemStorageValue(address, slot, deleted.Values[i])
		if err != nil {
			return AccountStateWipeResult{Address: address}, err
		}
		result.Storage = append(result.Storage, StorageWipeItem{Key: slot, Value: value})
	}
	indexStageStart := time.Now()
	t.stageArchiveIndexes(storageKeys, false)
	codeIndexKeys := make([][]byte, 0, len(codeChunks))
	for _, chunk := range codeChunks {
		codeIndexKeys = append(codeIndexKeys, archiveStemCodeIndexKey(address, chunk))
	}
	t.stageArchiveIndexes(codeIndexKeys, false)
	result.IndexStageNanos = time.Since(indexStageStart).Nanoseconds()
	result.CodeChunks = len(codeChunks)
	result.StemRecords = deleted.StemCount
	return result, nil
}

// UpdateContractCode 使用 code hash 作为 code 域键。
// 首字节 xor 0x02 是 code 域分隔，避免与账户和 storage 键冲突。
func (t *ArchiveTrie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	if t.stem != nil {
		chunks := trieutils.ChunkifyBinaryCode(code)
		oldChunks, err := t.archiveStemCodeChunks(address)
		if err != nil {
			return err
		}
		updates := make([]archivetrie.StemUpdate, 0, len(oldChunks)+len(chunks)/common.HashLength)
		for _, chunk := range oldChunks {
			updates = append(updates, archivetrie.StemUpdate{Key: trieutils.BinaryTreeCodeChunkKey(address, chunk), Delete: true})
		}
		for offset, chunk := 0, uint64(0); offset < len(chunks); offset, chunk = offset+common.HashLength, chunk+1 {
			updates = append(updates, archivetrie.StemUpdate{
				Key:   trieutils.BinaryTreeCodeChunkKey(address, chunk),
				Value: common.CopyBytes(chunks[offset : offset+common.HashLength]),
			})
		}
		if err := t.stem.ApplyBatch(updates); err != nil {
			return err
		}
		for _, chunk := range oldChunks {
			t.stageArchiveIndex(archiveStemCodeIndexKey(address, chunk), false)
		}
		for chunk := uint64(0); chunk < uint64(len(chunks)/common.HashLength); chunk++ {
			t.stageArchiveIndex(archiveStemCodeIndexKey(address, chunk), true)
		}
		return nil
	}
	key := codeHash.Bytes()
	key[0] ^= 0x02 // XOR domain 2 for code
	return t.trie.Put(key, code)
}

// Hash 计算当前下层 trie root，不触发 commit。
func (t *ArchiveTrie) Hash() common.Hash {
	h, _ := t.trie.Hash()
	return common.BytesToHash(h)
}

// Commit 提交 ArchiveTrie 的所有 dirty shard 并更新 triedb。
// 返回的 NodeSet 为 nil，因为 wrapper 已经在内部完成 merged NodeSet 和 raw batch 的原子更新。
func (t *ArchiveTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet) {
	commitStart := time.Now()
	block := t.currentBlockNum()
	var stopWatchdog func()
	if t.trie != nil && t.trie.Config() != nil && t.trie.Config().CommitWatchdogSeconds > 0 {
		done := make(chan struct{})
		threshold := time.Duration(t.trie.Config().CommitWatchdogSeconds) * time.Second
		go func() {
			timer := time.NewTimer(threshold)
			defer timer.Stop()
			select {
			case <-timer.C:
				fmt.Printf("[ASCT_COMMIT_WATCHDOG] block=%d threshold=%v elapsed=%v\n", block, threshold, time.Since(commitStart))
				if p := pprof.Lookup("goroutine"); p != nil {
					_ = p.WriteTo(os.Stdout, 2)
				}
			case <-done:
			}
		}()
		stopWatchdog = func() { close(done) }
		defer stopWatchdog()
	}
	// Shard-aware commit: persist changed subtrees, hash their binary paths to
	// the root, then update triedb once.
	if adapter, ok := t.trie.Database().(*archiveDBAdapter); ok {
		if err := t.trie.FinishAsyncPrune(); err != nil {
			return common.Hash{}, nil
		}
		var rawBatch ethdb.Batch
		if disk := adapter.diskDB(); disk != nil {
			rawBatch = disk.NewBatch()
			defer rawBatch.Reset()
		}
		indexOps := t.snapshotArchiveIndexOps()
		// 1. 为多 owner 的原子更新准备 MergedNodeSet。
		merged := trienode.NewMergedNodeSet()
		dirtyShards := t.trie.GetDirtyShards()
		shardRoots := make(map[int][]byte, len(dirtyShards))

		// 2. 并行提交互不依赖的 dirty shards。
		// 每个 worker 本地收集 NodeSet 和 raw 写入，主 goroutine 再按结果统一合并。
		shardCommitStart := time.Now()
		type shardCommitResult struct {
			id        int
			root      []byte
			nodes     *trienode.NodeSet
			raw       *rawBatchBuffer
			diag      archivetrie.ShardCommitDiagnostics
			commitDur time.Duration
			err       error
		}
		workers := 1
		if t.trie.Config().UsePathStorage() {
			workers = runtime.GOMAXPROCS(0)
			if t.trie.Config().CommitWorkers > 0 && t.trie.Config().CommitWorkers < workers {
				workers = t.trie.Config().CommitWorkers
			}
			if workers > len(dirtyShards) {
				workers = len(dirtyShards)
			}
		}
		if workers < 0 {
			workers = 0
		}
		resultBuffer := workers
		if resultBuffer < 1 {
			resultBuffer = 1
		}
		resultCh := make(chan shardCommitResult, resultBuffer)
		if workers > 0 {
			jobs := make(chan int, len(dirtyShards))
			var wg sync.WaitGroup
			for worker := 0; worker < workers; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for index := range jobs {
						id := dirtyShards[index]
						nodes := trienode.NewNodeSet(common.Hash{})
						raw := new(rawBatchBuffer)
						batch := &nodeSetBatcher{adapter: adapter, nodes: nodes, rawBatch: raw}
						shardStart := time.Now()
						// 使用 destructive commit：提交后 shard 只保留 rootHash，不再把整棵
						// 内存 root 常驻在 activeBT 中。否则主网长回放会把碰过的 shard 子树
						// 全部留在堆里，看起来就像内存泄漏。
						root, diag, err := t.trie.CommitShardToBatchWithDiagnostics(id, batch, true)
						resultCh <- shardCommitResult{id: id, root: root, nodes: nodes, raw: raw, diag: diag, commitDur: time.Since(shardStart), err: err}
					}
				}()
			}
			for index := range dirtyShards {
				jobs <- index
			}
			close(jobs)
			go func() {
				wg.Wait()
				close(resultCh)
			}()
		} else {
			close(resultCh)
		}
		maxShardID := 0
		var maxShardDiag archivetrie.ShardCommitDiagnostics
		var rawTotalOps, rawTotalBytes int64
		var rawMaxShardID int64
		var rawMaxOps, rawMaxBytes int64
		var firstErr error
		for result := range resultCh {
			id := result.id
			if result.err != nil {
				if firstErr == nil {
					firstErr = result.err
				}
				continue
			}
			if firstErr != nil {
				continue
			}
			if len(result.root) > 0 {
				shardRoots[id] = result.root
			} else {
				shardRoots[id] = make([]byte, common.HashLength)
			}
			result.nodes.Owner = common.BytesToHash(shardRoots[id])
			if len(result.nodes.Nodes) > 0 {
				if err := merged.Merge(result.nodes); err != nil {
					firstErr = err
					continue
				}
			}
			if result.diag.TotalNanos > maxShardDiag.TotalNanos {
				maxShardID = id
				maxShardDiag = result.diag
			}
			rawOps, rawBytes := result.raw.stats()
			rawTotalOps += int64(rawOps)
			rawTotalBytes += int64(rawBytes)
			if int64(rawBytes) > rawMaxBytes {
				rawMaxShardID = int64(id)
				rawMaxOps = int64(rawOps)
				rawMaxBytes = int64(rawBytes)
			}
			if rawBatch != nil {
				replayStart := time.Now()
				if err := result.raw.replay(rawBatch); err != nil {
					firstErr = err
					continue
				}
				if result.commitDur > 500*time.Millisecond || time.Since(replayStart) > 500*time.Millisecond {
					fmt.Printf("[ASCT_WRAPPER_SHARD_DIAG] block=%d shard=%d commit=%v replay=%v nodes=%d rawOps=%d rawBytes=%d %s\n",
						block, id, result.commitDur, time.Since(replayStart), len(result.nodes.Nodes), rawOps, rawBytes, result.diag.String())
				}
			}
		}
		if firstErr != nil {
			return common.Hash{}, nil
		}
		shardCommitDuration := time.Since(shardCommitStart)
		archivetrie.RecordWrapperShardCommitMax(maxShardID, maxShardDiag)

		// 3. Persist the binary branches from changed shards to the global root.
		rootNodes := trienode.NewNodeSet(common.Hash{})
		rootBatch := &nodeSetBatcher{adapter: adapter, nodes: rootNodes, rawBatch: rawBatch}
		rootHashStart := time.Now()
		h, err := t.trie.CommitRootToBatch(shardRoots, dirtyShards, rootBatch)
		if err != nil {
			return common.Hash{}, nil
		}
		rootHashDuration := time.Since(rootHashStart)
		root := common.BytesToHash(h)
		if len(rootNodes.Nodes) > 0 {
			if err := merged.Merge(rootNodes); err != nil {
				return common.Hash{}, nil
			}
		}

		// 4. 合并 adapter 暂存的节点和值，补齐 commit 期间绕过 shard batcher 的写入。
		adapterMergeStart := time.Now()
		adapter.mu.Lock()
		if adapter.pendingNodes != nil && len(adapter.pendingNodes.Nodes) > 0 {
			if err := merged.Merge(adapter.pendingNodes); err != nil {
				adapter.mu.Unlock()
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
				return common.Hash{}, nil
			}
			adapter.values = nil
		}
		adapter.mu.Unlock()
		adapterMergeDuration := time.Since(adapterMergeStart)

		// 5. 通过 triedb.Update 原子登记新 root 和 merged NodeSet。
		var updateDuration time.Duration
		type updater interface {
			Update(common.Hash, common.Hash, uint64, *trienode.MergedNodeSet, interface{}) error
		}
		if u, ok := t.db.(updater); ok {
			updateStart := time.Now()
			if err := u.Update(root, t.originRoot, block, merged, nil); err != nil {
				return common.Hash{}, nil
			}
			updateDuration = time.Since(updateStart)
		}
		var batchWriteDuration time.Duration
		var batchOps, batchBytes int
		if rawBatch != nil {
			if err := applyArchiveIndexOps(rawBatch, indexOps); err != nil {
				return common.Hash{}, nil
			}
			batchOps, batchBytes = rawBatchStats(rawBatch)
			batchWriteStart := time.Now()
			if err := rawBatch.Write(); err != nil {
				return common.Hash{}, nil
			}
			batchWriteDuration = time.Since(batchWriteStart)
			t.clearArchiveIndexOps(indexOps)
		} else if len(indexOps) > 0 {
			batch := t.trie.Database().NewBatch()
			if err := applyArchiveIndexOps(batch, indexOps); err != nil {
				return common.Hash{}, nil
			}
			if err := batch.Write(); err != nil {
				return common.Hash{}, nil
			}
			batch.Reset()
			t.clearArchiveIndexOps(indexOps)
		}

		// 6. 保存 active trie，下一块继续复用。
		type trieSetter interface {
			SetArchiveTrie(interface{})
		}
		if ts, ok := t.db.(trieSetter); ok {
			ts.SetArchiveTrie(t.trie)
		}

		t.originRoot = root
		totalDuration := time.Since(commitStart)
		archivetrie.RecordWrapperCommitDiagnostics(totalDuration, shardCommitDuration, rootHashDuration, adapterMergeDuration, updateDuration, batchWriteDuration, len(dirtyShards))
		// 每次 commit 都记录缓存体量，主 CSV 可以持续观察 NodeCache_MB 是否顶到上限。
		// 慢提交时下面还会额外记录完整 runtime.MemStats。
		cacheDiag := t.trie.NodeCacheDiagnostics()
		globalCacheDiag := archiveNodeCacheDiagnostics()
		cacheDiag.Entries += globalCacheDiag.Entries
		cacheDiag.Bytes += globalCacheDiag.Bytes
		cacheDiag.EntryLimit += globalCacheDiag.EntryLimit
		cacheDiag.BytesLimit += globalCacheDiag.BytesLimit
		cacheDiag.Shards += globalCacheDiag.Shards
		cacheDiag.Hits += globalCacheDiag.Hits
		cacheDiag.Misses += globalCacheDiag.Misses
		cacheDiag.Evictions += globalCacheDiag.Evictions
		cacheDiag.LockContentions += globalCacheDiag.LockContentions
		cacheDiag.LockWaitNanos += globalCacheDiag.LockWaitNanos
		cacheDiag.DBGets += globalCacheDiag.DBGets
		cacheDiag.DBGetNanos += globalCacheDiag.DBGetNanos
		cacheDiag.DBLoadBytes += globalCacheDiag.DBLoadBytes
		nodeCacheEntries := cacheDiag.Entries
		nodeCacheBytes := cacheDiag.Bytes
		archivetrie.RecordNodeCacheDiagnostics(cacheDiag)
		archivetrie.RecordWrapperResourceDiagnostics(rawTotalOps, rawTotalBytes, rawMaxShardID, rawMaxOps, rawMaxBytes, nodeCacheEntries, nodeCacheBytes, 0, 0, 0, 0, 0, 0, 0)
		if totalDuration > 5*time.Second || shardCommitDuration > 5*time.Second || rootHashDuration > 5*time.Second || batchWriteDuration > 5*time.Second {
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			var lastPause uint64
			if mem.NumGC > 0 {
				lastPause = mem.PauseNs[(mem.NumGC+255)%256]
			}
			archivetrie.RecordWrapperResourceDiagnostics(rawTotalOps, rawTotalBytes, rawMaxShardID, rawMaxOps, rawMaxBytes, nodeCacheEntries, nodeCacheBytes, mem.HeapAlloc, mem.HeapSys, mem.HeapInuse, mem.Sys, uint64(mem.NumGC), mem.PauseTotalNs, lastPause)
			fmt.Printf("[ASCT_WRAPPER_COMMIT_DIAG] block=%d total=%v shardCommit=%v rootHash=%v adapterMerge=%v trieDBUpdate=%v batchWrite=%v dirtyShards=%d mergedOwners=%d rawBatchOps=%d rawBatchBytes=%d\n",
				block, totalDuration, shardCommitDuration, rootHashDuration, adapterMergeDuration, updateDuration, batchWriteDuration, len(dirtyShards), len(merged.Sets), batchOps, batchBytes)
		}
		// NodeSet 已经在内部写入 triedb，这里返回 nil 避免上层重复处理。
		return root, nil
	}

	fallbackStart := time.Now()
	batch := t.trie.Database().NewBatch()
	defer batch.Reset()
	h, err := t.trie.CommitToBatch(batch, true)
	if err != nil {
		return common.Hash{}, nil
	}
	indexOps := t.snapshotArchiveIndexOps()
	if err := applyArchiveIndexOps(batch, indexOps); err != nil {
		return common.Hash{}, nil
	}
	if err := batch.Write(); err != nil {
		return common.Hash{}, nil
	}
	t.clearArchiveIndexOps(indexOps)
	archivetrie.RecordWrapperCommitDiagnostics(time.Since(commitStart), time.Since(fallbackStart), 0, 0, 0, 0, 0)
	return common.BytesToHash(h), nil
}

// Witness 当前未实现，返回 nil 保持 state.Trie 接口兼容。
func (t *ArchiveTrie) Witness() map[string]struct{} { return nil }

// NodeIterator 返回 leaf-only iterator，主要服务 storage wiping 和调试遍历。
func (t *ArchiveTrie) NodeIterator(startKey []byte) (NodeIterator, error) {
	// For now, we only support a simple leaf-only iterator if startKey is nil.
	// This is primarily for debugging or tools that need to dump the trie.
	if t.stem != nil {
		return newArchiveStemStorageIterator(common.Address{}, t.stem, true), nil
	}
	return newArchiveStorageIterator(common.Address{}, t.trie, true), nil
}

// Prove 暂未实现；ArchiveTrie 当前实验路径不依赖 Merkle proof。
func (t *ArchiveTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return errors.New("not implemented")
}

// IsVerkle 明确声明该实现不是 Verkle trie。
func (t *ArchiveTrie) IsVerkle() bool { return false }

// PruneNextShard 触发下一分片裁剪，并把耗时写入全局实验统计。
func (t *ArchiveTrie) PruneNextShard() error {
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

// ArchiveStorageTrie 是单个账户 storage 的 state.Trie 视图。
// 它不持有独立树，所有读写都会加上账户前缀后代理到父 ArchiveTrie。
type ArchiveStorageTrie struct {
	address common.Address
	bt      *ArchiveTrie
}

// NewArchiveStorageTrie 创建指定账户的 storage 视图。
func NewArchiveStorageTrie(address common.Address, bt *ArchiveTrie) *ArchiveStorageTrie {
	return &ArchiveStorageTrie{address: address, bt: bt}
}

// GetKey 对 storage 视图没有额外映射需求。
func (s *ArchiveStorageTrie) GetKey(key []byte) []byte { return nil }

// GetAccount 不属于 storage 子 trie 能力范围。
func (s *ArchiveStorageTrie) GetAccount(address common.Address) (*types.StateAccount, error) {
	return nil, errors.New("not supported")
}

// GetStorage 忽略传入地址，始终读取构造时绑定账户的 storage。
func (s *ArchiveStorageTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	return s.bt.GetStorage(s.address, key)
}

// UpdateAccount 不属于 storage 子 trie 能力范围。
func (s *ArchiveStorageTrie) UpdateAccount(address common.Address, acc *types.StateAccount, codeLen int) error {
	return errors.New("not supported")
}

// UpdateAccountRLP 不属于 storage 子 trie 能力范围。
func (s *ArchiveStorageTrie) UpdateAccountRLP(address common.Address, account []byte, codeLen int) error {
	return errors.New("not supported")
}

// UpdateStorage 写入构造时绑定账户的 storage slot。
func (s *ArchiveStorageTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	return s.bt.UpdateStorage(s.address, key, value)
}

// UpdateStorageBatch applies one state object's storage changes by stem.
func (s *ArchiveStorageTrie) UpdateStorageBatch(addr common.Address, updates []StorageUpdate) error {
	return s.bt.UpdateStorageBatch(s.address, updates)
}

// DeleteAccount 不属于 storage 子 trie 能力范围。
func (s *ArchiveStorageTrie) DeleteAccount(address common.Address) error {
	return errors.New("not supported")
}

// DeleteStorage 删除构造时绑定账户的 storage slot。
func (s *ArchiveStorageTrie) DeleteStorage(addr common.Address, key []byte) error {
	return s.bt.DeleteStorage(s.address, key)
}

// UpdateContractCode 不属于 storage 子 trie 能力范围。
func (s *ArchiveStorageTrie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return errors.New("not supported")
}

// Hash 对 storage 视图返回空 hash；真实 root 由父 ArchiveTrie 统一计算。
func (s *ArchiveStorageTrie) Hash() common.Hash {
	return common.Hash{}
}

// Commit 对 storage 视图不执行实际提交；父 ArchiveTrie 负责统一 commit。
func (s *ArchiveStorageTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet) {
	return common.Hash{}, nil
}

// Witness 当前未实现，返回 nil 保持接口兼容。
func (s *ArchiveStorageTrie) Witness() map[string]struct{} { return nil }

// NodeIterator 只遍历当前账户 storage 前缀下的叶子。
func (s *ArchiveStorageTrie) NodeIterator(startKey []byte) (NodeIterator, error) {
	if s.bt.stem != nil {
		return newArchiveStemStorageIterator(s.address, s.bt.stem, false), nil
	}
	return newArchiveStorageIterator(s.address, s.bt.trie, false), nil
}

// Prove 暂未实现；实验路径不依赖 storage proof。
func (s *ArchiveStorageTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return errors.New("not implemented")
}

// IsVerkle 明确声明 storage 视图不是 Verkle trie。
func (s *ArchiveStorageTrie) IsVerkle() bool { return false }

// PruneNextShard 仍委托给父 ArchiveTrie，因为裁剪发生在全局归档树上。
func (s *ArchiveStorageTrie) PruneNextShard() error { return s.bt.PruneNextShard() }

// archiveStorageIterator 为 storage wiping 提供 leaf-only NodeIterator。
// 它在构造时一次性收集匹配叶子，Next 只移动游标。
type archiveStorageIterator struct {
	prefix []byte
	leaves []leafKV
	index  int
	err    error
}

// leafKV 保存 iterator 需要暴露的叶子键值。
type leafKV struct {
	key   []byte
	value []byte
}

// newArchiveStorageIterator 创建全局或单账户 storage 迭代器。
// isGlobal=true 时遍历所有叶子；否则只遍历指定账户的 storage 域前缀。
func newArchiveStorageIterator(address common.Address, bt *archivetrie.Trie, isGlobal bool) *archiveStorageIterator {
	var prefix []byte
	if !isGlobal {
		prefix = make([]byte, 20)
		copy(prefix, address.Bytes())
		prefix[0] ^= 0x01
	}
	it := &archiveStorageIterator{
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

func newArchiveStemStorageIterator(address common.Address, stem *archivetrie.StemTrie, isGlobal bool) *archiveStorageIterator {
	it := &archiveStorageIterator{index: -1}
	if stem == nil {
		return it
	}
	it.err = stem.ForEach(func(treeKey, encoded []byte) bool {
		if isGlobal {
			value := encoded
			if _, _, storageValue, ok := decodeArchiveStemStorageRecord(encoded); ok {
				value = storageValue
			}
			it.leaves = append(it.leaves, leafKV{key: common.CopyBytes(treeKey), value: common.CopyBytes(value)})
			return true
		}
		recordAddr, slot, value, ok := decodeArchiveStemStorageRecord(encoded)
		if ok && recordAddr == address {
			it.leaves = append(it.leaves, leafKV{key: slot.Bytes(), value: value})
		}
		return true
	})
	return it
}

// Next 移动到下一片已收集叶子。
func (it *archiveStorageIterator) Next(bool) bool {
	it.index++
	return it.index < len(it.leaves)
}

// Error 返回构造或遍历过程中的错误；当前实现会把错误保存在 it.err。
func (it *archiveStorageIterator) Error() error      { return it.err }
func (it *archiveStorageIterator) Hash() common.Hash { return common.Hash{} }

// Parent 不暴露内部节点关系，返回空 hash。
func (it *archiveStorageIterator) Parent() common.Hash { return common.Hash{} }

// Path 不暴露内部路径，返回 nil。
func (it *archiveStorageIterator) Path() []byte { return nil }

// NodeBlob 返回 nil，因为该 iterator 只暴露 leaf，不暴露内部节点 blob。
func (it *archiveStorageIterator) NodeBlob() []byte { return nil }
func (it *archiveStorageIterator) Leaf() bool       { return true }

// LeafKey 返回当前叶子的 storage key；单账户遍历时已去掉账户前缀。
func (it *archiveStorageIterator) LeafKey() []byte { return it.leaves[it.index].key }

// LeafBlob 返回当前叶子的 value。
func (it *archiveStorageIterator) LeafBlob() []byte { return it.leaves[it.index].value }

// LeafProof 当前未提供，返回 nil。
func (it *archiveStorageIterator) LeafProof() [][]byte { return nil }

// AddResolver 当前 iterator 不需要外部 resolver。
func (it *archiveStorageIterator) AddResolver(resolver NodeResolver) {}
