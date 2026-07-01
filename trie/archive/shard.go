package archive

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie/archive/cuckoo"
	"github.com/ethereum/go-ethereum/trie/archive/ecmh"
)

// Shard 表示 ASCT 中一个 key 前缀分片。
//
// 它是加锁、剪枝、懒加载和提交的基本单位。destructive commit 后可以卸载 live root，
// 后续访问再用 rootHash 从数据库懒加载回来。
type Shard struct {
	mu         sync.RWMutex // Protects root, rootHash, and staleSet
	root       Node
	db         KVStore
	hasher     Hasher
	pruning    bool
	staleSet   map[string]struct{} // Set of hashes as string keys (for pruning)
	rootHash   []byte              // Last persisted root hash for lazy loading
	nodePaths  map[string]persistedNodePath
	nodeCache  *nodeBlobCache
	pointCache *commitmentPointCache

	// Shard specific state
	id       int
	isPruned bool // "本年度已剪枝 / 未剪枝"

	// Reference to global config
	config         *Config
	globalEpochBit func() byte

	// scratch buffer for temporary bit operations
	scratch []byte

	// Internal state for ECMH and Filter operations
	ecmh           *ecmh.Committer
	stats          *TrieStats
	statsMut       sync.Mutex
	lastCommitDiag ShardCommitDiagnostics

	// Flat value writes are staged until commit. The tree stores only valueRef.
	pendingFlatValues       map[string][]byte
	pendingFlatValueDeletes map[string]struct{}
	stagedFlatValues        []map[string][]byte

	// Node pool for reusing internal and leaf nodes
	pool *NodePool
}

// NewShard 创建一个新的 Shard（若提供 rootHash 则从 DB 加载根节点）。
func NewShard(id int, db KVStore, hasher Hasher, config *Config, rootHash []byte, pruning bool, globalEpochBit func() byte) (*Shard, error) {
	return newShard(id, db, hasher, config, rootHash, pruning, globalEpochBit, newNodeBlobCacheWithBytesLimit(configNodeCacheLimit(config), configNodeCacheBytesLimit(config)), newCommitmentPointCache(configCommitmentPointCacheLimit(config)))
}

func newShard(id int, db KVStore, hasher Hasher, config *Config, rootHash []byte, pruning bool, globalEpochBit func() byte, nodeCache *nodeBlobCache, pointCache *commitmentPointCache) (*Shard, error) {
	s := &Shard{
		id:                      id,
		db:                      db,
		hasher:                  hasher,
		pruning:                 pruning,
		staleSet:                make(map[string]struct{}),
		nodePaths:               make(map[string]persistedNodePath),
		nodeCache:               nodeCache,
		pointCache:              pointCache,
		config:                  config,
		globalEpochBit:          globalEpochBit,
		isPruned:                false,
		scratch:                 make([]byte, 128),
		ecmh:                    ecmh.New(),
		stats:                   &TrieStats{},
		pendingFlatValues:       make(map[string][]byte),
		pendingFlatValueDeletes: make(map[string]struct{}),
		pool:                    NewNodePool(),
	}
	if len(rootHash) > 0 {
		s.registerNodePath(rootHash, nil, 0)
		node, err := s.loadNode(rootHash)
		if err != nil {
			return nil, err
		}
		s.root = node
	}
	return s, nil
}

func configNodeCacheLimit(config *Config) int {
	if config == nil {
		return DefaultNodeCacheLimit
	}
	return config.NodeCacheLimit
}

func configNodeCacheBytesLimit(config *Config) int64 {
	if config == nil {
		return DefaultNodeCacheBytesLimit
	}
	return config.NodeCacheBytesLimit
}

func configCommitmentPointCacheLimit(config *Config) int {
	if config == nil {
		return DefaultCommitmentPointCacheLimit
	}
	return config.CommitmentPointCacheLimit
}

// ClearCaches 清除分片内存中的缓存数据。
func (s *Shard) ClearCaches() {
	if s.root != nil {
		if in, ok := s.root.(*InternalNode); ok {
			in.ClearCaches()
		}
	}
	s.staleSet = make(map[string]struct{})
}

// Reset updates the shard's root hash and clears memory root to force lazy loading.
// If the memory root already matches the shardRoot, we keep it to preserve hot nodes.
func (s *Shard) Reset(shardRoot []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.root != nil && bytes.Equal(s.root.Hash(), shardRoot) {
		return
	}
	s.rootHash = shardRoot
	s.root = nil
	s.nodePaths = make(map[string]persistedNodePath)
	s.registerNodePath(shardRoot, nil, 0)
	s.pendingFlatValues = make(map[string][]byte)
	s.pendingFlatValueDeletes = make(map[string]struct{})
	s.stagedFlatValues = nil
}

// loadNode 根据哈希从 DB 读取并反序列化节点。
func (s *Shard) loadNode(hash []byte) (Node, error) {
	if len(hash) == 0 {
		return nil, nil
	}
	storageKey := hash
	var persisted persistedNodePath
	if s.config != nil && s.config.UsePathStorage() {
		var ok bool
		persisted, ok = s.nodePaths[string(hash)]
		if !ok {
			return nil, fmt.Errorf("node path not found: hash %x in shard %d", hash, s.id)
		}
		storageKey = pathNodeKey(s.id, persisted.path, persisted.bits)
	}
	data, ok := s.getCachedNodeBlob(storageKey)
	if !ok {
		var err error
		data, err = s.db.Get(storageKey)
		recordPathDBGetIfEnabled(s.config)
		if err != nil {
			if s.config != nil && s.config.UsePathStorage() {
				return nil, fmt.Errorf("load node at shard %d path %x/%d hash %x: %w", s.id, persisted.path, persisted.bits, hash, err)
			}
			return nil, fmt.Errorf("load node at shard %d hash %x: %w", s.id, hash, err)
		}
		s.cacheNodeBlob(storageKey, data)
	}
	if data == nil {
		return nil, fmt.Errorf("node not found: hash %x in shard %d", hash, s.id)
	}
	node, err := DeserializeNode(data)
	if err != nil {
		return nil, err
	}
	if s.config != nil && s.config.UsePathStorage() && !bytes.Equal(s.hasher.Hash(data), hash) {
		if ok {
			s.removeCachedNodeBlob(storageKey)
			data, err = s.db.Get(storageKey)
			recordPathDBGetIfEnabled(s.config)
			if err != nil {
				return nil, fmt.Errorf("load node at shard %d path %x/%d hash %x: %w", s.id, persisted.path, persisted.bits, hash, err)
			}
			if data == nil {
				return nil, fmt.Errorf("node not found: hash %x in shard %d", hash, s.id)
			}
			s.cacheNodeBlob(storageKey, data)
			node, err = DeserializeNode(data)
			if err != nil {
				return nil, err
			}
		}
		if !bytes.Equal(s.hasher.Hash(data), hash) {
			return nil, fmt.Errorf("node hash mismatch at path %x/%d in shard %d", persisted.path, persisted.bits, s.id)
		}
	}
	node.SetHash(hash)
	node.SetOriginalHash(hash) // 记录节点当前持久化版本，用于之后修剪
	if s.config != nil && s.config.UsePathStorage() {
		node.SetStoragePath(persisted.path, persisted.bits)
		s.registerChildPaths(node, persisted.path, persisted.bits)
	}
	return node, nil
}

func (s *Shard) loadNodeAtPath(hash, path []byte, bits int) (Node, error) {
	if len(hash) == 0 {
		return nil, nil
	}
	storageKey := hash
	if s.config != nil && s.config.UsePathStorage() {
		storageKey = pathNodeKey(s.id, path, bits)
	}
	data, ok := s.getCachedNodeBlob(storageKey)
	if !ok {
		var err error
		data, err = s.db.Get(storageKey)
		recordPathDBGetIfEnabled(s.config)
		if err != nil {
			if s.config != nil && s.config.UsePathStorage() {
				return nil, fmt.Errorf("load node at shard %d path %x/%d hash %x: %w", s.id, path, bits, hash, err)
			}
			return nil, fmt.Errorf("load node at shard %d hash %x: %w", s.id, hash, err)
		}
		s.cacheNodeBlob(storageKey, data)
	}
	if data == nil {
		return nil, fmt.Errorf("node not found: hash %x in shard %d", hash, s.id)
	}
	node, err := DeserializeNode(data)
	if err != nil {
		return nil, err
	}
	if s.config != nil && s.config.UsePathStorage() && !bytes.Equal(s.hasher.Hash(data), hash) {
		if ok {
			s.removeCachedNodeBlob(storageKey)
			data, err = s.db.Get(storageKey)
			recordPathDBGetIfEnabled(s.config)
			if err != nil {
				return nil, fmt.Errorf("load node at shard %d path %x/%d hash %x: %w", s.id, path, bits, hash, err)
			}
			if data == nil {
				return nil, fmt.Errorf("node not found: hash %x in shard %d", hash, s.id)
			}
			s.cacheNodeBlob(storageKey, data)
			node, err = DeserializeNode(data)
			if err != nil {
				return nil, err
			}
		}
		if !bytes.Equal(s.hasher.Hash(data), hash) {
			return nil, fmt.Errorf("node hash mismatch at path %x/%d in shard %d", path, bits, s.id)
		}
	}
	node.SetHash(hash)
	node.SetOriginalHash(hash)
	if s.config != nil && s.config.UsePathStorage() {
		node.SetStoragePath(path, bits)
	}
	return node, nil
}

func (s *Shard) loadChildNode(parent *InternalNode, bit byte, hash []byte) (Node, error) {
	if len(hash) == 0 {
		return nil, nil
	}
	if s.config == nil || !s.config.UsePathStorage() {
		return s.loadNode(hash)
	}
	path, bits := s.childNodeStoragePath(parent, bit)
	if persisted, ok := s.nodePaths[string(hash)]; ok {
		path, bits = persisted.path, persisted.bits
	}
	node, err := s.loadNodeAtPath(hash, path, bits)
	if err != nil {
		return nil, err
	}
	s.registerChildPaths(node, path, bits)
	return node, nil
}

func (s *Shard) prefetchNodeAtPath(hash, path []byte, bits int, visited *int, limit int) {
	if len(hash) == 0 || visited == nil || (limit > 0 && *visited >= limit) {
		return
	}
	node, err := s.loadNodeAtPath(hash, path, bits)
	if err != nil || node == nil {
		return
	}
	*visited++
	n, ok := node.(*InternalNode)
	if !ok {
		return
	}
	if len(n.LeftHash) > 0 {
		childPath, childBits := s.childStoragePath(path, bits, n, 0)
		s.prefetchNodeAtPath(n.LeftHash, childPath, childBits, visited, limit)
	}
	if len(n.RightHash) > 0 {
		childPath, childBits := s.childStoragePath(path, bits, n, 1)
		s.prefetchNodeAtPath(n.RightHash, childPath, childBits, visited, limit)
	}
}

func (s *Shard) registerNodePath(hash, path []byte, bits int) {
	if s.config == nil || !s.config.UsePathStorage() || len(hash) == 0 {
		return
	}
	if s.nodePaths == nil {
		s.nodePaths = make(map[string]persistedNodePath)
	}
	s.nodePaths[string(hash)] = persistedNodePath{
		path: append([]byte(nil), path...),
		bits: bits,
	}
}

func (s *Shard) getCachedNodeBlob(storageKey []byte) ([]byte, bool) {
	data, ok := s.nodeCache.get(storageKey)
	recordNodeCacheLookupIfEnabled(s.config, ok)
	return data, ok
}

func (s *Shard) cacheNodeBlob(storageKey, data []byte) {
	s.nodeCache.add(storageKey, data)
}

func (s *Shard) removeCachedNodeBlob(storageKey []byte) {
	s.nodeCache.remove(storageKey)
}

func (s *Shard) childStoragePath(parentPath []byte, parentBits int, n *InternalNode, bit byte) ([]byte, int) {
	nodePath, nodeBits := s.prependPath(n.Path, n.PathBits, parentPath, parentBits)
	return s.concatPath(nodePath, nodeBits, bit, nil, 0)
}

func (s *Shard) childNodeStoragePath(n *InternalNode, bit byte) ([]byte, int) {
	parentPath, parentBits := n.StoragePath()
	return s.childStoragePath(parentPath, parentBits, n, bit)
}

func (s *Shard) registerChildPaths(node Node, path []byte, bits int) {
	n, ok := node.(*InternalNode)
	if !ok {
		return
	}
	if len(n.LeftHash) > 0 {
		childPath, childBits := s.childStoragePath(path, bits, n, 0)
		s.registerNodePath(n.LeftHash, childPath, childBits)
	}
	if len(n.RightHash) > 0 {
		childPath, childBits := s.childStoragePath(path, bits, n, 1)
		s.registerNodePath(n.RightHash, childPath, childBits)
	}
}

func (s *Shard) archiveMayContainKey(node Node, key []byte, depth int) bool {
	if node == nil {
		return false
	}
	if present, ok := s.subtreeArchivePresence(node); ok && !present {
		return false
	}

	switch n := node.(type) {
	case *ArchiveBucketNode:
		return s.archiveBucketMayContainKey(n, key)
	case *LeafNode:
		return false
	case *InternalNode:
		for _, bucket := range n.StubList {
			if s.archiveBucketMayContainKey(bucket, key) {
				return true
			}
		}
		if n.PathBits > 0 {
			matched := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
			if matched != n.PathBits {
				return false
			}
			depth += n.PathBits
		}

		bit := s.getBit(key, depth)
		var child Node
		var childEpoch byte
		var hasChildHash bool
		if bit == 0 {
			child = n.Left
			childEpoch = n.LeftEpoch
			hasChildHash = len(n.LeftHash) > 0
		} else {
			child = n.Right
			childEpoch = n.RightEpoch
			hasChildHash = len(n.RightHash) > 0
		}
		if child != nil {
			return s.archiveMayContainKey(child, key, depth+1)
		}
		if present, ok := storedSubtreeArchivePresence(childEpoch); ok {
			if !present {
				return false
			}
			loaded, err := s.loadChildNode(n, bit, childHashForBit(n, bit))
			if err != nil || loaded == nil {
				return true
			}
			if bit == 0 {
				n.Left = loaded
			} else {
				n.Right = loaded
			}
			return s.archiveMayContainKey(loaded, key, depth+1)
		}
		if !hasChildHash {
			return false
		}
		loaded, err := s.loadChildNode(n, bit, childHashForBit(n, bit))
		if err != nil || loaded == nil {
			return true
		}
		if bit == 0 {
			n.Left = loaded
		} else {
			n.Right = loaded
		}
		return s.archiveMayContainKey(loaded, key, depth+1)
	default:
		return false
	}
}

func childHashForBit(n *InternalNode, bit byte) []byte {
	if n == nil {
		return nil
	}
	if bit == 0 {
		return n.LeftHash
	}
	return n.RightHash
}

func (s *Shard) archiveBucketMayContainKey(bucket *ArchiveBucketNode, key []byte) bool {
	if bucket == nil || bucket.Count == 0 {
		return false
	}
	matched := s.commonPrefixLen(bucket.Path, bucket.PathBits, key, 0)
	if matched != bucket.PathBits {
		return false
	}
	filter := s.archiveBucketFilter(bucket)
	if filter == nil {
		return true
	}
	innerDepth := bucket.PathBits
	shardKey := s.getSuffix(key, innerDepth, nil)
	suffixBits := len(key)*8 - innerDepth
	keyWithLen := archiveItemKey(suffixBits, shardKey)
	return filter.Lookup(keyWithLen)
}

func (s *Shard) archiveBucketFilter(bucket *ArchiveBucketNode) *cuckoo.Filter {
	if bucket == nil || s.config == nil {
		return nil
	}
	bucket.cacheMu.RLock()
	filter := bucket.cachedFilter
	bucket.cacheMu.RUnlock()
	if filter != nil {
		return filter
	}
	if len(bucket.Filter) == 0 {
		return nil
	}
	bucket.cacheMu.Lock()
	defer bucket.cacheMu.Unlock()
	if bucket.cachedFilter != nil {
		return bucket.cachedFilter
	}
	filter = cuckoo.New(s.config.CuckooBuckets, s.config.CuckooSlots)
	if err := filter.Decode(bucket.Filter, s.config.CuckooBuckets, s.config.CuckooSlots); err != nil {
		return nil
	}
	bucket.cachedFilter = filter
	return filter
}

// Get 在单个 shard 内查找 key。顺序是先走热 child，再查侧挂 StubList，
// 最后处理直接命中的 ArchiveBucketNode。
func (s *Shard) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.root == nil && len(s.rootHash) > 0 {
		var err error
		s.root, err = s.loadNode(s.rootHash)
		if err != nil {
			return nil, err
		}
	}
	if s.root == nil {
		atomic.AddInt64(&common.BinaryMissNonExistentCount, 1)
		return nil, ErrNodeNotFound
	}
	// Shards start at certain depth
	val, fromArchive, err := s.get(s.root, key, s.config.ShardDepth)
	if err != nil {
		atomic.AddInt64(&common.BinaryMissNonExistentCount, 1)
		return nil, err
	}

	_ = fromArchive

	return val, nil
}

func (s *Shard) get(node Node, key []byte, depth int) ([]byte, bool, error) {
	if node == nil {
		return nil, false, ErrNodeNotFound
	}

	switch n := node.(type) {
	case *LeafNode:
		matchLen := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
		if matchLen == n.PathBits && depth+matchLen == len(key)*8 {
			atomic.AddInt64(&common.BinaryHitCount, 1)
			val, err := s.getFlatValue(key)
			return val, false, err
		}
		return nil, false, ErrNodeNotFound

	case *InternalNode:
		// [热路径优先]：首先尝试在当前子树的热路径中查找
		hotDepth := depth
		if n.PathBits > 0 {
			matched := s.commonPrefixLen(n.Path, n.PathBits, key, hotDepth)
			if matched == n.PathBits {
				hotDepth += n.PathBits
				bit := s.getBit(key, hotDepth)
				var next Node
				var nextHash []byte
				if bit == 0 {
					next = n.Left
					nextHash = n.LeftHash
				} else {
					next = n.Right
					nextHash = n.RightHash
				}

				if next == nil && len(nextHash) > 0 {
					loaded, err := s.loadNode(nextHash)
					if err != nil {
						return nil, false, err
					}
					if loaded != nil {
						if bit == 0 {
							n.Left = loaded
						} else {
							n.Right = loaded
						}
						next = loaded
					}
				}

				if next != nil {
					val, fromArch, err := s.get(next, key, hotDepth+1)
					if err == nil {
						return val, fromArch, nil
					}
				}
			}
		} else {
			// PathBits == 0, 直接尝试孩子
			bit := s.getBit(key, hotDepth)
			var next Node
			var nextHash []byte
			if bit == 0 {
				next = n.Left
				nextHash = n.LeftHash
			} else {
				next = n.Right
				nextHash = n.RightHash
			}

			if next == nil && len(nextHash) > 0 {
				loaded, err := s.loadNode(nextHash)
				if err != nil {
					return nil, false, err
				}
				if loaded != nil {
					if bit == 0 {
						n.Left = loaded
					} else {
						n.Right = loaded
					}
					next = loaded
				}
			}

			if next != nil {
				val, fromArch, err := s.get(next, key, hotDepth+1)
				if err == nil {
					return val, fromArch, nil
				}
			}
		}

		// [归档桶次之]：热路径未命中，按“后进先出”（栈）顺序查找侧挂的 StubList
		for i := len(n.StubList) - 1; i >= 0; i-- {
			bucket := n.StubList[i]
			val, fromArch, err := s.getFromBucket(bucket, key, depth)
			if err == nil {
				return val, fromArch, nil
			}
		}

		return nil, false, ErrNodeNotFound

	case *ArchiveBucketNode:
		return s.getFromBucket(n, key, depth)

	default:
		return nil, false, errors.New("unknown node type")
	}
}

// getFromBucket 分三步验证冷桶 membership：bucket path 前缀、Cuckoo filter、
// entry suffix 精确匹配。返回的真实 value 仍来自 flat value store，并用 valueRef 校验。
func (s *Shard) getFromBucket(bucket *ArchiveBucketNode, key []byte, _ int) ([]byte, bool, error) {
	proofStart := time.Now()

	// 匹配位前缀
	matched := s.commonPrefixLen(bucket.Path, bucket.PathBits, key, 0)
	if matched != bucket.PathBits {
		return nil, false, ErrNodeNotFound
	}

	// 获取或解码过滤器
	bucket.cacheMu.RLock()
	filter := bucket.cachedFilter
	bucket.cacheMu.RUnlock()

	if filter == nil {
		bucket.cacheMu.Lock()
		if bucket.cachedFilter == nil {
			f := cuckoo.New(s.config.CuckooBuckets, s.config.CuckooSlots)
			if err := f.Decode(bucket.Filter, s.config.CuckooBuckets, s.config.CuckooSlots); err == nil {
				bucket.cachedFilter = f
			}
		}
		filter = bucket.cachedFilter
		bucket.cacheMu.Unlock()
	}

	if filter != nil {
		innerDepth := bucket.PathBits
		shardKey := s.getSuffix(key, innerDepth, nil)
		suffixBits := len(key)*8 - innerDepth
		keyWithLen := archiveItemKey(suffixBits, shardKey)
		if !filter.Lookup(keyWithLen) {
			return nil, false, ErrNodeNotFound
		}
	}

	keys, err := s.bucketKeys(bucket)
	if err == nil && keys != nil {
		innerDepth := bucket.PathBits
		keyBits := len(key) * 8
		for _, item := range keys {
			if innerDepth+item.SuffixBits == keyBits {
				match := s.suffixMatches(key, innerDepth, item.Suffix, item.SuffixBits)
				if match {
					atomic.AddInt64(&common.BinaryMissExistentCount, 1)
					genDur := time.Since(proofStart).Nanoseconds()
					atomic.AddInt64(&common.BinaryProofGenTime, genDur)
					updateAtomicMax(&common.BinaryProofGenTimeMax, genDur)

					ok, verifDur := s.verifyBucket(bucket)
					atomic.AddInt64(&common.BinaryProofVerifTime, verifDur)
					if !ok {
						return nil, false, errors.New("archive bucket proof verification failed")
					}
					val, err := s.getFlatValue(key)
					if err != nil {
						return nil, true, err
					}
					if len(item.ValueRef) == common.HashLength && !bytes.Equal(item.ValueRef, valueRefForKeyValue(key, val)) {
						return nil, true, errors.New("archive bucket valueRef verification failed")
					}
					return val, true, nil
				}
			}
		}
		s.statsMut.Lock()
		s.stats.FalsePositiveCount++
		s.statsMut.Unlock()
		atomic.AddInt64(&common.BinaryCycleFPCount, 1)
		atomic.AddInt64(&common.BinaryTrieFPInBlock, 1)
		common.BinaryStatsMu.Lock()
		common.BinaryFPDistribution = append(common.BinaryFPDistribution, int64(len(keys)))
		common.BinaryStatsMu.Unlock()
	}

	return nil, false, ErrNodeNotFound
}

func updateAtomicMax(addr *int64, val int64) {
	for {
		old := atomic.LoadInt64(addr)
		if val <= old || atomic.CompareAndSwapInt64(addr, old, val) {
			return
		}
	}
}

// Put 把 key 写回热树。如果该 key 有冷归档副本，会先删除旧冷 entry，再插入新热 leaf。
func (s *Shard) Put(key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putLocked(key, value)
}

func (s *Shard) PutBatch(entries []KeyValue) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range entries {
		if err := s.putLocked(entry.Key, entry.Value); err != nil {
			return err
		}
	}
	return nil
}

// putLocked 暂存 flat value，删除旧冷 membership，然后插入只包含新 valueRef 的热 leaf。
func (s *Shard) putLocked(key []byte, value []byte) error {
	if s.root == nil && len(s.rootHash) > 0 {
		var err error
		s.root, err = s.loadNode(s.rootHash)
		if err != nil {
			return err
		}
	}
	if s.archiveMayContainKey(s.root, key, s.config.ShardDepth) {
		if s.hasFlatValue(key) {
			removed, err := s.removeArchivedVersionForWrite(key)
			recordArchivePromotionCheckIfEnabled(s.config, removed)
			if err != nil {
				return err
			}
		} else {
			recordArchivePromotionCheckIfEnabled(s.config, false)
		}
	}
	valHash := s.stageValueForKey(key, value)

	// Shard start depth
	newRoot, err := s.insert(s.root, key, s.config.ShardDepth, valHash)
	if err != nil {
		return err
	}
	s.root = newRoot
	return nil
}

func (s *Shard) removeArchivedVersionForWrite(key []byte) (bool, error) {
	if s.root == nil {
		return false, nil
	}
	removed, err := s.removeFromStubList(s.root, key, s.config.ShardDepth)
	if err != nil {
		return false, err
	}
	if removed {
		s.cleanupRootAfterArchiveDelete()
	}
	return removed, nil
}

func (s *Shard) cleanupRootAfterArchiveDelete() {
	if s.root == nil {
		return
	}
	switch n := s.root.(type) {
	case *ArchiveBucketNode:
		if n.Count == 0 {
			s.markNodeStale(n)
			s.root = nil
		}
	case *InternalNode:
		if len(n.StubList) == 0 && n.Left == nil && n.Right == nil && len(n.LeftHash) == 0 && len(n.RightHash) == 0 {
			s.markNodeStale(n)
			s.root = nil
		}
	}
}

func (s *Shard) markNodeStale(node Node) {
	s.markPersistedNodeStale(node)
}

// Activate 实现显式激活：从 StubList 查找并移除匹配项，然后执行常规插入
func (s *Shard) Activate(key []byte, value []byte) error {
	if s.root == nil && len(s.rootHash) > 0 {
		var err error
		s.root, err = s.loadNode(s.rootHash)
		if err != nil {
			return err
		}
	}

	// 1. 深度搜索并从现有 StubList 中移除该 key
	if _, err := s.removeArchivedVersionForWrite(key); err != nil {
		return err
	}

	// 2. 执行常规插入
	valHash := s.stageValueForKey(key, value)

	var err error
	s.root, err = s.insert(s.root, key, s.config.ShardDepth, valHash)
	return err
}

func (s *Shard) removeFromStubList(node Node, key []byte, depth int) (bool, error) {
	if node == nil {
		return false, nil
	}

	switch n := node.(type) {
	case *InternalNode:
		// [定点移除]：只需检查当前节点侧挂的 StubList 是否包含匹配的前缀
		for i := 0; i < len(n.StubList); i++ {
			bucket := n.StubList[i]
			matched := s.commonPrefixLen(bucket.Path, bucket.PathBits, key, 0)
			if matched == bucket.PathBits {
				items, err := s.bucketKeys(bucket)
				if err != nil {
					return false, err
				}

				innerDepth := bucket.PathBits
				keyBits := len(key) * 8
				for _, item := range items {
					if innerDepth+item.SuffixBits == keyBits && s.suffixMatches(key, innerDepth, item.Suffix, item.SuffixBits) {
						// 命中：执行盲删除
						oldHash := s.ensureBucketHash(bucket)

						s.blindDeleteFromBucket(bucket, []ArchivedKV{{
							Suffix:     item.Suffix,
							SuffixBits: item.SuffixBits,
							Value:      item.ValueRef,
						}})

						if bucket.Count == 0 {
							if s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) && len(oldHash) > 0 {
								s.staleSet[string(oldHash)] = struct{}{}
							}
							n.StubList = append(n.StubList[:i], n.StubList[i+1:]...)
						}
						n.SetDirty(true)
						return true, nil
					}
				}
			}
		}

		// 按路径下探，不扫全树
		if n.PathBits > 0 {
			matched := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
			if matched != n.PathBits {
				return false, nil // 路径不通，肯定不在这个子树下的 StubList
			}
			depth += n.PathBits
		}

		bit := s.getBit(key, depth)
		var next Node
		var nextHash []byte
		if bit == 0 {
			next = n.Left
			nextHash = n.LeftHash
		} else {
			next = n.Right
			nextHash = n.RightHash
		}

		if next == nil && len(nextHash) > 0 {
			loaded, _ := s.loadChildNode(n, bit, nextHash)
			if loaded != nil {
				if bit == 0 {
					n.Left = loaded
				} else {
					n.Right = loaded
				}
				next = loaded
			}
		}

		if next != nil {
			removed, err := s.removeFromStubList(next, key, depth+1)
			if err != nil {
				return false, err
			}
			if removed {
				if bucket, ok := next.(*ArchiveBucketNode); ok && bucket.Count == 0 {
					if oldHash := s.ensureBucketHash(bucket); s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) && len(oldHash) > 0 {
						s.staleSet[string(oldHash)] = struct{}{}
					}
					if bit == 0 {
						n.Left, n.LeftHash = nil, nil
						n.LeftEpoch = 0
					} else {
						n.Right, n.RightHash = nil, nil
						n.RightEpoch = 0
					}
				}
				n.SetDirty(true)
				s.refreshInternalEpochMask(n)
				return true, nil
			}
		}
	case *ArchiveBucketNode:
		// 如果根节点直接就是桶
		matched := s.commonPrefixLen(n.Path, n.PathBits, key, 0)
		if matched == n.PathBits {
			items, err := s.bucketKeys(n)
			if err != nil {
				return false, err
			}

			innerDepth := n.PathBits
			keyBits := len(key) * 8
			for _, item := range items {
				if innerDepth+item.SuffixBits == keyBits && s.suffixMatches(key, innerDepth, item.Suffix, item.SuffixBits) {
					oldHash := s.ensureBucketHash(n)

					s.blindDeleteFromBucket(n, []ArchivedKV{{
						Suffix:     item.Suffix,
						SuffixBits: item.SuffixBits,
						Value:      item.ValueRef,
					}})
					if n.Count == 0 {
						if s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) && len(oldHash) > 0 {
							s.staleSet[string(oldHash)] = struct{}{}
						}
						// 此处无法直接移除 node，需由调用者处理 s.root = nil
					}
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (s *Shard) insert(node Node, key []byte, depth int, valueHash []byte) (Node, error) {
	// 空位：创建叶子
	if node == nil {
		// 叶子后缀为 key[depth:] 的位序列
		pathBits := len(key)*8 - depth
		path := s.getSuffix(key, depth, nil)
		leaf := s.pool.GetLeaf()
		leaf.Path = path
		leaf.PathBits = pathBits
		leaf.ValueHash = valueHash
		leaf.SetDirty(true)
		s.updateEpoch(leaf) // Update epoch for new leaf
		return leaf, nil
	}

	switch n := node.(type) {
	case *LeafNode:
		// 检查冲突（共同前缀位数）
		matchBits := s.commonPrefixLen(n.Path, n.PathBits, key, depth)

		// 完全匹配：更新值哈希
		if matchBits == n.PathBits && depth+matchBits == len(key)*8 {
			n.ValueHash = valueHash
			n.SetDirty(true)
			s.updateEpoch(n)
			s.markPersistedNodeStale(n)
			return n, nil
		}

		// [Robust] 绝不分叉已达 256 位极限的节点内容。
		if matchBits == n.PathBits {
			n.ValueHash = valueHash
			n.PathBits = len(key)*8 - depth
			n.Path = s.getSuffix(key, depth, nil)
			n.SetDirty(true)
			s.updateEpoch(n)
			return n, nil
		}

		// 分裂逻辑... (为了篇幅，这里应该包含完整的 insert 分裂逻辑)
		s.markPersistedNodeStale(n)

		oldLeafSuffix := s.shiftBits(n.Path, n.PathBits, matchBits+1, nil)
		tmpSuffix := s.getSuffix(key, depth, s.scratch)
		newLeafSuffix := s.shiftBits(tmpSuffix, len(key)*8-depth, matchBits+1, nil)

		oldLeaf := s.pool.GetLeaf()
		oldLeaf.Path = oldLeafSuffix
		oldLeaf.PathBits = n.PathBits - (matchBits + 1)
		oldLeaf.ValueHash = n.ValueHash
		oldLeaf.SetEpoch(n.Epoch())

		newLeaf := s.pool.GetLeaf()
		newLeaf.Path = newLeafSuffix
		newLeaf.PathBits = (len(key)*8 - depth) - (matchBits + 1)
		newLeaf.ValueHash = valueHash
		s.updateEpoch(newLeaf)

		prefixPath := s.prefixBits(n.Path, matchBits, nil)
		splitBit := s.getBitFromBytes(n.Path, matchBits)

		splitNode := s.pool.GetInternal()
		splitNode.Path = prefixPath
		splitNode.PathBits = matchBits
		if splitBit == 0 {
			splitNode.Left = oldLeaf
			splitNode.Right = newLeaf
		} else {
			splitNode.Left = newLeaf
			splitNode.Right = oldLeaf
		}
		s.updateEpoch(splitNode)
		s.refreshInternalEpochMask(splitNode)
		return splitNode, nil

	case *InternalNode:
		if !n.dirty {
			s.markPersistedNodeStale(n)
		}
		n.SetDirty(true)

		if n.PathBits > 0 {
			matched := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
			if matched < n.PathBits {
				prefixPath := s.prefixBits(n.Path, matched, nil)
				parent := s.pool.GetInternal()
				parent.Path = prefixPath
				parent.PathBits = matched

				oldBit := s.getBitFromBytes(n.Path, matched)
				n.Path = s.shiftBits(n.Path, n.PathBits, matched+1, nil)
				n.PathBits -= (matched + 1)

				s.distributeStubs(n, parent, []byte{}, 0, oldBit)

				if oldBit == 0 {
					parent.Left = n
				} else {
					parent.Right = n
				}

				newBit := s.getBit(key, depth+matched)
				newLeafPath := s.getSuffix(key, depth+matched+1, nil)
				newLeafBits := len(key)*8 - (depth + matched + 1)
				newLeaf := s.pool.GetLeaf()
				newLeaf.Path = newLeafPath
				newLeaf.PathBits = newLeafBits
				newLeaf.ValueHash = valueHash
				s.updateEpoch(newLeaf)

				if newBit == 0 {
					parent.Left = newLeaf
				} else {
					parent.Right = newLeaf
				}

				s.updateEpoch(parent)
				s.refreshInternalEpochMask(parent)
				return parent, nil
			}
			depth += n.PathBits
		}

		bit := s.getBit(key, depth)
		if bit == 0 {
			if n.Left == nil && len(n.LeftHash) > 0 {
				var err error
				n.Left, err = s.loadChildNode(n, 0, n.LeftHash)
				if err != nil {
					return nil, err
				}
			}
			newLeft, err := s.insert(n.Left, key, depth+1, valueHash)
			if err != nil {
				return nil, err
			}
			n.Left = newLeft
		} else {
			if n.Right == nil && len(n.RightHash) > 0 {
				var err error
				n.Right, err = s.loadChildNode(n, 1, n.RightHash)
				if err != nil {
					return nil, err
				}
			}
			newRight, err := s.insert(n.Right, key, depth+1, valueHash)
			if err != nil {
				return nil, err
			}
			n.Right = newRight
		}

		s.updateEpoch(n)
		s.refreshInternalEpochMask(n)
		return s.shrinkLocal(n), nil

	case *ArchiveBucketNode:
		parent := s.pool.GetInternal()
		if n.PathBits <= depth {
			if splitNode, ok, err := s.splitArchiveBucketForHotInsert(n, depth+1); err != nil {
				return nil, err
			} else if ok && splitNode != n {
				return s.insert(splitNode, key, depth, valueHash)
			}
			s.attachStubs(parent, []*ArchiveBucketNode{n})
			parent.SetDirty(true)
			s.refreshInternalEpochMask(parent)
			return s.insert(parent, key, depth, valueHash)
		}

		bucketSuffix := s.shiftBits(n.Path, n.PathBits, depth, nil)
		bucketSuffixBits := n.PathBits - depth
		matchBits := s.commonPrefixLen(bucketSuffix, bucketSuffixBits, key, depth)
		parent.Path = s.prefixBits(bucketSuffix, matchBits, nil)
		parent.PathBits = matchBits

		if matchBits == bucketSuffixBits {
			if splitNode, ok, err := s.splitArchiveBucketForHotInsert(n, depth+matchBits+1); err != nil {
				return nil, err
			} else if ok && splitNode != n {
				return s.insert(splitNode, key, depth, valueHash)
			}
			s.attachStubs(parent, []*ArchiveBucketNode{n})
		} else {
			bucketBit := s.getBitFromBytes(bucketSuffix, matchBits)
			if bucketBit == 0 {
				parent.Left = n
				parent.LeftEpoch = n.Epoch()
			} else {
				parent.Right = n
				parent.RightEpoch = n.Epoch()
			}
		}

		newBit := s.getBit(key, depth+matchBits)
		newLeafPath := s.getSuffix(key, depth+matchBits+1, nil)
		newLeafBits := len(key)*8 - (depth + matchBits + 1)
		newLeaf := s.pool.GetLeaf()
		newLeaf.Path = newLeafPath
		newLeaf.PathBits = newLeafBits
		newLeaf.ValueHash = valueHash
		s.updateEpoch(newLeaf)
		if newBit == 0 {
			parent.Left = newLeaf
		} else {
			parent.Right = newLeaf
		}

		parent.SetDirty(true)
		s.refreshInternalEpochMask(parent)
		return parent, nil

	default:
		return nil, errors.New("unknown node type")
	}
}

// Delete 删除指定 key。
func (s *Shard) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.root == nil && len(s.rootHash) > 0 {
		var err error
		s.root, err = s.loadNode(s.rootHash)
		if err != nil {
			return err
		}
	}
	if s.root != nil {
		removed, err := s.removeFromStubList(s.root, key, s.config.ShardDepth)
		if err != nil {
			return err
		}
		if removed {
			s.cleanupRootAfterArchiveDelete()
		}
	}
	s.stageFlatDeleteForKey(key)
	// 递归剪枝
	newRoot, _, err := s.delete(s.root, key, s.config.ShardDepth)
	if err != nil {
		if err == ErrNodeNotFound {
			return nil
		}
		return err
	}
	s.root = newRoot
	return nil
}

func (s *Shard) delete(node Node, key []byte, depth int) (Node, bool, error) {
	if node == nil {
		return nil, false, ErrNodeNotFound
	}

	switch n := node.(type) {
	case *LeafNode:
		matchLen := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
		if matchLen == n.PathBits && depth+matchLen == len(key)*8 {
			s.markPersistedNodeStale(n)
			return nil, true, nil
		}
		return node, false, ErrNodeNotFound

	case *InternalNode:
		if n.PathBits > 0 {
			matched := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
			if matched != n.PathBits {
				return node, false, ErrNodeNotFound
			}
			depth += n.PathBits
		}

		bit := s.getBit(key, depth)
		var child Node
		var childHash []byte

		if bit == 0 {
			child = n.Left
			childHash = n.LeftHash
		} else {
			child = n.Right
			childHash = n.RightHash
		}

		if child == nil && len(childHash) > 0 {
			loaded, _ := s.loadChildNode(n, bit, childHash)
			child = loaded
			if bit == 0 {
				n.Left = child
			} else {
				n.Right = child
			}
		}

		newChild, deleted, err := s.delete(child, key, depth+1)
		if err != nil {
			return node, false, err
		}

		if !deleted {
			return node, false, nil
		}

		if !n.dirty {
			s.markPersistedNodeStale(n)
		}
		n.SetDirty(true)

		if bit == 0 {
			n.Left = newChild
			n.LeftHash = nil
		} else {
			n.Right = newChild
			n.RightHash = nil
		}

		var remaining ChildInfo
		count := 0
		if n.Left != nil || len(n.LeftHash) > 0 {
			count++
			remaining = ChildInfo{node: n.Left, hash: n.LeftHash, bit: 0}
		}
		if n.Right != nil || len(n.RightHash) > 0 {
			count++
			remaining = ChildInfo{node: n.Right, hash: n.RightHash, bit: 1}
		}

		if count == 0 {
			return nil, true, nil
		}

		if count == 1 {
			if n.PathBits > 0 {
				s.updateEpoch(n)
				s.refreshInternalEpochMask(n)
				return n, true, nil
			}

			target := remaining.node
			if target == nil {
				target, _ = s.loadNode(remaining.hash)
			}

			if leaf, ok := target.(*LeafNode); ok {
				newP, newB := s.concatPath(n.Path, n.PathBits, remaining.bit, leaf.Path, leaf.PathBits)
				leaf.Path, leaf.PathBits = newP, newB
				leaf.SetDirty(true)
				s.markPersistedNodeStale(leaf)
				return leaf, true, nil
			}

			s.updateEpoch(n)
			s.refreshInternalEpochMask(n)
			return n, true, nil
		}

		s.updateEpoch(n)
		s.refreshInternalEpochMask(n)
		return n, true, nil
	case *ArchiveBucketNode:
		removed, err := s.removeFromStubList(n, key, depth)
		if err != nil {
			return node, false, err
		}
		if !removed {
			return node, false, ErrNodeNotFound
		}
		if n.Count == 0 {
			return nil, true, nil
		}
		return n, true, nil
	default:
		return node, false, errors.New("unknown node type")
	}
}

// Hash 计算分片根哈希。
func (s *Shard) Hash() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.root == nil {
		return s.rootHash, nil
	}
	count := 0
	return s.commit(s.root, nil, &count, false, nil, 0, nil)
}

type dummyBatcher struct{}

func (b *dummyBatcher) Put(key, value []byte) error { return nil }
func (b *dummyBatcher) Delete(key []byte) error     { return nil }
func (b *dummyBatcher) Write() error                { return nil }
func (b *dummyBatcher) Reset()                      {}
func (b *dummyBatcher) ValueSize() int              { return 0 }

type ChildInfo struct {
	node Node
	hash []byte
	bit  byte
}

// CommitToBatch 递归提交 dirty trie metadata、stale delete 和 pending value。
// destructive=true 时会在提交后卸载 live root，用 rootHash 作为后续懒加载入口。
func (s *Shard) CommitToBatch(batch Batcher, destructive bool) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.root == nil {
		if err := s.commitPendingValues(batch); err != nil {
			return nil, err
		}
		if err := s.commitStaleDeletes(batch); err != nil {
			return nil, err
		}
		s.rootHash = nil
		return nil, nil
	}

	nodeCount := 0
	rootHash, err := s.commit(s.root, batch, &nodeCount, destructive, nil, 0, nil)
	if err != nil {
		return nil, err
	}

	if err := s.commitStaleDeletes(batch); err != nil {
		return nil, err
	}

	if err := s.commitPendingValues(batch); err != nil {
		return nil, err
	}

	if destructive {
		s.root = nil
		s.rootHash = rootHash
		if s.config != nil && s.config.UsePathStorage() {
			s.nodePaths = make(map[string]persistedNodePath)
			s.registerNodePath(rootHash, nil, 0)
		}
	}
	return rootHash, nil
}

func (s *Shard) CommitToBatchWithDiagnostics(batch Batcher, destructive bool) ([]byte, ShardCommitDiagnostics, error) {
	totalStart := time.Now()
	lockStart := time.Now()
	s.mu.Lock()
	lockWait := time.Since(lockStart)
	defer s.mu.Unlock()

	diag := ShardCommitDiagnostics{
		LockWaitNanos:           lockWait.Nanoseconds(),
		StaleSetLen:             int64(len(s.staleSet)),
		PendingFlatValuePuts:    int64(len(s.pendingFlatValues)),
		PendingFlatValueDeletes: int64(len(s.pendingFlatValueDeletes)),
	}
	if s.root == nil {
		diag.RootWasNil = true
		pendingStart := time.Now()
		if err := s.commitPendingValues(batch); err != nil {
			diag.PendingValueNanos = time.Since(pendingStart).Nanoseconds()
			diag.TotalNanos = time.Since(totalStart).Nanoseconds()
			s.lastCommitDiag = diag
			return nil, diag, err
		}
		diag.PendingValueNanos = time.Since(pendingStart).Nanoseconds()
		staleStart := time.Now()
		if err := s.commitStaleDeletes(batch); err != nil {
			diag.StaleDeleteNanos = time.Since(staleStart).Nanoseconds()
			diag.TotalNanos = time.Since(totalStart).Nanoseconds()
			s.lastCommitDiag = diag
			return nil, diag, err
		}
		diag.StaleDeleteNanos = time.Since(staleStart).Nanoseconds()
		s.rootHash = nil
		diag.TotalNanos = time.Since(totalStart).Nanoseconds()
		s.lastCommitDiag = diag
		return nil, diag, nil
	}

	nodeCount := 0
	rootCommitStart := time.Now()
	workStats := new(commitWorkStats)
	rootHash, err := s.commit(s.root, batch, &nodeCount, destructive, nil, 0, workStats)
	diag.RootCommitNanos = time.Since(rootCommitStart).Nanoseconds()
	diag.NodeCount = int64(nodeCount)
	workStats.applyTo(&diag)
	if err != nil {
		diag.TotalNanos = time.Since(totalStart).Nanoseconds()
		s.lastCommitDiag = diag
		return nil, diag, err
	}

	staleStart := time.Now()
	if err := s.commitStaleDeletes(batch); err != nil {
		diag.StaleDeleteNanos = time.Since(staleStart).Nanoseconds()
		diag.TotalNanos = time.Since(totalStart).Nanoseconds()
		s.lastCommitDiag = diag
		return nil, diag, err
	}
	diag.StaleDeleteNanos = time.Since(staleStart).Nanoseconds()

	pendingStart := time.Now()
	if err := s.commitPendingValues(batch); err != nil {
		diag.PendingValueNanos = time.Since(pendingStart).Nanoseconds()
		diag.TotalNanos = time.Since(totalStart).Nanoseconds()
		s.lastCommitDiag = diag
		return nil, diag, err
	}
	diag.PendingValueNanos = time.Since(pendingStart).Nanoseconds()

	if destructive {
		s.root = nil
		s.rootHash = rootHash
		if s.config != nil && s.config.UsePathStorage() {
			s.nodePaths = make(map[string]persistedNodePath)
			s.registerNodePath(rootHash, nil, 0)
		}
	}
	diag.TotalNanos = time.Since(totalStart).Nanoseconds()
	s.lastCommitDiag = diag
	return rootHash, diag, nil
}

func (s *Shard) commitStaleDeletes(batch Batcher) error {
	if !s.pruning || batch == nil || len(s.staleSet) == 0 {
		return nil
	}
	if s.config != nil && !s.config.PhysicalDelete {
		s.staleSet = make(map[string]struct{})
		return nil
	}
	for h := range s.staleSet {
		if s.config != nil && s.config.UsePathStorage() && !bytes.HasPrefix([]byte(h), pathNodePrefix) {
			continue
		}
		s.removeCachedNodeBlob([]byte(h))
		if err := batch.Delete([]byte(h)); err != nil {
			return err
		}
	}
	s.staleSet = make(map[string]struct{})
	return nil
}

// ForEach iterates over all leaves in the shard.
func (s *Shard) ForEach(prefix []byte, bits int, fn func(key, value []byte) bool) {
	if s.root == nil {
		return
	}
	s.forEach(s.root, prefix, bits, fn)
}

func (s *Shard) ForEachPrefix(prefix []byte, bits int, matchPrefix []byte, matchBits int, fn func(key, value []byte) bool) {
	if s.root == nil {
		return
	}
	s.forEachPrefix(s.root, prefix, bits, matchPrefix, matchBits, fn)
}

func (s *Shard) archiveItemFlatValue(bucket *ArchiveBucketNode, kv ArchivedKV) ([]byte, []byte) {
	fullK, _ := s.prependPath(kv.Suffix, kv.SuffixBits, bucket.Path, bucket.PathBits)
	if val, err := s.getFlatValue(fullK); err == nil {
		return fullK, val
	}
	return fullK, nil
}

func (s *Shard) forEach(node Node, prefix []byte, bits int, fn func(key, value []byte) bool) bool {
	if node == nil {
		return true
	}
	switch n := node.(type) {
	case *LeafNode:
		fullKey, _ := s.prependPath(n.Path, n.PathBits, prefix, bits)
		val, _ := s.getFlatValue(fullKey)
		return fn(fullKey, val)
	case *ArchiveBucketNode:
		kvs, err := s.bucketItemsWithValueRefs(n)
		if err != nil {
			return true
		}
		for _, kv := range kvs {
			fullK, value := s.archiveItemFlatValue(n, kv)
			if !fn(fullK, value) {
				return false
			}
		}
		return true
	case *InternalNode:
		newPrefix := prefix
		newBits := bits
		if n.PathBits > 0 {
			newPrefix, _ = s.prependPath(n.Path, n.PathBits, prefix, bits)
			newBits += n.PathBits
		}
		// Left child (bit 0)
		leftPrefix, leftBits := s.appendBit(newPrefix, newBits, 0)
		if n.Left != nil {
			if !s.forEach(n.Left, leftPrefix, leftBits, fn) {
				return false
			}
		} else if len(n.LeftHash) > 0 {
			loaded, _ := s.loadChildNode(n, 0, n.LeftHash)
			if loaded != nil {
				if !s.forEach(loaded, leftPrefix, leftBits, fn) {
					return false
				}
			}
		}
		// Right child (bit 1)
		rightPrefix, rightBits := s.appendBit(newPrefix, newBits, 1)
		if n.Right != nil {
			if !s.forEach(n.Right, rightPrefix, rightBits, fn) {
				return false
			}
		} else if len(n.RightHash) > 0 {
			loaded, _ := s.loadChildNode(n, 1, n.RightHash)
			if loaded != nil {
				if !s.forEach(loaded, rightPrefix, rightBits, fn) {
					return false
				}
			}
		}
		// Iterate over archived buckets
		for _, bucket := range n.StubList {
			kvs, err := s.bucketItemsWithValueRefs(bucket)
			if err == nil {
				for _, kv := range kvs {
					// bucket.Path is the absolute path; kv.Suffix is relative to bucket.Path
					fullK, value := s.archiveItemFlatValue(bucket, kv)
					if !fn(fullK, value) {
						return false
					}
				}
			}
		}
	}
	return true
}

// Commit 提交改动并修剪。
func (s *Shard) Commit() ([]byte, error) {
	batch := s.db.NewBatch()
	defer batch.Reset()

	rootHash, err := s.CommitToBatch(batch, true)
	if err != nil {
		return nil, err
	}

	if err := batch.Write(); err != nil {
		return nil, err
	}

	return rootHash, nil
}

func (s *Shard) forEachPrefix(node Node, prefix []byte, bits int, matchPrefix []byte, matchBits int, fn func(key, value []byte) bool) bool {
	if node == nil {
		return true
	}
	if matchBits <= 0 {
		return s.forEach(node, prefix, bits, fn)
	}
	switch n := node.(type) {
	case *LeafNode:
		fullKey, fullBits := s.prependPath(n.Path, n.PathBits, prefix, bits)
		if !hasBitPrefix(fullKey, fullBits, matchPrefix, matchBits) {
			return true
		}
		val, _ := s.getFlatValue(fullKey)
		return fn(fullKey, val)
	case *ArchiveBucketNode:
		return s.forEachArchiveBucketPrefix(n, matchPrefix, matchBits, fn)
	case *InternalNode:
		newPrefix := prefix
		newBits := bits
		if n.PathBits > 0 {
			newPrefix, newBits = s.prependPath(n.Path, n.PathBits, prefix, bits)
		}
		if !bitPrefixesOverlap(newPrefix, newBits, matchPrefix, matchBits) {
			return true
		}

		leftPrefix, leftBits := s.appendBit(newPrefix, newBits, 0)
		if bitPrefixesOverlap(leftPrefix, leftBits, matchPrefix, matchBits) {
			if n.Left != nil {
				if !s.forEachPrefix(n.Left, leftPrefix, leftBits, matchPrefix, matchBits, fn) {
					return false
				}
			} else if len(n.LeftHash) > 0 {
				loaded, _ := s.loadChildNode(n, 0, n.LeftHash)
				if loaded != nil && !s.forEachPrefix(loaded, leftPrefix, leftBits, matchPrefix, matchBits, fn) {
					return false
				}
			}
		}

		rightPrefix, rightBits := s.appendBit(newPrefix, newBits, 1)
		if bitPrefixesOverlap(rightPrefix, rightBits, matchPrefix, matchBits) {
			if n.Right != nil {
				if !s.forEachPrefix(n.Right, rightPrefix, rightBits, matchPrefix, matchBits, fn) {
					return false
				}
			} else if len(n.RightHash) > 0 {
				loaded, _ := s.loadChildNode(n, 1, n.RightHash)
				if loaded != nil && !s.forEachPrefix(loaded, rightPrefix, rightBits, matchPrefix, matchBits, fn) {
					return false
				}
			}
		}

		for _, bucket := range n.StubList {
			if !s.forEachArchiveBucketPrefix(bucket, matchPrefix, matchBits, fn) {
				return false
			}
		}
	}
	return true
}

func (s *Shard) forEachArchiveBucketPrefix(bucket *ArchiveBucketNode, matchPrefix []byte, matchBits int, fn func(key, value []byte) bool) bool {
	if bucket == nil || !bitPrefixesOverlap(bucket.Path, bucket.PathBits, matchPrefix, matchBits) {
		return true
	}
	kvs, err := s.bucketItemsWithValueRefs(bucket)
	if err != nil {
		return true
	}
	for _, kv := range kvs {
		fullK, fullBits := s.prependPath(kv.Suffix, kv.SuffixBits, bucket.Path, bucket.PathBits)
		if !hasBitPrefix(fullK, fullBits, matchPrefix, matchBits) {
			continue
		}
		_, value := s.archiveItemFlatValue(bucket, kv)
		if !fn(fullK, value) {
			return false
		}
	}
	return true
}

func (s *Shard) commit(node Node, batch Batcher, nodeCount *int, destructive bool, path []byte, pathBits int, stats *commitWorkStats) ([]byte, error) {
	if node == nil {
		return nil, nil
	}
	nodeStart := time.Now()
	nodeBytes := 0
	defer func() {
		if stats != nil {
			stats.addNode(time.Since(nodeStart), node.Type(), nodeBytes)
		}
	}()

	if nodeCount != nil {
		*nodeCount++
	}

	if s.config != nil && s.config.UsePathStorage() && !node.IsDirty() {
		oldPath, oldBits := node.StoragePath()
		if oldBits != pathBits || !bytes.Equal(oldPath, path) {
			node.SetDirty(true)
		}
	}
	if !node.IsDirty() {
		return node.Hash(), nil
	}

	switch n := node.(type) {
	case *ArchiveBucketNode:
		h := n.Hash()

		serializeStart := time.Now()
		data, err := n.Serialize()
		if stats != nil {
			stats.serializeNanos += time.Since(serializeStart).Nanoseconds()
		}
		if err != nil {
			return nil, err
		}
		nodeBytes = len(data)
		if len(h) == 0 {
			hashStart := time.Now()
			h = append([]byte{}, s.hasher.Hash(data)...)
			if stats != nil {
				stats.hashNanos += time.Since(hashStart).Nanoseconds()
			}
			n.SetHash(h)
		}

		if batch != nil {
			persistStart := time.Now()
			if err := s.persistNode(batch, n, h, data, path, pathBits, destructive, stats); err != nil {
				if stats != nil {
					stats.persistNanos += time.Since(persistStart).Nanoseconds()
				}
				return nil, err
			}
			if stats != nil {
				stats.persistNanos += time.Since(persistStart).Nanoseconds()
			}
		}

		return h, nil

	case *InternalNode:
		if n.Left != nil {
			childPath, childBits := s.childStoragePath(path, pathBits, n, 0)
			h, err := s.commit(n.Left, batch, nodeCount, destructive, childPath, childBits, stats)
			if err != nil {
				return nil, err
			}
			n.LeftHash = h
			n.LeftEpoch = n.Left.Epoch()
			if destructive {
				n.Left = nil
			}
		} else if len(n.LeftHash) == 0 {
			n.LeftEpoch = 0
		}
		if n.Right != nil {
			childPath, childBits := s.childStoragePath(path, pathBits, n, 1)
			h, err := s.commit(n.Right, batch, nodeCount, destructive, childPath, childBits, stats)
			if err != nil {
				return nil, err
			}
			n.RightHash = h
			n.RightEpoch = n.Right.Epoch()
			if destructive {
				n.Right = nil
			}
		} else if len(n.RightHash) == 0 {
			n.RightEpoch = 0
		}

		// Do not update epoch during commit; epoch reflects access pattern, not persistence.
		// Updating it here would cause Prune to skip nodes that have not been accessed.
		s.refreshInternalEpochMask(n)

		serializeStart := time.Now()
		data, err := n.Serialize()
		if stats != nil {
			stats.serializeNanos += time.Since(serializeStart).Nanoseconds()
		}
		if err != nil {
			return nil, err
		}
		nodeBytes = len(data)

		hashStart := time.Now()
		h := append([]byte{}, s.hasher.Hash(data)...)
		if stats != nil {
			stats.hashNanos += time.Since(hashStart).Nanoseconds()
		}
		n.SetHash(h)

		if batch != nil {
			persistStart := time.Now()
			if err := s.persistNode(batch, n, h, data, path, pathBits, destructive, stats); err != nil {
				if stats != nil {
					stats.persistNanos += time.Since(persistStart).Nanoseconds()
				}
				return nil, err
			}
			if stats != nil {
				stats.persistNanos += time.Since(persistStart).Nanoseconds()
			}
		}
		return h, nil

	case *LeafNode:
		// commit 只负责序列化当前节点；epoch 变化属于写入/剪枝语义，不属于落盘语义。
		serializeStart := time.Now()
		data, err := n.Serialize()
		if stats != nil {
			stats.serializeNanos += time.Since(serializeStart).Nanoseconds()
		}
		if err != nil {
			return nil, err
		}
		nodeBytes = len(data)
		hashStart := time.Now()
		h := append([]byte{}, s.hasher.Hash(data)...)
		if stats != nil {
			stats.hashNanos += time.Since(hashStart).Nanoseconds()
		}
		n.SetHash(h)

		if batch != nil {
			persistStart := time.Now()
			if err = s.persistNode(batch, n, h, data, path, pathBits, destructive, stats); err != nil {
				if stats != nil {
					stats.persistNanos += time.Since(persistStart).Nanoseconds()
				}
				return nil, err
			}
			if stats != nil {
				stats.persistNanos += time.Since(persistStart).Nanoseconds()
			}
		}
		return h, nil

	default:
		return nil, errors.New("unknown node type")
	}
}

func (s *Shard) persistNode(batch Batcher, node Node, hash, data, path []byte, pathBits int, destructive bool, stats *commitWorkStats) error {
	key := hash
	var (
		oldHash []byte
		oldPath []byte
		oldBits int
	)
	bookkeepStart := time.Now()
	if s.config != nil && s.config.UsePathStorage() {
		oldPath, oldBits = node.StoragePath()
		oldHash = node.OriginalHash()
		if s.config.PhysicalDelete && len(node.OriginalHash()) > 0 && (oldBits != pathBits || !bytes.Equal(oldPath, path)) {
			s.staleSet[string(pathNodeKey(s.id, oldPath, oldBits))] = struct{}{}
		}
		key = pathNodeKey(s.id, path, pathBits)
		delete(s.staleSet, string(key))
	}
	if stats != nil {
		stats.bookkeepNanos += time.Since(bookkeepStart).Nanoseconds()
	}
	batchPutStart := time.Now()
	if err := batch.Put(key, data); err != nil {
		if stats != nil {
			stats.batchPutNanos += time.Since(batchPutStart).Nanoseconds()
		}
		return err
	}
	if stats != nil {
		stats.batchPutNanos += time.Since(batchPutStart).Nanoseconds()
	}
	cacheStart := time.Now()
	if s.shouldWarmPersistedNodeCache(node, pathBits) {
		s.cacheNodeBlob(key, data)
	}
	if stats != nil {
		stats.cacheNanos += time.Since(cacheStart).Nanoseconds()
	}
	bookkeepStart = time.Now()
	if s.config != nil && s.config.UsePathStorage() && len(oldHash) > 0 && !bytes.Equal(oldHash, hash) {
		if persisted, ok := s.nodePaths[string(oldHash)]; ok &&
			persisted.bits == oldBits && bytes.Equal(persisted.path, oldPath) {
			delete(s.nodePaths, string(oldHash))
		}
	}
	node.SetDirty(false)
	node.SetOriginalHash(hash)
	if s.config != nil && s.config.UsePathStorage() {
		node.SetStoragePath(path, pathBits)
		if !destructive {
			s.registerNodePath(hash, path, pathBits)
			s.registerChildPaths(node, path, pathBits)
		}
	}
	if stats != nil {
		stats.bookkeepNanos += time.Since(bookkeepStart).Nanoseconds()
	}
	return nil
}

func (s *Shard) shouldWarmPersistedNodeCache(node Node, pathBits int) bool {
	if s.config != nil && s.config.UsePathStorage() {
		warmPathBits := s.config.NodeCacheWarmPathBits
		if warmPathBits < -1 {
			return false
		}
		if pathBits == 0 {
			return true
		}
		if _, ok := node.(*InternalNode); !ok {
			return false
		}
		if warmPathBits == 0 {
			warmPathBits = DefaultNodeCacheWarmPathBits
		}
		if warmPathBits < 0 {
			return false
		}
		return pathBits <= warmPathBits
	}
	return true
}

func (s *Shard) updateEpoch(node Node) {
	if node == nil {
		return
	}

	global := s.globalEpochBit()

	switch n := node.(type) {
	case *LeafNode:
		n.epoch = global

	case *InternalNode:
		n.epoch = (n.epoch &^ epochTimeBit) | (global & epochTimeBit)
	}
}

func (s *Shard) subtreeEpochMask(node Node) (byte, bool) {
	switch n := node.(type) {
	case nil:
		return 0, true
	case *LeafNode:
		return leafEpochMask(n.Epoch()), true
	case *ArchiveBucketNode:
		return 0, true
	case *InternalNode:
		if mask, ok := storedSubtreeEpochMask(n.Epoch()); ok {
			return mask, true
		}
		return s.computeInternalEpochMask(n)
	default:
		return 0, false
	}
}

func (s *Shard) subtreeArchivePresence(node Node) (bool, bool) {
	switch n := node.(type) {
	case nil:
		return false, true
	case *LeafNode:
		return false, true
	case *ArchiveBucketNode:
		return true, true
	case *InternalNode:
		if present, ok := storedSubtreeArchivePresence(n.Epoch()); ok {
			return present, true
		}
		return s.computeInternalArchivePresence(n)
	default:
		return false, false
	}
}

func (s *Shard) childEpochMask(node Node, epoch byte, hasHash bool) (byte, bool) {
	if node != nil {
		return s.subtreeEpochMask(node)
	}
	if !hasHash {
		return 0, true
	}
	if mask, ok := storedSubtreeEpochMask(epoch); ok {
		return mask, true
	}
	return 0, false
}

func (s *Shard) childArchivePresence(node Node, epoch byte, hasHash bool) (bool, bool) {
	if node != nil {
		return s.subtreeArchivePresence(node)
	}
	if !hasHash {
		return false, true
	}
	if present, ok := storedSubtreeArchivePresence(epoch); ok {
		return present, true
	}
	return false, false
}

func (s *Shard) computeInternalEpochMask(n *InternalNode) (byte, bool) {
	leftMask, leftOK := s.childEpochMask(n.Left, n.LeftEpoch, len(n.LeftHash) > 0)
	rightMask, rightOK := s.childEpochMask(n.Right, n.RightEpoch, len(n.RightHash) > 0)
	if !leftOK || !rightOK {
		return 0, false
	}
	return leftMask | rightMask, true
}

func (s *Shard) computeInternalArchivePresence(n *InternalNode) (bool, bool) {
	if len(n.StubList) > 0 {
		return true, true
	}
	leftPresent, leftOK := s.childArchivePresence(n.Left, n.LeftEpoch, len(n.LeftHash) > 0)
	rightPresent, rightOK := s.childArchivePresence(n.Right, n.RightEpoch, len(n.RightHash) > 0)
	if !leftOK || !rightOK {
		return false, false
	}
	return leftPresent || rightPresent, true
}

func (s *Shard) refreshInternalEpochMask(n *InternalNode) {
	mask, ok := s.computeInternalEpochMask(n)
	if !ok {
		n.epoch = clearStoredSubtreeEpochMask(n.epoch)
	} else {
		n.epoch = setStoredSubtreeEpochMask(n.epoch, mask)
	}
	present, ok := s.computeInternalArchivePresence(n)
	if !ok {
		n.epoch = clearStoredSubtreeArchivePresence(n.epoch)
		return
	}
	n.epoch = setStoredSubtreeArchivePresence(n.epoch, present)
}

func (s *Shard) shrinkLocal(n *InternalNode) Node {
	var remaining Node
	var remainingBit byte
	var remainingHash []byte

	childCount := 0
	if n.Left != nil || len(n.LeftHash) > 0 {
		childCount++
		remaining = n.Left
		remainingHash = n.LeftHash
		remainingBit = 0
	}
	if n.Right != nil || len(n.RightHash) > 0 {
		childCount++
		remaining = n.Right
		remainingHash = n.RightHash
		remainingBit = 1
	}

	// 如果没有或者有多个主树子节点，无法收缩。
	if childCount != 1 {
		return n
	}

	// A node with side-mounted archive buckets must remain reachable at its
	// current path. Merging it into the only hot child would make buckets from
	// the pruned sibling unreachable from the parent traversal path.
	if len(n.StubList) > 0 {
		return n
	}

	if remaining == nil && len(remainingHash) > 0 {
		var err error
		remaining, err = s.loadChildNode(n, remainingBit, remainingHash)
		if err != nil {
			return n // 无法加载，降级
		}
	}

	// 场景 1：如果当前节点含有归档桶 (StubList)，则绝不能完全压缩掉。
	// 我们只能将其路径延展，维持其作为 InternalNode 的容器角色。
	if len(n.StubList) > 0 {
		switch child := remaining.(type) {
		case *InternalNode:
			// 合并路径并提升子节点的桶
			newP, newB := s.concatPath(n.Path, n.PathBits, remainingBit, child.Path, child.PathBits)
			s.moveStubs(child, n, remainingBit) // 注意：这里是将 child 的桶提升到 n
			n.Path, n.PathBits = newP, newB
			n.Left, n.LeftHash = child.Left, child.LeftHash
			n.Right, n.RightHash = child.Right, child.RightHash
			n.SetDirty(true)
			s.refreshInternalEpochMask(n)
			return n

		case *LeafNode:
			// 路径延长以覆盖叶子，但保持 n 作为容器以承载 StubList
			newP, newB := s.concatPath(n.Path, n.PathBits, remainingBit, child.Path, child.PathBits)
			n.Path, n.PathBits = newP, newB
			if remainingBit == 0 {
				n.Left, n.LeftHash = child, nil
				n.Right, n.RightHash = nil, nil
			} else {
				n.Left, n.LeftHash = nil, nil
				n.Right, n.RightHash = child, nil
			}
			child.Path, child.PathBits = nil, 0
			n.SetDirty(true)
			s.refreshInternalEpochMask(n)
			return n
		}
	}

	// 场景 2：没有归档桶的纯主树合并。可以将 InternalNode 完全压缩掉。
	if inChild, ok := remaining.(*InternalNode); ok {
		newCP, newCB := s.concatPath(n.Path, n.PathBits, remainingBit, inChild.Path, inChild.PathBits)
		inChild.Path, inChild.PathBits = newCP, newCB
		inChild.SetDirty(true)
		s.markPersistedNodeStale(inChild)
		s.refreshInternalEpochMask(inChild)
		return inChild
	}

	if leaf, ok := remaining.(*LeafNode); ok {
		newLP, newLB := s.concatPath(n.Path, n.PathBits, remainingBit, leaf.Path, leaf.PathBits)
		leaf.Path, leaf.PathBits = newLP, newLB
		leaf.SetDirty(true)
		s.markPersistedNodeStale(leaf)
		return leaf
	}

	return n
}

// moveStubs 将 n 上的桶移动到其子节点 child 上。
func (s *Shard) shrinkPromote(n *InternalNode) (Node, []*ArchiveBucketNode) {
	var remaining Node
	var remainingBit byte
	var remainingHash []byte

	childCount := 0
	if n.Left != nil || len(n.LeftHash) > 0 {
		childCount++
		remaining = n.Left
		remainingHash = n.LeftHash
		remainingBit = 0
	}
	if n.Right != nil || len(n.RightHash) > 0 {
		childCount++
		remaining = n.Right
		remainingHash = n.RightHash
		remainingBit = 1
	}

	if childCount > 1 {
		return n, nil
	}
	if childCount == 0 {
		if len(n.StubList) == 0 {
			return n, nil
		}
		promoted := n.StubList
		n.StubList = nil
		s.markPersistedNodeStale(n)
		return nil, promoted
	}

	if remaining == nil && len(remainingHash) > 0 {
		var err error
		remaining, err = s.loadChildNode(n, remainingBit, remainingHash)
		if err != nil {
			return n, nil
		}
	}

	promoted := n.StubList
	if len(promoted) > 0 {
		n.StubList = nil
		s.markPersistedNodeStale(n)
	}

	if inChild, ok := remaining.(*InternalNode); ok {
		newCP, newCB := s.concatPath(n.Path, n.PathBits, remainingBit, inChild.Path, inChild.PathBits)
		inChild.Path, inChild.PathBits = newCP, newCB
		inChild.SetDirty(true)
		s.markPersistedNodeStale(inChild)
		s.refreshInternalEpochMask(inChild)
		return inChild, promoted
	}

	if leaf, ok := remaining.(*LeafNode); ok {
		newLP, newLB := s.concatPath(n.Path, n.PathBits, remainingBit, leaf.Path, leaf.PathBits)
		leaf.Path, leaf.PathBits = newLP, newLB
		leaf.SetDirty(true)
		s.markPersistedNodeStale(leaf)
		return leaf, promoted
	}

	if len(promoted) > 0 {
		n.StubList = promoted
	}
	return n, nil
}

func (s *Shard) moveStubs(n *InternalNode, child *InternalNode, bit byte) {
	if len(n.StubList) > 0 {
		s.attachStubs(child, n.StubList)
		n.StubList = nil // 清空
	}
}

// stripPrefix 从原路径中剥离指定的入口前缀。
func (s *Shard) stripPrefix(path []byte, bits int, ignoreBit byte, prefix []byte, prefixBits int) ([]byte, int) {
	// 在 V16.Final 中，如果 bits < prefixBits，说明前缀不匹配或非法。
	if bits < prefixBits {
		return nil, 0
	}
	return s.shiftBits(path, bits, prefixBits, nil), bits - prefixBits
}

func (s *Shard) distributeStubs(n *InternalNode, parent *InternalNode, prefix []byte, prefixBits int, bit byte) {
	if len(n.StubList) > 0 {
		s.attachStubs(parent, n.StubList)
		n.StubList = nil
	}
}
