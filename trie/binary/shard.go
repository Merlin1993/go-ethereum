package binary

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/binary/cuckoo"
	"github.com/ethereum/go-ethereum/trie/binary/ecmh"
)

var inlineValueMarker = []byte{'B', 'I', 'V', '1'}

func encodeInlineValue(value []byte) []byte {
	ref := make([]byte, len(inlineValueMarker)+len(value))
	copy(ref, inlineValueMarker)
	copy(ref[len(inlineValueMarker):], value)
	return ref
}

func decodeInlineValue(ref []byte) ([]byte, bool) {
	if len(ref) == common.HashLength || !bytes.HasPrefix(ref, inlineValueMarker) {
		return nil, false
	}
	return common.CopyBytes(ref[len(inlineValueMarker):]), true
}

// Shard 表示一个二叉 Merkle Patricia Trie 的分片（子树）。
type Shard struct {
	mu       sync.RWMutex // Protects root, rootHash, and staleSet
	root     Node
	db       KVStore
	hasher   Hasher
	pruning  bool
	staleSet map[string]struct{} // Set of hashes as string keys (for pruning)
	rootHash []byte              // Last persisted root hash for lazy loading

	// Shard specific state
	id       int
	isPruned bool // "本年度已剪枝 / 未剪枝"

	// Reference to global config
	config         *Config
	globalEpochBit func() byte

	// scratch buffer for temporary bit operations
	scratch []byte

	// Internal state for ECMH and Filter operations
	ecmh     *ecmh.Committer
	stats    *TrieStats
	statsMut sync.Mutex

	// Pending archive data to be written to ArchiveDB during commit
	// Key is the bucket hash
	pendingArchives map[string][]byte

	// Pending appends to existing buckets. Key is the NEW bucket metadata hash.
	pendingAppends map[string]appendTask

	// Pending deletes from existing buckets. Key is the NEW bucket metadata hash.
	pendingDeletes map[string]deleteTask

	// Pending whole archive data deletes. Key is the obsolete bucket hash.
	pendingArchiveDeletes map[string]int

	// Value writes are staged until commit. When ArchiveDB is configured they
	// are written to the archive/value store; otherwise they fall back to stateDB.
	pendingValues       map[string][]byte
	pendingValueDeletes map[string]struct{}
	stagedValues        []map[string][]byte

	// Node pool for reusing internal and leaf nodes
	pool *NodePool
}

type appendTask struct {
	oldHash  []byte
	newItems []ArchivedKV
}

type deleteTask struct {
	oldHash     []byte
	deleteItems []ArchivedKV
}

func archiveDataKey(hash []byte) []byte {
	if len(hash) == 0 {
		return nil
	}
	key := make([]byte, len(hash)+1)
	copy(key, hash)
	key[len(hash)] = 0x01
	return key
}

func valueDataKey(hash []byte) []byte {
	if len(hash) == 0 {
		return nil
	}
	key := make([]byte, len(hash)+1)
	copy(key, hash)
	key[len(hash)] = 0x02
	return key
}

// NewShard 创建一个新的 Shard（若提供 rootHash 则从 DB 加载根节点）。
func NewShard(id int, db KVStore, hasher Hasher, config *Config, rootHash []byte, pruning bool, globalEpochBit func() byte) (*Shard, error) {
	s := &Shard{
		id:                    id,
		db:                    db,
		hasher:                hasher,
		pruning:               pruning,
		staleSet:              make(map[string]struct{}),
		config:                config,
		globalEpochBit:        globalEpochBit,
		isPruned:              false,
		scratch:               make([]byte, 128),
		ecmh:                  ecmh.New(),
		stats:                 &TrieStats{},
		pendingArchives:       make(map[string][]byte),
		pendingAppends:        make(map[string]appendTask),
		pendingDeletes:        make(map[string]deleteTask),
		pendingArchiveDeletes: make(map[string]int),
		pendingValues:         make(map[string][]byte),
		pendingValueDeletes:   make(map[string]struct{}),
		pool:                  NewNodePool(),
	}
	if len(rootHash) > 0 {
		node, err := s.loadNode(rootHash)
		if err != nil {
			return nil, err
		}
		s.root = node
	}
	return s, nil
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
	s.pendingValues = make(map[string][]byte)
	s.pendingValueDeletes = make(map[string]struct{})
	s.stagedValues = nil
}

// loadNode 根据哈希从 DB 读取并反序列化节点。
func (s *Shard) loadNode(hash []byte) (Node, error) {
	if len(hash) == 0 {
		return nil, nil
	}
	data, err := s.db.Get(hash)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("node not found: hash %x in shard %d", hash, s.id)
	}
	node, err := DeserializeNode(data)
	if err != nil {
		return nil, err
	}
	node.SetHash(hash)
	node.SetOriginalHash(hash) // 记录节点当前持久化版本，用于之后修剪
	return node, nil
}

func (s *Shard) getBucketData(hash []byte) ([]byte, error) {
	if data, ok := s.pendingArchives[string(hash)]; ok {
		// fmt.Printf("Shard %d: Found %x in pendingArchives\n", s.id, hash)
		return data, nil
	}
	if task, ok := s.pendingAppends[string(hash)]; ok {
		// 加载旧数据并追加
		if bytes.Equal(task.oldHash, hash) {
			return nil, errors.New("infinite recursion in getBucketData (append)")
		}
		oldData, err := s.getBucketData(task.oldHash)
		if err != nil {
			return nil, err
		}
		items, err := s.deserializeArchivedKV(oldData)
		if err != nil {
			return nil, err
		}
		items = append(items, task.newItems...)
		return s.serializeArchivedKV(items)
	}
	if task, ok := s.pendingDeletes[string(hash)]; ok {
		// 加载旧数据并过滤删除项
		if bytes.Equal(task.oldHash, hash) {
			return nil, errors.New("infinite recursion in getBucketData (delete)")
		}
		oldData, err := s.getBucketData(task.oldHash)
		if err != nil {
			return nil, err
		}
		items, err := s.deserializeArchivedKV(oldData)
		if err != nil {
			return nil, err
		}
		newItems := make([]ArchivedKV, 0, len(items))
		for _, it := range items {
			found := false
			for _, del := range task.deleteItems {
				if it.SuffixBits == del.SuffixBits && bytes.Equal(it.Suffix, del.Suffix) {
					found = true
					break
				}
			}
			if !found {
				newItems = append(newItems, it)
			}
		}
		return s.serializeArchivedKV(newItems)
	}
	data, err := s.getStoredBucketData(hash)
	if err == nil {
		s.statsMut.Lock()
		s.stats.ArchiveReadCount++
		s.statsMut.Unlock()
	}
	return data, err
}

func (s *Shard) getStoredBucketData(hash []byte) ([]byte, error) {
	if s.config.ArchiveDB != nil {
		return s.config.ArchiveDB.GetBucket(archiveDataKey(hash))
	}
	return s.db.Get(archiveDataKey(hash))
}

func (s *Shard) HasPendingArchiveWrites() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pendingArchives) > 0 || len(s.pendingAppends) > 0 || len(s.pendingDeletes) > 0 || len(s.pendingArchiveDeletes) > 0
}

func (s *Shard) markArchiveDataDelete(hash []byte, oldSize int) {
	if len(hash) == 0 {
		return
	}
	key := string(hash)
	if _, ok := s.pendingArchives[key]; ok {
		delete(s.pendingArchives, key)
		return
	}
	if task, ok := s.pendingAppends[key]; ok {
		delete(s.pendingAppends, key)
		if !bytes.Equal(task.oldHash, hash) {
			s.markArchiveDataDelete(task.oldHash, -1)
		}
		return
	}
	if task, ok := s.pendingDeletes[key]; ok {
		delete(s.pendingDeletes, key)
		if !bytes.Equal(task.oldHash, hash) {
			s.markArchiveDataDelete(task.oldHash, -1)
		}
		return
	}
	if s.pendingArchiveDeletes == nil {
		s.pendingArchiveDeletes = make(map[string]int)
	}
	if prev, ok := s.pendingArchiveDeletes[key]; !ok || (prev < 0 && oldSize >= 0) {
		s.pendingArchiveDeletes[key] = oldSize
	}
}

func (s *Shard) attachStubs(parent *InternalNode, stubs []*ArchiveBucketNode) {
	if parent == nil || len(stubs) == 0 {
		return
	}
	s.markPersistedNodeStale(parent)
	parent.StubList = append(parent.StubList, stubs...)
	s.compactStubList(parent)
	parent.SetDirty(true)
}

func (s *Shard) compactStubList(parent *InternalNode) {
	if parent == nil || len(parent.StubList) < 2 {
		return
	}
	limit := s.config.ResolveArchiveBucketSize()
	if limit <= 0 {
		limit = int(^uint(0) >> 1)
	}

	stubs := make([]*ArchiveBucketNode, 0, len(parent.StubList))
	changed := false
	for _, bucket := range parent.StubList {
		if bucket == nil {
			changed = true
			continue
		}
		stubs = append(stubs, bucket)
	}
	if len(stubs) < 2 {
		if changed {
			parent.StubList = stubs
			parent.SetDirty(true)
		}
		return
	}

	sort.SliceStable(stubs, func(i, j int) bool {
		return s.archiveBucketPathLess(stubs[i], stubs[j])
	})

	newList := make([]*ArchiveBucketNode, 0, len(stubs))
	for i := 0; i < len(stubs); {
		bucket := stubs[i]
		total := bucket.Count
		commonPath := s.prefixBits(bucket.Path, bucket.PathBits, nil)
		commonBits := bucket.PathBits
		end := i

		for j := i + 1; j < len(stubs); j++ {
			next := stubs[j]
			if total+next.Count > uint64(limit) {
				break
			}
			commonPath, commonBits = s.commonArchivePath(commonPath, commonBits, next.Path, next.PathBits)
			total += next.Count
			end = j
		}

		if end == i {
			newList = append(newList, bucket)
			i++
			continue
		}

		group := stubs[i : end+1]
		items := make([]ArchivedKV, 0, total)
		oldHashes := make([][]byte, 0, len(group))
		oldSizes := make([]int, 0, len(group))
		ok := true
		for _, oldBucket := range group {
			hash := s.ensureBucketHash(oldBucket)
			data, err := s.getBucketData(hash)
			if err != nil {
				ok = false
				break
			}
			bucketItems, err := s.deserializeArchivedKV(data)
			if err != nil {
				ok = false
				break
			}
			for _, item := range bucketItems {
				absPath, absBits := s.prependPath(item.Suffix, item.SuffixBits, oldBucket.Path, oldBucket.PathBits)
				suffix, suffixBits := s.stripPrefix(absPath, absBits, 0, commonPath, commonBits)
				items = append(items, ArchivedKV{
					Suffix:     suffix,
					SuffixBits: suffixBits,
					Value:      item.Value,
				})
			}
			oldHashes = append(oldHashes, common.CopyBytes(hash))
			oldSizes = append(oldSizes, len(data))
		}
		if !ok {
			newList = append(newList, group...)
			i = end + 1
			continue
		}

		merged := &ArchiveBucketNode{
			Path:     common.CopyBytes(commonPath),
			PathBits: commonBits,
			dirty:    true,
		}
		s.recomputeBucket(merged, items)
		for j := range group {
			if s.pruning && len(oldHashes[j]) > 0 {
				s.staleSet[string(oldHashes[j])] = struct{}{}
			}
			s.markArchiveDataDelete(oldHashes[j], oldSizes[j])
		}
		newList = append(newList, merged)
		changed = true
		i = end + 1
	}
	if changed {
		parent.StubList = newList
		parent.SetDirty(true)
	}
}

func (s *Shard) archiveBucketPathLess(a, b *ArchiveBucketNode) bool {
	if a == nil || b == nil {
		return b != nil
	}
	limit := a.PathBits
	if b.PathBits < limit {
		limit = b.PathBits
	}
	for i := 0; i < limit; i++ {
		abit := s.getBitFromBytes(a.Path, i)
		bbit := s.getBitFromBytes(b.Path, i)
		if abit != bbit {
			return abit < bbit
		}
	}
	return a.PathBits < b.PathBits
}

func (s *Shard) commonArchivePath(a []byte, aBits int, b []byte, bBits int) ([]byte, int) {
	limit := aBits
	if bBits < limit {
		limit = bBits
	}
	matched := 0
	for matched < limit {
		if s.getBitFromBytes(a, matched) != s.getBitFromBytes(b, matched) {
			break
		}
		matched++
	}
	return s.prefixBits(a, matched, nil), matched
}

func (s *Shard) stageValue(value []byte) []byte {
	return s.stageValueWithRef(crypto.Keccak256Hash(value).Bytes(), value)
}

func (s *Shard) stageValueForKey(key []byte, value []byte) []byte {
	if s.config != nil && s.config.InlineValueThreshold > 0 &&
		len(value) <= s.config.InlineValueThreshold &&
		len(value)+len(inlineValueMarker) <= 255 &&
		len(value)+len(inlineValueMarker) != common.HashLength {
		return encodeInlineValue(value)
	}
	if s.config != nil && s.config.DeleteOldValues {
		return s.stageValueWithRef(crypto.Keccak256Hash(key, value).Bytes(), value)
	}
	return s.stageValue(value)
}

func (s *Shard) stageValueWithRef(valueHash []byte, value []byte) []byte {
	if s.pendingValues == nil {
		s.pendingValues = make(map[string][]byte)
	}
	key := string(valueHash)
	if _, ok := s.pendingValues[key]; !ok {
		s.pendingValues[key] = common.CopyBytes(value)
	}
	delete(s.pendingValueDeletes, key)
	return valueHash
}

func (s *Shard) releaseValue(valueRef []byte) {
	if s.config == nil || !s.config.DeleteOldValues || len(valueRef) == 0 {
		return
	}
	if _, ok := decodeInlineValue(valueRef); ok {
		return
	}
	key := string(valueRef)
	if _, ok := s.pendingValues[key]; ok {
		delete(s.pendingValues, key)
	}
	if s.pendingValueDeletes == nil {
		s.pendingValueDeletes = make(map[string]struct{})
	}
	s.pendingValueDeletes[key] = struct{}{}
}

func (s *Shard) getValue(valueHash []byte) ([]byte, error) {
	if len(valueHash) == 0 {
		return nil, ErrNodeNotFound
	}
	if value, ok := decodeInlineValue(valueHash); ok {
		return value, nil
	}
	key := string(valueHash)
	if val, ok := s.pendingValues[key]; ok {
		return val, nil
	}
	for i := len(s.stagedValues) - 1; i >= 0; i-- {
		if val, ok := s.stagedValues[i][key]; ok {
			return val, nil
		}
	}
	return s.getStoredValue(valueHash)
}

func (s *Shard) commitPendingValues(batch Batcher) error {
	if len(s.pendingValues) == 0 && len(s.pendingValueDeletes) == 0 {
		return nil
	}
	values := s.pendingValues
	if err := s.commitValueStore(batch, values, s.pendingValueDeletes); err != nil {
		return err
	}
	s.stagedValues = append(s.stagedValues, values)
	if len(s.stagedValues) > 2 {
		s.stagedValues = append([]map[string][]byte(nil), s.stagedValues[len(s.stagedValues)-2:]...)
	}
	s.pendingValues = make(map[string][]byte)
	s.pendingValueDeletes = make(map[string]struct{})
	return nil
}

func (s *Shard) getStoredValue(valueHash []byte) ([]byte, error) {
	dataKey := valueDataKey(valueHash)
	if s.config != nil && s.config.ArchiveDB != nil {
		if value, err := s.config.ArchiveDB.GetBucket(dataKey); err == nil && value != nil {
			return value, nil
		}
	}
	if value, err := s.db.Get(dataKey); err == nil && value != nil {
		return value, nil
	}
	if value, err := s.db.Get(valueHash); err == nil && value != nil {
		return value, nil
	}
	return nil, ErrNodeNotFound
}

func (s *Shard) commitValueStore(batch Batcher, values map[string][]byte, deletes map[string]struct{}) error {
	if s.config != nil && s.config.ArchiveDB != nil {
		if err := s.commitValuesToArchiveStore(values, deletes); err != nil {
			return err
		}
		if batch != nil {
			for h := range deletes {
				if err := batch.Delete(valueDataKey([]byte(h))); err != nil {
					return err
				}
				if err := batch.Delete([]byte(h)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if batch == nil {
		for h := range deletes {
			if err := s.db.Delete(valueDataKey([]byte(h))); err != nil {
				return err
			}
			if err := s.db.Delete([]byte(h)); err != nil {
				return err
			}
		}
		for h, value := range values {
			if err := s.db.Put(valueDataKey([]byte(h)), value); err != nil {
				return err
			}
		}
		return nil
	}
	for h := range deletes {
		if err := batch.Delete(valueDataKey([]byte(h))); err != nil {
			return err
		}
		if err := batch.Delete([]byte(h)); err != nil {
			return err
		}
	}
	for h, value := range values {
		if err := batch.Put(valueDataKey([]byte(h)), value); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) commitValuesToArchiveStore(values map[string][]byte, deletes map[string]struct{}) error {
	if batchStore, ok := s.config.ArchiveDB.(interface{ NewBatch() Batcher }); ok {
		batch := batchStore.NewBatch()
		defer batch.Reset()
		for h := range deletes {
			if err := batch.Delete(valueDataKey([]byte(h))); err != nil {
				return err
			}
		}
		for h, value := range values {
			if err := batch.Put(valueDataKey([]byte(h)), value); err != nil {
				return err
			}
		}
		return batch.Write()
	}
	for h := range deletes {
		if err := s.config.ArchiveDB.DeleteBucket(valueDataKey([]byte(h))); err != nil {
			return err
		}
	}
	for h, value := range values {
		if err := s.config.ArchiveDB.PutBucket(valueDataKey([]byte(h)), value); err != nil {
			return err
		}
	}
	return nil
}

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

	// [NEW] 自动赎回：如果命中归档桶，则自动将其移回热路径
	if fromArchive {
		s.Activate(key, val)
	}

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
			val, err := s.getValue(n.ValueHash)
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

	bucket.cacheMu.RLock()
	items := bucket.cachedItems
	bucket.cacheMu.RUnlock()

	if items == nil {
		bucket.cacheMu.Lock()
		if bucket.cachedItems == nil {
			bucketData, err := s.getBucketData(s.ensureBucketHash(bucket))
			if err == nil {
				itms, _ := s.deserializeArchivedKV(bucketData)
				if s.shouldCacheArchivedItems(len(itms)) {
					bucket.cachedItems = itms
				}
				items = itms
			}
		}
		if items == nil {
			items = bucket.cachedItems
		}
		bucket.cacheMu.Unlock()
	}

	if items != nil {
		innerDepth := bucket.PathBits
		keyBits := len(key) * 8
		for _, item := range items {
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
					val, err := s.getValue(item.Value)
					return val, true, err
				}
			}
		}
		s.statsMut.Lock()
		s.stats.FalsePositiveCount++
		s.statsMut.Unlock()
		atomic.AddInt64(&common.BinaryCycleFPCount, 1)
		atomic.AddInt64(&common.BinaryTrieFPInBlock, 1)
		common.BinaryStatsMu.Lock()
		common.BinaryFPDistribution = append(common.BinaryFPDistribution, int64(len(items)))
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

func (s *Shard) Put(key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.root == nil && len(s.rootHash) > 0 {
		var err error
		s.root, err = s.loadNode(s.rootHash)
		if err != nil {
			return err
		}
	}
	s.removeArchivedVersionForWrite(key)
	valHash := s.stageValueForKey(key, value)

	// Shard start depth
	newRoot, err := s.insert(s.root, key, s.config.ShardDepth, valHash)
	if err != nil {
		return err
	}
	s.root = newRoot
	return nil
}

func (s *Shard) removeArchivedVersionForWrite(key []byte) bool {
	if s.root == nil {
		return false
	}
	removed := s.removeFromStubList(s.root, key, s.config.ShardDepth)
	if removed {
		s.cleanupRootAfterArchiveDelete()
	}
	return removed
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
	if !s.pruning || node == nil {
		return
	}
	if h := node.OriginalHash(); len(h) > 0 {
		s.staleSet[string(h)] = struct{}{}
		return
	}
	if h := node.Hash(); len(h) > 0 {
		s.staleSet[string(h)] = struct{}{}
	}
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
	s.removeArchivedVersionForWrite(key)

	// 2. 执行常规插入
	valHash := s.stageValueForKey(key, value)

	var err error
	s.root, err = s.insert(s.root, key, s.config.ShardDepth, valHash)
	return err
}

func (s *Shard) removeFromStubList(node Node, key []byte, depth int) bool {
	if node == nil {
		return false
	}

	switch n := node.(type) {
	case *InternalNode:
		// [定点移除]：只需检查当前节点侧挂的 StubList 是否包含匹配的前缀
		for i := 0; i < len(n.StubList); i++ {
			bucket := n.StubList[i]
			matched := s.commonPrefixLen(bucket.Path, bucket.PathBits, key, 0)
			if matched == bucket.PathBits {
				bucket.cacheMu.RLock()
				items := bucket.cachedItems
				bucket.cacheMu.RUnlock()

				if items == nil {
					bucketData, err := s.getBucketData(s.ensureBucketHash(bucket))
					if err != nil {
						continue
					}
					items, _ = s.deserializeArchivedKV(bucketData)
				}

				innerDepth := bucket.PathBits
				keyBits := len(key) * 8
				for _, item := range items {
					if innerDepth+item.SuffixBits == keyBits && s.suffixMatches(key, innerDepth, item.Suffix, item.SuffixBits) {
						// 命中：执行盲删除
						oldHash := s.ensureBucketHash(bucket)
						oldSize := -1
						if bucketData, err := s.getBucketData(oldHash); err == nil {
							oldSize = len(bucketData)
						}

						s.releaseValue(item.Value)
						s.blindDeleteFromBucket(bucket, []ArchivedKV{item})

						if bucket.Count == 0 {
							s.markArchiveDataDelete(oldHash, oldSize)
							n.StubList = append(n.StubList[:i], n.StubList[i+1:]...)
						}
						n.SetDirty(true)
						return true
					}
				}
			}
		}

		// 按路径下探，不扫全树
		if n.PathBits > 0 {
			matched := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
			if matched != n.PathBits {
				return false // 路径不通，肯定不在这个子树下的 StubList
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
			loaded, _ := s.loadNode(nextHash)
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
			if s.removeFromStubList(next, key, depth+1) {
				if bucket, ok := next.(*ArchiveBucketNode); ok && bucket.Count == 0 {
					s.markArchiveDataDelete(s.ensureBucketHash(bucket), -1)
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
				return true
			}
		}
	case *ArchiveBucketNode:
		// 如果根节点直接就是桶
		matched := s.commonPrefixLen(n.Path, n.PathBits, key, 0)
		if matched == n.PathBits {
			n.cacheMu.RLock()
			items := n.cachedItems
			n.cacheMu.RUnlock()

			if items == nil {
				bucketData, err := s.getBucketData(n.Hash())
				if err != nil {
					return false
				}
				items, _ = s.deserializeArchivedKV(bucketData)
			}

			innerDepth := n.PathBits
			keyBits := len(key) * 8
			for _, item := range items {
				if innerDepth+item.SuffixBits == keyBits && s.suffixMatches(key, innerDepth, item.Suffix, item.SuffixBits) {
					oldHash := s.ensureBucketHash(n)
					oldSize := -1
					if bucketData, err := s.getBucketData(oldHash); err == nil {
						oldSize = len(bucketData)
					}

					s.releaseValue(item.Value)
					s.blindDeleteFromBucket(n, []ArchivedKV{item})
					if n.Count == 0 {
						s.markArchiveDataDelete(oldHash, oldSize)
						// 此处无法直接移除 node，需由调用者处理 s.root = nil
					}
					return true
				}
			}
		}
	}
	return false
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
			if !bytes.Equal(n.ValueHash, valueHash) {
				s.releaseValue(n.ValueHash)
			}
			n.ValueHash = valueHash
			n.SetDirty(true)
			s.updateEpoch(n)
			if s.pruning && len(n.OriginalHash()) > 0 {
				s.staleSet[string(n.OriginalHash())] = struct{}{}
			}
			return n, nil
		}

		// [Robust] 绝不分叉已达 256 位极限的节点内容。
		if matchBits == n.PathBits {
			if !bytes.Equal(n.ValueHash, valueHash) {
				s.releaseValue(n.ValueHash)
			}
			n.ValueHash = valueHash
			n.PathBits = len(key)*8 - depth
			n.Path = s.getSuffix(key, depth, nil)
			n.SetDirty(true)
			s.updateEpoch(n)
			return n, nil
		}

		// 分裂逻辑... (为了篇幅，这里应该包含完整的 insert 分裂逻辑)
		if s.pruning && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}

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
		if s.pruning && !n.dirty && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
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
				n.Left, err = s.loadNode(n.LeftHash)
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
				n.Right, err = s.loadNode(n.RightHash)
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
		if n.PathBits > depth {
			if s.getBitFromBytes(n.Path, depth) == 0 {
				parent.Left = n
				parent.LeftEpoch = n.Epoch()
			} else {
				parent.Right = n
				parent.RightEpoch = n.Epoch()
			}
		} else {
			s.attachStubs(parent, []*ArchiveBucketNode{n})
		}
		parent.SetDirty(true)
		s.refreshInternalEpochMask(parent)
		return s.insert(parent, key, depth, valueHash)

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
		if s.removeFromStubList(s.root, key, s.config.ShardDepth) {
			s.cleanupRootAfterArchiveDelete()
		}
	}
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
			s.releaseValue(n.ValueHash)
			if s.pruning && len(n.OriginalHash()) > 0 {
				s.staleSet[string(n.OriginalHash())] = struct{}{}
			}
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
			loaded, _ := s.loadNode(childHash)
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

		if s.pruning && !n.dirty && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
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
				if s.pruning && len(leaf.OriginalHash()) > 0 {
					s.staleSet[string(leaf.OriginalHash())] = struct{}{}
				}
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
		if !s.removeFromStubList(n, key, depth) {
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
	return s.commit(s.root, nil, &count, false)
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

// CommitToBatch 递归提交改动。
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
		return s.rootHash, nil
	}

	nodeCount := 0
	rootHash, err := s.commit(s.root, batch, &nodeCount, destructive)
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
	}
	return rootHash, nil
}

func (s *Shard) commitStaleDeletes(batch Batcher) error {
	if !s.pruning || batch == nil || len(s.staleSet) == 0 {
		return nil
	}
	for h := range s.staleSet {
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

func (s *Shard) forEach(node Node, prefix []byte, bits int, fn func(key, value []byte) bool) bool {
	if node == nil {
		return true
	}
	switch n := node.(type) {
	case *LeafNode:
		fullKey, _ := s.prependPath(n.Path, n.PathBits, prefix, bits)
		val, _ := s.getValue(n.ValueHash)
		return fn(fullKey, val)
	case *ArchiveBucketNode:
		data, err := s.getBucketData(s.ensureBucketHash(n))
		if err != nil {
			return true
		}
		kvs, _ := s.deserializeArchivedKV(data)
		for _, kv := range kvs {
			fullK, _ := s.prependPath(kv.Suffix, kv.SuffixBits, n.Path, n.PathBits)
			if !fn(fullK, kv.Value) {
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
			loaded, _ := s.loadNode(n.LeftHash)
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
			loaded, _ := s.loadNode(n.RightHash)
			if loaded != nil {
				if !s.forEach(loaded, rightPrefix, rightBits, fn) {
					return false
				}
			}
		}
		// Iterate over archived buckets
		for _, bucket := range n.StubList {
			data, err := s.getBucketData(s.ensureBucketHash(bucket))
			if err == nil {
				kvs, _ := s.deserializeArchivedKV(data)
				for _, kv := range kvs {
					// bucket.Path is the absolute path; kv.Suffix is relative to bucket.Path
					fullK, _ := s.prependPath(kv.Suffix, kv.SuffixBits, bucket.Path, bucket.PathBits)
					if !fn(fullK, kv.Value) {
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

func (s *Shard) commit(node Node, batch Batcher, nodeCount *int, destructive bool) ([]byte, error) {
	if node == nil {
		return nil, nil
	}

	if nodeCount != nil {
		*nodeCount++
	}

	if !node.IsDirty() {
		return node.Hash(), nil
	}

	switch n := node.(type) {
	case *ArchiveBucketNode:
		h := n.Hash()

		data, err := n.Serialize()
		if err != nil {
			return nil, err
		}
		if len(h) == 0 {
			h = append([]byte{}, s.hasher.Hash(data)...)
			n.SetHash(h)
		}

		if batch != nil {
			n.SetDirty(false)
			n.SetOriginalHash(h)
			if err := batch.Put(h, data); err != nil {
				return nil, err
			}
		}

		return h, nil

	case *InternalNode:
		if n.Left != nil {
			h, err := s.commit(n.Left, batch, nodeCount, destructive)
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
			h, err := s.commit(n.Right, batch, nodeCount, destructive)
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

		data, err := n.Serialize()
		if err != nil {
			return nil, err
		}

		h := append([]byte{}, s.hasher.Hash(data)...)
		n.SetHash(h)

		if batch != nil {
			n.SetDirty(false)
			n.SetOriginalHash(h)
			if err := batch.Put(h, data); err != nil {
				return nil, err
			}
		}
		return h, nil

	case *LeafNode:
		// [FIX] Do NOT updateEpoch during commit — same reason as InternalNode.
		data, err := n.Serialize()
		if err != nil {
			return nil, err
		}
		h := append([]byte{}, s.hasher.Hash(data)...)
		n.SetHash(h)

		if batch != nil {
			n.SetDirty(false)
			n.SetOriginalHash(h)
			err = batch.Put(h, data)
			if err != nil {
				return nil, err
			}
		}
		return h, nil

	default:
		return nil, errors.New("unknown node type")
	}
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

func (s *Shard) computeInternalEpochMask(n *InternalNode) (byte, bool) {
	leftMask, leftOK := s.childEpochMask(n.Left, n.LeftEpoch, len(n.LeftHash) > 0)
	rightMask, rightOK := s.childEpochMask(n.Right, n.RightEpoch, len(n.RightHash) > 0)
	if !leftOK || !rightOK {
		return 0, false
	}
	return leftMask | rightMask, true
}

func (s *Shard) refreshInternalEpochMask(n *InternalNode) {
	mask, ok := s.computeInternalEpochMask(n)
	if !ok {
		n.epoch = clearStoredSubtreeEpochMask(n.epoch)
		return
	}
	n.epoch = setStoredSubtreeEpochMask(n.epoch, mask)
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
		remaining, err = s.loadNode(remainingHash)
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
		if s.pruning && len(inChild.OriginalHash()) > 0 {
			s.staleSet[string(inChild.OriginalHash())] = struct{}{}
		}
		s.refreshInternalEpochMask(inChild)
		return inChild
	}

	if leaf, ok := remaining.(*LeafNode); ok {
		newLP, newLB := s.concatPath(n.Path, n.PathBits, remainingBit, leaf.Path, leaf.PathBits)
		leaf.Path, leaf.PathBits = newLP, newLB
		leaf.SetDirty(true)
		if s.pruning && len(leaf.OriginalHash()) > 0 {
			s.staleSet[string(leaf.OriginalHash())] = struct{}{}
		}
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
		if s.pruning && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}
		return nil, promoted
	}

	if remaining == nil && len(remainingHash) > 0 {
		var err error
		remaining, err = s.loadNode(remainingHash)
		if err != nil {
			return n, nil
		}
	}

	promoted := n.StubList
	if len(promoted) > 0 {
		n.StubList = nil
		if s.pruning && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}
	}

	if inChild, ok := remaining.(*InternalNode); ok {
		newCP, newCB := s.concatPath(n.Path, n.PathBits, remainingBit, inChild.Path, inChild.PathBits)
		inChild.Path, inChild.PathBits = newCP, newCB
		inChild.SetDirty(true)
		if s.pruning && len(inChild.OriginalHash()) > 0 {
			s.staleSet[string(inChild.OriginalHash())] = struct{}{}
		}
		s.refreshInternalEpochMask(inChild)
		return inChild, promoted
	}

	if leaf, ok := remaining.(*LeafNode); ok {
		newLP, newLB := s.concatPath(n.Path, n.PathBits, remainingBit, leaf.Path, leaf.PathBits)
		leaf.Path, leaf.PathBits = newLP, newLB
		leaf.SetDirty(true)
		if s.pruning && len(leaf.OriginalHash()) > 0 {
			s.staleSet[string(leaf.OriginalHash())] = struct{}{}
		}
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

func (s *Shard) FlushArchives() error {
	flushStart := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	pendingArchives := len(s.pendingArchives)
	pendingAppends := len(s.pendingAppends)
	pendingDeletes := len(s.pendingDeletes)
	pendingArchiveDeletes := len(s.pendingArchiveDeletes)
	defer func() {
		recordShardFlushDiagnostics(time.Since(flushStart).Nanoseconds(), pendingArchives, pendingAppends, pendingDeletes, pendingArchiveDeletes)
	}()
	for h, data := range s.pendingArchives {
		if err := s.putBucketDataWithOldSize([]byte(h), data, 0); err != nil {
			return err
		}
	}
	for h, task := range s.pendingAppends {
		oldData, err := s.getBucketData(task.oldHash)
		if err != nil {
			return err
		}
		items, err := s.deserializeArchivedKV(oldData)
		if err != nil {
			return err
		}
		items = append(items, task.newItems...)
		if limit := s.config.ResolveArchiveBucketSize(); limit > 0 && len(items) > limit {
			return fmt.Errorf("archive bucket append overflow: items=%d limit=%d", len(items), limit)
		}
		data, err := s.serializeArchivedKV(items)
		if err != nil {
			return err
		}
		if err := s.putBucketDataWithOldSize([]byte(h), data, 0); err != nil {
			return err
		}
		if err := s.deleteBucketDataWithOldSize(task.oldHash, len(oldData)); err != nil {
			return err
		}
	}
	for h, task := range s.pendingDeletes {
		hash := []byte(h)
		oldData, err := s.getBucketData(task.oldHash)
		if err != nil {
			return err
		}
		items, err := s.deserializeArchivedKV(oldData)
		if err != nil {
			return err
		}
		newItems := make([]ArchivedKV, 0, len(items))
		for _, it := range items {
			found := false
			for _, del := range task.deleteItems {
				if it.SuffixBits == del.SuffixBits && bytes.Equal(it.Suffix, del.Suffix) {
					found = true
					break
				}
			}
			if !found {
				newItems = append(newItems, it)
			}
		}
		if len(newItems) > 0 {
			data, err := s.serializeArchivedKV(newItems)
			if err != nil {
				return err
			}
			if err := s.putBucketDataWithOldSize(hash, data, 0); err != nil {
				return err
			}
		} else if err := s.deleteBucketDataWithOldSize(hash, 0); err != nil {
			return err
		}
		if err := s.deleteBucketDataWithOldSize(task.oldHash, len(oldData)); err != nil {
			return err
		}
	}
	for h, oldSize := range s.pendingArchiveDeletes {
		if err := s.deleteBucketDataWithOldSize([]byte(h), oldSize); err != nil {
			return err
		}
	}
	s.pendingArchives = make(map[string][]byte)
	s.pendingAppends = make(map[string]appendTask)
	s.pendingDeletes = make(map[string]deleteTask)
	s.pendingArchiveDeletes = make(map[string]int)
	return nil
}

func (s *Shard) putBucketData(hash []byte, data []byte) error {
	return s.putBucketDataWithOldSize(hash, data, -1)
}

func (s *Shard) putBucketDataWithOldSize(hash []byte, data []byte, oldSize int) error {
	dataKey := archiveDataKey(hash)
	if oldSize < 0 {
		oldSize = 0
		if oldData, err := s.getStoredBucketData(hash); err == nil {
			oldSize = len(oldData)
		}
	}
	if s.config.ArchiveDB != nil {
		if err := s.config.ArchiveDB.PutBucket(dataKey, data); err != nil {
			return err
		}
	} else {
		if err := s.db.Put(dataKey, data); err != nil {
			return err
		}
	}
	s.statsMut.Lock()
	s.stats.ArchiveStorageSize += int64(len(data) - oldSize)
	s.statsMut.Unlock()
	return nil
}

func (s *Shard) deleteBucketData(hash []byte) error {
	return s.deleteBucketDataWithOldSize(hash, -1)
}

func (s *Shard) deleteBucketDataWithOldSize(hash []byte, oldSize int) error {
	if len(hash) == 0 {
		return nil
	}
	dataKey := archiveDataKey(hash)
	if oldSize < 0 {
		oldSize = 0
		if oldData, err := s.getStoredBucketData(hash); err == nil {
			oldSize = len(oldData)
		}
	}
	if s.config.ArchiveDB != nil {
		if err := s.config.ArchiveDB.DeleteBucket(dataKey); err != nil {
			return err
		}
	} else {
		if err := s.db.Delete(dataKey); err != nil {
			return err
		}
	}
	if oldSize > 0 {
		s.statsMut.Lock()
		s.stats.ArchiveStorageSize -= int64(oldSize)
		s.statsMut.Unlock()
	}
	return nil
}
