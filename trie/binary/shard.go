package binary

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/binary/cuckoo"
	"github.com/ethereum/go-ethereum/trie/binary/ecmh"
)

// Shard 表示一个二叉 Merkle Patricia Trie 的分片（子树）。
type Shard struct {
	root     Node
	db       KVStore
	hasher   Hasher
	pruning  bool
	staleSet map[string]struct{} // 以哈希字节为键的集合（用于修剪）
	rootHash []byte              // 记录持久化后的根哈希，用于内存根丢失时重新加载

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

// NewShard 创建一个新的 Shard（若提供 rootHash 则从 DB 加载根节点）。
func NewShard(id int, db KVStore, hasher Hasher, config *Config, rootHash []byte, pruning bool, globalEpochBit func() byte) (*Shard, error) {
	s := &Shard{
		id:              id,
		db:              db,
		hasher:          hasher,
		pruning:         pruning,
		staleSet:        make(map[string]struct{}),
		config:          config,
		globalEpochBit:  globalEpochBit,
		isPruned:        false,
		scratch:         make([]byte, 128),
		ecmh:            ecmh.New(),
		stats:           &TrieStats{},
		pendingArchives: make(map[string][]byte),
		pendingAppends:  make(map[string]appendTask),
		pendingDeletes:  make(map[string]deleteTask),
		pool:            NewNodePool(),
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

// loadNode 根据哈希从 DB 读取并反序列化节点。
func (s *Shard) loadNode(hash []byte) (Node, error) {
	data, err := s.db.Get(hash)
	if data == nil {
		return nil, ErrNodeNotFound
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
	if s.config.ArchiveDB == nil {
		return nil, errors.New("archive db not set")
	}
	data, err := s.config.ArchiveDB.GetBucket(hash)
	return data, err
}

// Get 返回指定 key 的值哈希（不存在则返回 ErrNodeNotFound）。
func (s *Shard) Get(key []byte) ([]byte, error) {
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
		// 检查叶子后缀是否完全匹配 key 的剩余位
		matchLen := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
		if matchLen == n.PathBits && depth+matchLen == len(key)*8 {
			atomic.AddInt64(&common.BinaryHitCount, 1)
			val, err := s.db.Get(n.ValueHash)
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
		recoveryStart := time.Now()
		for i := len(n.StubList) - 1; i >= 0; i-- {
			bucket := n.StubList[i]
			matched := s.commonPrefixLen(bucket.Path, bucket.PathBits, key, depth)
			if matched == bucket.PathBits {
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

				if filter == nil {
					continue
				}

				innerDepth := depth + n.PathBits

				// 使用相对于桶起始位置的路径位进行布谷鸟过滤器查询
				// 注意：item.Suffix 是相对于 n 的。
				shardKey := s.getSuffix(key, innerDepth, nil)
				lookup := filter.Lookup(shardKey)
				if !lookup {
					continue
				}

				s.statsMut.Lock()
				s.stats.ArchiveReadCount++
				s.statsMut.Unlock()

				// 获取或反序列化桶数据
				bucket.cacheMu.RLock()
				items := bucket.cachedItems
				bucket.cacheMu.RUnlock()

				if items == nil {
					bucket.cacheMu.Lock()
					if bucket.cachedItems == nil {
						bucketData, err := s.getBucketData(bucket.Hash())
						if err == nil {
							itms, err := s.deserializeArchivedKV(bucketData)
							if err == nil {
								bucket.cachedItems = itms
							}
						}
					}
					items = bucket.cachedItems
					bucket.cacheMu.Unlock()
				}

				if items == nil {
					continue
				}

				keyBits := len(key) * 8
				for _, item := range items {
					if innerDepth+item.SuffixBits == keyBits {
						if s.suffixMatches(key, innerDepth, item.Suffix, item.SuffixBits) {
							// 命中归档项，记录耗时
							dur := time.Since(recoveryStart).Nanoseconds()
							atomic.AddInt64(&common.BinaryProofGenTime, dur)
							for {
								oldMax := atomic.LoadInt64(&common.BinaryProofGenTimeMax)
								if dur <= oldMax || atomic.CompareAndSwapInt64(&common.BinaryProofGenTimeMax, oldMax, dur) {
									break
								}
							}

							atomic.AddInt64(&common.BinaryMissExistentCount, 1)

							// [NEW] 验证存储挂出的 StubList 数据是否可信 (ECMH 验证)
							ok, vTime := s.verifyBucket(bucket)
							atomic.AddInt64(&common.BinaryProofVerifTime, vTime)
							if !ok {
								return nil, false, errors.New("archive bucket commitment verification failed")
							}

							val, err := s.db.Get(item.Value)
							return val, true, err
						}
					}
				}
				// 过滤器查询存在，但数据实际不存在 -> 假阳性
				atomic.AddInt64(&common.BinaryCycleFPCount, 1)
				atomic.AddInt64(&common.BinaryTrieFPInBlock, 1)

				common.BinaryStatsMu.Lock()
				common.BinaryFPDistribution = append(common.BinaryFPDistribution, int64(bucket.Count))
				common.BinaryStatsMu.Unlock()
			}
		}

		return nil, false, ErrNodeNotFound

	case *ArchiveBucketNode:
		return s.getFromBucket(n, key, depth)

	default:
		return nil, false, errors.New("unknown node type")
	}
}

func (s *Shard) getFromBucket(bucket *ArchiveBucketNode, key []byte, depth int) ([]byte, bool, error) {
	recoveryStart := time.Now()
	// 匹配位前缀
	matched := s.commonPrefixLen(bucket.Path, bucket.PathBits, key, depth)
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
		innerDepth := depth + bucket.PathBits
		shardKey := s.getSuffix(key, innerDepth, nil)
		if !filter.Lookup(shardKey) {
			return nil, false, ErrNodeNotFound
		}
	}

	bucket.cacheMu.RLock()
	items := bucket.cachedItems
	bucket.cacheMu.RUnlock()

	if items == nil {
		bucket.cacheMu.Lock()
		if bucket.cachedItems == nil {
			bucketData, err := s.getBucketData(bucket.Hash())
			if err == nil {
				itms, _ := s.deserializeArchivedKV(bucketData)
				bucket.cachedItems = itms
			}
		}
		items = bucket.cachedItems
		bucket.cacheMu.Unlock()
	}

	if items == nil {
		return nil, false, ErrNodeNotFound
	}

	innerDepth := depth + bucket.PathBits
	keyBits := len(key) * 8
	for _, item := range items {
		// fmt.Printf("  Item %d: SuffixBits %d, match: %v\n", i, item.SuffixBits, innerDepth+item.SuffixBits == keyBits)
		if innerDepth+item.SuffixBits == keyBits {
			match := s.suffixMatches(key, innerDepth, item.Suffix, item.SuffixBits)
			// fmt.Printf("    suffixMatches: %v\n", match)
			if match {
				// 命中归档项
				atomic.AddInt64(&common.BinaryMissExistentCount, 1)

				// 记录耗时
				dur := time.Since(recoveryStart).Nanoseconds()
				atomic.AddInt64(&common.BinaryProofGenTime, dur)
				for {
					oldMax := atomic.LoadInt64(&common.BinaryProofGenTimeMax)
					if dur <= oldMax || atomic.CompareAndSwapInt64(&common.BinaryProofGenTimeMax, oldMax, dur) {
						break
					}
				}

				val, err := s.db.Get(item.Value)
				return val, true, err
			}
		}
	}
	return nil, false, ErrNodeNotFound
}

func (s *Shard) Put(key []byte, value []byte) error {
	valHash := s.hasher.Hash(value)
	if err := s.db.Put(valHash, value); err != nil {
		return err
	}

	// Shard start depth
	newRoot, err := s.insert(s.root, key, s.config.ShardDepth, valHash)
	if err != nil {
		return err
	}
	s.root = newRoot
	return nil
}

// Activate 实现显式激活：从 StubList 查找并移除匹配项，然后执行常规插入
func (s *Shard) Activate(key []byte, value []byte) error {
	// 1. 深度搜索并从现有 StubList 中移除该 key
	if s.root != nil {
		// 已知 key，只需按路径下探并在沿途 Node 的 StubList 中定点查找
		ok := s.removeFromStubList(s.root, key, s.config.ShardDepth)
		if !ok {
			// Failed to remove, but we continue to insert
		}
	}

	// 2. 执行常规插入
	valHash := crypto.Keccak256Hash(value).Bytes()
	s.db.Put(valHash, value)

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
			matched := s.commonPrefixLen(bucket.Path, bucket.PathBits, key, depth)
			if matched == bucket.PathBits {
				bucket.cacheMu.RLock()
				items := bucket.cachedItems
				bucket.cacheMu.RUnlock()

				if items == nil {
					bucketData, err := s.getBucketData(bucket.Hash())
					if err != nil {
						continue
					}
					items, _ = s.deserializeArchivedKV(bucketData)
				}

				innerDepth := depth + bucket.PathBits
				keyBits := len(key) * 8
				for _, item := range items {
					if innerDepth+item.SuffixBits == keyBits && s.suffixMatches(key, innerDepth, item.Suffix, item.SuffixBits) {
						// 命中：执行盲删除
						s.blindDeleteFromBucket(bucket, []ArchivedKV{item})

						if bucket.Count == 0 {
							if s.config.ArchiveDB != nil {
								s.config.ArchiveDB.DeleteBucket(bucket.Hash())
							}
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
			return s.removeFromStubList(next, key, depth+1)
		}
	case *ArchiveBucketNode:
		// 如果根节点直接就是桶
		matched := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
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

			innerDepth := depth + n.PathBits
			keyBits := len(key) * 8
			for _, item := range items {
				if innerDepth+item.SuffixBits == keyBits && s.suffixMatches(key, innerDepth, item.Suffix, item.SuffixBits) {
					s.blindDeleteFromBucket(n, []ArchivedKV{item})
					if n.Count == 0 {
						if s.config.ArchiveDB != nil {
							s.config.ArchiveDB.DeleteBucket(n.Hash())
						}
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
			n.ValueHash = valueHash
			n.SetDirty(true)
			s.updateEpoch(n) // Update epoch
			// 若叶子已持久化过，记录旧哈希用于修剪
			if s.pruning && len(n.OriginalHash()) > 0 {
				s.staleSet[string(n.OriginalHash())] = struct{}{}
			}
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

				s.distributeStubs(n, parent)

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
				return parent, nil
			}
			depth += n.PathBits
		}

		bit := s.getBit(key, depth)
		if bit == 0 {
			if n.Left == nil && len(n.LeftHash) > 0 {
				loaded, _ := s.loadNode(n.LeftHash)
				n.Left = loaded
			}
			newLeft, err := s.insert(n.Left, key, depth+1, valueHash)
			if err != nil {
				return nil, err
			}
			n.Left = newLeft
		} else {
			if n.Right == nil && len(n.RightHash) > 0 {
				loaded, _ := s.loadNode(n.RightHash)
				n.Right = loaded
			}
			newRight, err := s.insert(n.Right, key, depth+1, valueHash)
			if err != nil {
				return nil, err
			}
			n.Right = newRight
		}

		s.updateEpoch(n)
		return s.shrink(n), nil

	case *ArchiveBucketNode:
		// 如果插入一个已归档的桶，直接把它当成一个“冷”节点处理：
		// 最简单的办法：创建一个 InternalNode，把桶挂载到 StubList
		parent := s.pool.GetInternal()
		parent.Path = n.Path
		parent.PathBits = n.PathBits
		parent.StubList = append(parent.StubList, n)
		parent.SetDirty(true)
		return s.insert(parent, key, depth, valueHash)

	default:
		return nil, errors.New("unknown node type")
	}
}

// Delete 删除指定 key。
func (s *Shard) Delete(key []byte) error {
	if s.root != nil {
		s.removeFromStubList(s.root, key, s.config.ShardDepth)
	}
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
				return n, true, nil
			}

			target := remaining.node
			if target == nil {
				target, _ = s.loadNode(remaining.hash)
			}

			if leaf, ok := target.(*LeafNode); ok {
				leaf.Path = s.concatPath(n.Path, n.PathBits, remaining.bit, leaf.Path, leaf.PathBits)
				leaf.PathBits = n.PathBits + 1 + leaf.PathBits
				leaf.SetDirty(true)
				if s.pruning && len(leaf.OriginalHash()) > 0 {
					s.staleSet[string(leaf.OriginalHash())] = struct{}{}
				}
				return leaf, true, nil
			}

			s.updateEpoch(n)
			return n, true, nil
		}

		s.updateEpoch(n)
		return n, true, nil
	default:
		return node, false, errors.New("unknown node type")
	}
}

// Hash 计算分片根哈希。
func (s *Shard) Hash() ([]byte, error) {
	if s.root == nil {
		return nil, nil
	}
	count := 0
	return s.commit(s.root, &dummyBatcher{}, &count, false)
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
	if s.root == nil {
		return nil, nil
	}

	if s.pruning {
		s.Prune()
	}

	nodeCount := 0
	rootHash, err := s.commit(s.root, batch, &nodeCount, destructive)
	if err != nil {
		return nil, err
	}

	if s.pruning && destructive {
		for h := range s.staleSet {
			err := batch.Delete([]byte(h))
			if err != nil {
				return nil, err
			}
		}
		s.staleSet = make(map[string]struct{})
	}
	return rootHash, nil
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
		fullKey := s.prependPath(n.Path, n.PathBits, prefix, bits)
		val, _ := s.db.Get(n.ValueHash)
		return fn(fullKey, val)
	case *InternalNode:
		newPrefix := prefix
		newBits := bits
		if n.PathBits > 0 {
			newPrefix = s.prependPath(n.Path, n.PathBits, prefix, bits)
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
			data, err := s.getBucketData(bucket.Hash())
			if err == nil {
				kvs, _ := s.deserializeArchivedKV(data)
				for _, kv := range kvs {
					fullK := s.prependPath(kv.Suffix, kv.SuffixBits, newPrefix, newBits)
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
		}

		// [NEW] 也需提交 StubList 中的 bucket
		for _, bucket := range n.StubList {
			if bucket.IsDirty() {
				_, err := s.commit(bucket, batch, nodeCount, destructive)
				if err != nil {
					return nil, err
				}
			}
		}

		if batch != nil {
			s.updateEpoch(n)
		}

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
		if batch != nil {
			s.updateEpoch(n)
		}

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
		anyOne := false
		allOne := true
		count := 0

		if n.Left != nil || len(n.LeftHash) > 0 {
			count++
			b := byte(0)
			if n.Left != nil {
				b = n.Left.Epoch() & 1
			} else {
				b = n.LeftEpoch & 1
			}
			if b == 1 {
				anyOne = true
			} else {
				allOne = false
			}
		}

		if n.Right != nil || len(n.RightHash) > 0 {
			count++
			b := byte(0)
			if n.Right != nil {
				b = n.Right.Epoch() & 1
			} else {
				b = n.RightEpoch & 1
			}
			if b == 1 {
				anyOne = true
			} else {
				allOne = false
			}
		}

		var bit0, bit1 byte
		if count > 0 {
			if anyOne {
				bit0 = 1
			}
			if allOne {
				bit1 = 1
			}
		}

		n.epoch = (bit1 << 1) | bit0
	}
}

func (s *Shard) shrink(n *InternalNode) Node {
	if len(n.StubList) > 0 {
		return n
	}

	var remaining Node
	var remainingBit byte
	var remainingHash []byte

	if n.Left != nil || len(n.LeftHash) > 0 {
		remaining = n.Left
		remainingHash = n.LeftHash
		remainingBit = 0
	}
	if n.Right != nil || len(n.RightHash) > 0 {
		if remaining != nil || len(remainingHash) > 0 {
			return n
		}
		remaining = n.Right
		remainingHash = n.RightHash
		remainingBit = 1
	}

	if n.PathBits > 0 {
		return n
	}

	if remaining == nil {
		remaining, _ = s.loadNode(remainingHash)
	}

	if inChild, ok := remaining.(*InternalNode); ok {
		inChild.Path = s.prependBit(inChild.Path, inChild.PathBits, remainingBit, nil)
		inChild.PathBits++
		inChild.SetDirty(true)
		if s.pruning && len(inChild.OriginalHash()) > 0 {
			s.staleSet[string(inChild.OriginalHash())] = struct{}{}
		}
		return inChild
	}

	if leaf, ok := remaining.(*LeafNode); ok {
		leaf.Path = s.prependBit(leaf.Path, leaf.PathBits, remainingBit, nil)
		leaf.PathBits++
		leaf.SetDirty(true)
		if s.pruning && len(leaf.OriginalHash()) > 0 {
			s.staleSet[string(leaf.OriginalHash())] = struct{}{}
		}
		return leaf
	}

	return n
}
