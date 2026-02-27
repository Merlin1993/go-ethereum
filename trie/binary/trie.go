package binary

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/binary/cuckoo"
	"github.com/ethereum/go-ethereum/trie/binary/ecmh"
)

var (
	ErrNodeNotFound = errors.New("node not found")
)

// ArchiveStore identifies the interface to store and retrieve archived bucket data.
type ArchiveStore interface {
	PutBucket(hash []byte, data []byte) error
	GetBucket(hash []byte) ([]byte, error)
	DeleteBucket(hash []byte) error
}

// Config holds the configuration parameters for the Trie.
type Config struct {
	ShardDepth        int          // Number of bits for shard routing (default 16)
	ArchiveBucketSize int          // Max number of items in an archive bucket before splitting (default 100)
	ArchiveDB         ArchiveStore // Separate store for archive data
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() *Config {
	return &Config{
		ShardDepth:        16,
		ArchiveBucketSize: 100,
	}
}

// TrieStats holds statistics about the Trie.
type TrieStats struct {
	BucketCount      int   // Total number of archive buckets
	ArchivedDataSize int64 // Total number of archived KV pairs
	MaxBucketsPath   int   // Max number of buckets on a single path

	ArchiveReadCount   int64 // Number of times archive store was accessed
	FalsePositiveCount int64 // Number of false positives from Cuckoo Filter
	TotalProofSize     int64 // Total size of generated proofs
	ExistProofCount    int64 // Count of existence proofs
	NonExistProofCount int64 // Count of non-existence proofs
}

// Shard 表示一个二叉 Merkle Patricia Trie 的分片（子树）。
type Shard struct {
	root     Node
	db       KVStore
	hasher   Hasher
	pruning  bool
	staleSet map[string]struct{} // 以哈希字节为键的集合（用于修剪）

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

// loadNode 根据哈希从 DB 读取并反序列化节点。
func (s *Shard) loadNode(hash []byte) (Node, error) {
	data, err := s.db.Get(hash)
	if err != nil {
		return nil, err
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
	return s.config.ArchiveDB.GetBucket(hash)
}

// Get 返回指定 key 的值哈希（不存在则返回 ErrNodeNotFound）。
func (s *Shard) Get(key []byte) ([]byte, error) {
	if s.root == nil {
		return nil, ErrNodeNotFound
	}
	// Shards start at certain depth
	return s.get(s.root, key, s.config.ShardDepth)
}

func (s *Shard) get(node Node, key []byte, depth int) ([]byte, error) {
	if node == nil {
		return nil, ErrNodeNotFound
	}

	switch n := node.(type) {
	case *LeafNode:
		// 检查叶子后缀是否完全匹配 key 的剩余位
		matchLen := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
		if matchLen == n.PathBits && depth+matchLen == len(key)*8 {
			return n.ValueHash, nil
		}
		return nil, ErrNodeNotFound

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
					val, err := s.get(next, key, hotDepth+1)
					if err == nil {
						return val, nil
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
				val, err := s.get(next, key, hotDepth+1)
				if err == nil {
					return val, nil
				}
			}
		}

		// [归档桶次之]：热路径未命中，按“后进先出”（栈）顺序查找侧挂的 StubList
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
						f := cuckoo.New()
						if err := f.Decode(bucket.Filter); err == nil {
							bucket.cachedFilter = f
						}
					}
					filter = bucket.cachedFilter
					bucket.cacheMu.Unlock()
				}

				if filter == nil {
					continue
				}

				// 使用分片相对路径位进行布谷鸟过滤器查询
				shardKey := key[s.config.ShardDepth/8:]
				if !filter.Lookup(shardKey) {
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

				innerDepth := s.config.ShardDepth
				keyBits := len(key) * 8
				for _, item := range items {
					if innerDepth+item.SuffixBits == keyBits {
						if s.suffixMatches(item.Suffix, item.SuffixBits, key, innerDepth) {
							s.statsMut.Lock()
							s.stats.ExistProofCount++
							s.stats.TotalProofSize += int64(len(item.Value) + len(item.Suffix) + 8)
							s.statsMut.Unlock()
							return item.Value, nil
						}
					}
				}
			}
		}

		return nil, ErrNodeNotFound

	default:
		return nil, errors.New("unknown node type")
	}
}

// suffixMatches 检查给定的位序列后缀是否与 key 从 depth 开始的部分匹配
func (s *Shard) suffixMatches(suffix []byte, suffixBits int, key []byte, depth int) bool {
	for i := 0; i < suffixBits; i++ {
		bitS := s.getBitFromBytes(suffix, i)
		bitK := s.getBit(key, depth+i)
		if bitS != bitK {
			return false
		}
	}
	return true
}

// Put 插入/更新指定 key 的值（以哈希存储）。
func (s *Shard) Put(key []byte, value []byte) error {
	valHash := s.hasher.Hash(value)

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
		s.removeFromStubList(s.root, key, s.config.ShardDepth)
	}

	// 2. 执行常规插入
	var err error
	s.root, err = s.insert(s.root, key, s.config.ShardDepth, crypto.Keccak256Hash(value).Bytes())
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
				// 既然已经匹配到桶前缀，直接读取该桶并尝试删除
				bucketData, err := s.getBucketData(bucket.Hash())
				if err != nil {
					continue
				}
				items, _ := s.deserializeArchivedKV(bucketData)

				innerDepth := s.config.ShardDepth
				keyBits := len(key) * 8
				for _, item := range items {
					if innerDepth+item.SuffixBits == keyBits && s.suffixMatches(item.Suffix, item.SuffixBits, key, innerDepth) {
						// 命中：执行盲删除
						s.blindDeleteFromBucket(bucket, []ArchivedKV{item})

						if bucket.Count == 0 {
							s.config.ArchiveDB.DeleteBucket(bucket.Hash())
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

		// 冲突/分裂（不使用扩展节点）：
		// - 在分裂位创建内部节点，左右挂载两个叶子；
		// - 将共同前缀向上展开为一串内部节点。

		// So:
		// Create a chain of Internal Nodes for the shared prefix.
		// Root (d=0) -> Internal(0) -> Internal(1) -> Split(2).

		// Let's implement this expansion.

		// Common prefix length: matchBits.
		// We need to create `matchBits` Internal nodes?
		// Wait, `node` (Leaf) was covering these bits.
		// Now we replace `node` with a structure.
		// 1. Mark `node` as stale (it's being replaced/modified).
		if s.pruning && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}

		oldLeafSuffix := s.shiftBits(n.Path, n.PathBits, matchBits+1, nil)

		// Optimize allocation using scratch buffer for temporary suffix
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

		// 释放旧叶子（如果它是新创建的或不再被引用，但在 trie 中由于是持久化结构，通常在 Commit 后清理或者直接由 GC 处理，POOL 这里仅对临时节点有效，但 insert 中创建的是持久节点。不过如果我们有明确的释放点可以 Put）
		// 在这里直接替换了 n，n 可能以后会被 GC。

		return splitNode, nil

	case *InternalNode:
		// [子树修改标记]
		if s.pruning && !n.dirty && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}
		n.SetDirty(true)

		// 1. 先匹配内部节点路径前缀
		if n.PathBits > 0 {
			matched := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
			if matched < n.PathBits {
				// [分裂逻辑]：当前节点路径与新 key 产生分叉
				prefixPath := s.prefixBits(n.Path, matched, nil)
				parent := s.pool.GetInternal()
				parent.Path = prefixPath
				parent.PathBits = matched

				oldBit := s.getBitFromBytes(n.Path, matched)
				n.Path = s.shiftBits(n.Path, n.PathBits, matched+1, nil)
				n.PathBits -= (matched + 1)

				// [StubList 分配]：根据前缀位，将原桶分配给新的子分支
				s.distributeStubs(n, parent)

				if oldBit == 0 {
					parent.Left = n
				} else {
					parent.Right = n
				}

				// 新 key 分支
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

		// 2. 递归向下插入
		bit := s.getBit(key, depth)
		if bit == 0 {
			if n.Left == nil && len(n.LeftHash) > 0 {
				n.Left, _ = s.loadNode(n.LeftHash)
			}
			newLeft, err := s.insert(n.Left, key, depth+1, valueHash)
			if err != nil {
				return nil, err
			}
			n.Left = newLeft
		} else {
			if n.Right == nil && len(n.RightHash) > 0 {
				n.Right, _ = s.loadNode(n.RightHash)
			}
			newRight, err := s.insert(n.Right, key, depth+1, valueHash)
			if err != nil {
				return nil, err
			}
			n.Right = newRight
		}

		s.updateEpoch(n)
		// 3. [积极收缩]：如果任一孩子为空，尝试合并
		return s.shrink(n), nil

	default:
		return nil, errors.New("unknown node type")
	}
}

// Delete 删除指定 key，如果 key 不存在则为无操作。
func (s *Shard) Delete(key []byte) error {
	if s.root != nil {
		s.removeFromStubList(s.root, key, s.config.ShardDepth)
	}
	newRoot, _, err := s.delete(s.root, key, s.config.ShardDepth)
	if err != nil {
		if err == ErrNodeNotFound {
			return nil // Deleting non-existent key is no-op
		}
		return err
	}
	s.root = newRoot
	return nil
}

// 删除实现返回：新子树节点、是否发生删除、错误信息
func (s *Shard) delete(node Node, key []byte, depth int) (Node, bool, error) {
	if node == nil {
		return nil, false, ErrNodeNotFound
	}

	switch n := node.(type) {
	case *LeafNode:
		// 检查叶子是否与 key 完全匹配
		matchLen := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
		if matchLen == n.PathBits && depth+matchLen == len(key)*8 {
			// 命中：删除叶子并记录旧哈希以便修剪
			if s.pruning && len(n.OriginalHash()) > 0 {
				s.staleSet[string(n.OriginalHash())] = struct{}{}
			}
			return nil, true, nil
		}
		return node, false, ErrNodeNotFound

	case *InternalNode:
		// 先匹配内部节点的路径前缀
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

		// 需要时加载子节点
		if child == nil && len(childHash) > 0 {
			loaded, err := s.loadNode(childHash)
			if err != nil {
				return node, false, err
			}
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

		// 标记当前内部节点为 dirty，并记录旧哈希
		if s.pruning && !n.dirty && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}
		n.SetDirty(true)

		if bit == 0 {
			n.Left = newChild
			n.LeftHash = nil // Clear hash as child might be deleted/modified
		} else {
			n.Right = newChild
			n.RightHash = nil // Clear hash
		}

		// 结构收缩：统计仍存在的子节点（指针或哈希）
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
			// 当前节点无子：删除
			return nil, true, nil
		}

		if count == 1 {
			// 若内部节点自身带有路径，则不再向下合并，避免丢失前缀信息
			if n.PathBits > 0 {
				s.updateEpoch(n)
				return n, true, nil
			}

			target := remaining.node
			if target == nil {
				target, err = s.loadNode(remaining.hash)
				if err != nil {
					return nil, false, err
				}
			}

			if leaf, ok := target.(*LeafNode); ok {
				// 合并路径：当前节点路径 + 分离位 + 叶子路径
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

		// Update epoch
		s.updateEpoch(n)
		return n, true, nil
	default:
		return node, false, errors.New("unknown node type")
	}
}

type ChildInfo struct {
	node Node
	hash []byte
	bit  byte
}

// CommitToBatch 递归提交改动到提供的 Batcher 中，返回最新根哈希。
// 注意：此方法不会调用 batch.Write()。
func (s *Shard) CommitToBatch(batch Batcher) ([]byte, error) {
	if s.root == nil {
		return nil, nil
	}
	nodeCount := 0
	rootHash, err := s.commit(s.root, batch, &nodeCount)
	if err != nil {
		return nil, err
	}

	// Pruning
	if s.pruning {
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

// Commit 递归提交改动并执行修剪，返回最新根哈希（空树返回 nil）。
func (s *Shard) Commit() ([]byte, error) {
	batch := s.db.NewBatch()
	defer batch.Reset()

	rootHash, err := s.CommitToBatch(batch)
	if err != nil {
		return nil, err
	}

	if err := batch.Write(); err != nil {
		return nil, err
	}

	return rootHash, nil
}

// FlushArchives 将缓存中的归档数据同步到 ArchiveDB。
// 此操作通常在 Commit 之后执行，以避免影响主树提交的时间统计。
func (s *Shard) FlushArchives() error {
	if s.config.ArchiveDB == nil {
		return nil
	}

	// 1. 处理全量写入 (new buckets)
	for h, data := range s.pendingArchives {
		if err := s.config.ArchiveDB.PutBucket([]byte(h), data); err != nil {
			return err
		}
		delete(s.pendingArchives, h)
	}

	// 2. 处理“盲追加”写入 (blind appends)
	for newHash, task := range s.pendingAppends {
		// 加载旧数据
		oldData, err := s.config.ArchiveDB.GetBucket(task.oldHash)
		if err != nil {
			return err
		}
		items, err := s.deserializeArchivedKV(oldData)
		if err != nil {
			return err
		}

		// 合并新数据
		items = append(items, task.newItems...)
		newData, err := s.serializeArchivedKV(items)
		if err != nil {
			return err
		}

		// 写入新版本
		if err := s.config.ArchiveDB.PutBucket([]byte(newHash), newData); err != nil {
			return err
		}
		delete(s.pendingAppends, newHash)
	}

	// 3. 处理“盲删除”写入 (blind deletes)
	for newHash, task := range s.pendingDeletes {
		// 加载旧数据
		oldData, err := s.config.ArchiveDB.GetBucket(task.oldHash)
		if err != nil {
			return err
		}
		items, err := s.deserializeArchivedKV(oldData)
		if err != nil {
			return err
		}

		// 过滤删除项
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

		// 写入新版本或删除空桶
		if len(newItems) == 0 {
			if err := s.config.ArchiveDB.DeleteBucket(task.oldHash); err != nil {
				return err
			}
		} else {
			newData, err := s.serializeArchivedKV(newItems)
			if err != nil {
				return err
			}
			if err := s.config.ArchiveDB.PutBucket([]byte(newHash), newData); err != nil {
				return err
			}
		}
		delete(s.pendingDeletes, newHash)
	}

	return nil
}

func (s *Shard) commit(node Node, batch Batcher, nodeCount *int) ([]byte, error) {
	if !node.IsDirty() {
		return node.Hash(), nil
	}
	if nodeCount != nil {
		*nodeCount++
	}
	if nodeCount != nil {
		*nodeCount++
	}

	switch n := node.(type) {
	case *ArchiveBucketNode:
		// 提交归档桶
		data, err := n.Serialize()
		if err != nil {
			return nil, err
		}
		h := s.hasher.Hash(data)
		n.SetHash(h)
		n.SetDirty(false)
		n.SetOriginalHash(h)

		// 1. 写入主数据库（承诺部分）
		if err := batch.Put(h, data); err != nil {
			return nil, err
		}

		return h, nil

	case *InternalNode:
		// 在持久化前，根据子节点刷新 epoch（避免在每次插入/删除时重复计算）
		s.updateEpoch(n)

		// Commit children
		if n.Left != nil {
			h, err := s.commit(n.Left, batch, nodeCount)
			if err != nil {
				return nil, err
			}
			n.LeftHash = h
			n.LeftEpoch = n.Left.Epoch()
		}
		if n.Right != nil {
			h, err := s.commit(n.Right, batch, nodeCount)
			if err != nil {
				return nil, err
			}
			n.RightHash = h
			n.RightEpoch = n.Right.Epoch()
		}

		// 序列化内部节点（包含 epoch 与子节点哈希）
		data, err := n.Serialize()
		if err != nil {
			return nil, err
		}

		// Hash
		h := s.hasher.Hash(data)
		n.SetHash(h)
		n.SetDirty(false)
		n.SetOriginalHash(h) // It's now persisted (logically)

		// Write to DB
		err = batch.Put(h, data)
		if err != nil {
			return nil, err
		}
		return h, nil

	case *LeafNode:
		// 在持久化前根据当前分片剪枝状态与全局年度位刷新叶子 epoch
		s.updateEpoch(n)

		// 序列化叶子节点
		data, err := n.Serialize()
		if err != nil {
			return nil, err
		}

		// Hash
		h := s.hasher.Hash(data)
		n.SetHash(h)
		n.SetDirty(false)
		n.SetOriginalHash(h)

		// Write to DB
		err = batch.Put(h, data)
		if err != nil {
			return nil, err
		}
		return h, nil

	default:
		return nil, errors.New("unknown node type")
	}
}

func (s *Shard) setBitInBytes(data []byte, bitIdx int, val byte) {
	byteIdx := bitIdx / 8
	bitOffset := 7 - (bitIdx % 8)
	if val == 1 {
		data[byteIdx] |= (1 << bitOffset)
	} else {
		data[byteIdx] &= ^(1 << bitOffset)
	}
}

// 工具方法

func getBit(key []byte, depth int) byte {
	byteIdx := depth / 8
	bitIdx := 7 - (depth % 8)
	if byteIdx >= len(key) {
		return 0
	}
	if (key[byteIdx] & (1 << bitIdx)) != 0 {
		return 1
	}
	return 0
}

func (s *Shard) getBit(key []byte, depth int) byte {
	return getBit(key, depth)
}

func (s *Shard) getBitFromBytes(data []byte, bitIndex int) byte {
	byteIdx := bitIndex / 8
	bitIdx := 7 - (bitIndex % 8)
	if byteIdx >= len(data) {
		return 0
	}
	if (data[byteIdx] & (1 << bitIdx)) != 0 {
		return 1
	}
	return 0
}

func (s *Shard) commonPrefixLen(a []byte, aBits int, b []byte, bStartBit int) int {
	// a 为路径后缀（按位），b 为完整 key（从 bStartBit 位开始对齐比较）
	// [OPTIMIZED] 优先使用字节比较提速
	matched := 0
	// 1. 尝试按字节对齐比较 (如果 bStartBit 是 8 的倍数且 aBits >= 8)
	if bStartBit%8 == 0 && aBits >= 8 {
		bIdx := bStartBit / 8
		for matched+8 <= aBits && bIdx < len(b) {
			if a[matched/8] != b[bIdx] {
				break
			}
			matched += 8
			bIdx++
		}
	}

	// 2. 剩余位逐位比较
	for matched < aBits {
		bitA := s.getBitFromBytes(a, matched)
		bitB := s.getBit(b, bStartBit+matched)
		if bitA != bitB {
			break
		}
		matched++
	}
	return matched
}

func (s *Shard) getSuffix(key []byte, depth int, buf []byte) []byte {
	// 提取从 depth 位开始的 key 剩余位，并按高位在前压缩为字节数组返回
	totalBits := len(key)*8 - depth
	if totalBits <= 0 {
		return nil
	}

	// 位移压缩（逐位处理，原型实现即可）
	size := (totalBits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	for i := 0; i < totalBits; i++ {
		if s.getBit(key, depth+i) == 1 {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

func (s *Shard) shiftBits(data []byte, bits int, shift int, buf []byte) []byte {
	// 跳过前 shift 位，返回剩余位的字节压缩表示
	newBits := bits - shift
	if newBits <= 0 {
		return nil
	}
	size := (newBits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	for i := 0; i < newBits; i++ {
		// 原位下标 = shift + i
		if s.getBitFromBytes(data, shift+i) == 1 {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

func (s *Shard) prefixBits(data []byte, bits int, buf []byte) []byte {
	if bits <= 0 {
		return nil
	}
	size := (bits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	for i := 0; i < bits; i++ {
		if s.getBitFromBytes(data, i) == 1 {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

// prependBit 在位序列前部增加一位。用于在树收缩或路径调整时重新计算路径。
func (s *Shard) prependBit(data []byte, bits int, bit byte, buf []byte) []byte {
	newBits := bits + 1
	size := (newBits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	if bit == 1 {
		res[0] |= 0x80
	}
	for i := 0; i < bits; i++ {
		if s.getBitFromBytes(data, i) == 1 {
			byteIdx := (i + 1) / 8
			bitIdx := 7 - ((i + 1) % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

func (s *Shard) appendBit(data []byte, bits int, bit byte) ([]byte, int) {
	newBits := bits + 1
	size := (newBits + 7) / 8
	res := make([]byte, size)
	copy(res, data)
	if bit == 1 {
		byteIdx := bits / 8
		bitOffset := 7 - (bits % 8)
		res[byteIdx] |= (1 << bitOffset)
	}
	return res, newBits
}

func (s *Shard) copyBits(dst []byte, dstStart int, src []byte, srcBits int) {
	for i := 0; i < srcBits; i++ {
		if s.getBitFromBytes(src, i) == 1 {
			byteIdx := (dstStart + i) / 8
			bitIdx := 7 - ((dstStart + i) % 8)
			dst[byteIdx] |= (1 << bitIdx)
		}
	}
}

func (s *Shard) updateEpoch(node Node) {
	if node == nil {
		return
	}

	global := s.globalEpochBit()

	switch n := node.(type) {
	case *LeafNode:
		// 叶子 bit0：新写入/更新的节点始终标记为当前全局年度位，确保其为“热”数据
		n.epoch = global

	case *InternalNode:
		anyOne := false
		allOne := true
		count := 0

		// 左孩子
		if n.Left != nil || len(n.LeftHash) > 0 {
			count++
			b := byte(0)
			if n.Left != nil {
				b = n.Left.Epoch() & 1
			} else {
				// [OPTIMIZED] 使用缓存的 LeftEpoch，不再调用 loadNode
				b = n.LeftEpoch & 1
			}
			if b == 1 {
				anyOne = true
			} else {
				allOne = false
			}
		}

		// 右孩子
		if n.Right != nil || len(n.RightHash) > 0 {
			count++
			b := byte(0)
			if n.Right != nil {
				b = n.Right.Epoch() & 1
			} else {
				// [OPTIMIZED] 使用缓存的 RightEpoch，不再调用 loadNode
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

		// bit0: 子树存在 1；bit1: 子树全为 1
		n.epoch = (bit1 << 1) | bit0
	}
}

// Prune：触发分片归档聚合逻辑

func (s *Shard) Prune(global byte) error {
	// 深度优先递归，收集过期项并进行自底向上的桶聚合
	newRoot, archivedItems, err := s.pruneAndArchive(s.root, global, s.config.ShardDepth, nil, 0)
	if err != nil {
		return err
	}

	if len(archivedItems) > 0 {
		newRoot = s.mountAtDeepest(newRoot, s.config.ShardDepth, archivedItems)
	}

	s.root = newRoot
	s.isPruned = true
	return nil
}

func (s *Shard) pruneAndArchive(node Node, global byte, depth int, pathFromShardRoot []byte, pathFromShardRootBits int) (Node, []ArchivedKV, error) {
	if node == nil {
		return nil, nil, nil
	}

	// [Epoch 剪枝跳过优化]：依据 bit0 (anyOne) 与 bit1 (allOne) 决策
	epoch := node.Epoch()
	bit0 := epoch & 1        // 存在 1
	bit1 := (epoch >> 1) & 1 // 全是 1

	if global == 1 {
		if bit1 == 1 { // 全是 1，说明没有需要归档的 0 项，跳过
			return node, nil, nil
		}
	} else {
		if bit0 == 0 { // 全是 0，说明没有需要归档的 1 项，跳过
			return node, nil, nil
		}
	}

	switch n := node.(type) {
	case *LeafNode:
		// [过期判定]：n.Epoch()&1 != global 则说明是旧版本，需要归档
		if n.Epoch()&1 != global {
			if s.pruning && len(n.OriginalHash()) > 0 {
				s.staleSet[string(n.OriginalHash())] = struct{}{}
			}

			// 还原相对于分片起始深度的全路径位
			fullSuffixBits := pathFromShardRootBits + n.PathBits
			fullSuffix := make([]byte, (fullSuffixBits+7)/8)
			s.copyBits(fullSuffix, 0, pathFromShardRoot, pathFromShardRootBits)
			s.copyBits(fullSuffix, pathFromShardRootBits, n.Path, n.PathBits)

			it := ArchivedKV{
				Suffix:     fullSuffix,
				SuffixBits: fullSuffixBits,
				Value:      n.ValueHash,
			}
			return nil, []ArchivedKV{it}, nil
		}
		return n, nil, nil

	case *InternalNode:
		// 1. 递归子节点
		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, _ = s.loadNode(n.LeftHash)
		}
		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, _ = s.loadNode(n.RightHash)
		}

		// 构建进入子节点前的完整路径路径
		newPathBits := pathFromShardRootBits + n.PathBits
		newPath := make([]byte, (newPathBits+7)/8)
		s.copyBits(newPath, 0, pathFromShardRoot, pathFromShardRootBits)
		s.copyBits(newPath, pathFromShardRootBits, n.Path, n.PathBits)

		pathForLeft, bitsForLeft := s.appendBit(newPath, newPathBits, 0)
		pathForRight, bitsForRight := s.appendBit(newPath, newPathBits, 1)

		newLeft, leftArchived, err := s.pruneAndArchive(n.Left, global, depth+n.PathBits+1, pathForLeft, bitsForLeft)
		if err != nil {
			return nil, nil, err
		}
		newRight, rightArchived, err := s.pruneAndArchive(n.Right, global, depth+n.PathBits+1, pathForRight, bitsForRight)
		if err != nil {
			return nil, nil, err
		}

		n.Left = newLeft
		n.Right = newRight

		merged := append(leftArchived, rightArchived...)

		// 4. [归档决策]：
		// A. 尝试追加到本层已有的通用桶 (PathBits 0)
		for _, bucket := range n.StubList {
			if bucket.PathBits == 0 {
				if bucket.Count+uint64(len(merged)) <= uint64(s.config.ArchiveBucketSize) {
					s.blindAppendToBucket(bucket, merged)
					bucket.SetDirty(true)
					return s.shrink(n), nil, nil
				}
				break
			}
		}

		// B. 如果 len >= config.ArchiveBucketSize，本层无法追加，则下探
		if len(merged) >= s.config.ArchiveBucketSize {
			// 修改：mountAtDeepest 现在由于使用了相对于分片的 Suffix，不再需要 shiftBits
			n.Left = s.mountAtDeepest(n.Left, depth+n.PathBits+1, leftArchived)
			n.Right = s.mountAtDeepest(n.Right, depth+n.PathBits+1, rightArchived)
			n.SetDirty(true)
			return s.shrink(n), nil, nil
		}

		// 如果节点变脏（由于子节点被删除），执行收缩并标记
		if n.Left != newLeft || n.Right != newRight {
			n.SetDirty(true)
			if s.pruning && len(n.OriginalHash()) > 0 {
				s.staleSet[string(n.OriginalHash())] = struct{}{}
			}
			return s.shrink(n), merged, nil
		}

		return n, merged, nil
	}
	return node, nil, nil
}

// Trie Manager

type Trie struct {
	config           *Config
	shards           []*Shard
	dirtyShards      map[int]struct{}
	dirtyList        []int // 记录本次迭代中发生改变的分片索引，用于 O(Dirty) 收集
	dirtyArchive     map[int]struct{}
	dirtyArchiveList []int    // 记录待刷盘的归档分片
	shardRoots       []byte   // 预分配的 [NumShards * 32] 缓冲区，维护所有分片最新的根哈希
	topTree          *TopTree // [NEW] 16-ary tree for efficient root hash computation
	cachedRoot       []byte   // 缓存的全局根哈希
	rootDirty        bool     // 标记全局根哈希是否需要重新计算
	db               KVStore
	hasher           Hasher
	pruning          bool
	globalEpochBit   byte
	insertCount      int
	pruneShardIdx    int
	prunedShardCount int

	shardMu sync.RWMutex
}

// IsDirty returns true if the Trie has uncommitted changes.
func (t *Trie) IsDirty() bool {
	t.shardMu.RLock()
	defer t.shardMu.RUnlock()
	return t.rootDirty
}

func NewTrie(root []byte, db KVStore, hasher Hasher, config *Config, pruning bool) *Trie {
	if config == nil {
		config = DefaultConfig()
	}
	numShards := 1 << config.ShardDepth
	t := &Trie{
		config:           config,
		shards:           make([]*Shard, numShards),
		dirtyShards:      make(map[int]struct{}),
		dirtyList:        make([]int, 0, 128),
		dirtyArchive:     make(map[int]struct{}),
		dirtyArchiveList: make([]int, 0, 128),
		shardRoots:       make([]byte, numShards*32),
		rootDirty:        true,
		db:               db,
		hasher:           hasher,
		pruning:          pruning,
	}
	// Initialize the 16-ary TopTree for O(Dirty) root hash computation
	batch := t.db.NewBatch()
	t.topTree = NewTopTree(hasher, batch)
	if len(root) > 0 {
		t.Load(root)
	}
	return t
}

func (t *Trie) Load(root []byte) error {
	if len(root) == 0 {
		return nil
	}
	data, err := t.db.Get(root)
	if err != nil {
		return err
	}

	if len(data) == len(t.shardRoots) {
		copy(t.shardRoots, data)
	} else if len(data) == 513 && data[0] == 0xD0 {
		if err := t.parseTopTree(data, 0, 0); err != nil {
			return err
		}
		if t.topTree != nil && t.topTree.root != nil {
			t.topTree.root.Hash = root
			t.topTree.root.Dirty = false
		}
	} else {
		return errors.New("invalid shard roots data size")
	}

	t.cachedRoot = append([]byte{}, root...)
	t.rootDirty = false
	return nil
}

// parseTopTree recursively traverses the 16-ary TopTree down to level 3
// and populates the t.shardRoots flat array.
func (t *Trie) parseTopTree(data []byte, level int, prefix int) error {
	if len(data) != 513 {
		return fmt.Errorf("invalid TopNode size at level %d: %d", level, len(data))
	}
	if data[0] != byte(0xD0+level) {
		return fmt.Errorf("invalid TopNode header at level %d: %x", level, data[0])
	}

	for i := 0; i < 16; i++ {
		start := 1 + i*32
		h := data[start : start+32]

		empty := true
		for _, b := range h {
			if b != 0 {
				empty = false
				break
			}
		}
		if empty {
			continue
		}

		if level == 3 {
			shardID := (prefix << 4) | i
			copy(t.shardRoots[shardID*32:(shardID+1)*32], h)
		} else {
			childData, err := t.db.Get(h)
			if err != nil {
				return err
			}
			childPrefix := (prefix << 4) | i
			if err := t.parseTopTree(childData, level+1, childPrefix); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Trie) SetGlobalEpoch(bit byte) {
	if bit != t.globalEpochBit {
		t.globalEpochBit = bit
		// Reset prune status of all shards
		t.shardMu.RLock()
		defer t.shardMu.RUnlock()
		for _, s := range t.shards {
			if s != nil {
				s.isPruned = false
			}
		}
	}
}

func (t *Trie) Put(key []byte, value []byte) error {
	t.shardMu.Lock()
	defer t.shardMu.Unlock()

	// Store value in DB for retrieval during prune (Requirement)
	valHash := t.hasher.Hash(value)
	t.db.Put(valHash, value)

	shardID := t.getShardID(key)
	s, err := t.getOrCreateShardLocked(shardID)
	if err != nil {
		return err
	}
	err = s.Put(key, value)
	if err != nil {
		return err
	}
	if _, ok := t.dirtyShards[shardID]; !ok {
		t.dirtyShards[shardID] = struct{}{}
		t.dirtyList = append(t.dirtyList, shardID)
		t.rootDirty = true
	}
	return nil
}

func (t *Trie) Get(key []byte) ([]byte, error) {
	id := t.getShardID(key)

	t.shardMu.RLock()
	s := t.shards[id]
	t.shardMu.RUnlock()

	if s == nil {
		var err error
		s, err = t.getOrCreateShard(id)
		if err != nil {
			return nil, err
		}
	}
	return s.Get(key)
}

func (t *Trie) BatchDelete(key []byte) error {
	t.shardMu.Lock()
	defer t.shardMu.Unlock()

	shardID := t.getShardID(key)
	s, err := t.getOrCreateShardLocked(shardID)
	if err != nil {
		return err
	}
	err = s.Delete(key)
	if err != nil {
		return err
	}
	if _, ok := t.dirtyShards[shardID]; !ok {
		t.dirtyShards[shardID] = struct{}{}
		t.dirtyList = append(t.dirtyList, shardID)
		t.rootDirty = true
	}
	return nil
}

// Activate 显式激活一个归档的 key。从侧挂桶移除并重注入热路径。
func (t *Trie) Activate(key []byte, value []byte) error {
	t.shardMu.Lock()
	defer t.shardMu.Unlock()

	shardID := t.getShardID(key)
	s, err := t.getOrCreateShardLocked(shardID)
	if err != nil {
		return err
	}
	err = s.Activate(key, value)
	if err != nil {
		return err
	}
	if _, ok := t.dirtyShards[shardID]; !ok {
		t.dirtyShards[shardID] = struct{}{}
		t.dirtyList = append(t.dirtyList, shardID)
		t.rootDirty = true
	}
	if _, ok := t.dirtyArchive[shardID]; !ok {
		t.dirtyArchive[shardID] = struct{}{}
		t.dirtyArchiveList = append(t.dirtyArchiveList, shardID)
	}
	return nil
}

// CommitToBatch 递归提交所有脏分片改动到提供的 Batcher 中，返回最新根哈希。
// 注意：此方法不会调用 batch.Write()。
func (t *Trie) CommitToBatch(batch Batcher) ([]byte, error) {
	t.shardMu.Lock()
	defer t.shardMu.Unlock()

	if !t.rootDirty {
		return t.cachedRoot, nil
	}
	// [OPTIMIZED] Only commit dirty shards
	if len(t.dirtyList) == 0 {
		return t.hasher.Hash(t.shardRoots), nil
	}

	numShards := len(t.shards)
	hashes := make([][]byte, numShards)
	errs := make([]error, numShards)

	var wg sync.WaitGroup
	// Limit concurrency to NumCPU to avoid overwhelming the system
	nCPU := runtime.NumCPU()
	sem := make(chan struct{}, nCPU)

	// Collect shards to commit
	toCommit := t.dirtyList

	for _, id := range toCommit {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			h, err := t.shards[idx].CommitToBatch(batch)
			hashes[idx] = h
			errs[idx] = err
		}(id)
	}
	wg.Wait()

	// Fill in unchanged shard roots (In O(Dirty) mode, we already have them in t.shardRoots)
	// Update shardRoots for dirty ones
	for _, id := range toCommit {
		if errs[id] != nil {
			return nil, errs[id]
		}
		h := hashes[id]
		if len(h) == 0 {
			copy(t.shardRoots[id*32:(id+1)*32], make([]byte, 32))
		} else {
			copy(t.shardRoots[id*32:(id+1)*32], h)
		}

		// Mark for archive flush
		if _, ok := t.dirtyArchive[id]; !ok {
			t.dirtyArchive[id] = struct{}{}
			t.dirtyArchiveList = append(t.dirtyArchiveList, id)
		}
	}

	// [OPTIMIZATION] 16-ary Top Tree Aggregation replacing flat 2MB hash
	if t.topTree == nil {
		t.topTree = NewTopTree(t.hasher, batch) // Initialize on demand or in NewTrie
	} else {
		t.topTree.db = batch // Update batcher reference
	}

	var err error
	t.cachedRoot, err = t.topTree.Compute(t.shardRoots, toCommit)
	if err != nil {
		return nil, fmt.Errorf("top tree compute error: %w", err)
	}

	// Clear dirty shards after commit
	t.dirtyShards = make(map[int]struct{})
	t.dirtyList = make([]int, 0, 128)
	t.rootDirty = false

	// [PERSISTENCE] Store the top-level 16-ary root
	// The internal TopNodes were persisted during topTree.Compute()
	// No need to persist the 2MB flat array anymore.
	return t.cachedRoot, nil
}

func (t *Trie) Commit() ([]byte, error) {
	batch := t.db.NewBatch()
	defer batch.Reset()

	root, err := t.CommitToBatch(batch)
	if err != nil {
		return nil, err
	}

	if err := batch.Write(); err != nil {
		return nil, err
	}
	return root, nil
}

func (t *Trie) PruneNextShard() error {
	shardID := t.pruneShardIdx
	s, err := t.getOrCreateShard(shardID)
	if err != nil {
		return err
	}
	numShards := len(t.shards)
	t.pruneShardIdx = (t.pruneShardIdx + 1) % numShards

	wasPruned := s.isPruned
	err = s.Prune(t.globalEpochBit)
	if err != nil {
		return err
	}
	// 若本分片在当前年度首次完成剪枝，计数 +1
	if !wasPruned && s.isPruned {
		t.prunedShardCount++
		if _, ok := t.dirtyArchive[shardID]; !ok {
			t.dirtyArchive[shardID] = struct{}{}
			t.dirtyArchiveList = append(t.dirtyArchiveList, shardID)
		}
		// 当所有分片均已剪枝，自动切换年度，并重置所有分片的剪枝标记与计数
		if t.prunedShardCount >= numShards {
			if t.globalEpochBit == 0 {
				t.globalEpochBit = 1
			} else {
				t.globalEpochBit = 0
			}
			t.prunedShardCount = 0
			t.shardMu.RLock()
			for _, s := range t.shards {
				if s != nil {
					s.isPruned = false
				}
			}
			t.shardMu.RUnlock()
		}
	}
	return nil
}

func (t *Trie) getOrCreateShard(id int) (*Shard, error) {
	t.shardMu.RLock()
	s := t.shards[id]
	t.shardMu.RUnlock()

	if s != nil {
		return s, nil
	}

	t.shardMu.Lock()
	defer t.shardMu.Unlock()

	return t.getOrCreateShardLocked(id)
}

func (t *Trie) getOrCreateShardLocked(id int) (*Shard, error) {
	// Check again in case it was created while waiting for lock
	if t.shards[id] != nil {
		return t.shards[id], nil
	}

	// Note: We need to load shard roots if they exist.
	rootHash := t.shardRoots[id*32 : (id+1)*32]
	// Check if the hash is all zeros (empty shard)
	isEmpty := true
	for _, b := range rootHash {
		if b != 0 {
			isEmpty = false
			break
		}
	}
	if isEmpty {
		rootHash = nil
	}

	s, err := NewShard(id, t.db, t.hasher, t.config, rootHash, t.pruning, func() byte { return t.globalEpochBit })
	if err != nil {
		return nil, err
	}
	t.shards[id] = s
	return s, nil
}

func (t *Trie) getShardID(key []byte) int {
	depth := t.config.ShardDepth
	bytesNeeded := (depth + 7) / 8
	if len(key) < bytesNeeded {
		return 0
	}

	res := 0
	for i := 0; i < depth; i++ {
		if getBit(key, i) == 1 {
			res |= (1 << (depth - 1 - i))
		}
	}
	return res
}

// Stats returns the statistics for the entire Trie.
func (t *Trie) Stats() *TrieStats {
	stats := &TrieStats{}
	for _, shard := range t.shards {
		if shard != nil {
			shard.accumulateStats(stats)
		}
	}
	return stats
}

// FlushArchives 同步所有分片的归档缓存到数据库。
func (t *Trie) FlushArchives() error {
	if len(t.dirtyArchiveList) == 0 {
		return nil
	}
	for _, id := range t.dirtyArchiveList {
		s := t.shards[id]
		if s != nil {
			if err := s.FlushArchives(); err != nil {
				return err
			}
		}
	}
	t.dirtyArchive = make(map[int]struct{})
	t.dirtyArchiveList = make([]int, 0, 128)
	return nil
}

func (s *Shard) accumulateStats(stats *TrieStats) {
	if s.root == nil {
		return
	}
	s.nodeStats(s.root, 0, stats)
}

func (s *Shard) nodeStats(node Node, currentPathBuckets int, stats *TrieStats) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *InternalNode:
		// Process buckets at this node
		numBuckets := len(n.StubList)
		stats.BucketCount += numBuckets
		for _, bucket := range n.StubList {
			stats.ArchivedDataSize += int64(bucket.Count)
		}

		newPathBuckets := currentPathBuckets + numBuckets
		if newPathBuckets > stats.MaxBucketsPath {
			stats.MaxBucketsPath = newPathBuckets
		}

		// Recurse to children
		if n.Left != nil {
			s.nodeStats(n.Left, newPathBuckets, stats)
		} else if len(n.LeftHash) > 0 {
			// In a real implementation, we might not want to load all nodes for stats
			// but for this task we assume we can or just count what's in memory.
			// Let's at least try to load if we want accurate stats.
			loaded, _ := s.loadNode(n.LeftHash)
			if loaded != nil {
				s.nodeStats(loaded, newPathBuckets, stats)
			}
		}

		if n.Right != nil {
			s.nodeStats(n.Right, newPathBuckets, stats)
		} else if len(n.RightHash) > 0 {
			loaded, _ := s.loadNode(n.RightHash)
			if loaded != nil {
				s.nodeStats(loaded, newPathBuckets, stats)
			}
		}
	case *LeafNode:
		// Leaf nodes don't have buckets in this implementation
	}
}

// -----------------------------------------------------------------------------
// [归档核心逻辑]
// 以下包含树扫描、节点聚合以及桶分裂（下探 2 层）的实现。
// -----------------------------------------------------------------------------

// ArchiveTrigger 对外提供的触发函数，遍历所有分片并执行归档聚合逻辑。
// -----------------------------------------------------------------------------
// [归档辅助工具]
// -----------------------------------------------------------------------------

func (s *Shard) prependPath(base []byte, baseBits int, prefix []byte, prefixBits int) []byte {
	res := make([]byte, (baseBits+prefixBits+7)/8)
	s.copyBits(res, 0, prefix, prefixBits)
	s.copyBits(res, prefixBits, base, baseBits)
	return res
}

// mountAtDeepest 将归档项集合挂载到当前子树中最深的、路径匹配的 InternalNode 上
func (s *Shard) mountAtDeepest(node Node, depth int, newItems []ArchivedKV) Node {
	if len(newItems) == 0 {
		return node
	}
	if node == nil {
		// 如果节点为空，则在此创建一个容器节点并挂载桶
		bucket := NewArchiveBucketNode(nil, 0, nil, nil, 0)
		s.recomputeBucket(bucket, newItems)
		bucket.SetDirty(true)
		parent := NewInternalNode(nil, nil)
		parent.StubList = append(parent.StubList, bucket)
		parent.SetDirty(true)
		return parent
	}

	switch n := node.(type) {
	case *InternalNode:
		// 尝试向下寻找分支
		allGoLeft := true
		allGoRight := true

		// 如果节点有 Path，则先验证前缀匹配
		// 注意：it.Suffix 是相对于 shard root 的，所以偏移量是 depth - s.config.ShardDepth
		if n.PathBits > 0 {
			for _, it := range newItems {
				matched := s.commonPrefixLen(n.Path, n.PathBits, it.Suffix, depth-s.config.ShardDepth)
				if matched != n.PathBits {
					allGoLeft, allGoRight = false, false
					break
				}
				// 检查是否有足够的剩余位进行分支
				if it.SuffixBits <= (depth - s.config.ShardDepth + n.PathBits) {
					allGoLeft, allGoRight = false, false
					break
				}
				bit := s.getBitFromBytes(it.Suffix, depth-s.config.ShardDepth+n.PathBits)
				if bit == 0 {
					allGoRight = false
				} else {
					allGoLeft = false
				}
			}
		} else {
			for _, it := range newItems {
				if it.SuffixBits <= (depth - s.config.ShardDepth) {
					allGoLeft, allGoRight = false, false
					break
				}
				bit := s.getBitFromBytes(it.Suffix, depth-s.config.ShardDepth)
				if bit == 0 {
					allGoRight = false
				} else {
					allGoLeft = false
				}
			}
		}

		if allGoLeft {
			if n.Left == nil {
				n.Left = NewInternalNode(nil, nil)
			}
			n.Left = s.mountAtDeepest(n.Left, depth+n.PathBits+1, newItems)
			n.SetDirty(true)
			return n
		}
		if allGoRight {
			if n.Right == nil {
				n.Right = NewInternalNode(nil, nil)
			}
			n.Right = s.mountAtDeepest(n.Right, depth+n.PathBits+1, newItems)
			n.SetDirty(true)
			return n
		}

		// 3. 尝试追加到现有桶
		for _, bucket := range n.StubList {
			matched := s.commonPrefixLen(bucket.Path, bucket.PathBits, newItems[0].Suffix, depth)
			if matched == bucket.PathBits {
				// 命中：尝试“盲追加”
				if bucket.Count+uint64(len(newItems)) <= uint64(s.config.ArchiveBucketSize) {
					s.blindAppendToBucket(bucket, newItems)
					bucket.SetDirty(true)
					n.SetDirty(true)
					return n
				}
				// 超过阈值，不追加，继续分裂（见下文）
				break
			}
		}

		// 否则新建桶
		bucket := NewArchiveBucketNode(nil, 0, nil, nil, 0)
		s.recomputeBucket(bucket, newItems)
		bucket.SetDirty(true)
		n.StubList = append(n.StubList, bucket)
		n.SetDirty(true)
		return n

	default:
		// 如果是 LeafNode，对其进行包装以容纳挂载桶
		parent := NewInternalNode(nil, nil)
		if n != nil {
			if leaf, ok := n.(*LeafNode); ok {
				bit := s.getBitFromBytes(leaf.Path, 0)
				leaf.Path = s.shiftBits(leaf.Path, leaf.PathBits, 1, nil)
				leaf.PathBits--
				if bit == 0 {
					parent.Left = leaf
				} else {
					parent.Right = leaf
				}
			}
		}
		bucket := NewArchiveBucketNode(nil, 0, nil, nil, 0)
		s.recomputeBucket(bucket, newItems)
		bucket.SetDirty(true)
		parent.StubList = append(parent.StubList, bucket)
		parent.SetDirty(true)
		return parent
	}
}

// distributeStubs 在节点分裂时，将原有的 StubList 分配给更深层的子节点（如果前缀匹配）
func (s *Shard) distributeStubs(oldInternal *InternalNode, newParent *InternalNode) {
	stubs := oldInternal.StubList
	if len(stubs) == 0 {
		return
	}

	oldNodeBit := s.getBitFromBytes(oldInternal.Path, 0) // 分裂位对应的旧节点方向
	var newStubs []*ArchiveBucketNode
	var passedStubs []*ArchiveBucketNode

	for _, bucket := range stubs {
		if bucket.PathBits > 0 {
			bucketBit := s.getBitFromBytes(bucket.Path, 0)
			if bucketBit == oldNodeBit {
				// 匹配旧节点方向，下移
				bucket.Path = s.shiftBits(bucket.Path, bucket.PathBits, 1, nil)
				bucket.PathBits--
				newStubs = append(newStubs, bucket)
				continue
			}
		}
		// 不匹配或 PathBits 为 0，保留在新 parent（即当前分裂层级）
		passedStubs = append(passedStubs, bucket)
	}
	oldInternal.StubList = newStubs
	newParent.StubList = passedStubs
}

// shrink 实现积极收缩：若节点只有一个孩子，则将其与孩子合并以保持紧凑
func (s *Shard) shrink(n *InternalNode) Node {
	if n.Left != nil && n.Right != nil {
		return n
	}
	if n.Left == nil && n.Right == nil {
		if len(n.StubList) == 0 {
			return nil
		}
		return n
	}

	// 只有一个孩子，且如果该孩子是热数据节点，尝试合并路径
	var child Node
	bit := byte(0)
	if n.Left != nil {
		child = n.Left
	} else {
		child = n.Right
		bit = 1
	}

	// 如果当前节点有 StubList，收缩会导致挂载关系混乱，暂不收缩
	if len(n.StubList) > 0 {
		return n
	}

	switch c := child.(type) {
	case *LeafNode:
		c.Path = s.concatPath(n.Path, n.PathBits, bit, c.Path, c.PathBits)
		c.PathBits = n.PathBits + 1 + c.PathBits
		c.SetDirty(true)
		return c
	case *InternalNode:
		c.Path = s.concatPath(n.Path, n.PathBits, bit, c.Path, c.PathBits)
		c.PathBits = n.PathBits + 1 + c.PathBits
		c.SetDirty(true)
		return c
	}
	return n
}

func (s *Shard) concatPath(p1 []byte, b1 int, bit byte, p2 []byte, b2 int) []byte {
	total := b1 + 1 + b2
	res := make([]byte, (total+7)/8)
	s.copyBits(res, 0, p1, b1)
	s.setBitInBytes(res, b1, bit)
	s.copyBits(res, b1+1, p2, b2)
	return res
}
