package binary

import "sort"

// MaxPathBits is the maximum number of bits a key path can have in the binary trie.
// Storage keys use compositeKey = address(20 bytes) + slot(32 bytes) = 52 bytes = 416 bits.
// This must be the ceiling for all path-related overflow checks.
const MaxPathBits = 416

const (
	NodeStorageHash = "hash"
	NodeStoragePath = "path"
)

// ArchiveStore identifies the interface to store and retrieve archived bucket data.
type ArchiveStore interface {
	PutBucket(hash []byte, data []byte) error
	GetBucket(hash []byte) ([]byte, error)
	DeleteBucket(hash []byte) error
}

type FlatValueReader interface {
	GetFlatValue(key []byte) ([]byte, error)
}

// Config holds the configuration parameters for the Trie.
type Config struct {
	ShardDepth                int          // Number of bits for shard routing (default 16)
	ArchiveBucketSize         int          // Max number of items in an archive bucket before splitting (default 100)
	ArchiveItemCacheLimit     int          // Max decoded archived items cached per bucket; 0 disables item caching, negative keeps all
	NodeCacheLimit            int          // Max serialized node blobs cached in process; 0 uses default, negative disables
	NodeCacheWarmPathBits     int          // Path-mode eager warming depth; 0 uses default, -1 keeps root-only, <-1 disables eager warming
	CommitmentPointCacheLimit int          // Max decoded ECMH commitment points cached in process; 0 uses default, negative disables
	EnablePathDiagnostics     bool         // Record path/cache diagnostics; disabled by default for hot experiments
	CompactArchiveStubs       bool         // Merge adjacent archive stubs synchronously; expensive on hot pruning paths
	ArchiveDB                 ArchiveStore // Separate store for archive data
	FlatReader                FlatValueReader
	CuckooBuckets             int  // Number of buckets in cuckoo filter (default 32)
	CuckooSlots               int  // Slots per bucket in cuckoo filter (default 4)
	InlineValueThreshold      int  // Inline values up to this size into leaf/archive refs; 0 disables
	DeleteOldValues           bool // Use key-bound value refs and delete superseded external value blobs
	NodeStorageScheme         string
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() *Config {
	return &Config{
		ShardDepth:                16,
		ArchiveBucketSize:         100,
		ArchiveItemCacheLimit:     -1,
		NodeCacheLimit:            DefaultNodeCacheLimit,
		NodeCacheWarmPathBits:     DefaultNodeCacheWarmPathBits,
		CommitmentPointCacheLimit: DefaultCommitmentPointCacheLimit,
		CompactArchiveStubs:       true,
		CuckooBuckets:             32,
		CuckooSlots:               4,
		NodeStorageScheme:         NodeStorageHash,
	}
}

func (c *Config) UsePathStorage() bool {
	return c != nil && c.NodeStorageScheme == NodeStoragePath
}

// ResolveArchiveBucketSize returns the restricted bucket size based on cuckoo configuration.
func (c *Config) ResolveArchiveBucketSize() int {
	if c.CuckooSlots == 2 && c.CuckooBuckets == 8 {
		return 10
	}
	if c.CuckooSlots == 4 && c.CuckooBuckets == 8 {
		return 30
	}
	if c.CuckooSlots == 4 && c.CuckooBuckets == 16 {
		return 60
	}
	if c.CuckooSlots == 4 && c.CuckooBuckets == 32 {
		return 100
	}
	return c.ArchiveBucketSize
}

// TrieStats holds statistics about the Trie.
type TrieStats struct {
	BucketCount      int     // Total number of archive buckets
	LeafCount        int64   // Total number of reachable leaf nodes
	ArchivedDataSize int64   // Total number of archived KV pairs
	MaxBucketsPath   int     // Max number of buckets on a single path
	BucketItemsAvg   float64 // Average number of archived KV pairs per bucket
	BucketItemsP50   int     // P50 archived KV pairs per bucket
	BucketItemsP95   int     // P95 archived KV pairs per bucket
	BucketItemsP99   int     // P99 archived KV pairs per bucket
	BucketItemsMax   int     // Max archived KV pairs in a bucket

	bucketItemHist map[int]int

	ArchiveReadCount   int64 // Number of times archive store was accessed
	FalsePositiveCount int64 // Number of false positives from Cuckoo Filter
	TotalProofSize     int64 // Total size of generated proofs
	ExistProofCount    int64 // Count of existence proofs
	NonExistProofCount int64 // Count of non-existence proofs
	ArchiveStorageSize int64 // Total size of archived data persisted in archive storage
}

// Stats returns the statistics for the entire Trie.
func (t *Trie) Stats() *TrieStats {
	stats := &TrieStats{bucketItemHist: make(map[int]int)}

	t.shardsMu.RLock()
	shards := make([]*Shard, len(t.shards))
	copy(shards, t.shards)
	t.shardsMu.RUnlock()

	seen := make(map[int]struct{}, len(shards))
	for i, shard := range shards {
		if shard == nil {
			continue
		}
		seen[i] = struct{}{}
		shard.accumulateStatsIsolated(stats)
	}

	if t.topTree != nil {
		roots := make(map[int][]byte)
		t.topTree.ForEachShardRoot(func(id int, hash []byte) {
			if id < 0 || id >= len(shards) {
				return
			}
			if _, ok := seen[id]; ok {
				return
			}
			roots[id] = hash
		})

		for id, root := range roots {
			shardID := id
			shard := newStatsShardView(shardID, t.db, t.hasher, t.config, nil, root, t.pruning, func() byte {
				if shardID < t.pruneShardIdx {
					return t.globalEpochBit
				}
				return t.globalEpochBit ^ 1
			})
			shard.accumulateStats(stats)
		}
	}

	stats.finalizeBucketItemStats()
	return stats
}

func newStatsShardView(id int, db KVStore, hasher Hasher, config *Config, nodeCache *nodeBlobCache, rootHash []byte, pruning bool, globalEpochBit func() byte) *Shard {
	return &Shard{
		id:             id,
		db:             db,
		hasher:         hasher,
		config:         config,
		nodeCache:      nodeCache,
		rootHash:       append([]byte(nil), rootHash...),
		nodePaths:      make(map[string]persistedNodePath),
		pruning:        pruning,
		globalEpochBit: globalEpochBit,
		stats:          &TrieStats{},
	}
}

func (s *TrieStats) addBucketItemCount(count uint64) {
	if count > uint64(^uint(0)>>1) {
		count = uint64(^uint(0) >> 1)
	}
	c := int(count)
	s.bucketItemHist[c]++
	if c > s.BucketItemsMax {
		s.BucketItemsMax = c
	}
}

func (s *TrieStats) finalizeBucketItemStats() {
	if s.BucketCount == 0 {
		return
	}
	s.BucketItemsAvg = float64(s.ArchivedDataSize) / float64(s.BucketCount)
	if len(s.bucketItemHist) == 0 {
		return
	}

	keys := make([]int, 0, len(s.bucketItemHist))
	for count := range s.bucketItemHist {
		keys = append(keys, count)
	}
	sort.Ints(keys)

	p50Rank := percentileRank(s.BucketCount, 0.50)
	p95Rank := percentileRank(s.BucketCount, 0.95)
	p99Rank := percentileRank(s.BucketCount, 0.99)
	seen := 0
	for _, count := range keys {
		seen += s.bucketItemHist[count]
		if s.BucketItemsP50 == 0 && seen >= p50Rank {
			s.BucketItemsP50 = count
		}
		if s.BucketItemsP95 == 0 && seen >= p95Rank {
			s.BucketItemsP95 = count
		}
		if s.BucketItemsP99 == 0 && seen >= p99Rank {
			s.BucketItemsP99 = count
			break
		}
	}
}

func percentileRank(total int, p float64) int {
	rank := int(float64(total) * p)
	if rank < 1 {
		return 1
	}
	if rank > total {
		return total
	}
	return rank
}

func (s *Shard) accumulateStats(stats *TrieStats) {
	s.accumulateStatsWithCache(stats, s.nodeCache)
}

func (s *Shard) accumulateStatsIsolated(stats *TrieStats) {
	s.accumulateStatsWithCache(stats, nil)
}

func (s *Shard) accumulateStatsWithCache(stats *TrieStats, nodeCache *nodeBlobCache) {
	s.mu.RLock()
	root := s.root
	rootHash := append([]byte(nil), s.rootHash...)
	s.mu.RUnlock()

	if s.stats != nil {
		s.statsMut.Lock()
		stats.ArchiveReadCount += s.stats.ArchiveReadCount
		stats.FalsePositiveCount += s.stats.FalsePositiveCount
		stats.ArchiveStorageSize += s.stats.ArchiveStorageSize
		s.statsMut.Unlock()
	}

	if root == nil && len(rootHash) > 0 {
		view := newStatsShardView(s.id, s.db, s.hasher, s.config, nodeCache, rootHash, s.pruning, s.globalEpochBit)
		loaded, err := view.loadNodeAtPath(rootHash, nil, 0)
		if err == nil {
			root = loaded
			view.nodeStatsAtPath(root, nil, 0, 0, stats)
			return
		}
	}

	if root == nil {
		return
	}
	s.nodeStatsAtPath(root, nil, 0, 0, stats)
}

func (s *Shard) nodeStats(node Node, currentPathBuckets int, stats *TrieStats) {
	s.nodeStatsAtPath(node, nil, 0, currentPathBuckets, stats)
}

func (s *Shard) nodeStatsAtPath(node Node, path []byte, pathBits int, currentPathBuckets int, stats *TrieStats) {
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
			stats.addBucketItemCount(bucket.Count)
		}

		newPathBuckets := currentPathBuckets + numBuckets
		if newPathBuckets > stats.MaxBucketsPath {
			stats.MaxBucketsPath = newPathBuckets
		}

		// Recurse to children
		leftPath, leftBits := s.childStoragePath(path, pathBits, n, 0)
		if n.Left != nil {
			s.nodeStatsAtPath(n.Left, leftPath, leftBits, newPathBuckets, stats)
		} else if len(n.LeftHash) > 0 {
			loaded, _ := s.loadNodeAtPath(n.LeftHash, leftPath, leftBits)
			if loaded != nil {
				s.nodeStatsAtPath(loaded, leftPath, leftBits, newPathBuckets, stats)
			}
		}

		rightPath, rightBits := s.childStoragePath(path, pathBits, n, 1)
		if n.Right != nil {
			s.nodeStatsAtPath(n.Right, rightPath, rightBits, newPathBuckets, stats)
		} else if len(n.RightHash) > 0 {
			loaded, _ := s.loadNodeAtPath(n.RightHash, rightPath, rightBits)
			if loaded != nil {
				s.nodeStatsAtPath(loaded, rightPath, rightBits, newPathBuckets, stats)
			}
		}
	case *LeafNode:
		stats.LeafCount++
	case *ArchiveBucketNode:
		stats.BucketCount++
		stats.ArchivedDataSize += int64(n.Count)
		stats.addBucketItemCount(n.Count)
		if currentPathBuckets+1 > stats.MaxBucketsPath {
			stats.MaxBucketsPath = currentPathBuckets + 1
		}
	}
}
