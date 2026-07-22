package archive

import "sort"

// MaxPathBits is the maximum number of bits a key path can have in the archive trie.
// Storage keys use compositeKey = address(20 bytes) + slot(32 bytes) = 52 bytes = 416 bits.
// This must be the ceiling for all path-related overflow checks.
const MaxPathBits = 416

const (
	NodeStorageHash = "hash"
	NodeStoragePath = "path"
)

type FlatValueReader interface {
	GetFlatValue(key []byte) ([]byte, error)
}

// Config holds the configuration parameters for the Trie.
type Config struct {
	ShardDepth                int   // Number of bits for shard routing (default 16)
	ArchiveBucketSize         int   // Max number of items in an archive bucket before splitting (default 100)
	NodeCacheLimit            int   // Max serialized node blobs cached in process; 0 uses default, negative disables
	NodeCacheBytesLimit       int64 // Max serialized node blob bytes cached in process; 0 uses default, negative disables byte cap
	NodeCacheWarmPathBits     int   // Path-mode eager warming depth; 0 uses default, -1 keeps root-only, <-1 disables eager warming
	CommitmentPointCacheLimit int   // Max decoded ECMH commitment points cached in process; 0 uses default, negative disables
	EnablePathDiagnostics     bool  // Record path/cache diagnostics; disabled by default for hot experiments
	CompactArchiveStubs       bool  // Merge adjacent archive stubs synchronously; expensive on hot pruning paths
	ArchiveStubMaxBucketsPath int   // Max side-mounted archive buckets at one node before pressure-sinking; 0 uses default, negative disables
	AsyncPrune                bool  // Run shard pruning in the background and apply it before root commit
	CommitWorkers             int   // Max parallel shard commit workers; 0 uses default
	CommitWatchdogSeconds     int   // Dump goroutines if one wrapper commit exceeds this many seconds; 0 disables
	PhysicalDelete            bool  // Physically delete obsolete trie nodes; false leaves unreachable path nodes for offline cleanup
	StemMode                  bool  // Group 32-byte binary-tree keys by their 31-byte stem for whole-stem archiving
	FlatReader                FlatValueReader
	CuckooBuckets             int // Number of buckets in cuckoo filter (default 32)
	CuckooSlots               int // Slots per bucket in cuckoo filter (default 4)
	NodeStorageScheme         string
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() *Config {
	return &Config{
		ShardDepth:                16,
		ArchiveBucketSize:         100,
		NodeCacheLimit:            DefaultNodeCacheLimit,
		NodeCacheBytesLimit:       DefaultNodeCacheBytesLimit,
		NodeCacheWarmPathBits:     DefaultNodeCacheWarmPathBits,
		CommitmentPointCacheLimit: DefaultCommitmentPointCacheLimit,
		CompactArchiveStubs:       true,
		ArchiveStubMaxBucketsPath: 64,
		CommitWorkers:             DefaultCommitWorkers,
		PhysicalDelete:            true,
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
	BucketCount                      int     // Total number of archive buckets
	LeafCount                        int64   // Total number of reachable leaf nodes
	ArchivedDataSize                 int64   // Total number of archived KV pairs
	ActiveLogicalValues              int64   // Active suffix values (equals LeafCount outside stem mode)
	ArchivedLogicalValues            int64   // Archived suffix values (equals ArchivedDataSize outside stem mode)
	ActiveLogicalValueReadFailures   int64   // Active stem payloads that could not be read or decoded
	ArchivedLogicalValueReadFailures int64   // Archived stem payloads that could not be read or decoded
	RootBucketCount                  int     // Buckets held directly at the shard root
	RootArchivedSize                 int64   // Archived KV pairs held directly at the shard root
	RootLeafBucketCount              int     // Shard roots that are standalone archive buckets
	RootLeafArchivedSize             int64   // Archived KV pairs in standalone shard-root buckets
	MaxBucketsPath                   int     // Max bucket-bearing nodes on a sequential lookup path; StubList fanout is tracked separately
	StubBucketCount                  int     // Buckets held in InternalNode.StubList
	StubArchivedSize                 int64   // Archived KV pairs held in StubList buckets
	RootStubBucketCount              int     // StubList buckets held directly on the shard root
	RootStubArchivedSize             int64   // Archived KV pairs in root StubList buckets
	DeepStubBucketCount              int     // StubList buckets held below the shard root
	DeepStubArchivedSize             int64   // Archived KV pairs in non-root StubList buckets
	ChildBucketCount                 int     // Buckets placed on ordinary child edges
	ChildArchivedSize                int64   // Archived KV pairs held in ordinary child-edge buckets
	MaxStubListBuckets               int     // Max StubList bucket count on one internal node
	MaxStubListItems                 int64   // Archived KV pairs in the max-bucket StubList
	MaxRootStubBuckets               int     // Max root StubList bucket count on one shard root
	MaxRootStubItems                 int64   // Archived KV pairs in the max root StubList
	MaxDeepStubBuckets               int     // Max non-root StubList bucket count on one internal node
	MaxDeepStubItems                 int64   // Archived KV pairs in the max non-root StubList
	BucketItemsAvg                   float64 // Average number of archived KV pairs per bucket
	BucketItemsP50                   int     // P50 archived KV pairs per bucket
	BucketItemsP95                   int     // P95 archived KV pairs per bucket
	BucketItemsP99                   int     // P99 archived KV pairs per bucket
	BucketItemsMax                   int     // Max archived KV pairs in a bucket

	bucketItemHist map[int]int

	FalsePositiveCount int64 // Number of false positives from Cuckoo Filter
	TotalProofSize     int64 // Total size of generated proofs
	ExistProofCount    int64 // Count of existence proofs
	NonExistProofCount int64 // Count of non-existence proofs
}

// Stats returns the statistics for the entire Trie.
func (t *Trie) Stats() *TrieStats {
	_ = t.finishAsyncPrune()
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
		if shard.accumulateStatsIsolated(stats) {
			seen[i] = struct{}{}
		}
	}

	if roots, err := t.allShardRoots(); err == nil {
		for id, root := range roots {
			if id < 0 || id >= len(shards) {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
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

func (s *Shard) accumulateStats(stats *TrieStats) bool {
	return s.accumulateStatsWithCache(stats, s.nodeCache)
}

func (s *Shard) accumulateStatsIsolated(stats *TrieStats) bool {
	return s.accumulateStatsWithCache(stats, nil)
}

func (s *Shard) accumulateStatsWithCache(stats *TrieStats, nodeCache *nodeBlobCache) bool {
	s.mu.RLock()
	root := s.root
	rootHash := append([]byte(nil), s.rootHash...)
	s.mu.RUnlock()

	if s.stats != nil {
		s.statsMut.Lock()
		stats.FalsePositiveCount += s.stats.FalsePositiveCount
		s.statsMut.Unlock()
	}

	if root == nil && len(rootHash) > 0 {
		view := newStatsShardView(s.id, s.db, s.hasher, s.config, nodeCache, rootHash, s.pruning, s.globalEpochBit)
		loaded, err := view.loadNodeAtPath(rootHash, nil, 0)
		if err == nil {
			root = loaded
			view.nodeStatsAtPath(root, nil, 0, 0, stats)
			return true
		}
	}

	if root == nil {
		return false
	}
	s.nodeStatsAtPath(root, nil, 0, 0, stats)
	return true
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
		atShardRoot := pathBits == 0 && currentPathBuckets == 0
		stats.BucketCount += numBuckets
		stats.StubBucketCount += numBuckets
		var stubItems int64
		if atShardRoot {
			stats.RootBucketCount += numBuckets
			stats.RootStubBucketCount += numBuckets
		} else {
			stats.DeepStubBucketCount += numBuckets
		}
		for _, bucket := range n.StubList {
			stats.ArchivedDataSize += int64(bucket.Count)
			logicalValues, failures := s.archiveLogicalValueCount(bucket)
			stats.ArchivedLogicalValues += logicalValues
			stats.ArchivedLogicalValueReadFailures += failures
			stats.StubArchivedSize += int64(bucket.Count)
			stubItems += int64(bucket.Count)
			if atShardRoot {
				stats.RootArchivedSize += int64(bucket.Count)
				stats.RootStubArchivedSize += int64(bucket.Count)
			} else {
				stats.DeepStubArchivedSize += int64(bucket.Count)
			}
			stats.addBucketItemCount(bucket.Count)
		}
		if numBuckets > stats.MaxStubListBuckets || (numBuckets == stats.MaxStubListBuckets && stubItems > stats.MaxStubListItems) {
			stats.MaxStubListBuckets = numBuckets
			stats.MaxStubListItems = stubItems
		}
		if atShardRoot {
			if numBuckets > stats.MaxRootStubBuckets || (numBuckets == stats.MaxRootStubBuckets && stubItems > stats.MaxRootStubItems) {
				stats.MaxRootStubBuckets = numBuckets
				stats.MaxRootStubItems = stubItems
			}
		} else if numBuckets > stats.MaxDeepStubBuckets || (numBuckets == stats.MaxDeepStubBuckets && stubItems > stats.MaxDeepStubItems) {
			stats.MaxDeepStubBuckets = numBuckets
			stats.MaxDeepStubItems = stubItems
		}

		// A StubList is a side-mounted bucket set at one trie node. A key path can
		// encounter this archive stop once; sibling fanout pressure is reported by
		// MaxStubListBuckets instead of being folded into MaxBucketsPath.
		newPathBuckets := currentPathBuckets
		if numBuckets > 0 {
			newPathBuckets++
		}
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
		if s.config == nil || !s.config.StemMode {
			stats.ActiveLogicalValues++
		} else {
			localKey, localBits := s.prependPath(n.Path, n.PathBits, path, pathBits)
			shardPrefix, shardBits := s.getShardPrefix()
			fullKey, fullBits := s.prependPath(localKey, localBits, shardPrefix, shardBits)
			if fullBits != StemSize*8 {
				stats.ActiveLogicalValueReadFailures++
				break
			}
			payload, err := s.getFlatValue(fullKey)
			if err != nil {
				stats.ActiveLogicalValueReadFailures++
				break
			}
			count, err := stemEncodedValueCount(payload)
			if err != nil {
				stats.ActiveLogicalValueReadFailures++
				break
			}
			stats.ActiveLogicalValues += int64(count)
		}
	case *ArchiveBucketNode:
		stats.BucketCount++
		stats.ArchivedDataSize += int64(n.Count)
		logicalValues, failures := s.archiveLogicalValueCount(n)
		stats.ArchivedLogicalValues += logicalValues
		stats.ArchivedLogicalValueReadFailures += failures
		stats.addBucketItemCount(n.Count)
		if pathBits == 0 && currentPathBuckets == 0 {
			stats.RootBucketCount++
			stats.RootArchivedSize += int64(n.Count)
			stats.RootLeafBucketCount++
			stats.RootLeafArchivedSize += int64(n.Count)
		} else {
			stats.ChildBucketCount++
			stats.ChildArchivedSize += int64(n.Count)
		}
		if currentPathBuckets+1 > stats.MaxBucketsPath {
			stats.MaxBucketsPath = currentPathBuckets + 1
		}
	}
}

func (s *Shard) archiveLogicalValueCount(bucket *ArchiveBucketNode) (int64, int64) {
	if bucket == nil {
		return 0, 0
	}
	if s.config == nil || !s.config.StemMode {
		return int64(bucket.Count), 0
	}
	items, err := s.bucketItemsWithValueRefs(bucket)
	if err != nil {
		return 0, int64(bucket.Count)
	}
	var count, failures int64
	for _, item := range items {
		_, payload := s.archiveItemFlatValue(bucket, item)
		values, err := stemEncodedValueCount(payload)
		if err == nil {
			count += int64(values)
		} else {
			failures++
		}
	}
	return count, failures
}
