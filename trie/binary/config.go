package binary

// MaxPathBits is the maximum number of bits a key path can have in the binary trie.
// Storage keys use compositeKey = address(20 bytes) + slot(32 bytes) = 52 bytes = 416 bits.
// This must be the ceiling for all path-related overflow checks.
const MaxPathBits = 416

// ArchiveStore identifies the interface to store and retrieve archived bucket data.
type ArchiveStore interface {
	PutBucket(hash []byte, data []byte) error
	GetBucket(hash []byte) ([]byte, error)
	DeleteBucket(hash []byte) error
}

// Config holds the configuration parameters for the Trie.
type Config struct {
	ShardDepth            int          // Number of bits for shard routing (default 16)
	ArchiveBucketSize     int          // Max number of items in an archive bucket before splitting (default 100)
	ArchiveItemCacheLimit int          // Max decoded archived items cached per bucket; 0 disables item caching, negative keeps all
	ArchiveDB             ArchiveStore // Separate store for archive data
	CuckooBuckets         int          // Number of buckets in cuckoo filter (default 32)
	CuckooSlots           int          // Slots per bucket in cuckoo filter (default 4)
	ShardCacheLimit       int          // [NEW] Maximum number of shards to keep in memory
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() *Config {
	return &Config{
		ShardDepth:            16,
		ArchiveBucketSize:     100,
		ArchiveItemCacheLimit: -1,
		CuckooBuckets:         32,
		CuckooSlots:           4,
		ShardCacheLimit:       1024, // [NEW] Default 1024 shards
	}
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
	BucketCount      int   // Total number of archive buckets
	LeafCount        int64 // Total number of reachable leaf nodes
	ArchivedDataSize int64 // Total number of archived KV pairs
	MaxBucketsPath   int   // Max number of buckets on a single path

	ArchiveReadCount   int64 // Number of times archive store was accessed
	FalsePositiveCount int64 // Number of false positives from Cuckoo Filter
	TotalProofSize     int64 // Total size of generated proofs
	ExistProofCount    int64 // Count of existence proofs
	NonExistProofCount int64 // Count of non-existence proofs
	ArchiveStorageSize int64 // [NEW] Total size of archived data in bytes
}

// Stats returns the statistics for the entire Trie.
func (t *Trie) Stats() *TrieStats {
	stats := &TrieStats{}
	numShards := 1 << t.config.ShardDepth
	for i := 0; i < numShards; i++ {
		shard, err := t.getOrCreateShard(i)
		if err == nil && shard != nil {
			shard.accumulateStats(stats)
		}
	}
	return stats
}

func (s *Shard) accumulateStats(stats *TrieStats) {
	s.mu.RLock()
	if s.root == nil && len(s.rootHash) > 0 {
		s.mu.RUnlock()
		s.mu.Lock()
		if s.root == nil {
			node, _ := s.loadNode(s.rootHash)
			s.root = node
		}
		s.mu.Unlock()
		s.mu.RLock()
	}
	root := s.root
	s.mu.RUnlock()

	if root == nil {
		return
	}
	s.nodeStats(root, 0, stats)
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
		stats.LeafCount++
	case *ArchiveBucketNode:
		stats.BucketCount++
		stats.ArchivedDataSize += int64(n.Count)
		// [NEW] 统计归档数据的字节大小
		if data, err := s.getBucketData(s.ensureBucketHash(n)); err == nil {
			stats.ArchiveStorageSize += int64(len(data))
		}
		if currentPathBuckets+1 > stats.MaxBucketsPath {
			stats.MaxBucketsPath = currentPathBuckets + 1
		}
	}
}
