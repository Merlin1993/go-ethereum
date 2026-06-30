package archive

import "math/rand"

// ArchiveFilterFPStats summarizes direct Cuckoo filter false-positive sampling.
type ArchiveFilterFPStats struct {
	BucketCount    int64
	SampledBuckets int64
	Samples        int64
	FalsePositives int64
	Rate           float64
}

// SampleArchiveFilterFalsePositives samples non-member suffixes directly against
// archive bucket filters. It is a diagnostic helper and does not affect roots.
func (t *Trie) SampleArchiveFilterFalsePositives(samplesPerBucket int, seed int64) *ArchiveFilterFPStats {
	stats := &ArchiveFilterFPStats{}
	if t == nil || samplesPerBucket <= 0 {
		return stats
	}
	rng := rand.New(rand.NewSource(seed))

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
		shard.sampleArchiveFilterFPIsolated(stats, rng, samplesPerBucket)
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
			shard.sampleArchiveFilterFP(stats, rng, samplesPerBucket)
		}
	}

	if stats.Samples > 0 {
		stats.Rate = float64(stats.FalsePositives) / float64(stats.Samples)
	}
	return stats
}

func (s *Shard) sampleArchiveFilterFPIsolated(stats *ArchiveFilterFPStats, rng *rand.Rand, samplesPerBucket int) {
	s.sampleArchiveFilterFPWithCache(stats, rng, samplesPerBucket, nil)
}

func (s *Shard) sampleArchiveFilterFP(stats *ArchiveFilterFPStats, rng *rand.Rand, samplesPerBucket int) {
	s.sampleArchiveFilterFPWithCache(stats, rng, samplesPerBucket, s.nodeCache)
}

func (s *Shard) sampleArchiveFilterFPWithCache(stats *ArchiveFilterFPStats, rng *rand.Rand, samplesPerBucket int, nodeCache *nodeBlobCache) {
	if s == nil || stats == nil || rng == nil || samplesPerBucket <= 0 {
		return
	}
	s.mu.RLock()
	root := s.root
	rootHash := append([]byte(nil), s.rootHash...)
	s.mu.RUnlock()

	if root == nil && len(rootHash) > 0 {
		view := newStatsShardView(s.id, s.db, s.hasher, s.config, nodeCache, rootHash, s.pruning, s.globalEpochBit)
		loaded, err := view.loadNodeAtPath(rootHash, nil, 0)
		if err == nil && loaded != nil {
			view.sampleArchiveFilterFPNode(loaded, nil, 0, stats, rng, samplesPerBucket)
		}
		return
	}
	s.sampleArchiveFilterFPNode(root, nil, 0, stats, rng, samplesPerBucket)
}

func (s *Shard) sampleArchiveFilterFPNode(node Node, path []byte, pathBits int, stats *ArchiveFilterFPStats, rng *rand.Rand, samplesPerBucket int) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *InternalNode:
		for _, bucket := range n.StubList {
			s.sampleArchiveFilterFPBucket(bucket, stats, rng, samplesPerBucket)
		}
		leftPath, leftBits := s.childStoragePath(path, pathBits, n, 0)
		if n.Left != nil {
			s.sampleArchiveFilterFPNode(n.Left, leftPath, leftBits, stats, rng, samplesPerBucket)
		} else if len(n.LeftHash) > 0 {
			loaded, _ := s.loadNodeAtPath(n.LeftHash, leftPath, leftBits)
			if loaded != nil {
				s.sampleArchiveFilterFPNode(loaded, leftPath, leftBits, stats, rng, samplesPerBucket)
			}
		}
		rightPath, rightBits := s.childStoragePath(path, pathBits, n, 1)
		if n.Right != nil {
			s.sampleArchiveFilterFPNode(n.Right, rightPath, rightBits, stats, rng, samplesPerBucket)
		} else if len(n.RightHash) > 0 {
			loaded, _ := s.loadNodeAtPath(n.RightHash, rightPath, rightBits)
			if loaded != nil {
				s.sampleArchiveFilterFPNode(loaded, rightPath, rightBits, stats, rng, samplesPerBucket)
			}
		}
	case *ArchiveBucketNode:
		s.sampleArchiveFilterFPBucket(n, stats, rng, samplesPerBucket)
	}
}

func (s *Shard) sampleArchiveFilterFPBucket(bucket *ArchiveBucketNode, stats *ArchiveFilterFPStats, rng *rand.Rand, samplesPerBucket int) {
	if bucket == nil || bucket.Count == 0 {
		return
	}
	stats.BucketCount++
	filter := s.archiveBucketFilter(bucket)
	if filter == nil {
		return
	}
	keys, err := s.bucketKeys(bucket)
	if err != nil || len(keys) == 0 {
		return
	}
	existing := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		existing[string(archiveItemKey(key.SuffixBits, key.Suffix))] = struct{}{}
	}

	stats.SampledBuckets++
	for i := 0; i < samplesPerBucket; i++ {
		template := keys[rng.Intn(len(keys))]
		keyWithLen, ok := randomNonMemberArchiveSuffix(rng, template.SuffixBits, existing)
		if !ok {
			continue
		}
		stats.Samples++
		if filter.Lookup(keyWithLen) {
			stats.FalsePositives++
		}
	}
}

func randomNonMemberArchiveSuffix(rng *rand.Rand, suffixBits int, existing map[string]struct{}) ([]byte, bool) {
	if suffixBits < 0 {
		return nil, false
	}
	byteLen := (suffixBits + 7) / 8
	for attempt := 0; attempt < 32; attempt++ {
		suffix := make([]byte, byteLen)
		for i := range suffix {
			suffix[i] = byte(rng.Intn(256))
		}
		if rem := suffixBits % 8; rem != 0 && len(suffix) > 0 {
			suffix[len(suffix)-1] &= byte(0xff << (8 - rem))
		}
		keyWithLen := archiveItemKey(suffixBits, suffix)
		if _, ok := existing[string(keyWithLen)]; !ok {
			return keyWithLen, true
		}
	}
	return nil, false
}
