package archive

import (
	"crypto/rand"
	"testing"
)

func TestConfigAndStats(t *testing.T) {
	// 1. Test ShardDepth configuration
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := &Config{
		ShardDepth:        4,
		ArchiveBucketSize: 5,
	}
	trie := NewTrie(nil, db, hasher, config, true)

	if len(trie.shards) != 16 {
		t.Errorf("Expected 16 shards, got %d", len(trie.shards))
	}

	// 2. Put some data and verify Stats()
	keys := make([][]byte, 20)
	for i := 0; i < 20; i++ {
		keys[i] = make([]byte, 32)
		rand.Read(keys[i])
		// Path routing happens on first 4 bits
		trie.Put(keys[i], []byte("val"))
	}
	trie.Commit()

	// 3. Trigger pruning to create archive buckets
	// Fresh writes start on epoch bit 1; seeding global to 1 makes shard 0
	// flip to 0, and the rest of this pruning pass archives the same epoch.
	trie.SetGlobalEpoch(1)
	// Prune all shards
	for i := 0; i < 16; i++ {
		trie.pruneShardIdx = i
		trie.PruneNextShard()
	}
	trie.Commit()

	stats := trie.Stats()
	if stats.BucketCount == 0 {
		t.Errorf("Expected at least one bucket after pruning, got 0")
	}
	if stats.ArchivedDataSize != 20 {
		t.Errorf("Expected 20 archived items, got %d", stats.ArchivedDataSize)
	}
	t.Logf("Stats: Buckets=%d, Items=%d, MaxPath=%d", stats.BucketCount, stats.ArchivedDataSize, stats.MaxBucketsPath)

	// 4. Test ArchiveBucketSize
	// Data size 20, BucketSize 5. Individual buckets should split if many items hit the same path.
}

func TestStatsDoesNotCreateEmptyShards(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := &Config{
		ShardDepth:        8,
		ArchiveBucketSize: 5,
	}
	trie := NewTrie(nil, db, hasher, config, true)

	key := make([]byte, 32)
	key[0] = 0x42
	if err := trie.Put(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	root, err := trie.Commit()
	if err != nil {
		t.Fatal(err)
	}

	reloaded := NewTrie(root, db, hasher, config, true)
	if loaded := countLoadedShardsForTest(reloaded); loaded != 0 {
		t.Fatalf("expected reload to keep shards lazy, got %d loaded shards", loaded)
	}

	stats := reloaded.Stats()
	if stats.LeafCount != 1 {
		t.Fatalf("expected Stats to count one persisted leaf, got %d", stats.LeafCount)
	}
	if loaded := countLoadedShardsForTest(reloaded); loaded != 0 {
		t.Fatalf("Stats should not install shard objects, got %d loaded shards", loaded)
	}
}

func TestStatsFallsBackToCommittedShardRoot(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := &Config{
		ShardDepth:        8,
		ArchiveBucketSize: 5,
	}
	trie := NewTrie(nil, db, hasher, config, true)

	key := make([]byte, 32)
	key[0] = 0x42
	if err := trie.Put(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := trie.Commit(); err != nil {
		t.Fatal(err)
	}

	shard := trie.shards[0x42]
	if shard == nil {
		t.Fatal("expected loaded shard")
	}
	shard.mu.Lock()
	shard.root = nil
	shard.rootHash = nil
	shard.mu.Unlock()

	stats := trie.Stats()
	if stats.LeafCount != 1 {
		t.Fatalf("expected Stats to fall back to the committed shard root, got %d leaves", stats.LeafCount)
	}
}

func TestStatsSeparatesStubAndChildArchiveBuckets(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	shard, err := NewShard(0, db, hasher, config, nil, true, func() byte { return 0 })
	if err != nil {
		t.Fatal(err)
	}

	root := &InternalNode{
		StubList: []*ArchiveBucketNode{{Count: 2}},
		Left:     &ArchiveBucketNode{Count: 3},
		Right:    &InternalNode{StubList: []*ArchiveBucketNode{{Count: 4}}},
	}
	stats := &TrieStats{bucketItemHist: make(map[int]int)}
	shard.nodeStatsAtPath(root, nil, 0, 0, stats)
	stats.finalizeBucketItemStats()

	if stats.BucketCount != 3 || stats.ArchivedDataSize != 9 {
		t.Fatalf("unexpected total stats: buckets=%d items=%d", stats.BucketCount, stats.ArchivedDataSize)
	}
	if stats.StubBucketCount != 2 || stats.StubArchivedSize != 6 {
		t.Fatalf("unexpected stub stats: buckets=%d items=%d", stats.StubBucketCount, stats.StubArchivedSize)
	}
	if stats.RootBucketCount != 1 || stats.RootArchivedSize != 2 {
		t.Fatalf("unexpected root bucket stats: buckets=%d items=%d", stats.RootBucketCount, stats.RootArchivedSize)
	}
	if stats.RootLeafBucketCount != 0 || stats.RootLeafArchivedSize != 0 {
		t.Fatalf("root StubList bucket should not count as root leaf: buckets=%d items=%d", stats.RootLeafBucketCount, stats.RootLeafArchivedSize)
	}
	if stats.RootStubBucketCount != 1 || stats.RootStubArchivedSize != 2 {
		t.Fatalf("unexpected root stub stats: buckets=%d items=%d", stats.RootStubBucketCount, stats.RootStubArchivedSize)
	}
	if stats.DeepStubBucketCount != 1 || stats.DeepStubArchivedSize != 4 {
		t.Fatalf("unexpected deep stub stats: buckets=%d items=%d", stats.DeepStubBucketCount, stats.DeepStubArchivedSize)
	}
	if stats.ChildBucketCount != 1 || stats.ChildArchivedSize != 3 {
		t.Fatalf("unexpected child stats: buckets=%d items=%d", stats.ChildBucketCount, stats.ChildArchivedSize)
	}
	if stats.MaxBucketsPath != 2 {
		t.Fatalf("expected stub plus child bucket on one path, got %d", stats.MaxBucketsPath)
	}

	rootBucketStats := &TrieStats{bucketItemHist: make(map[int]int)}
	shard.nodeStatsAtPath(&ArchiveBucketNode{Count: 5}, nil, 0, 0, rootBucketStats)
	if rootBucketStats.RootBucketCount != 1 || rootBucketStats.RootArchivedSize != 5 {
		t.Fatalf("shard-root archive bucket should count as root: buckets=%d items=%d", rootBucketStats.RootBucketCount, rootBucketStats.RootArchivedSize)
	}
	if rootBucketStats.RootLeafBucketCount != 1 || rootBucketStats.RootLeafArchivedSize != 5 {
		t.Fatalf("standalone shard-root archive bucket should count as root leaf: buckets=%d items=%d", rootBucketStats.RootLeafBucketCount, rootBucketStats.RootLeafArchivedSize)
	}
	if rootBucketStats.ChildBucketCount != 0 || rootBucketStats.ChildArchivedSize != 0 {
		t.Fatalf("shard-root archive bucket should not count as child: buckets=%d items=%d", rootBucketStats.ChildBucketCount, rootBucketStats.ChildArchivedSize)
	}
}

func TestStatsCountsStubListAsSinglePathStop(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	shard, err := NewShard(0, db, hasher, config, nil, true, func() byte { return 0 })
	if err != nil {
		t.Fatal(err)
	}

	rootStubs := make([]*ArchiveBucketNode, 64)
	for i := range rootStubs {
		rootStubs[i] = &ArchiveBucketNode{Count: 1}
	}
	root := &InternalNode{
		StubList: rootStubs,
		Left:     &ArchiveBucketNode{Count: 7},
	}

	stats := &TrieStats{bucketItemHist: make(map[int]int)}
	shard.nodeStatsAtPath(root, nil, 0, 0, stats)

	if stats.BucketCount != 65 || stats.ArchivedDataSize != 71 {
		t.Fatalf("unexpected totals: buckets=%d items=%d", stats.BucketCount, stats.ArchivedDataSize)
	}
	if stats.MaxBucketsPath != 2 {
		t.Fatalf("same-node StubList fanout should count as one path stop, got MaxBucketsPath=%d", stats.MaxBucketsPath)
	}
	if stats.MaxRootStubBuckets != 64 || stats.MaxRootStubItems != 64 {
		t.Fatalf("root StubList fanout should be tracked separately: buckets=%d items=%d", stats.MaxRootStubBuckets, stats.MaxRootStubItems)
	}
}

func TestStatsMaxStubListItemsUpdatesOnEqualBucketCount(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	shard, err := NewShard(0, db, hasher, config, nil, true, func() byte { return 0 })
	if err != nil {
		t.Fatal(err)
	}

	stats := &TrieStats{bucketItemHist: make(map[int]int)}
	shard.nodeStatsAtPath(&InternalNode{
		StubList: []*ArchiveBucketNode{{Count: 1}},
	}, nil, 0, 0, stats)
	shard.nodeStatsAtPath(&InternalNode{
		StubList: []*ArchiveBucketNode{{Count: 23}},
	}, nil, 0, 0, stats)
	shard.nodeStatsAtPath(&InternalNode{
		Left: &InternalNode{StubList: []*ArchiveBucketNode{{Count: 5}}},
	}, nil, 0, 0, stats)
	shard.nodeStatsAtPath(&InternalNode{
		Left: &InternalNode{StubList: []*ArchiveBucketNode{{Count: 17}}},
	}, nil, 0, 0, stats)

	if stats.MaxStubListBuckets != 1 || stats.MaxStubListItems != 23 {
		t.Fatalf("max stub list items should update on equal bucket count: buckets=%d items=%d", stats.MaxStubListBuckets, stats.MaxStubListItems)
	}
	if stats.MaxRootStubBuckets != 1 || stats.MaxRootStubItems != 23 {
		t.Fatalf("max root stub items should update on equal bucket count: buckets=%d items=%d", stats.MaxRootStubBuckets, stats.MaxRootStubItems)
	}
	if stats.MaxDeepStubBuckets != 1 || stats.MaxDeepStubItems != 17 {
		t.Fatalf("max deep stub items should update on equal bucket count: buckets=%d items=%d", stats.MaxDeepStubBuckets, stats.MaxDeepStubItems)
	}
}

func TestShardLocalRootBucketsExposeCrossShardSparsity(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ShardDepth = 4
	trie := NewTrie(nil, db, hasher, config, true)

	for id := 0; id < 8; id++ {
		shard, err := NewShard(id, db, hasher, config, nil, true, func() byte { return 0 })
		if err != nil {
			t.Fatal(err)
		}
		shard.root = &ArchiveBucketNode{Count: 1}
		trie.shards[id] = shard
	}

	stats := trie.Stats()
	if stats.BucketCount != 8 || stats.ArchivedDataSize != 8 {
		t.Fatalf("unexpected aggregate stats: buckets=%d items=%d", stats.BucketCount, stats.ArchivedDataSize)
	}
	if stats.RootLeafBucketCount != 8 || stats.RootLeafArchivedSize != 8 {
		t.Fatalf("expected all sparse buckets to be standalone shard roots: buckets=%d items=%d", stats.RootLeafBucketCount, stats.RootLeafArchivedSize)
	}
	if stats.BucketItemsAvg != 1 || stats.BucketItemsP95 != 1 || stats.BucketItemsMax != 1 {
		t.Fatalf("unexpected bucket occupancy: avg=%.2f p95=%d max=%d", stats.BucketItemsAvg, stats.BucketItemsP95, stats.BucketItemsMax)
	}
}

func countLoadedShardsForTest(trie *Trie) int {
	trie.shardsMu.RLock()
	defer trie.shardsMu.RUnlock()

	count := 0
	for _, shard := range trie.shards {
		if shard != nil {
			count++
		}
	}
	return count
}
