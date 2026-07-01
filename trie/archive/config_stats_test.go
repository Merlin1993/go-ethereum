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
