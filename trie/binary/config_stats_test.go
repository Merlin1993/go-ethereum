package binary

import (
	"crypto/rand"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func TestConfigAndStats(t *testing.T) {
	// 1. Test ShardDepth configuration
	db := &MemoryDBAdapter{memorydb.New()}
	hasher := NewPooledKeccakHasher()
	config := &Config{
		ShardDepth:        4,
		ArchiveBucketSize: 5,
		ArchiveDB:         db,
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
	// Initially global bit was 0, so items have bit0=1.
	// To archive them, we set global bit to 1 and prune.
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
