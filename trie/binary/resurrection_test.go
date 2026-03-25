package binary

import (
	"bytes"
	"crypto/rand"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestResurrectionMetrics(t *testing.T) {
	trie, _ := setupTrie()

	// 1. Reset metrics
	atomic.StoreInt64(&common.BinaryMissExistentCount, 0)
	atomic.StoreInt64(&common.BinaryProofGenTime, 0)
	atomic.StoreInt64(&common.BinaryProofVerifTime, 0)

	// 2. Put data and move to archive
	key := make([]byte, 32)
	rand.Read(key)
	key[0], key[1] = 0x00, 0x01 // Shard 1
	val := []byte("resurrection-test-value")

	trie.Put(key, val)
	trie.SetGlobalEpoch(0)
	trie.Commit()
	trie.FlushArchives()

	// Move to archive by changing global epoch and pruning
	trie.SetGlobalEpoch(1)
	trie.pruneShardIdx = 1
	trie.PruneNextShard()
	trie.Commit()
	trie.FlushArchives()

	// 3. Verify it's in archive
	stats := trie.Stats()
	if stats.ArchivedDataSize != 1 {
		t.Fatalf("Expected 1 archived item, got %d", stats.ArchivedDataSize)
	}

	// Manual DB check
	// Find the shard
	shardID := trie.getShardID(key)
	shard := trie.shards[shardID]
	if shard == nil {
		t.Fatalf("Shard %d not loaded", shardID)
	}
	// The root of the shard should be an ArchiveBucketNode or an InternalNode containing it
	// But let's check what's in pendingArchives etc.
	t.Logf("Shard %d pendingArchives: %d", shardID, len(shard.pendingArchives))

	// 4. Get the data (Resurrection)
	got, err := trie.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("VALUE MISMATCH CAUGHT: got %x, want %x", got, val)
	}

	// 5. Check metrics
	missEx := atomic.LoadInt64(&common.BinaryMissExistentCount)
	genTime := atomic.LoadInt64(&common.BinaryProofGenTime)
	genTimeMax := atomic.LoadInt64(&common.BinaryProofGenTimeMax)
	verifTime := atomic.LoadInt64(&common.BinaryProofVerifTime)
	verifTimeMax := atomic.LoadInt64(&common.BinaryProofVerifTimeMax)
	totalSize := atomic.LoadInt64(&common.BinaryTotalProofSize)

	t.Logf("MissExistentCount: %d", missEx)
	t.Logf("ProofGenTime: %d ns (Max: %d ns)", genTime, genTimeMax)
	t.Logf("ProofVerifTime: %d ns (Max: %d ns)", verifTime, verifTimeMax)
	t.Logf("TotalProofSize: %d bytes", totalSize)

	common.BinaryStatsMu.Lock()
	t.Logf("ItemProofSizes count: %d", len(common.BinaryItemProofSizes))
	common.BinaryStatsMu.Unlock()

	if missEx != 1 {
		t.Errorf("Expected MissExistentCount 1, got %d", missEx)
	}
	if genTime < 0 {
		t.Errorf("Expected ProofGenTime >= 0, got %d", genTime)
	}
	if verifTime < 0 {
		t.Errorf("Expected ProofVerifTime >= 0, got %d", verifTime)
	}
	if verifTime > genTime {
		t.Errorf("ProofVerifTime (%d) should be less than or equal to ProofGenTime (%d)", verifTime, genTime)
	}
}
