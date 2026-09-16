// package archive — regression for the B3 cross-bucket stale membership repair.
package archive

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// TestSelfHealRepairsCrossBucketStaleMembership mirrors the DIAG dump shape:
// the key sits in one bucket with a stale valueRef and in another bucket
// carrying the flat-verified ref. Get must drop the stale membership, keep
// the verified one, and return the correct value instead of failing.
func TestSelfHealRepairsCrossBucketStaleMembership(t *testing.T) {
	t.Parallel()

	db := NewMemoryDBAdapter()
	config := DefaultConfig()
	config.ShardDepth = 0
	config.CuckooBuckets = 64
	config.CuckooSlots = 4
	shard, err := NewShard(0, db, NewPooledKeccakHasher(), config, nil, true, func() byte { return 0 })
	if err != nil {
		t.Fatalf("new shard: %v", err)
	}

	key := common.Hex2Bytes("fc18a0e13941303b0693c926b1d87443b0fec19618732037c266941546234b00")
	value := []byte("payload-under-test")
	verifiedRef := shard.stageValueForKey(key, value)
	staleRef := common.Hex2Bytes("695b55134f38979c27d0ebcefd16a5858a2c4068afbde54a7dfe3675e4001f99")

	// Verified membership: a bucket holding the key with the current ref.
	goodBucket := shard.buildArchiveBucket([]ArchivedKV{{
		Suffix:     append([]byte{}, key...),
		SuffixBits: len(key) * 8,
		Value:      common.CopyBytes(verifiedRef),
	}}, nil, 0).(*ArchiveBucketNode)

	// Stale duplicate: same key in a second bucket with the older ref — the
	// exact cross-bucket double membership produced at batch 132297.
	staleBucket := &ArchiveBucketNode{Path: []byte{0xfc}, PathBits: 8}
	shard.recomputeBucket(staleBucket, []ArchivedKV{{
		Suffix:     append([]byte{}, key[1:]...),
		SuffixBits: len(key)*8 - 8,
		Value:      staleRef,
	}})

	// A surviving leaf on the other side keeps the hot probe away from the key.
	survivingKey := bytes.Repeat([]byte{0x80}, 32)
	survivingRef := shard.stageValueForKey(survivingKey, []byte("surviving-value"))
	root := &InternalNode{
		Left:     NewLeafNode(survivingKey, len(survivingKey)*8, survivingRef),
		StubList: []*ArchiveBucketNode{goodBucket, staleBucket},
		dirty:    true,
	}
	shard.refreshInternalEpochMask(root)
	shard.root = root

	// Get must self-heal rather than fail verification.
	got, err := shard.Get(key)
	if err != nil {
		t.Fatalf("Get after stale membership injection: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("wrong payload returned: %x", got)
	}

	// The stale bucket must no longer carry the key.
	keys, err := shard.bucketKeys(staleBucket)
	if err != nil {
		t.Fatalf("bucketKeys: %v", err)
	}
	for _, item := range keys {
		if item.SuffixBits == len(key)*8-8 && bytes.Equal(item.Suffix, key[1:]) {
			t.Fatal("stale membership survived self-heal")
		}
	}

	// The verified bucket keeps the membership (until read-activation removes
	// it, depending on config) and a re-read still resolves.
	if _, err := shard.Get(key); err != nil {
		t.Fatalf("re-read after heal: %v", err)
	}
}
