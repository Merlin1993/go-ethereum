package archive

import (
	"bytes"
	"strings"
	"testing"
)

// bucketContainsKeyForTest reports whether an archive bucket stores the given
// key (mirrors removeArchiveKeyFromBucket's membership logic without mutating).
func (s *Shard) bucketContainsKeyForTest(bucket *ArchiveBucketNode, key []byte) bool {
	if bucket == nil || bucket.Count == 0 {
		return false
	}
	items, err := s.bucketKeys(bucket)
	if err != nil || items == nil {
		return false
	}
	innerDepth := bucket.PathBits
	keyBits := len(key) * 8
	for _, item := range items {
		if innerDepth+item.SuffixBits == keyBits && s.suffixMatches(key, innerDepth, item.Suffix, item.SuffixBits) {
			return true
		}
	}
	return false
}

// treeHasKeyArchived walks the hot tree (StubList + child edges) looking for a
// surviving archived entry for key.
func treeHasKeyArchived(s *Shard, n Node, key []byte) bool {
	if n == nil {
		return false
	}
	switch node := n.(type) {
	case *ArchiveBucketNode:
		return s.bucketContainsKeyForTest(node, key)
	case *InternalNode:
		for _, b := range node.StubList {
			if s.bucketContainsKeyForTest(b, key) {
				return true
			}
		}
		return treeHasKeyArchived(s, node.Left, key) || treeHasKeyArchived(s, node.Right, key)
	}
	return false
}

// TestArchiveStalePresenceRemovesStaleArchivedOnOverwrite reproduces the root
// cause of the "archive bucket valueRef verification failed" corruption seen in
// key-level AMT runs.
//
// A stale stored archive-presence=false bit on an internal node (which can be
// left behind after destructive-commit reloads) used to make the write path's
// archiveMayContainKey short-circuit to false, so putValueRefLocked skipped
// removeArchivedVersionForWrite. The archived valueRef then survived an
// overwrite while the flat value moved on, leaving a stale committed valueRef
// that a later Read would fail to verify.
//
// This test hand-forges the stale presence, overwrites the key, and asserts the
// write path still cleans up the archived entry. It is RED before the fix and
// GREEN after.
func TestArchiveStalePresenceRemovesStaleArchivedOnOverwrite(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	cfg := DefaultConfig()
	cfg.ShardDepth = 4
	cfg.CompactArchiveStubs = false
	cfg.CuckooBuckets = 32
	cfg.CuckooSlots = 4

	shard, err := NewShard(0, db, hasher, cfg, nil, true, func() byte { return 0 })
	if err != nil {
		t.Fatalf("new shard: %v", err)
	}

	key := bytes.Repeat([]byte{0x11}, 32)
	pathBits := cfg.ShardDepth
	path := make([]byte, (pathBits+7)/8)
	for i := 0; i < pathBits; i++ {
		if shard.getBit(key, i) == 1 {
			path[i/8] |= 0x80 >> (i % 8)
		}
	}

	oldVal := []byte("archived-value-v1")
	ref, err := shard.storedValueRef(key, oldVal)
	if err != nil {
		t.Fatalf("storedValueRef: %v", err)
	}
	if len(ref) != 32 {
		t.Fatalf("expected 32-byte valueRef, got %d", len(ref))
	}

	// Build an archive bucket holding key -> oldVal's valueRef, attached to a
	// freshly-forged internal root whose archive presence bit is stale-false.
	archNode := shard.buildArchiveBucket([]ArchivedKV{{
		Suffix:     bytes.Clone(key),
		SuffixBits: len(key) * 8,
		Value:      ref,
	}}, path, pathBits)
	bucket, ok := archNode.(*ArchiveBucketNode)
	if !ok {
		t.Fatalf("expected ArchiveBucketNode, got %T", archNode)
	}
	root := &InternalNode{Path: path, PathBits: pathBits, dirty: true}
	root.StubList = []*ArchiveBucketNode{bucket}
	root.epoch = setStoredSubtreeArchivePresence(root.epoch, false) // presence valid but false (stale)
	shard.root = root

	// Sanity: the archived entry must currently exist and be reachable.
	if !treeHasKeyArchived(shard, shard.root, key) {
		t.Fatal("precondition: archived entry for key must exist before overwrite")
	}

	// Overwrite the key. The write path must consult the actual tree structure
	// (like the read path does) and remove the stale archived valueRef.
	newVal := []byte("hot-value-v2")
	if err := shard.Put(key, newVal); err != nil {
		t.Fatalf("put: %v", err)
	}

	if treeHasKeyArchived(shard, shard.root, key) {
		t.Fatalf("BUG REPRODUCED: stale archived valueRef survived the overwrite because " +
			"archiveMayContainKey hard-stopped on a stale presence=false bit; " +
			"the surviving entry would fail archive bucket valueRef verification on the next read")
	}
}

// TestArchiveStalePresenceProduceVerifyFailure documents the downstream
// symptom: a stale archived valueRef reachable from a hot-lacking tree makes
// Get fail with "archive bucket valueRef verification failed".
func TestArchiveStalePresenceProduceVerifyFailure(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	cfg := DefaultConfig()
	cfg.ShardDepth = 4
	cfg.CompactArchiveStubs = false
	cfg.CuckooBuckets = 32
	cfg.CuckooSlots = 4

	shard, err := NewShard(0, db, hasher, cfg, nil, true, func() byte { return 0 })
	if err != nil {
		t.Fatalf("new shard: %v", err)
	}

	key := bytes.Repeat([]byte{0xAB}, 32)
	pathBits := cfg.ShardDepth
	path := make([]byte, (pathBits+7)/8)
	for i := 0; i < pathBits; i++ {
		if shard.getBit(key, i) == 1 {
			path[i/8] |= 0x80 >> (i % 8)
		}
	}

	// Flat value has already moved on to v2.
	shard.stageValueForKey(key, []byte("current-v2"))
	// But the archive bucket still holds a valueRef computed from the OLD v1.
	oldRef, err := shard.storedValueRef(key, []byte("old-v1"))
	if err != nil {
		t.Fatalf("storedValueRef: %v", err)
	}
	archNode := shard.buildArchiveBucket([]ArchivedKV{{
		Suffix:     bytes.Clone(key),
		SuffixBits: len(key) * 8,
		Value:      oldRef,
	}}, path, pathBits)
	bucket, ok := archNode.(*ArchiveBucketNode)
	if !ok {
		t.Fatalf("expected ArchiveBucketNode, got %T", archNode)
	}
	root := &InternalNode{Path: path, PathBits: pathBits, dirty: true}
	root.StubList = []*ArchiveBucketNode{bucket}
	shard.root = root

	if _, err := shard.Get(key); err == nil || !strings.Contains(err.Error(), "archive bucket valueRef verification failed") {
		t.Fatalf("expected stale archive valueRef verification failure, got err=%v", err)
	}
}
