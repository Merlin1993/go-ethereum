package archive

import (
	"bytes"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

// TestVRefFailureDumpCoversStaleMembership exercises the diagnostic
// instrumentation: a bucket holding a stale valueRef for a key whose flat
// value is newer must produce a dump that (a) names the failing key and refs,
// (b) marks the stale membership ORPHAN, and (c) traces the membership back
// to the recorded write that created it.
func TestVRefFailureDumpCoversStaleMembership(t *testing.T) {
	dumpPath := t.TempDir() + "/vref.dump"
	t.Setenv("AMT_VREF_DUMP", dumpPath)
	vRefDebugForced.Store(true)
	defer vRefDebugForced.Store(false)
	defer CloseVRefSink()

	db := NewMemoryDBAdapter()
	config := DefaultConfig()
	config.ShardDepth = 0
	config.CuckooBuckets = 64
	config.CuckooSlots = 4
	shard, err := NewShard(0, db, NewPooledKeccakHasher(), config, nil, true, func() byte { return 0 })
	if err != nil {
		t.Fatalf("new shard: %v", err)
	}

	key := bytes.Repeat([]byte{0x01}, 32)
	staleRef := valueRefForKeyValue(key, []byte("stale-value"))

	// The flat store holds the current value; the bucket stores the stale ref.
	currentRef := shard.stageValueForKey(key, []byte("current-value"))

	staleBucket := shard.buildArchiveBucket([]ArchivedKV{{
		Suffix:     append([]byte{}, key...),
		SuffixBits: len(key) * 8,
		Value:      staleRef,
	}}, nil, 0).(*ArchiveBucketNode)

	// A surviving leaf on the other side keeps the root from collapsing and
	// keeps Get's hot probe away from the target key.
	survivingKey := bytes.Repeat([]byte{0x80}, 32)
	survivingRef := shard.stageValueForKey(survivingKey, []byte("surviving-value"))
	survivingLeaf := NewLeafNode(survivingKey, len(survivingKey)*8, survivingRef)
	root := &InternalNode{Left: survivingLeaf, StubList: []*ArchiveBucketNode{staleBucket}, dirty: true}
	shard.refreshInternalEpochMask(root)
	shard.root = root

	_, err = shard.Get(key)
	if err == nil || !strings.Contains(err.Error(), "valueRef verification failed") {
		t.Fatalf("expected verification failure, got: %v", err)
	}

	raw, err := os.ReadFile(dumpPath)
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	dump := string(raw)
	for _, want := range []string{
		"site=shard.Get",
		"key=" + hex.EncodeToString(key),
		"stored_ref=" + hex.EncodeToString(staleRef),
		"actual_ref=" + hex.EncodeToString(currentRef),
		"location=archive-bucket",
		"verdict=ORPHAN",
		"keyRecord",
		"bucketStill=present",
	} {
		if !strings.Contains(dump, want) {
			t.Fatalf("dump missing %q:\n%s", want, dump)
		}
	}
}
