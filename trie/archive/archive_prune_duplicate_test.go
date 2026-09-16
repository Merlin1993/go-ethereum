package archive

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestPruneRemovesDuplicateArchiveMembershipsBeforeAbsorption(t *testing.T) {
	db := NewMemoryDBAdapter()
	config := DefaultConfig()
	config.ShardDepth = 0
	config.ArchiveBucketSize = 8
	config.CuckooBuckets = 64
	config.CuckooSlots = 4
	shard, err := NewShard(0, db, NewPooledKeccakHasher(), config, nil, true, func() byte { return 0 })
	if err != nil {
		t.Fatalf("new shard: %v", err)
	}

	key := bytes.Repeat([]byte{0x01}, 32)
	currentRef := shard.stageValueForKey(key, []byte("current-value"))
	staleRef := valueRefForKeyValue(key, []byte("stale-value"))

	deepBits := 1
	deepPath := shard.prefixBits(key, deepBits, nil)
	deepBucket := shard.buildArchiveBucket([]ArchivedKV{{
		Suffix:     common.CopyBytes(key),
		SuffixBits: len(key) * 8,
		Value:      staleRef,
	}}, deepPath, deepBits).(*ArchiveBucketNode)
	rootBucket := shard.buildArchiveBucket([]ArchivedKV{{
		Suffix:     common.CopyBytes(key),
		SuffixBits: len(key) * 8,
		Value:      staleRef,
	}}, nil, 0).(*ArchiveBucketNode)

	// Keep both the deeper node and the root branched after pruning. If either
	// collapses, its stub is promoted and attachment deduplicates the key before
	// the ordering bug can surface.
	coldPath := shard.prefixBits(key, 3, nil)
	coldSuffix, coldBits := shard.stripPrefix(key, len(key)*8, 0, coldPath, 3)
	coldLeaf := NewLeafNode(coldSuffix, coldBits, currentRef)

	coldSiblingKey := bytes.Repeat([]byte{0x20}, 32)
	coldSiblingPath := shard.prefixBits(coldSiblingKey, 3, nil)
	coldSiblingSuffix, coldSiblingBits := shard.stripPrefix(coldSiblingKey, len(coldSiblingKey)*8, 0, coldSiblingPath, 3)
	coldSiblingRef := shard.stageValueForKey(coldSiblingKey, []byte("cold-sibling-value"))
	coldSiblingLeaf := NewLeafNode(coldSiblingSuffix, coldSiblingBits, coldSiblingRef)
	coldSiblingLeaf.SetEpoch(1)
	coldBranch := &InternalNode{Left: coldLeaf, Right: coldSiblingLeaf, dirty: true}
	shard.refreshInternalEpochMask(coldBranch)

	survivingKey := bytes.Repeat([]byte{0x40}, 32)
	survivingPath := shard.prefixBits(survivingKey, 2, nil)
	survivingSuffix, survivingBits := shard.stripPrefix(survivingKey, len(survivingKey)*8, 0, survivingPath, 2)
	survivingRef := shard.stageValueForKey(survivingKey, []byte("surviving-value"))
	survivingLeaf := NewLeafNode(survivingSuffix, survivingBits, survivingRef)
	survivingLeaf.SetEpoch(1)
	deep := &InternalNode{Left: coldBranch, Right: survivingLeaf, StubList: []*ArchiveBucketNode{deepBucket}, dirty: true}
	shard.refreshInternalEpochMask(deep)

	rootSurvivingKey := bytes.Repeat([]byte{0x80}, 32)
	rootSurvivingPath := shard.prefixBits(rootSurvivingKey, 1, nil)
	rootSurvivingSuffix, rootSurvivingBits := shard.stripPrefix(rootSurvivingKey, len(rootSurvivingKey)*8, 0, rootSurvivingPath, 1)
	rootSurvivingRef := shard.stageValueForKey(rootSurvivingKey, []byte("root-surviving-value"))
	rootSurvivingLeaf := NewLeafNode(rootSurvivingSuffix, rootSurvivingBits, rootSurvivingRef)
	rootSurvivingLeaf.SetEpoch(1)
	root := &InternalNode{Left: deep, Right: rootSurvivingLeaf, StubList: []*ArchiveBucketNode{rootBucket}, dirty: true}
	shard.refreshInternalEpochMask(root)
	shard.root = root

	if err := shard.Prune(1); err != nil {
		t.Fatalf("prune: %v", err)
	}

	var collectArchived func(Node) ([]ArchivedKV, error)
	collectArchived = func(node Node) ([]ArchivedKV, error) {
		switch n := node.(type) {
		case *ArchiveBucketNode:
			items, err := shard.bucketItemsWithValueRefs(n)
			if err != nil {
				return nil, err
			}
			out := make([]ArchivedKV, 0, len(items))
			for _, item := range items {
				suffix, bits := shard.prependPath(item.Suffix, item.SuffixBits, n.Path, n.PathBits)
				out = append(out, ArchivedKV{Suffix: suffix, SuffixBits: bits, Value: item.Value})
			}
			return out, nil
		case *InternalNode:
			out, err := collectArchived(n.Left)
			if err != nil {
				return nil, err
			}
			right, err := collectArchived(n.Right)
			if err != nil {
				return nil, err
			}
			out = append(out, right...)
			for _, bucket := range n.StubList {
				items, err := collectArchived(bucket)
				if err != nil {
					return nil, err
				}
				out = append(out, items...)
			}
			return out, nil
		default:
			return nil, nil
		}
	}
	archived, err := collectArchived(shard.root)
	if err != nil {
		t.Fatalf("collect archived items: %v", err)
	}
	memberships := make([][]byte, 0, 2)
	for _, item := range archived {
		t.Logf("archived: bits=%d suffix=%x value=%x", item.SuffixBits, item.Suffix, item.Value)
		if item.SuffixBits == len(key)*8 && bytes.Equal(item.Suffix, key) {
			memberships = append(memberships, item.Value)
		}
	}
	if len(memberships) != 1 {
		t.Fatalf("duplicate archive membership survived prune: memberships=%d refs=%x", len(memberships), memberships)
	}
	if !bytes.Equal(memberships[0], currentRef) {
		t.Fatalf("surviving archive valueRef mismatch: got=%x want=%x", memberships[0], currentRef)
	}
}
