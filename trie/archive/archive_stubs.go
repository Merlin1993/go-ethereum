package archive

import (
	"github.com/ethereum/go-ethereum/common"
)

// StubList 是小归档桶的短期侧挂缓冲。
//
// 一轮剪枝经常只产生很小的冷 bucket。如果每个小桶都立刻下沉到 child edge，
// 路径会很快变长；因此先挂在父节点上，尽量和后续 bucket 合并，成熟或路径压力过高时再下沉。
func (s *Shard) attachStubs(parent *InternalNode, stubs []*ArchiveBucketNode) {
	_, _ = s.attachStubsInternal(parent, stubs, nil, 0, false)
}

func (s *Shard) attachStubsAtPath(parent *InternalNode, stubs []*ArchiveBucketNode, nodePath []byte, nodeBits int) (bool, error) {
	return s.attachStubsInternal(parent, stubs, nodePath, nodeBits, true)
}

func (s *Shard) attachStubsInternal(parent *InternalNode, stubs []*ArchiveBucketNode, nodePath []byte, nodeBits int, sinkMature bool) (bool, error) {
	if parent == nil || len(stubs) == 0 {
		return false, nil
	}
	s.markPersistedNodeStale(parent)
	changed := false
	for _, stub := range stubs {
		if stub == nil {
			continue
		}
		s.detachArchiveStub(stub)
		var target *ArchiveBucketNode
		if s.config != nil && s.config.CompactArchiveStubs {
			target = s.mergeStubIntoList(parent, stub, nodePath, nodeBits, sinkMature)
		} else {
			parent.StubList = append(parent.StubList, stub)
			target = stub
		}
		changed = true
		if sinkMature && s.shouldSinkSideMountedBucket(target) {
			sunk, err := s.sinkSpecificMatureStub(parent, nodePath, nodeBits, target)
			if err != nil {
				return changed, err
			}
			if sunk {
				changed = true
			}
		}
	}
	if sinkMature {
		pressureChanged, err := s.relieveStubPathPressure(parent, nodePath, nodeBits)
		if err != nil {
			return changed, err
		}
		if pressureChanged {
			changed = true
		}
	}
	s.refreshInternalEpochMask(parent)
	parent.SetDirty(true)
	return changed, nil
}

func (s *Shard) detachArchiveStub(bucket *ArchiveBucketNode) {
	if bucket == nil || s.config == nil || !s.config.UsePathStorage() {
		return
	}
	bucket.originalHash = nil
	bucket.storagePath = nil
	bucket.storageBits = 0
}

// mergeStubIntoList 把新 bucket 和最合适的现有侧挂 bucket 反复合并，
// 前提是合并后仍不超过 bucket 容量上限。
func (s *Shard) mergeStubIntoList(parent *InternalNode, stub *ArchiveBucketNode, nodePath []byte, nodeBits int, sinkMature bool) *ArchiveBucketNode {
	if parent == nil || stub == nil {
		return nil
	}
	limit := 0
	if s.config != nil {
		limit = s.config.ResolveArchiveBucketSize()
	}
	if limit <= 0 {
		limit = int(^uint(0) >> 1)
	}

	current := stub
	for {
		if sinkMature && s.shouldSinkSideMountedBucket(current) && s.canSinkStubAtPath(current, nodePath, nodeBits) {
			parent.StubList = append(parent.StubList, current)
			return current
		}
		idx, path, bits := s.findMergeCandidate(parent.StubList, current, limit)
		if idx < 0 {
			parent.StubList = append(parent.StubList, current)
			return current
		}
		merged, ok := s.mergeArchiveBuckets(parent.StubList[idx], current, path, bits)
		if !ok {
			parent.StubList = append(parent.StubList, current)
			return current
		}
		parent.StubList = append(parent.StubList[:idx], parent.StubList[idx+1:]...)
		current = merged
	}
}

// findMergeCandidate 选择公共绝对路径前缀最长的可合并 bucket，
// 让合并后的 bucket 尽量保持空间局部性。
func (s *Shard) findMergeCandidate(stubs []*ArchiveBucketNode, target *ArchiveBucketNode, limit int) (int, []byte, int) {
	bestIdx := -1
	bestBits := -1
	var bestPath []byte
	for i, bucket := range stubs {
		if bucket == nil || target.Count+bucket.Count > uint64(limit) {
			continue
		}
		path, bits := s.commonArchivePath(target.Path, target.PathBits, bucket.Path, bucket.PathBits)
		if bits > bestBits {
			bestIdx = i
			bestBits = bits
			bestPath = path
		}
	}
	return bestIdx, bestPath, bestBits
}

// mergeArchiveBuckets 在两个侧挂 bucket 的公共路径前缀下重建本地 suffix、filter 和 ECMH。
func (s *Shard) mergeArchiveBuckets(a, b *ArchiveBucketNode, path []byte, bits int) (*ArchiveBucketNode, bool) {
	if a == nil || b == nil {
		return nil, false
	}
	group := []*ArchiveBucketNode{a, b}
	items := make([]ArchivedKV, 0, int(a.Count+b.Count))
	oldHashes := make([][]byte, 0, len(group))
	for _, bucket := range group {
		hash := s.ensureBucketHash(bucket)
		bucketKeys, err := s.bucketKeys(bucket)
		if err != nil {
			return nil, false
		}
		for _, key := range bucketKeys {
			absPath, absBits := s.prependPath(key.Suffix, key.SuffixBits, bucket.Path, bucket.PathBits)
			suffix, suffixBits := s.stripPrefix(absPath, absBits, 0, path, bits)
			items = append(items, ArchivedKV{
				Suffix:     suffix,
				SuffixBits: suffixBits,
				Value:      common.CopyBytes(key.ValueRef),
			})
		}
		oldHashes = append(oldHashes, common.CopyBytes(hash))
	}
	merged := &ArchiveBucketNode{
		Path:     common.CopyBytes(path),
		PathBits: bits,
		dirty:    true,
	}
	s.recomputeBucket(merged, items)
	for _, hash := range oldHashes {
		if s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) && len(hash) > 0 {
			s.staleSet[string(hash)] = struct{}{}
		}
	}
	return merged, true
}

func (s *Shard) archiveBucketPathLess(a, b *ArchiveBucketNode) bool {
	if a == nil || b == nil {
		return b != nil
	}
	limit := a.PathBits
	if b.PathBits < limit {
		limit = b.PathBits
	}
	for i := 0; i < limit; i++ {
		abit := s.getBitFromBytes(a.Path, i)
		bbit := s.getBitFromBytes(b.Path, i)
		if abit != bbit {
			return abit < bbit
		}
	}
	return a.PathBits < b.PathBits
}

func (s *Shard) commonArchivePath(a []byte, aBits int, b []byte, bBits int) ([]byte, int) {
	limit := aBits
	if bBits < limit {
		limit = bBits
	}
	matched := 0
	for matched < limit {
		if s.getBitFromBytes(a, matched) != s.getBitFromBytes(b, matched) {
			break
		}
		matched++
	}
	return s.prefixBits(a, matched, nil), matched
}
