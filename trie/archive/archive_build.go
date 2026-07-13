package archive

import (
	"runtime"
	"sync"
)

const parallelArchiveBuildThreshold = 256

// buildArchiveSubtreeFast 把归档 item 打包成 bucket 或紧凑的归档子树。
// 它会先压缩所有 item 的公共前缀，再继续按 bit 拆分，直到每个 bucket 都满足容量上限。
func (s *Shard) buildArchiveSubtreeFast(items []ArchivedKV, path []byte, bits int) Node {
	if len(items) == 0 {
		return nil
	}
	items = s.deduplicateArchiveItems(items, nil, 0)
	if len(items) == 0 {
		return nil
	}

	bucketSize := s.config.ResolveArchiveBucketSize()
	if len(items) <= bucketSize || bucketSize <= 0 || bits >= MaxPathBits {
		return s.buildArchiveBucket(items, path, bits)
	}

	basePath, baseBits := path, bits
	nodePath, nodePathBits := []byte(nil), 0
	commonBits := s.commonArchiveItemPrefixBits(items, bits)
	if commonBits > bits {
		commonPath := s.prefixBits(items[0].Suffix, commonBits, nil)
		nodePath, nodePathBits = s.stripPrefix(commonPath, commonBits, 0, path, bits)
		basePath, baseBits = commonPath, commonBits
	}
	if baseBits >= MaxPathBits {
		return s.buildArchiveBucket(items, basePath, baseBits)
	}

	split := s.partitionArchiveItemsByBit(items, baseBits)
	if split == 0 || split == len(items) {
		if bucketSize > 0 && len(items) > bucketSize && baseBits < MaxPathBits {
			nextBit := byte(0)
			if split == 0 {
				nextBit = 1
			}
			nextPath, nextBits := s.appendBit(basePath, baseBits, nextBit)
			return s.buildArchiveSubtreeFast(items, nextPath, nextBits)
		}
		return s.buildArchiveBucket(items, basePath, baseBits)
	}

	n := s.pool.GetInternal()
	n.Path = nodePath
	n.PathBits = nodePathBits
	n.SetDirty(true)

	lp, lb := s.appendBit(basePath, baseBits, 0)
	rp, rb := s.appendBit(basePath, baseBits, 1)
	var left, right Node
	if len(items) >= parallelArchiveBuildThreshold && runtime.GOMAXPROCS(0) > 1 {
		recordPruneArchiveBuildParallelIfEnabled(s.config)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			left = s.buildArchiveSubtreeFast(items[:split], lp, lb)
		}()
		go func() {
			defer wg.Done()
			right = s.buildArchiveSubtreeFast(items[split:], rp, rb)
		}()
		wg.Wait()
	} else {
		left = s.buildArchiveSubtreeFast(items[:split], lp, lb)
		right = s.buildArchiveSubtreeFast(items[split:], rp, rb)
	}

	n.Left = left
	if n.Left != nil {
		n.LeftEpoch = n.Left.Epoch()
	}

	n.Right = right
	if n.Right != nil {
		n.RightEpoch = n.Right.Epoch()
	}

	s.refreshInternalEpochMask(n)
	return n
}

// buildArchiveSubtreeForce 用在冷子树必须展开到至少 forceBits 的场景。
// 典型情况是冷子树要和已有热 child 并列，需要在同一条边深度上对齐。
func (s *Shard) buildArchiveSubtreeForce(items []ArchivedKV, path []byte, bits int, forceBits int) Node {
	if len(items) == 0 {
		return nil
	}
	items = s.deduplicateArchiveItems(items, nil, 0)
	if len(items) == 0 {
		return nil
	}
	bucketSize := s.config.ResolveArchiveBucketSize()
	if forceBits < bits {
		forceBits = bits
	}
	if (bucketSize <= 0 || len(items) <= bucketSize) && bits >= forceBits {
		return s.buildArchiveBucket(items, path, bits)
	}
	if bits >= MaxPathBits {
		return s.buildArchiveBucket(items, path, bits)
	}

	basePath, baseBits := path, bits
	nodePath, nodePathBits := []byte(nil), 0
	commonBits := s.commonArchiveItemPrefixBits(items, bits)
	if commonBits > bits && commonBits < forceBits {
		commonPath := s.prefixBits(items[0].Suffix, commonBits, nil)
		nodePath, nodePathBits = s.stripPrefix(commonPath, commonBits, 0, path, bits)
		basePath, baseBits = commonPath, commonBits
	}
	if baseBits >= MaxPathBits {
		return s.buildArchiveBucket(items, basePath, baseBits)
	}

	split := s.partitionArchiveItemsByBit(items, baseBits)
	if split == 0 || split == len(items) {
		nextBit := byte(0)
		if split == 0 {
			nextBit = 1
		}
		nextPath, nextBits := s.appendBit(basePath, baseBits, nextBit)
		return s.buildArchiveSubtreeForce(items, nextPath, nextBits, forceBits)
	}

	n := s.pool.GetInternal()
	n.Path = nodePath
	n.PathBits = nodePathBits
	n.SetDirty(true)

	lp, lb := s.appendBit(basePath, baseBits, 0)
	n.Left = s.buildArchiveSubtreeForce(items[:split], lp, lb, forceBits)
	if n.Left != nil {
		n.LeftEpoch = n.Left.Epoch()
	}

	rp, rb := s.appendBit(basePath, baseBits, 1)
	n.Right = s.buildArchiveSubtreeForce(items[split:], rp, rb, forceBits)
	if n.Right != nil {
		n.RightEpoch = n.Right.Epoch()
	}

	s.refreshInternalEpochMask(n)
	return n
}

// buildArchiveBucket 把绝对归档路径转换成 bucket 内部 suffix，并重算 filter 和 ECMH。
func (s *Shard) buildArchiveBucket(items []ArchivedKV, path []byte, bits int) Node {
	recordPruneBuildBucketIfEnabled(s.config, len(items))
	localItems := make([]ArchivedKV, len(items))
	for i := range items {
		p, b := s.stripPrefix(items[i].Suffix, items[i].SuffixBits, 0, path, bits)
		localItems[i] = ArchivedKV{
			Suffix:     p,
			SuffixBits: b,
			Value:      items[i].Value,
		}
	}

	bucket := &ArchiveBucketNode{
		Path:     path,
		PathBits: bits,
		dirty:    true,
	}
	s.recomputeBucket(bucket, localItems)
	return bucket
}

func (s *Shard) commonArchiveItemPrefixBits(items []ArchivedKV, start int) int {
	if len(items) == 0 {
		return start
	}
	limit := MaxPathBits
	for i := range items {
		if items[i].SuffixBits < limit {
			limit = items[i].SuffixBits
		}
	}
	if start >= limit {
		return start
	}
	for bit := start; bit < limit; bit++ {
		first := s.getBitFromBytes(items[0].Suffix, bit)
		for i := 1; i < len(items); i++ {
			if s.getBitFromBytes(items[i].Suffix, bit) != first {
				return bit
			}
		}
	}
	return limit
}

func (s *Shard) partitionArchiveItemsByBit(items []ArchivedKV, bit int) int {
	left, right := 0, len(items)-1
	for left <= right {
		for left <= right && (bit >= items[left].SuffixBits || s.getBitFromBytes(items[left].Suffix, bit) == 0) {
			left++
		}
		for left <= right && bit < items[right].SuffixBits && s.getBitFromBytes(items[right].Suffix, bit) == 1 {
			right--
		}
		if left < right {
			items[left], items[right] = items[right], items[left]
			left++
			right--
		}
	}
	return left
}

// collectAndAttachToStubListAtPath 为刚收集到的冷 item 选择最小可用表示。
//
// 小 bucket 会先留在 StubList，等待后续剪枝继续合并；更大的 bucket/subtree 会直接下沉到 child edge。
func (s *Shard) collectAndAttachToStubListAtPath(parent *InternalNode, items []ArchivedKV, absPath []byte, absBits int, nodePath []byte, nodeBits int) (bool, error) {
	if len(items) == 0 {
		return false, nil
	}

	archNode := s.buildArchiveSubtreeFast(items, absPath, absBits)
	if bucket, ok := archNode.(*ArchiveBucketNode); ok {
		if s.shouldSinkSideMountedBucket(bucket) && s.canSinkStubAtPath(bucket, nodePath, nodeBits) {
			return s.attachArchiveItemsToChildEdges(parent, items, nodePath, nodeBits)
		}
		if changed, err := s.attachStubsAtPath(parent, []*ArchiveBucketNode{bucket}, nodePath, nodeBits); err != nil {
			return changed, err
		}
	} else if _, ok := archNode.(*InternalNode); ok {
		if changed, err := s.attachArchiveItemsToChildEdges(parent, items, nodePath, nodeBits); err != nil {
			return changed, err
		}
	}
	parent.SetDirty(true)
	return true, nil
}

func (s *Shard) collectArchiveBucketStubs(node Node, out []*ArchiveBucketNode) []*ArchiveBucketNode {
	if node == nil {
		return out
	}
	switch n := node.(type) {
	case *ArchiveBucketNode:
		return append(out, n)
	case *InternalNode:
		out = s.collectArchiveBucketStubs(n.Left, out)
		out = s.collectArchiveBucketStubs(n.Right, out)
		for _, bucket := range n.StubList {
			out = s.collectArchiveBucketStubs(bucket, out)
		}
	}
	return out
}
