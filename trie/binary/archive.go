package binary

import (
	"runtime"
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

const parallelArchiveBuildThreshold = 256

// Prune 执行分片级别的状态剪枝和归档。
func (s *Shard) Prune(global byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.root == nil && len(s.rootHash) > 0 {
		var err error
		s.root, err = s.loadNode(s.rootHash)
		if err != nil {
			return err
		}
	}

	if s.root == nil {
		return nil
	}

	// [FIX] Do NOT early-return based on root.Epoch() alone.
	// Insert operations update the root's epoch, but child nodes may still have
	// stale epochs that need pruning. pruneAndArchive checks per-node epoch correctly.

	// 1. 获取当前分片的物理前缀 (Absolute Prefix)
	prefix, prefixBits := s.getShardPrefix()

	// 2. 递归剪枝并收集 MaxPathBits 范围内的绝对全路径项
	newRoot, items, promotedStubs, err := s.pruneAndArchive(s.root, prefix, prefixBits, global)
	if err != nil {
		return err
	}

	// 3. 处理结果合并到分片根节点
	promotedAttached := false
	if newRoot == nil {
		if len(items) > 0 {
			// 全量归档：构建归档子树并更新 root
			s.root = s.buildArchiveSubtreeFast(items, prefix, prefixBits)
		} else if len(promotedStubs) > 0 {
			s.root = s.newStubContainer(promotedStubs)
			promotedAttached = true
		} else {
			s.root = nil
		}
	} else {
		s.root = newRoot
		if len(items) > 0 {
			// items 已经是绝对路径，将其挂载到 root。
			// 对 InternalNode，将其挂入 StubList。
			if in, ok := s.root.(*InternalNode); ok {
				nodePath, nodeBits := prefix, prefixBits
				if in.PathBits > 0 {
					nodePath, nodeBits = s.prependPath(in.Path, in.PathBits, prefix, prefixBits)
				}
				if _, err := s.collectAndAttachToStubListAtPath(in, items, prefix, prefixBits, nodePath, nodeBits); err != nil {
					return err
				}
			}
		}
	}
	if len(promotedStubs) > 0 && !promotedAttached && s.root != nil {
		s.root = s.attachPromotedStubsToRoot(s.root, promotedStubs)
	}
	if in, ok := s.root.(*InternalNode); ok {
		nodePath, nodeBits := prefix, prefixBits
		if in.PathBits > 0 {
			nodePath, nodeBits = s.prependPath(in.Path, in.PathBits, prefix, prefixBits)
		}
		if _, err := s.sinkFirstMatureStub(in, nodePath, nodeBits); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) getShardPrefix() ([]byte, int) {
	depth := s.config.ShardDepth
	res := make([]byte, (depth+7)/8)
	for i := 0; i < depth; i++ {
		bit := byte((s.id >> (depth - 1 - i)) & 1)
		if bit == 1 {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res, depth
}

func (s *Shard) newStubContainer(stubs []*ArchiveBucketNode) Node {
	n := s.pool.GetInternal()
	s.attachStubs(n, stubs)
	return n
}

func (s *Shard) attachPromotedStubsToRoot(root Node, stubs []*ArchiveBucketNode) Node {
	if len(stubs) == 0 {
		return root
	}
	if root == nil {
		return s.newStubContainer(stubs)
	}
	if in, ok := root.(*InternalNode); ok {
		s.markPersistedNodeStale(in)
		s.attachStubs(in, stubs)
		in.SetDirty(true)
		return in
	}

	container := s.newStubContainer(stubs).(*InternalNode)
	if bucket, ok := root.(*ArchiveBucketNode); ok {
		s.attachStubs(container, []*ArchiveBucketNode{bucket})
		return container
	}
	bit, ok := s.detachLeadingPathBit(root)
	if !ok {
		// Root is the only place without a parent to receive promoted buckets.
		// Keep a minimal container rather than dropping either side.
		container.Left = root
		s.refreshInternalEpochMask(container)
		return container
	}
	if bit == 0 {
		container.Left = root
	} else {
		container.Right = root
	}
	s.refreshInternalEpochMask(container)
	return container
}

func (s *Shard) detachLeadingPathBit(node Node) (byte, bool) {
	switch n := node.(type) {
	case *InternalNode:
		if n.PathBits <= 0 {
			return 0, false
		}
		bit := s.getBitFromBytes(n.Path, 0)
		n.Path = s.shiftBits(n.Path, n.PathBits, 1, nil)
		n.PathBits--
		n.SetDirty(true)
		s.markPersistedNodeStale(n)
		return bit, true
	case *LeafNode:
		if n.PathBits <= 0 {
			return 0, false
		}
		bit := s.getBitFromBytes(n.Path, 0)
		n.Path = s.shiftBits(n.Path, n.PathBits, 1, nil)
		n.PathBits--
		n.SetDirty(true)
		s.markPersistedNodeStale(n)
		return bit, true
	default:
		return 0, false
	}
}

// pruneAndArchive 递归处理节点，items 返回值始终是 MaxPathBits 范围内的绝对路径。
func (s *Shard) pruneAndArchive(node Node, prefix []byte, prefixBits int, global byte) (Node, []ArchivedKV, []*ArchiveBucketNode, error) {
	if node == nil {
		return nil, nil, nil, nil
	}

	switch n := node.(type) {
	case *LeafNode:
		if (n.Epoch() & 1) != global {
			s.markPersistedNodeStale(n)
			s.releaseValue(n.ValueHash)
			// 组装绝对路径
			absP, absB := s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
			item := ArchivedKV{
				Suffix:     absP,
				SuffixBits: absB,
				Value:      n.ValueHash,
			}
			return nil, []ArchivedKV{item}, nil, nil
		}
		return n, nil, nil, nil

	case *InternalNode:
		recordPruneInternalVisitIfEnabled(s.config)
		var err error
		origStubCount := len(n.StubList)
		// [FIX] Do NOT use InternalNode.epoch for fast-path archival.
		// insert() updates InternalNode.epoch along the traversal path, which
		// makes them appear "current" even when their leaf children are stale.
		// Always recurse into children to check individual leaf epochs.
		if mask, ok := s.subtreeEpochMask(n); ok {
			hotMask := leafEpochMask(global)
			if mask&^hotMask == 0 {
				recordPruneHotSkipIfEnabled(s.config)
				return n, nil, nil, nil
			}
			hasArchive, err := s.subtreeContainsArchiveBucket(n)
			if err != nil {
				return nil, nil, nil, err
			}
			if len(n.StubList) == 0 && mask&hotMask == 0 && !hasArchive {
				recordPruneBulkCollectIfEnabled(s.config)
				items, stubs, err := s.collectLeavesAndMarkStaleRecursive(n, prefix, prefixBits)
				if err != nil {
					return nil, nil, nil, err
				}
				return nil, items, stubs, nil
			}
		}

		// 计算当前节点的内部全缀 (n.Path) 位，但不提前组合到 prefix，
		// 以便分别传递给左/右子树。
		currentP, currentB := prefix, prefixBits
		if n.PathBits > 0 {
			currentP, currentB = s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		}

		var allItems []ArchivedKV
		parentChanged := false
		var newLeft, newRight Node
		lp, lb := s.appendBit(currentP, currentB, 0)
		rp, rb := s.appendBit(currentP, currentB, 1)
		leftNeedsPrune := s.childMayNeedPrune(n.Left, n.LeftEpoch, len(n.LeftHash) > 0, global)
		recordPruneChildDecisionIfEnabled(s.config, leftNeedsPrune)
		if leftNeedsPrune {

			// 处理左子树
			if n.Left == nil && len(n.LeftHash) > 0 {
				n.Left, err = s.loadChildNode(n, 0, n.LeftHash)
				if err != nil {
					return nil, nil, nil, err
				}
			}
			leftHadChild := n.Left != nil || len(n.LeftHash) > 0
			var leftItems []ArchivedKV
			var leftStubs []*ArchiveBucketNode
			newLeft, leftItems, leftStubs, err = s.pruneAndArchive(n.Left, lp, lb, global)
			if err != nil {
				return nil, nil, nil, err
			}

			if newLeft == nil {
				if leftHadChild {
					parentChanged = true
				}
				if len(leftItems) > 0 {
					allItems = append(allItems, leftItems...)
					parentChanged = true
				}
				n.Left, n.LeftHash = nil, nil
				n.LeftEpoch = 0
			} else {
				if n.Left != newLeft {
					parentChanged = true
				}
				n.Left = newLeft
				allItems = append(allItems, leftItems...)
			}
			if n.Left != nil {
				collapsed, err := s.collapseSmallArchiveChildToStub(n.Left, lp, lb)
				if err != nil {
					return nil, nil, nil, err
				}
				if collapsed != nil {
					n.Left, n.LeftHash = nil, nil
					n.LeftEpoch = 0
					leftStubs = append(leftStubs, collapsed)
					parentChanged = true
				}
			}
			if len(leftStubs) > 0 {
				if _, err := s.attachStubsAtPath(n, leftStubs, currentP, currentB); err != nil {
					return nil, nil, nil, err
				}
				parentChanged = true
			}

			// 处理右子树
		}
		rightNeedsPrune := s.childMayNeedPrune(n.Right, n.RightEpoch, len(n.RightHash) > 0, global)
		recordPruneChildDecisionIfEnabled(s.config, rightNeedsPrune)
		if rightNeedsPrune && n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadChildNode(n, 1, n.RightHash)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		if rightNeedsPrune {
			rightHadChild := n.Right != nil || len(n.RightHash) > 0
			var rightItems []ArchivedKV
			var rightStubs []*ArchiveBucketNode
			newRight, rightItems, rightStubs, err = s.pruneAndArchive(n.Right, rp, rb, global)
			if err != nil {
				return nil, nil, nil, err
			}

			if newRight == nil {
				if rightHadChild {
					parentChanged = true
				}
				if len(rightItems) > 0 {
					allItems = append(allItems, rightItems...)
					parentChanged = true
				}
				n.Right, n.RightHash = nil, nil
				n.RightEpoch = 0
			} else {
				if n.Right != newRight {
					parentChanged = true
				}
				n.Right = newRight
				allItems = append(allItems, rightItems...)
			}
			if n.Right != nil {
				collapsed, err := s.collapseSmallArchiveChildToStub(n.Right, rp, rb)
				if err != nil {
					return nil, nil, nil, err
				}
				if collapsed != nil {
					n.Right, n.RightHash = nil, nil
					n.RightEpoch = 0
					rightStubs = append(rightStubs, collapsed)
					parentChanged = true
				}
			}
			if len(rightStubs) > 0 {
				if _, err := s.attachStubsAtPath(n, rightStubs, currentP, currentB); err != nil {
					return nil, nil, nil, err
				}
				parentChanged = true
			}
		}

		hasRemainingChildren := n.Left != nil || n.Right != nil || len(n.LeftHash) > 0 || len(n.RightHash) > 0
		if len(allItems) > 0 && (hasRemainingChildren || len(n.StubList) > 0) {
			if _, err := s.collectAndAttachToStubListAtPath(n, allItems, currentP, currentB, currentP, currentB); err != nil {
				return nil, nil, nil, err
			}
			allItems = nil
			parentChanged = true
			hasRemainingChildren = n.Left != nil || n.Right != nil || len(n.LeftHash) > 0 || len(n.RightHash) > 0
		}
		if !hasRemainingChildren && len(n.StubList) == 0 {
			if parentChanged {
				s.markPersistedNodeStale(n)
				n.SetDirty(true)
			}
			return nil, allItems, nil, nil
		}

		if parentChanged || len(n.StubList) != origStubCount ||
			(newLeft != nil && newLeft.IsDirty()) || (newRight != nil && newRight.IsDirty()) {
			s.markPersistedNodeStale(n)
			n.LeftHash, n.RightHash = nil, nil
			s.refreshInternalEpochMask(n)
			n.SetDirty(true)
		}

		resNode, promoted := s.shrinkPromote(n)
		return resNode, allItems, promoted, nil

	case *ArchiveBucketNode:
		// 已归档桶保持原样
		return n, nil, nil, nil

	default:
		return node, nil, nil, nil
	}
}

func (s *Shard) collectLeavesAndMarkStaleRecursive(node Node, prefix []byte, prefixBits int) ([]ArchivedKV, []*ArchiveBucketNode, error) {
	if node == nil {
		return nil, nil, nil
	}

	switch n := node.(type) {
	case *LeafNode:
		recordPruneCollectedLeafIfEnabled(s.config)
		s.markPersistedNodeStale(n)
		s.releaseValue(n.ValueHash)
		absP, absB := s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		return []ArchivedKV{{
			Suffix:     absP,
			SuffixBits: absB,
			Value:      n.ValueHash,
		}}, nil, nil

	case *InternalNode:
		s.markPersistedNodeStale(n)
		var err error
		currentP, currentB := prefix, prefixBits
		if n.PathBits > 0 {
			currentP, currentB = s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		}
		lp, lb := s.appendBit(currentP, currentB, 0)
		rp, rb := s.appendBit(currentP, currentB, 1)

		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, err = s.loadChildNode(n, 0, n.LeftHash)
			if err != nil {
				return nil, nil, err
			}
		}
		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadChildNode(n, 1, n.RightHash)
			if err != nil {
				return nil, nil, err
			}
		}

		left, leftStubs, err := s.collectLeavesAndMarkStaleRecursive(n.Left, lp, lb)
		if err != nil {
			return nil, nil, err
		}

		right, rightStubs, err := s.collectLeavesAndMarkStaleRecursive(n.Right, rp, rb)
		if err != nil {
			return nil, nil, err
		}

		allItems := append(left, right...)
		allStubs := append(leftStubs, rightStubs...)
		for _, bucket := range n.StubList {
			if bucket != nil {
				allStubs = append(allStubs, bucket)
			}
		}
		return allItems, allStubs, nil

	case *ArchiveBucketNode:
		recordPruneCollectedStubIfEnabled(s.config)
		return nil, []*ArchiveBucketNode{n}, nil

	default:
		return nil, nil, nil
	}
}

func (s *Shard) subtreeContainsArchiveBucket(node Node) (bool, error) {
	if present, ok := s.subtreeArchivePresence(node); ok {
		return present, nil
	}
	switch n := node.(type) {
	case nil:
		return false, nil
	case *ArchiveBucketNode:
		return true, nil
	case *LeafNode:
		return false, nil
	case *InternalNode:
		if len(n.StubList) > 0 {
			return true, nil
		}
		var err error
		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, err = s.loadChildNode(n, 0, n.LeftHash)
			if err != nil {
				return false, err
			}
		}
		hasArchive, err := s.subtreeContainsArchiveBucket(n.Left)
		if err != nil || hasArchive {
			return hasArchive, err
		}
		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadChildNode(n, 1, n.RightHash)
			if err != nil {
				return false, err
			}
		}
		return s.subtreeContainsArchiveBucket(n.Right)
	default:
		return false, nil
	}
}

func (s *Shard) collectLeavesRecursive(node Node, prefix []byte, prefixBits int) ([]ArchivedKV, error) {
	if node == nil {
		return nil, nil
	}

	switch n := node.(type) {
	case *LeafNode:
		absP, absB := s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		return []ArchivedKV{{
			Suffix:     absP,
			SuffixBits: absB,
			Value:      n.ValueHash,
		}}, nil

	case *InternalNode:
		var err error
		currentP, currentB := prefix, prefixBits
		if n.PathBits > 0 {
			currentP, currentB = s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		}
		lp, lb := s.appendBit(currentP, currentB, 0)
		rp, rb := s.appendBit(currentP, currentB, 1)

		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, err = s.loadChildNode(n, 0, n.LeftHash)
			if err != nil {
				return nil, err
			}
		}
		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadChildNode(n, 1, n.RightHash)
			if err != nil {
				return nil, err
			}
		}

		left, err := s.collectLeavesRecursive(n.Left, lp, lb)
		if err != nil {
			return nil, err
		}

		right, err := s.collectLeavesRecursive(n.Right, rp, rb)
		if err != nil {
			return nil, err
		}

		allItems := append(left, right...)

		for _, bucket := range n.StubList {
			bucketItems, err := s.collectLeavesRecursive(bucket, currentP, currentB)
			if err == nil {
				allItems = append(allItems, bucketItems...)
			}
		}
		return allItems, nil

	case *ArchiveBucketNode:
		items, err := s.bucketItemsWithValueRefs(n)
		if err != nil {
			return nil, err
		}

		// 统一导出为绝对物理路径：n.Path (桶绝对路径) + item.Suffix (桶相对后缀)
		results := make([]ArchivedKV, len(items))
		for i := range items {
			newS, newB := s.prependPath(items[i].Suffix, items[i].SuffixBits, n.Path, n.PathBits)
			results[i] = ArchivedKV{
				Suffix:     newS,
				SuffixBits: newB,
				Value:      items[i].Value,
			}
		}
		return results, nil

	default:
		return nil, nil
	}
}

func (s *Shard) markPersistedNodeStale(node Node) {
	if !s.pruning || node == nil {
		return
	}
	if s.config != nil && s.config.UsePathStorage() {
		path, bits := node.StoragePath()
		if path != nil || bits == 0 && len(node.OriginalHash()) > 0 {
			s.staleSet[string(pathNodeKey(s.id, path, bits))] = struct{}{}
		}
		return
	}
	if h := node.OriginalHash(); len(h) > 0 {
		s.staleSet[string(h)] = struct{}{}
	}
}

func (s *Shard) markSubtreeStaleRecursive(node Node) error {
	if !s.pruning || node == nil {
		return nil
	}
	s.markPersistedNodeStale(node)

	switch n := node.(type) {
	case *InternalNode:
		var err error
		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, err = s.loadChildNode(n, 0, n.LeftHash)
			if err != nil {
				return err
			}
		}
		if err := s.markSubtreeStaleRecursive(n.Left); err != nil {
			return err
		}

		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadChildNode(n, 1, n.RightHash)
			if err != nil {
				return err
			}
		}
		if err := s.markSubtreeStaleRecursive(n.Right); err != nil {
			return err
		}

		for _, bucket := range n.StubList {
			if err := s.markSubtreeStaleRecursive(bucket); err != nil {
				return err
			}
		}
	case *ArchiveBucketNode:
		hash := s.ensureBucketHash(n)
		if len(hash) > 0 {
			if s.config == nil || !s.config.UsePathStorage() {
				s.staleSet[string(hash)] = struct{}{}
			}
			s.markArchiveDataDelete(hash, -1)
		}
	}
	return nil
}

func (s *Shard) shouldSideMountArchiveItems(count int) bool {
	threshold := s.archiveSideMountSinkThreshold()
	return threshold <= 0 || count < threshold
}

func (s *Shard) archiveSideMountSinkThreshold() int {
	if s.config == nil {
		return 0
	}
	limit := s.config.ResolveArchiveBucketSize()
	if limit <= 0 {
		return 0
	}
	threshold := (limit*70 + 99) / 100
	if threshold < 1 {
		return 1
	}
	if threshold > limit {
		return limit
	}
	return threshold
}

func (s *Shard) shouldSinkSideMountedBucket(bucket *ArchiveBucketNode) bool {
	threshold := s.archiveSideMountSinkThreshold()
	return threshold > 0 && bucket != nil && bucket.Count >= uint64(threshold)
}

func (s *Shard) childMayNeedPrune(node Node, epoch byte, hasHash bool, global byte) bool {
	mask, ok := s.childEpochMask(node, epoch, hasHash)
	if !ok {
		return true
	}
	return mask&^leafEpochMask(global) != 0
}

func (s *Shard) collapseSmallArchiveChildToStub(node Node, entryPath []byte, entryBits int) (*ArchiveBucketNode, error) {
	limit := s.config.ResolveArchiveBucketSize()
	if limit <= 0 || node == nil {
		return nil, nil
	}
	count, archiveOnly, err := s.archiveOnlyItemCount(node)
	if err != nil || !archiveOnly || count == 0 || count > uint64(limit) {
		return nil, err
	}
	if !s.shouldSideMountArchiveItems(int(count)) {
		return nil, nil
	}
	items, err := s.collectLeavesRecursive(node, entryPath, entryBits)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 || len(items) > limit {
		return nil, nil
	}
	if err := s.markSubtreeStaleRecursive(node); err != nil {
		return nil, err
	}
	bucket, ok := s.buildArchiveBucket(items, entryPath, entryBits).(*ArchiveBucketNode)
	if !ok {
		return nil, nil
	}
	return bucket, nil
}

func (s *Shard) sinkFirstMatureStub(parent *InternalNode, nodePath []byte, nodeBits int) (bool, error) {
	if parent == nil || len(parent.StubList) == 0 {
		return false, nil
	}
	for i := 0; i < len(parent.StubList); i++ {
		bucket := parent.StubList[i]
		if !s.shouldSinkSideMountedBucket(bucket) {
			continue
		}
		sunk, err := s.sinkStubAtIndex(parent, nodePath, nodeBits, i)
		if err != nil || sunk {
			return sunk, err
		}
	}
	return false, nil
}

func (s *Shard) sinkSpecificMatureStub(parent *InternalNode, nodePath []byte, nodeBits int, target *ArchiveBucketNode) (bool, error) {
	if parent == nil || target == nil || !s.shouldSinkSideMountedBucket(target) {
		return false, nil
	}
	for i, bucket := range parent.StubList {
		if bucket == target {
			return s.sinkStubAtIndex(parent, nodePath, nodeBits, i)
		}
	}
	return false, nil
}

func (s *Shard) sinkStubAtIndex(parent *InternalNode, nodePath []byte, nodeBits int, index int) (bool, error) {
	if parent == nil || index < 0 || index >= len(parent.StubList) {
		return false, nil
	}
	bucket := parent.StubList[index]
	if !s.shouldSinkSideMountedBucket(bucket) {
		return false, nil
	}
	if !hasBitPrefix(bucket.Path, bucket.PathBits, nodePath, nodeBits) || bucket.PathBits <= nodeBits {
		return false, nil
	}

	bit := s.getBitFromBytes(bucket.Path, nodeBits)
	childPath, childBits := s.appendBit(nodePath, nodeBits, bit)
	var child Node
	var childHash []byte
	if bit == 0 {
		child, childHash = parent.Left, parent.LeftHash
	} else {
		child, childHash = parent.Right, parent.RightHash
	}
	if child == nil && len(childHash) > 0 {
		loaded, err := s.loadChildNode(parent, bit, childHash)
		if err != nil {
			return false, err
		}
		child = loaded
	}

	newChild, ok, err := s.sinkBucketIntoArchiveChild(child, bucket, childPath, childBits)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	parent.StubList = append(parent.StubList[:index], parent.StubList[index+1:]...)
	s.markPersistedNodeStale(parent)
	parent.SetDirty(true)
	if bit == 0 {
		parent.Left, parent.LeftHash = newChild, nil
		if newChild != nil {
			parent.LeftEpoch = newChild.Epoch()
		} else {
			parent.LeftEpoch = 0
		}
	} else {
		parent.Right, parent.RightHash = newChild, nil
		if newChild != nil {
			parent.RightEpoch = newChild.Epoch()
		} else {
			parent.RightEpoch = 0
		}
	}
	s.refreshInternalEpochMask(parent)
	return true, nil
}

func (s *Shard) sinkBucketIntoArchiveChild(child Node, bucket *ArchiveBucketNode, childPath []byte, childBits int) (Node, bool, error) {
	if bucket == nil {
		return child, false, nil
	}
	if child == nil {
		bucket.SetDirty(true)
		return bucket, true, nil
	}
	_, archiveOnly, err := s.archiveOnlyItemCount(child)
	if err != nil || !archiveOnly {
		if err != nil {
			return child, false, err
		}
		return s.rebuildMixedChildWithArchiveBucket(child, bucket, childPath, childBits)
	}
	childItems, err := s.collectLeavesRecursive(child, childPath, childBits)
	if err != nil {
		return child, false, err
	}
	bucketItems, err := s.collectLeavesRecursive(bucket, nil, 0)
	if err != nil {
		return child, false, err
	}
	items := append(childItems, bucketItems...)
	if len(items) == 0 {
		return child, false, nil
	}
	if err := s.markSubtreeStaleRecursive(child); err != nil {
		return child, false, err
	}
	if oldHash := s.ensureBucketHash(bucket); len(oldHash) > 0 {
		s.markArchiveDataDelete(oldHash, -1)
	}
	return s.buildArchiveSubtreeFast(items, childPath, childBits), true, nil
}

type hotArchiveLeaf struct {
	key       []byte
	valueHash []byte
}

func (s *Shard) splitArchiveBucketForHotInsert(bucket *ArchiveBucketNode, forceBits int) (Node, bool, error) {
	if bucket == nil || !s.shouldSinkSideMountedBucket(bucket) || forceBits > MaxPathBits {
		return bucket, false, nil
	}
	if forceBits <= bucket.PathBits {
		forceBits = bucket.PathBits + 1
	}
	if forceBits > MaxPathBits {
		return bucket, false, nil
	}
	items, err := s.collectLeavesRecursive(bucket, nil, 0)
	if err != nil {
		return bucket, false, err
	}
	if len(items) == 0 {
		return bucket, false, nil
	}
	s.markPersistedNodeStale(bucket)
	if oldHash := s.ensureBucketHash(bucket); len(oldHash) > 0 {
		s.markArchiveDataDelete(oldHash, -1)
	}
	return s.buildArchiveSubtreeForce(items, bucket.Path, bucket.PathBits, forceBits), true, nil
}

func (s *Shard) rebuildMixedChildWithArchiveBucket(child Node, bucket *ArchiveBucketNode, childPath []byte, childBits int) (Node, bool, error) {
	archiveItems, hotLeaves, err := s.collectArchiveItemsAndHotLeaves(child, childPath, childBits)
	if err != nil {
		return child, false, err
	}
	bucketItems, err := s.collectLeavesRecursive(bucket, nil, 0)
	if err != nil {
		return child, false, err
	}
	archiveItems = append(archiveItems, bucketItems...)

	var rebuilt Node
	if len(archiveItems) > 0 {
		rebuilt = s.buildArchiveSubtreeFast(archiveItems, childPath, childBits)
	}
	for _, leaf := range hotLeaves {
		rebuilt, err = s.insert(rebuilt, leaf.key, childBits, leaf.valueHash)
		if err != nil {
			return child, false, err
		}
	}
	if err := s.markSubtreeStaleRecursive(child); err != nil {
		return child, false, err
	}
	if oldHash := s.ensureBucketHash(bucket); len(oldHash) > 0 {
		s.markArchiveDataDelete(oldHash, -1)
	}
	if rebuilt != nil {
		rebuilt.SetDirty(true)
	}
	return rebuilt, true, nil
}

func (s *Shard) collectArchiveItemsAndHotLeaves(node Node, prefix []byte, prefixBits int) ([]ArchivedKV, []hotArchiveLeaf, error) {
	switch n := node.(type) {
	case nil:
		return nil, nil, nil
	case *LeafNode:
		key, bits := s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		if bits != MaxPathBits {
			key = s.prefixBits(key, bits, nil)
		}
		return nil, []hotArchiveLeaf{{
			key:       common.CopyBytes(key),
			valueHash: common.CopyBytes(n.ValueHash),
		}}, nil
	case *ArchiveBucketNode:
		items, err := s.collectLeavesRecursive(n, prefix, prefixBits)
		return items, nil, err
	case *InternalNode:
		var err error
		currentP, currentB := prefix, prefixBits
		if n.PathBits > 0 {
			currentP, currentB = s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		}
		lp, lb := s.appendBit(currentP, currentB, 0)
		rp, rb := s.appendBit(currentP, currentB, 1)
		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, err = s.loadChildNode(n, 0, n.LeftHash)
			if err != nil {
				return nil, nil, err
			}
		}
		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadChildNode(n, 1, n.RightHash)
			if err != nil {
				return nil, nil, err
			}
		}
		leftArchive, leftHot, err := s.collectArchiveItemsAndHotLeaves(n.Left, lp, lb)
		if err != nil {
			return nil, nil, err
		}
		rightArchive, rightHot, err := s.collectArchiveItemsAndHotLeaves(n.Right, rp, rb)
		if err != nil {
			return nil, nil, err
		}
		archiveItems := append(leftArchive, rightArchive...)
		hotLeaves := append(leftHot, rightHot...)
		for _, stub := range n.StubList {
			stubItems, err := s.collectLeavesRecursive(stub, currentP, currentB)
			if err != nil {
				return nil, nil, err
			}
			archiveItems = append(archiveItems, stubItems...)
		}
		return archiveItems, hotLeaves, nil
	default:
		return nil, nil, nil
	}
}

func (s *Shard) archiveOnlyItemCount(node Node) (uint64, bool, error) {
	switch n := node.(type) {
	case nil:
		return 0, true, nil
	case *LeafNode:
		return 0, false, nil
	case *ArchiveBucketNode:
		return n.Count, true, nil
	case *InternalNode:
		var err error
		var total uint64
		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, err = s.loadChildNode(n, 0, n.LeftHash)
			if err != nil {
				return 0, false, err
			}
		}
		leftCount, leftArchiveOnly, err := s.archiveOnlyItemCount(n.Left)
		if err != nil || !leftArchiveOnly {
			return 0, leftArchiveOnly, err
		}
		total += leftCount

		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadChildNode(n, 1, n.RightHash)
			if err != nil {
				return 0, false, err
			}
		}
		rightCount, rightArchiveOnly, err := s.archiveOnlyItemCount(n.Right)
		if err != nil || !rightArchiveOnly {
			return 0, rightArchiveOnly, err
		}
		total += rightCount

		for _, bucket := range n.StubList {
			if bucket != nil {
				total += bucket.Count
			}
		}
		return total, true, nil
	default:
		return 0, false, nil
	}
}

func (s *Shard) buildArchiveSubtree(items []ArchivedKV, path []byte, bits int) Node {
	if len(items) == 0 {
		return nil
	}

	limit := s.config.ResolveArchiveBucketSize()
	if len(items) > limit && limit > 0 && bits < MaxPathBits {
		var leftItems, rightItems []ArchivedKV
		for _, it := range items {
			if bits >= it.SuffixBits || s.getBitFromBytes(it.Suffix, bits) == 0 {
				leftItems = append(leftItems, it)
			} else {
				rightItems = append(rightItems, it)
			}
		}

		n := s.pool.GetInternal()
		n.SetDirty(true)
		if len(leftItems) > 0 {
			lp, lb := s.appendBit(path, bits, 0)
			n.Left = s.buildArchiveSubtree(leftItems, lp, lb)
			if n.Left != nil {
				n.LeftEpoch = n.Left.Epoch()
			}
		}
		if len(rightItems) > 0 {
			rp, rb := s.appendBit(path, bits, 1)
			n.Right = s.buildArchiveSubtree(rightItems, rp, rb)
			if n.Right != nil {
				n.RightEpoch = n.Right.Epoch()
			}
		}
		s.refreshInternalEpochMask(n)
		return n
	}

	// 达到桶大小限制，或者达到 MaxPathBits 极限，停止分裂。
	bucketSize := s.config.ResolveArchiveBucketSize()
	if len(items) <= bucketSize || bucketSize <= 0 || bits >= MaxPathBits {
		return s.buildArchiveBucket(items, path, bits)
	}
	return nil
}

func (s *Shard) buildArchiveSubtreeFast(items []ArchivedKV, path []byte, bits int) Node {
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

func (s *Shard) buildArchiveSubtreeForce(items []ArchivedKV, path []byte, bits int, forceBits int) Node {
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

func (s *Shard) collectAndAttachToStubList(parent *InternalNode, items []ArchivedKV, absPath []byte, absBits int) {
	if len(items) == 0 {
		return
	}
	// absPath/absBits is the absolute path for the archive entry point.
	// items are already absolute paths; buildArchiveSubtree will strip absPath.

	archNode := s.buildArchiveSubtreeFast(items, absPath, absBits)
	if bucket, ok := archNode.(*ArchiveBucketNode); ok {
		s.attachStubs(parent, []*ArchiveBucketNode{bucket})
	} else if in, ok := archNode.(*InternalNode); ok {
		s.flattenToStubList(parent, in)
	}
	parent.SetDirty(true)
}

func (s *Shard) collectAndAttachToStubListAtPath(parent *InternalNode, items []ArchivedKV, absPath []byte, absBits int, nodePath []byte, nodeBits int) (bool, error) {
	if len(items) == 0 {
		return false, nil
	}
	// absPath/absBits is the absolute path for the archive entry point.
	// items are already absolute paths; buildArchiveSubtree will strip absPath.

	archNode := s.buildArchiveSubtreeFast(items, absPath, absBits)
	if bucket, ok := archNode.(*ArchiveBucketNode); ok {
		if changed, err := s.attachStubsAtPath(parent, []*ArchiveBucketNode{bucket}, nodePath, nodeBits); err != nil {
			return changed, err
		}
	} else if in, ok := archNode.(*InternalNode); ok {
		if changed, err := s.flattenToStubListAtPath(parent, in, nodePath, nodeBits); err != nil {
			return changed, err
		}
	}
	parent.SetDirty(true)
	return true, nil
}

func (s *Shard) flattenToStubList(parent *InternalNode, sub *InternalNode) {
	_, _ = s.flattenToStubListAtPath(parent, sub, nil, 0)
}

func (s *Shard) flattenToStubListAtPath(parent *InternalNode, sub *InternalNode, nodePath []byte, nodeBits int) (bool, error) {
	stubs := s.collectArchiveBucketStubs(sub, nil)
	return s.attachStubsAtPath(parent, stubs, nodePath, nodeBits)
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

func (s *Shard) mergeArchiveSubtree(hot *InternalNode, cold Node) {
	if cold == nil {
		return
	}
	switch c := cold.(type) {
	case *ArchiveBucketNode:
		s.attachStubs(hot, []*ArchiveBucketNode{c})
	case *InternalNode:
		// 展平所有归档桶
		s.flattenToStubList(hot, c)
		hot.SetDirty(true)
	}
}

func (s *Shard) mountBucket(node *InternalNode, items []ArchivedKV) {
	bucket := &ArchiveBucketNode{}
	s.recomputeBucket(bucket, items)
	s.attachStubs(node, []*ArchiveBucketNode{bucket})
}
