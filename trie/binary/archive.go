package binary

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
				s.collectAndAttachToStubList(in, items, prefix, prefixBits)
			}
		}
	}
	if len(promotedStubs) > 0 && !promotedAttached && s.root != nil {
		s.root = s.attachPromotedStubsToRoot(s.root, promotedStubs)
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
		if s.pruning && len(in.OriginalHash()) > 0 {
			s.staleSet[string(in.OriginalHash())] = struct{}{}
		}
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
		if s.pruning && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}
		return bit, true
	case *LeafNode:
		if n.PathBits <= 0 {
			return 0, false
		}
		bit := s.getBitFromBytes(n.Path, 0)
		n.Path = s.shiftBits(n.Path, n.PathBits, 1, nil)
		n.PathBits--
		n.SetDirty(true)
		if s.pruning && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}
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
		var err error
		origStubCount := len(n.StubList)
		// [FIX] Do NOT use InternalNode.epoch for fast-path archival.
		// insert() updates InternalNode.epoch along the traversal path, which
		// makes them appear "current" even when their leaf children are stale.
		// Always recurse into children to check individual leaf epochs.
		if mask, ok := s.subtreeEpochMask(n); ok {
			hotMask := leafEpochMask(global)
			if mask&^hotMask == 0 {
				return n, nil, nil, nil
			}
			hasArchive, err := s.subtreeContainsArchiveBucket(n)
			if err != nil {
				return nil, nil, nil, err
			}
			if len(n.StubList) == 0 && mask&hotMask == 0 && !hasArchive {
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

		// 处理左子树
		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, err = s.loadNode(n.LeftHash)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		leftHadChild := n.Left != nil || len(n.LeftHash) > 0
		lp, lb := s.appendBit(currentP, currentB, 0)
		newLeft, leftItems, leftStubs, err := s.pruneAndArchive(n.Left, lp, lb, global)
		if err != nil {
			return nil, nil, nil, err
		}
		if len(leftStubs) > 0 {
			s.attachStubs(n, leftStubs)
			parentChanged = true
		}

		if newLeft == nil {
			if leftHadChild {
				parentChanged = true
			}
			if len(leftItems) > 0 {
				n.Left = s.buildArchiveSubtreeFast(leftItems, lp, lb)
				n.LeftHash = nil
				n.LeftEpoch = n.Left.Epoch()
				parentChanged = true
			} else {
				n.Left, n.LeftHash = nil, nil
				n.LeftEpoch = 0
			}
		} else {
			if n.Left != newLeft {
				parentChanged = true
			}
			n.Left = newLeft
			allItems = append(allItems, leftItems...)
		}

		// 处理右子树
		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadNode(n.RightHash)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		rightHadChild := n.Right != nil || len(n.RightHash) > 0
		rp, rb := s.appendBit(currentP, currentB, 1)
		newRight, rightItems, rightStubs, err := s.pruneAndArchive(n.Right, rp, rb, global)
		if err != nil {
			return nil, nil, nil, err
		}
		if len(rightStubs) > 0 {
			s.attachStubs(n, rightStubs)
			parentChanged = true
		}

		if newRight == nil {
			if rightHadChild {
				parentChanged = true
			}
			if len(rightItems) > 0 {
				n.Right = s.buildArchiveSubtreeFast(rightItems, rp, rb)
				n.RightHash = nil
				n.RightEpoch = n.Right.Epoch()
				parentChanged = true
			} else {
				n.Right, n.RightHash = nil, nil
				n.RightEpoch = 0
			}
		} else {
			if n.Right != newRight {
				parentChanged = true
			}
			n.Right = newRight
			allItems = append(allItems, rightItems...)
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
		s.markPersistedNodeStale(n)
		absP, absB := s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		return []ArchivedKV{{
			Suffix:     absP,
			SuffixBits: absB,
			Value:      n.ValueHash,
		}}, nil, nil

	case *InternalNode:
		s.markPersistedNodeStale(n)
		var err error
		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, err = s.loadNode(n.LeftHash)
			if err != nil {
				return nil, nil, err
			}
		}
		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadNode(n.RightHash)
			if err != nil {
				return nil, nil, err
			}
		}

		currentP, currentB := prefix, prefixBits
		if n.PathBits > 0 {
			currentP, currentB = s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		}

		lp, lb := s.appendBit(currentP, currentB, 0)
		left, leftStubs, err := s.collectLeavesAndMarkStaleRecursive(n.Left, lp, lb)
		if err != nil {
			return nil, nil, err
		}

		rp, rb := s.appendBit(currentP, currentB, 1)
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
		return nil, []*ArchiveBucketNode{n}, nil

	default:
		return nil, nil, nil
	}
}

func (s *Shard) subtreeContainsArchiveBucket(node Node) (bool, error) {
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
			n.Left, err = s.loadNode(n.LeftHash)
			if err != nil {
				return false, err
			}
		}
		hasArchive, err := s.subtreeContainsArchiveBucket(n.Left)
		if err != nil || hasArchive {
			return hasArchive, err
		}
		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadNode(n.RightHash)
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
		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, err = s.loadNode(n.LeftHash)
			if err != nil {
				return nil, err
			}
		}
		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadNode(n.RightHash)
			if err != nil {
				return nil, err
			}
		}

		currentP, currentB := prefix, prefixBits
		if n.PathBits > 0 {
			currentP, currentB = s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		}

		lp, lb := s.appendBit(currentP, currentB, 0)
		left, err := s.collectLeavesRecursive(n.Left, lp, lb)
		if err != nil {
			return nil, err
		}

		rp, rb := s.appendBit(currentP, currentB, 1)
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
		bucketData, err := s.getBucketData(s.ensureBucketHash(n))
		if err != nil {
			return nil, err
		}
		items, err := s.deserializeArchivedKV(bucketData)
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
			n.Left, err = s.loadNode(n.LeftHash)
			if err != nil {
				return err
			}
		}
		if err := s.markSubtreeStaleRecursive(n.Left); err != nil {
			return err
		}

		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadNode(n.RightHash)
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
			s.staleSet[string(hash)] = struct{}{}
			s.markArchiveDataDelete(hash, -1)
		}
	}
	return nil
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
		// 重要：存入桶之前，剥离物理前缀路径，确保桶内仅存储相对 Suffix。
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
		return s.buildArchiveBucket(items, basePath, baseBits)
	}

	n := s.pool.GetInternal()
	n.Path = nodePath
	n.PathBits = nodePathBits
	n.SetDirty(true)

	lp, lb := s.appendBit(basePath, baseBits, 0)
	n.Left = s.buildArchiveSubtreeFast(items[:split], lp, lb)
	if n.Left != nil {
		n.LeftEpoch = n.Left.Epoch()
	}

	rp, rb := s.appendBit(basePath, baseBits, 1)
	n.Right = s.buildArchiveSubtreeFast(items[split:], rp, rb)
	if n.Right != nil {
		n.RightEpoch = n.Right.Epoch()
	}

	s.refreshInternalEpochMask(n)
	return n
}

func (s *Shard) buildArchiveBucket(items []ArchivedKV, path []byte, bits int) Node {
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

func (s *Shard) flattenToStubList(parent *InternalNode, sub *InternalNode) {
	stubs := s.collectArchiveBucketStubs(sub, nil)
	s.attachStubs(parent, stubs)
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
