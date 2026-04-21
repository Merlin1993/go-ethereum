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
	newRoot, items, err := s.pruneAndArchive(s.root, prefix, prefixBits, global)
	if err != nil {
		return err
	}

	// 3. 处理结果合并到分片根节点
	if newRoot == nil {
		if len(items) > 0 {
			// 全量归档：构建归档子树并更新 root
			s.root = s.buildArchiveSubtree(items, prefix, prefixBits)
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

// pruneAndArchive 递归处理节点，items 返回值始终是 MaxPathBits 范围内的绝对路径。
func (s *Shard) pruneAndArchive(node Node, prefix []byte, prefixBits int, global byte) (Node, []ArchivedKV, error) {
	if node == nil {
		return nil, nil, nil
	}

	switch n := node.(type) {
	case *LeafNode:
		if (n.Epoch() & 1) != global {
			// 组装绝对路径
			absP, absB := s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
			item := ArchivedKV{
				Suffix:     absP,
				SuffixBits: absB,
				Value:      n.ValueHash,
			}
			return nil, []ArchivedKV{item}, nil
		}
		return n, nil, nil

	case *InternalNode:
		var err error
		origStubCount := len(n.StubList)
		// [FIX] Do NOT use InternalNode.epoch for fast-path archival.
		// insert() updates InternalNode.epoch along the traversal path, which
		// makes them appear "current" even when their leaf children are stale.
		// Always recurse into children to check individual leaf epochs.

		// 计算当前节点的内部全缀 (n.Path) 位，但不提前组合到 prefix，
		// 以便分别传递给左/右子树。
		currentP, currentB := prefix, prefixBits
		if n.PathBits > 0 {
			currentP, currentB = s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		}

		var allItems []ArchivedKV

		// 处理左子树
		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, err = s.loadNode(n.LeftHash)
			if err != nil {
				return nil, nil, err
			}
		}
		lp, lb := s.prependBit(currentP, currentB, 0)
		newLeft, leftItems, err := s.pruneAndArchive(n.Left, lp, lb, global)
		if err != nil {
			return nil, nil, err
		}

		if newLeft == nil && len(leftItems) > 0 {
			// items are already absolute paths; pass lp/lb as the absolute bucket entry path
			s.collectAndAttachToStubList(n, leftItems, lp, lb)
		} else {
			n.Left = newLeft
			allItems = append(allItems, leftItems...)
		}

		// 处理右子树
		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadNode(n.RightHash)
			if err != nil {
				return nil, nil, err
			}
		}
		rp, rb := s.prependBit(currentP, currentB, 1)
		newRight, rightItems, err := s.pruneAndArchive(n.Right, rp, rb, global)
		if err != nil {
			return nil, nil, err
		}

		if newRight == nil && len(rightItems) > 0 {
			s.collectAndAttachToStubList(n, rightItems, rp, rb)
		} else {
			n.Right = newRight
			allItems = append(allItems, rightItems...)
		}

		if n.Left != newLeft || n.Right != newRight || len(n.StubList) != origStubCount ||
			(newLeft != nil && newLeft.IsDirty()) || (newRight != nil && newRight.IsDirty()) {
			n.LeftHash, n.RightHash = nil, nil
			n.SetDirty(true)
		}

		resNode := s.shrink(n)
		return resNode, allItems, nil

	case *ArchiveBucketNode:
		// 已归档桶保持原样
		return n, nil, nil

	default:
		return node, nil, nil
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

		lp, lb := s.prependBit(currentP, currentB, 0)
		left, err := s.collectLeavesRecursive(n.Left, lp, lb)
		if err != nil {
			return nil, err
		}

		rp, rb := s.prependBit(currentP, currentB, 1)
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
		bucketData, err := s.getBucketData(n.Hash())
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

func (s *Shard) buildArchiveSubtree(items []ArchivedKV, path []byte, bits int) Node {
	if len(items) == 0 {
		return nil
	}

	// 达到桶大小限制，或者达到 MaxPathBits 极限，停止分裂。
	if len(items) <= s.config.ArchiveBucketSize || s.config.ArchiveBucketSize <= 0 || bits >= MaxPathBits {
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

	var leftItems, rightItems []ArchivedKV
	for _, it := range items {
		if bits >= it.SuffixBits {
			leftItems = append(leftItems, it)
			continue
		}
		if s.getBitFromBytes(it.Suffix, bits) == 0 {
			leftItems = append(leftItems, it)
		} else {
			rightItems = append(rightItems, it)
		}
	}

	if len(leftItems) == 0 || len(rightItems) == 0 {
		localItems := make([]ArchivedKV, len(items))
		for i := range items {
			p, b := s.stripPrefix(items[i].Suffix, items[i].SuffixBits, 0, path, bits)
			localItems[i] = ArchivedKV{Suffix: p, SuffixBits: b, Value: items[i].Value}
		}
		bucket := &ArchiveBucketNode{Path: path, PathBits: bits, dirty: true}
		s.recomputeBucket(bucket, localItems)
		return bucket
	}

	lp, lb := s.appendBit(path, bits, 0)
	leftNode := s.buildArchiveSubtree(leftItems, lp, lb)

	rp, rb := s.appendBit(path, bits, 1)
	rightNode := s.buildArchiveSubtree(rightItems, rp, rb)

	n := s.pool.GetInternal()
	n.Path = path
	n.PathBits = bits
	n.Left = leftNode
	n.Right = rightNode
	n.SetDirty(true)
	return n
}

func (s *Shard) collectAndAttachToStubList(parent *InternalNode, items []ArchivedKV, absPath []byte, absBits int) {
	if len(items) == 0 {
		return
	}
	// absPath/absBits is the absolute path for the archive entry point.
	// items are already absolute paths; buildArchiveSubtree will strip absPath.

	archNode := s.buildArchiveSubtree(items, absPath, absBits)
	if bucket, ok := archNode.(*ArchiveBucketNode); ok {
		parent.StubList = append(parent.StubList, bucket)
	} else if in, ok := archNode.(*InternalNode); ok {
		s.flattenToStubList(parent, in)
	}
	parent.SetDirty(true)
}

func (s *Shard) flattenToStubList(parent *InternalNode, sub *InternalNode) {
	if sub.Left != nil {
		s.flattenRecursive(parent, sub.Left)
	}
	if sub.Right != nil {
		s.flattenRecursive(parent, sub.Right)
	}
}

func (s *Shard) flattenRecursive(parent *InternalNode, node Node) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *ArchiveBucketNode:
		parent.StubList = append(parent.StubList, n)
	case *InternalNode:
		if n.Left != nil {
			s.flattenRecursive(parent, n.Left)
		}
		if n.Right != nil {
			s.flattenRecursive(parent, n.Right)
		}
	}
}

func (s *Shard) mergeArchiveSubtree(hot *InternalNode, cold Node) {
	if cold == nil {
		return
	}
	switch c := cold.(type) {
	case *ArchiveBucketNode:
		hot.StubList = append(hot.StubList, c)
		hot.SetDirty(true)
	case *InternalNode:
		// 展平所有归档桶
		s.flattenToStubList(hot, c)
		hot.SetDirty(true)
	}
}

func (s *Shard) mountBucket(node *InternalNode, items []ArchivedKV) {
	bucket := &ArchiveBucketNode{}
	s.recomputeBucket(bucket, items)
	node.StubList = append(node.StubList, bucket)
}
