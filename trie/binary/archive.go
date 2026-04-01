package binary

import (
	"bytes"
	"errors"
)

// Prune 执行剪枝逻辑：遍历树，寻找“冷”数据并将其归档到 StubList。
func (s *Shard) Prune() error {
	if s.root == nil && len(s.rootHash) > 0 {
		var err error
		s.root, err = s.loadNode(s.rootHash)
		if err != nil {
			return err
		}
	}
	// 递归剪枝
	newRoot, items, err := s.pruneAndArchive(s.root, s.config.ShardDepth)
	if err != nil {
		return err
	}

	if newRoot == nil && len(items) > 0 {
		// All content archived.
		s.root = s.buildArchiveSubtree(items, []byte{}, 0)
	} else if newRoot != nil && len(items) > 0 {
		// Root exists, but have top-level archived items.
		if in, ok := newRoot.(*InternalNode); ok {
			archNode := s.buildArchiveSubtree(items, in.Path, in.PathBits)
			if b, ok := archNode.(*ArchiveBucketNode); ok {
				in.StubList = append(in.StubList, b)
				in.SetDirty(true)
			} else if subIn, ok := archNode.(*InternalNode); ok {
				s.mergeArchiveSubtree(in, subIn)
			}
			s.root = in
		} else {
			// newRoot is a LeafNode, but we have items.
			// Must create a new InternalNode to hold both.
			root := s.pool.GetInternal()
			root.SetDirty(true)
			archNode := s.buildArchiveSubtree(items, []byte{}, 0)
			if b, ok := archNode.(*ArchiveBucketNode); ok {
				root.StubList = append(root.StubList, b)
			} else if subIn, ok := archNode.(*InternalNode); ok {
				s.mergeArchiveSubtree(root, subIn)
			}

			// We need to re-insert the newRoot into the root
			leaf := newRoot.(*LeafNode)
			var err error
			// Re-insert the leaf into the new root. insert starts at 0 if root is new.
			s.root, err = s.insert(root, leaf.Path, 0, leaf.ValueHash)
			if err != nil {
				return err
			}
		}
	} else {
		s.root = newRoot
	}

	return nil
}

func (s *Shard) pruneAndArchive(node Node, depth int) (Node, []ArchivedKV, error) {
	if node == nil {
		return nil, nil, nil
	}
	global := s.globalEpochBit()

	switch n := node.(type) {
	case *LeafNode:
		// 如果叶子节点的 Epoch 太旧，则归档
		if n.Epoch() != s.globalEpochBit() {
			items, err := s.collectLeavesRecursive(n, false) // Prepend n.Path
			if err != nil {
				return nil, nil, err
			}
			return nil, items, nil
		}
		return n, nil, nil

	case *InternalNode:
		// 如果子树中所有节点都不是当前年度位，则整个子树归档
		if (n.epoch & 1) != global {
			items, err := s.collectLeaves(n)
			if err != nil {
				return nil, nil, err
			}
			return nil, items, nil
		}
		var allItems []ArchivedKV
		// 处理左子树
		if n.Left != nil || len(n.LeftHash) > 0 {
			if n.Left == nil {
				loaded, err := s.loadNode(n.LeftHash)
				if err != nil {
					return nil, nil, err
				}
				n.Left = loaded
			}
			newLeft, leftItems, err := s.pruneAndArchive(n.Left, depth+1+n.PathBits)
			if err != nil {
				return n, nil, err
			}
			// 给 items 增加 0 位前缀
			for i := range leftItems {
				leftItems[i].Suffix = s.prependBit(leftItems[i].Suffix, leftItems[i].SuffixBits, 0, nil)
				leftItems[i].SuffixBits++
				if n.PathBits > 0 {
					leftItems[i].Suffix = s.prependPath(leftItems[i].Suffix, leftItems[i].SuffixBits, n.Path, n.PathBits)
					leftItems[i].SuffixBits += n.PathBits
				}
			}
			allItems = append(allItems, leftItems...)
			n.Left = newLeft
			if newLeft == nil {
				n.LeftHash = nil
				n.SetDirty(true)
			}
		}

		// 处理右子树
		if n.Right != nil || len(n.RightHash) > 0 {
			if n.Right == nil {
				loaded, err := s.loadNode(n.RightHash)
				if err != nil {
					return nil, nil, err
				}
				n.Right = loaded
			}
			newRight, rightItems, err := s.pruneAndArchive(n.Right, depth+1+n.PathBits)
			if err != nil {
				return n, nil, err
			}
			// 给 items 增加 1 位前缀
			for i := range rightItems {
				rightItems[i].Suffix = s.prependBit(rightItems[i].Suffix, rightItems[i].SuffixBits, 1, nil)
				rightItems[i].SuffixBits++
				if n.PathBits > 0 {
					rightItems[i].Suffix = s.prependPath(rightItems[i].Suffix, rightItems[i].SuffixBits, n.Path, n.PathBits)
					rightItems[i].SuffixBits += n.PathBits
				}
			}
			allItems = append(allItems, rightItems...)
			n.Right = newRight
			if newRight == nil {
				n.RightHash = nil
				n.SetDirty(true)
			}
		}

		// 收集已有的 StubList 中的项
		for _, bucket := range n.StubList {
			data, err := s.getBucketData(bucket.Hash())
			if err != nil {
				continue
			}
			bucketItems, _ := s.deserializeArchivedKV(data)
			for _, bitm := range bucketItems {
				bitm.Suffix = s.prependPath(bitm.Suffix, bitm.SuffixBits, bucket.Path, bucket.PathBits)
				bitm.SuffixBits += bucket.PathBits
				allItems = append(allItems, bitm)
			}
		}
		n.StubList = nil // 既然已经收集了，就清空

		// 如果由于修剪变空了，或是由于节点过期（Epoch 位不同），则收集所有剩余叶子并归档
		if (n.Left == nil && n.Right == nil) || n.Epoch() != s.globalEpochBit() {
			items, err := s.collectLeavesRecursive(n, false) // Prepend n.Path to return items relative to parent
			if err != nil {
				return nil, nil, err
			}
			return nil, append(allItems, items...), nil
		}

		// 收缩逻辑
		return s.shrink(n), allItems, nil
	}
	return node, nil, nil
}

func (s *Shard) collectLeaves(node Node) ([]ArchivedKV, error) {
	return s.collectLeavesRecursive(node, true)
}

func (s *Shard) collectLeavesRecursive(node Node, isTopLevel bool) ([]ArchivedKV, error) {
	if node == nil {
		return nil, nil
	}

	switch n := node.(type) {
	case *LeafNode:
		var suffix []byte
		var bits int
		if !isTopLevel {
			suffix = append([]byte{}, n.Path...)
			bits = n.PathBits
		}
		item := ArchivedKV{
			Suffix:     suffix,
			SuffixBits: bits,
			Value:      append([]byte{}, n.ValueHash...),
		}
		return []ArchivedKV{item}, nil

	case *InternalNode:
		var allItems []ArchivedKV
		if n.Left != nil || len(n.LeftHash) > 0 {
			if n.Left == nil {
				loaded, err := s.loadNode(n.LeftHash)
				if err != nil {
					return nil, err
				}
				n.Left = loaded
			}
			left, err := s.collectLeavesRecursive(n.Left, false)
			if err != nil {
				return nil, err
			}
			for i := range left {
				left[i].Suffix = s.prependBit(left[i].Suffix, left[i].SuffixBits, 0, nil)
				left[i].SuffixBits++
			}
			allItems = append(allItems, left...)
		}
		if n.Right != nil || len(n.RightHash) > 0 {
			if n.Right == nil {
				loaded, err := s.loadNode(n.RightHash)
				if err != nil {
					return nil, err
				}
				n.Right = loaded
			}
			right, err := s.collectLeavesRecursive(n.Right, false)
			if err != nil {
				return nil, err
			}
			for i := range right {
				right[i].Suffix = s.prependBit(right[i].Suffix, right[i].SuffixBits, 1, nil)
				right[i].SuffixBits++
			}
			allItems = append(allItems, right...)
		}

		// Also collect items from StubList
		for _, bucket := range n.StubList {
			data, err := s.getBucketData(bucket.Hash())
			if err != nil {
				return nil, err
			}
			bucketItems, _ := s.deserializeArchivedKV(data)
			for i := range bucketItems {
				bucketItems[i].Suffix = s.prependPath(bucketItems[i].Suffix, bucketItems[i].SuffixBits, bucket.Path, bucket.PathBits)
				bucketItems[i].SuffixBits += bucket.PathBits
				allItems = append(allItems, bucketItems[i])
			}
		}
		// FINAL STEP for InternalNode: prepend its OWN path if NOT top-level
		if !isTopLevel && n.PathBits > 0 {
			for i := range allItems {
				allItems[i].Suffix = s.prependPath(allItems[i].Suffix, allItems[i].SuffixBits, n.Path, n.PathBits)
				allItems[i].SuffixBits += n.PathBits
			}
		}
		return allItems, nil

	case *ArchiveBucketNode:
		data, err := s.getBucketData(n.Hash())
		if err != nil {
			return nil, err
		}
		items, _ := s.deserializeArchivedKV(data)
		if !isTopLevel && n.PathBits > 0 {
			for i := range items {
				// Prefix item suffix with bucket's path (relative to parent)
				items[i].Suffix = s.prependPath(items[i].Suffix, items[i].SuffixBits, n.Path, n.PathBits)
				items[i].SuffixBits += n.PathBits
			}
		} else if isTopLevel {
			// Suffixes in bucket are relative to bucket. If called at top level,
			// it means we want them relative to bucket, which is what they already are.
		}
		return items, nil
	}
	return nil, nil
}

// FlushArchives 将所有待写入的归档数据持久化到 ArchiveDB。
func (s *Shard) FlushArchives() error {
	if s.config.ArchiveDB == nil {
		return errors.New("archive DB not configured")
	}

	// 写入 pendingAppends
	for hash, task := range s.pendingAppends {
		oldData, err := s.getBucketData(task.oldHash)
		if err != nil {
			return err
		}
		oldItems, _ := s.deserializeArchivedKV(oldData)
		newItems := append(oldItems, task.newItems...)
		newData, _ := s.serializeArchivedKV(newItems)
		if err := s.config.ArchiveDB.PutBucket([]byte(hash), newData); err != nil {
			return err
		}
	}
	s.pendingAppends = make(map[string]appendTask)

	// 写入 pendingArchives
	for hash, data := range s.pendingArchives {
		if err := s.config.ArchiveDB.PutBucket([]byte(hash), data); err != nil {
			return err
		}
	}
	s.pendingArchives = make(map[string][]byte)

	// 写入 pendingDeletes
	for hash, task := range s.pendingDeletes {
		oldData, err := s.getBucketData(task.oldHash)
		if err != nil {
			return err
		}
		oldItems, _ := s.deserializeArchivedKV(oldData)

		// 盲删：过滤掉要删除的项
		newItems := make([]ArchivedKV, 0, len(oldItems))
		for _, it := range oldItems {
			found := false
			for _, dit := range task.deleteItems {
				if bytes.Equal(it.Suffix, dit.Suffix) && it.SuffixBits == dit.SuffixBits {
					found = true
					break
				}
			}
			if !found {
				newItems = append(newItems, it)
			}
		}
		newData, _ := s.serializeArchivedKV(newItems)
		if err := s.config.ArchiveDB.PutBucket([]byte(hash), newData); err != nil {
			return err
		}
	}
	s.pendingDeletes = make(map[string]deleteTask)

	return nil
}

func (s *Shard) mountAtDeepest(node *InternalNode, items []ArchivedKV) Node {
	if len(items) == 0 {
		return nil
	}
	bucket := &ArchiveBucketNode{
		Path:     append([]byte{}, node.Path...),
		PathBits: node.PathBits,
	}
	s.recomputeBucket(bucket, items)
	return bucket
}

func (s *Shard) distributeStubs(oldNode *InternalNode, parent *InternalNode) {
	if len(oldNode.StubList) == 0 {
		return
	}

	remainingStubs := make([]*ArchiveBucketNode, 0)
	parentBits := parent.PathBits

	for _, bucket := range oldNode.StubList {
		matched := s.commonPrefixLen(bucket.Path, bucket.PathBits, parent.Path, 0)
		if matched == parentBits {
			if bucket.PathBits > parentBits {
				bit := s.getBitFromBytes(bucket.Path, parentBits)
				bucket.Path = s.shiftBits(bucket.Path, bucket.PathBits, parentBits+1, nil)
				bucket.PathBits -= (parentBits + 1)

				// [FIX] Ensure we don't lose the bucket if child is nil
				target := parent.Left
				if bit != 0 {
					target = parent.Right
				}

				if target != nil {
					s.mountBucket(target, bucket)
				} else {
					// If child is nil, keep it in the parent's StubList instead of dropping it
					parent.StubList = append(parent.StubList, bucket)
					parent.SetDirty(true)
				}
			} else {
				parent.StubList = append(parent.StubList, bucket)
				parent.SetDirty(true)
			}
		} else {
			remainingStubs = append(remainingStubs, bucket)
		}
	}
	oldNode.StubList = remainingStubs
}

func (s *Shard) buildArchiveSubtree(items []ArchivedKV, path []byte, bits int) Node {
	if len(items) == 0 {
		return nil
	}

	if len(items) <= s.config.ArchiveBucketSize || s.config.ArchiveBucketSize <= 0 {
		bucket := &ArchiveBucketNode{
			Path:     append([]byte{}, path...),
			PathBits: bits,
		}
		// If bits > 0, we must shift the items to be relative to the bucket
		if bits > 0 {
			shiftedItems := make([]ArchivedKV, len(items))
			for i, it := range items {
				shiftedItems[i] = ArchivedKV{
					Suffix:     s.shiftBits(it.Suffix, it.SuffixBits, bits, nil),
					SuffixBits: it.SuffixBits - bits,
					Value:      it.Value,
				}
			}
			s.recomputeBucket(bucket, shiftedItems)
		} else {
			s.recomputeBucket(bucket, items)
		}
		bucket.SetDirty(true)
		return bucket
	}

	// Split items by next bit
	leftItems := make([]ArchivedKV, 0)
	rightItems := make([]ArchivedKV, 0)
	for _, it := range items {
		if it.SuffixBits > 0 {
			bit := s.getBitFromBytes(it.Suffix, 0)
			newIt := ArchivedKV{
				Suffix:     s.shiftBits(it.Suffix, it.SuffixBits, 1, nil),
				SuffixBits: it.SuffixBits - 1,
				Value:      it.Value,
			}
			if bit == 0 {
				leftItems = append(leftItems, newIt)
			} else {
				rightItems = append(rightItems, newIt)
			}
		} else {
			leftItems = append(leftItems, it) // Fallback
		}
	}

	// If cannot split further, force split by count
	if len(leftItems) == 0 || len(rightItems) == 0 {
		if len(items) == 0 {
			return nil
		}
		mid := len(items) / 2
		// [FIX] Ensure at least one item on one side if odd
		if mid == 0 && len(items) > 0 {
			mid = 1
		}
		leftItems = items[:mid]
		rightItems = items[mid:]
		// Construct an internal node that just acts as an aggregator if bit-split failed
		in := s.pool.GetInternal()
		in.Path = append([]byte{}, path...)
		in.PathBits = bits

		lB := &ArchiveBucketNode{Path: nil, PathBits: 0}
		s.recomputeBucket(lB, leftItems)
		rB := &ArchiveBucketNode{Path: nil, PathBits: 0}
		s.recomputeBucket(rB, rightItems)

		in.StubList = append(in.StubList, lB, rB)
		in.SetDirty(true)
		return in
	}

	in := s.pool.GetInternal()
	in.Path = append([]byte{}, path...)
	in.PathBits = bits
	in.Left = s.buildArchiveSubtree(leftItems, nil, 0)
	in.Right = s.buildArchiveSubtree(rightItems, nil, 0)
	in.SetDirty(true)
	return in
}

func (s *Shard) mergeArchiveSubtree(target *InternalNode, sub *InternalNode) {
	// [FIX] Always merge the StubList from sub into target
	if len(sub.StubList) > 0 {
		target.StubList = append(target.StubList, sub.StubList...)
		target.SetDirty(true)
	}
	// If sub has children, we need to recursively merge them into target
	// This ensures we don't lose the hot/cold nodes in the sub
	if sub.Left != nil || len(sub.LeftHash) > 0 {
		if target.Left == nil && len(target.LeftHash) == 0 {
			target.Left = sub.Left
			target.LeftHash = sub.LeftHash
			target.LeftEpoch = sub.LeftEpoch
		} else {
			// Deep merge required if both have Left
			// For simplicity in this stabilization phase, let's just collect all leaves from sub if we can't easily merge.
			if sub.Left != nil {
				if subInternal, ok := sub.Left.(*InternalNode); ok {
					if targetInternal, ok := target.Left.(*InternalNode); ok {
						s.mergeArchiveSubtree(targetInternal, subInternal)
					} else {
						// Target Left is Leaf, can't easily merge, move sub items to target StubList
						items, _ := s.collectLeaves(sub.Left)
						for i := range items {
							items[i].Suffix = s.prependBit(items[i].Suffix, items[i].SuffixBits, 0, nil)
							items[i].SuffixBits++
							if sub.PathBits > 0 {
								items[i].Suffix = s.prependPath(items[i].Suffix, items[i].SuffixBits, sub.Path, sub.PathBits)
								items[i].SuffixBits += sub.PathBits
							}
						}
						if len(items) > 0 {
							newSub := s.buildArchiveSubtree(items, []byte{}, 0)
							if bucket, ok := newSub.(*ArchiveBucketNode); ok {
								target.StubList = append(target.StubList, bucket)
							}
						}
					}
				}
			}
		}
	}
	if sub.Right != nil || len(sub.RightHash) > 0 {
		if target.Right == nil && len(target.RightHash) == 0 {
			target.Right = sub.Right
			target.RightHash = sub.RightHash
			target.RightEpoch = sub.RightEpoch
		} else {
			// deep merge...
			if sub.Right != nil {
				if subInternal, ok := sub.Right.(*InternalNode); ok {
					if targetInternal, ok := target.Right.(*InternalNode); ok {
						s.mergeArchiveSubtree(targetInternal, subInternal)
					}
				}
			}
		}
	}
}

func (s *Shard) mountBucket(node Node, bucket *ArchiveBucketNode) {
	if node == nil {
		return
	}
	if n, ok := node.(*InternalNode); ok {
		n.StubList = append(n.StubList, bucket)
		n.SetDirty(true)
	}
}
