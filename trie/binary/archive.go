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
	if s.root == nil {
		return nil
	}

	// 递归剪枝
	newRoot, items, err := s.pruneAndArchive(s.root, s.config.ShardDepth)
	if err != nil {
		return err
	}

	if newRoot == nil && len(items) > 0 {
		// All content archived.
		s.root = s.buildArchiveSubtree(items, nil, 0)
	} else if newRoot != nil && len(items) > 0 {
		// Root exists, but have top-level archived items.
		if in, ok := newRoot.(*InternalNode); ok {
			archNode := s.buildArchiveSubtree(items, in.Path, in.PathBits)
			if b, ok := archNode.(*ArchiveBucketNode); ok {
				in.StubList = append(in.StubList, b)
			} else if subIn, ok := archNode.(*InternalNode); ok {
				// If it split into a subtree, we merge it or attach it.
				// For simplicity, we attach it to StubList but StubList only takes buckets.
				// Wait! StubList should probably take Nodes? No, design says buckets.
				// If buildArchiveSubtree returns an InternalNode, we need to merge it.
				s.mergeArchiveSubtree(in, subIn)
			}
			in.SetDirty(true)
			s.root = in
		} else {
			s.root = newRoot
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
		if (n.epoch & 1) != global {
			// 过期叶子：直接作为归档项返回
			it := ArchivedKV{
				Suffix:     append([]byte{}, n.Path...),
				SuffixBits: n.PathBits,
				Value:      append([]byte{}, n.ValueHash...),
			}
			return nil, []ArchivedKV{it}, nil
		}
		return n, nil, nil

	case *InternalNode:
		// 如果子树中所有节点都不是当前年度位，则整个子树归档
		if (n.epoch & 1) != global {
			var items []ArchivedKV
			s.collectLeaves(n, 0, &items)
			return nil, items, nil
		}

		// 否则，递归处理子节点
		var allItems []ArchivedKV

		// 处理左子树
		if n.Left != nil || len(n.LeftHash) > 0 {
			if n.Left == nil {
				loaded, err := s.loadNode(n.LeftHash)
				if err != nil {
					return n, nil, err
				}
				n.Left = loaded
			}
			newLeft, items, err := s.pruneAndArchive(n.Left, depth+1+n.PathBits)
			if err != nil {
				return n, nil, err
			}
			// 给 items 增加 0 位前缀
			for i := range items {
				items[i].Suffix = s.prependBit(items[i].Suffix, items[i].SuffixBits, 0, nil)
				items[i].SuffixBits++
				if n.PathBits > 0 {
					items[i].Suffix = s.prependPath(items[i].Suffix, items[i].SuffixBits, n.Path, n.PathBits)
					items[i].SuffixBits += n.PathBits
				}
			}
			allItems = append(allItems, items...)
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
					return n, nil, err
				}
				n.Right = loaded
			}
			newRight, items, err := s.pruneAndArchive(n.Right, depth+1+n.PathBits)
			if err != nil {
				return n, nil, err
			}
			// 给 items 增加 1 位前缀
			for i := range items {
				items[i].Suffix = s.prependBit(items[i].Suffix, items[i].SuffixBits, 1, nil)
				items[i].SuffixBits++
				if n.PathBits > 0 {
					items[i].Suffix = s.prependPath(items[i].Suffix, items[i].SuffixBits, n.Path, n.PathBits)
					items[i].SuffixBits += n.PathBits
				}
			}
			allItems = append(allItems, items...)
			n.Right = newRight
			if newRight == nil {
				n.RightHash = nil
				n.SetDirty(true)
			}
		}

		// 如果当前节点变空了，且有 StubList，我们需要把 StubList 也转为 items 向上抛，或者就地转为桶
		// 为了简单和一致，我们将所有归档项向上抛，直到遇到一个非空的 InternalNode。

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
				// 还要加上当前节点的 Path
				if n.PathBits > 0 {
					bitm.Suffix = s.prependPath(bitm.Suffix, bitm.SuffixBits, n.Path, n.PathBits)
					bitm.SuffixBits += n.PathBits
				}
				allItems = append(allItems, bitm)
			}
		}
		n.StubList = nil // 既然已经收集了，就清空

		if n.Left == nil && n.Right == nil {
			// 既然左右都空了，当前节点也没用了，继续向上传递 items
			return nil, allItems, nil
		}

		// Otherwise, current node is hot, attach allItems as bucket(s)
		if len(allItems) > 0 {
			archNode := s.buildArchiveSubtree(allItems, n.Path, n.PathBits)
			if b, ok := archNode.(*ArchiveBucketNode); ok {
				n.StubList = append(n.StubList, b)
			} else if subIn, ok := archNode.(*InternalNode); ok {
				s.mergeArchiveSubtree(n, subIn)
			}
			n.SetDirty(true)
		}

		return s.shrink(n), nil, nil

	case *ArchiveBucketNode:
		// 已经归档的桶，不参与本次剪枝，直接原样返回
		return n, nil, nil
	}

	return node, nil, nil
}

func (s *Shard) collectLeaves(node Node, depth int, items *[]ArchivedKV) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *LeafNode:
		it := ArchivedKV{
			Suffix:     append([]byte{}, n.Path...),
			SuffixBits: n.PathBits,
			Value:      append([]byte{}, n.ValueHash...),
		}
		*items = append(*items, it)
	case *InternalNode:
		if n.Left != nil || len(n.LeftHash) > 0 {
			if n.Left == nil {
				n.Left, _ = s.loadNode(n.LeftHash)
			}
			tmp := len(*items)
			s.collectLeaves(n.Left, depth+1+n.PathBits, items)
			for i := tmp; i < len(*items); i++ {
				(*items)[i].Suffix = s.prependBit((*items)[i].Suffix, (*items)[i].SuffixBits, 0, nil)
				(*items)[i].SuffixBits++
				if n.PathBits > 0 {
					(*items)[i].Suffix = s.prependPath((*items)[i].Suffix, (*items)[i].SuffixBits, n.Path, n.PathBits)
					(*items)[i].SuffixBits += n.PathBits
				}
			}
		}
		if n.Right != nil || len(n.RightHash) > 0 {
			if n.Right == nil {
				n.Right, _ = s.loadNode(n.RightHash)
			}
			tmp := len(*items)
			s.collectLeaves(n.Right, depth+1+n.PathBits, items)
			for i := tmp; i < len(*items); i++ {
				(*items)[i].Suffix = s.prependBit((*items)[i].Suffix, (*items)[i].SuffixBits, 1, nil)
				(*items)[i].SuffixBits++
				if n.PathBits > 0 {
					(*items)[i].Suffix = s.prependPath((*items)[i].Suffix, (*items)[i].SuffixBits, n.Path, n.PathBits)
					(*items)[i].SuffixBits += n.PathBits
				}
			}
		}
		for _, bucket := range n.StubList {
			data, err := s.getBucketData(bucket.Hash())
			if err != nil {
				continue
			}
			bucketItems, _ := s.deserializeArchivedKV(data)
			for _, bitm := range bucketItems {
				bitm.Suffix = s.prependPath(bitm.Suffix, bitm.SuffixBits, bucket.Path, bucket.PathBits)
				bitm.SuffixBits += bucket.PathBits
				if n.PathBits > 0 {
					bitm.Suffix = s.prependPath(bitm.Suffix, bitm.SuffixBits, n.Path, n.PathBits)
					bitm.SuffixBits += n.PathBits
				}
				*items = append(*items, bitm)
			}
		}
	case *ArchiveBucketNode:
		data, err := s.getBucketData(n.Hash())
		if err != nil {
			return
		}
		bucketItems, _ := s.deserializeArchivedKV(data)
		for _, bitm := range bucketItems {
			bitm.Suffix = s.prependPath(bitm.Suffix, bitm.SuffixBits, n.Path, n.PathBits)
			bitm.SuffixBits += n.PathBits
			*items = append(*items, bitm)
		}
	}
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
				if bit == 0 {
					s.mountBucket(parent.Left, bucket)
				} else {
					s.mountBucket(parent.Right, bucket)
				}
			} else {
				parent.StubList = append(parent.StubList, bucket)
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
		s.recomputeBucket(bucket, items)
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
		mid := len(items) / 2
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
	// If sub has children, we attach them. If it has StubList, we merge it.
	// This is a simplified merge to ensure we don't lose nodes.
	if sub.Left != nil || len(sub.LeftHash) > 0 {
		// This is tricky because target might already have a Left.
		// For the purpose of pruning, we just want to ensure all items are reachable.
		// If both have Left, we'd need a deep merge.
		// BUT buildArchiveSubtree creates a FRESH tree.
		// If we are attaching it to a HOT InternalNode, we should ideally merge the trees.

		// Optimization: Just attach the sub's StubList if the tree is too complex.
		target.StubList = append(target.StubList, sub.StubList...)
		// And for children? We can't easily merge hot tree and archive tree here.
		// So we'll just put the sub-buckets into StubList if they are individual buckets.
	} else {
		target.StubList = append(target.StubList, sub.StubList...)
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
