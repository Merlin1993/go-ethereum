package archive

import (
	"bytes"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Prune converts expired hot leaves in one shard into archive buckets.
// Unabsorbed items are aggregated at the shard root before overflow sinks.
func (s *Shard) Prune(global byte) error {
	_, err := s.PruneWithDiagnostics(global)
	return err
}

// PruneWithDiagnostics performs the same cleanup as Prune and reports where
// the time was spent. These phase timers remain available even when the more
// expensive per-node diagnostics are disabled.
func (s *Shard) PruneWithDiagnostics(global byte) (diag ShardPruneDiagnostics, err error) {
	totalStart := time.Now()
	lockStart := time.Now()
	s.mu.Lock()
	diag.LockWaitNanos = time.Since(lockStart).Nanoseconds()
	diag.DetailedCountersEnabled = s.config != nil && s.config.EnablePathDiagnostics
	defer func() {
		diag.TotalNanos = time.Since(totalStart).Nanoseconds()
		s.mu.Unlock()
	}()

	loadStart := time.Now()
	if s.root == nil && len(s.rootHash) > 0 {
		s.root, err = s.loadNode(s.rootHash)
		diag.RootLoadNanos = time.Since(loadStart).Nanoseconds()
		if err != nil {
			return diag, err
		}
	} else {
		diag.RootLoadNanos = time.Since(loadStart).Nanoseconds()
	}

	if s.root == nil {
		return diag, nil
	}

	// 涓嶈兘鍙湅 root epoch 鏉ヨ烦杩囧壀鏋濄€傚啓鍏ヤ細鍒锋柊鎻掑叆璺緞涓婄殑绁栧厛鑺傜偣锛?	// 浣嗘湭瑙︾鐨勫瓙鏍戜粛鍙兘淇濈暀鏃?epoch锛屾墍浠ュ繀椤荤户缁悜涓嬫鏌ャ€?
	// 1. 鑾峰彇褰撳墠鍒嗙墖鐨勭墿鐞嗗墠缂€ (Absolute Prefix)
	prefix, prefixBits := s.getShardPrefix()

	// 2. 閫掑綊鍓灊骞舵敹闆?MaxPathBits 鑼冨洿鍐呯殑缁濆鍏ㄨ矾寰勯」
	walkStart := time.Now()
	newRoot, items, promotedStubs, err := s.pruneAndArchive(s.root, prefix, prefixBits, global)
	diag.WalkNanos = time.Since(walkStart).Nanoseconds()
	if err != nil {
		return diag, err
	}

	finishStart := time.Now()
	s.root, err = s.finishRootArchivePool(newRoot, items, prefix, prefixBits)
	if err != nil {
		diag.FinishNanos = time.Since(finishStart).Nanoseconds()
		return diag, err
	}
	if len(promotedStubs) > 0 {
		s.root, err = s.attachPromotedStubsToRoot(s.root, promotedStubs)
		if err != nil {
			diag.FinishNanos = time.Since(finishStart).Nanoseconds()
			return diag, err
		}
	}
	diag.FinishNanos = time.Since(finishStart).Nanoseconds()
	return diag, nil
}

func (s *Shard) archiveItemsFromPromotedStubs(stubs []*ArchiveBucketNode) ([]ArchivedKV, error) {
	if len(stubs) == 0 {
		return nil, nil
	}
	items := make([]ArchivedKV, 0)
	for _, bucket := range stubs {
		if bucket == nil {
			continue
		}
		bucketItems, err := s.collectLeavesRecursive(bucket, nil, 0)
		if err != nil {
			return nil, err
		}
		items = append(items, bucketItems...)
		if oldHash := s.ensureBucketHash(bucket); len(oldHash) > 0 {
			if s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) {
				s.staleSet[string(oldHash)] = struct{}{}
			}
		}
		s.markPersistedNodeStale(bucket)
	}
	return items, nil
}

func (s *Shard) finishRootArchivePool(root Node, items []ArchivedKV, prefix []byte, prefixBits int) (Node, error) {
	if len(items) == 0 {
		return s.normalizeRootArchivePlacement(root, prefix, prefixBits)
	}

	if root != nil {
		var absorbed int
		var err error
		items, absorbed, err = s.absorbArchiveItemsIntoExistingBuckets(root, items, prefix, prefixBits)
		if err != nil {
			return root, err
		}
		recordPrunePathAbsorbedIfEnabled(s.config, absorbed)
		if len(items) == 0 {
			return s.normalizeRootArchivePlacement(root, prefix, prefixBits)
		}
	}

	s.sortArchiveItemsByKey(items)
	recordPruneRootPoolIfEnabled(s.config, len(items), s.estimatedArchiveBucketCount(len(items)))

	limit := s.config.ResolveArchiveBucketSize()
	if limit <= 0 || len(items) <= limit {
		bucket := s.buildArchiveBucket(items, prefix, prefixBits).(*ArchiveBucketNode)
		return s.attachRootArchiveBucket(root, bucket, prefix, prefixBits)
	}

	if root == nil {
		container := s.newStubContainer(nil).(*InternalNode)
		if err := s.placeRootArchiveItems(container, items, prefix, prefixBits); err != nil {
			return root, err
		}
		return s.normalizeRootArchivePlacement(container, prefix, prefixBits)
	}
	if in, ok := root.(*InternalNode); ok {
		nodePath, nodeBits := s.internalNodeArchivePath(in, prefix, prefixBits)
		oldStubs := in.StubList
		in.StubList = nil
		if err := s.placeRootArchiveItems(in, items, nodePath, nodeBits); err != nil {
			in.StubList = oldStubs
			return root, err
		}
		if len(oldStubs) > 0 {
			in.StubList = append(in.StubList, oldStubs...)
		}
		return s.normalizeRootArchivePlacement(in, prefix, prefixBits)
	}
	if existing, ok := root.(*ArchiveBucketNode); ok {
		existingItems, err := s.collectLeavesRecursive(existing, nil, 0)
		if err != nil {
			return root, err
		}
		items = append(existingItems, items...)
		s.sortArchiveItemsByKey(items)
		if oldHash := s.ensureBucketHash(existing); len(oldHash) > 0 {
			if s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) {
				s.staleSet[string(oldHash)] = struct{}{}
			}
		}
		s.markPersistedNodeStale(existing)
		container := s.newStubContainer(nil).(*InternalNode)
		if err := s.placeRootArchiveItems(container, items, prefix, prefixBits); err != nil {
			return root, err
		}
		return s.normalizeRootArchivePlacement(container, prefix, prefixBits)
	}
	rebuilt, _, err := s.sinkArchiveItemsIntoChild(root, items, prefix, prefixBits)
	if err != nil {
		return root, err
	}
	lifted, err := s.liftSparseArchiveBucketsToRoot(rebuilt)
	if err != nil {
		return root, err
	}
	return s.normalizeRootArchivePlacement(lifted, prefix, prefixBits)
}

func (s *Shard) normalizeRootArchivePlacement(root Node, prefix []byte, prefixBits int) (Node, error) {
	if in, ok := root.(*InternalNode); ok {
		return s.compactRootArchiveStubs(in, prefix, prefixBits)
	}
	bucket, ok := root.(*ArchiveBucketNode)
	if !ok || bucket == nil {
		return root, nil
	}
	threshold := s.archiveRootSideMountThreshold()
	if threshold > 0 && bucket.Count < uint64(threshold) {
		return root, nil
	}
	container := s.newStubContainer(nil).(*InternalNode)
	if oldHash := s.ensureBucketHash(bucket); len(oldHash) > 0 {
		if s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) {
			s.staleSet[string(oldHash)] = struct{}{}
		}
	}
	s.markPersistedNodeStale(bucket)
	if _, err := s.attachStubsAtPath(container, []*ArchiveBucketNode{bucket}, prefix, prefixBits); err != nil {
		return root, err
	}
	if len(container.StubList) == 0 && container.Left == nil && container.Right == nil {
		return root, nil
	}
	return s.compactRootArchiveStubs(container, prefix, prefixBits)
}

func (s *Shard) compactRootArchiveStubs(root *InternalNode, prefix []byte, prefixBits int) (Node, error) {
	if root == nil || s.config == nil || !s.config.CompactArchiveStubs {
		return root, nil
	}
	nodePath, nodeBits := s.internalNodeArchivePath(root, prefix, prefixBits)
	if !s.shouldRepackRootArchiveStubs(root.StubList, nodePath, nodeBits) {
		return root, nil
	}

	changed, handled, err := s.tryCompactRootArchiveStubsBlind(root, nodePath, nodeBits)
	if err != nil {
		return root, err
	}
	if handled {
		if changed {
			s.markPersistedNodeStale(root)
			root.SetDirty(true)
			s.refreshInternalEpochMask(root)
		}
		return root, nil
	}

	items, err := s.archiveItemsFromPromotedStubs(root.StubList)
	if err != nil {
		return root, err
	}
	if len(items) == 0 {
		root.StubList = nil
		s.markPersistedNodeStale(root)
		root.SetDirty(true)
		s.refreshInternalEpochMask(root)
		return root, nil
	}

	s.sortArchiveItemsByKey(items)
	root.StubList = nil
	s.markPersistedNodeStale(root)
	if root.PathBits > 0 && !archiveItemsMatchPath(items, nodePath, nodeBits) {
		bit, ok := s.detachLeadingPathBit(root)
		if ok {
			container := s.newStubContainer(nil).(*InternalNode)
			if bit == 0 {
				container.Left = root
				container.LeftEpoch = root.Epoch()
			} else {
				container.Right = root
				container.RightEpoch = root.Epoch()
			}
			s.refreshInternalEpochMask(container)
			if err := s.placeRootArchiveItems(container, items, prefix, prefixBits); err != nil {
				return root, err
			}
			container.SetDirty(true)
			return s.normalizeRootArchivePlacement(container, prefix, prefixBits)
		}
	}
	if err := s.placeRootArchiveItems(root, items, nodePath, nodeBits); err != nil {
		return root, err
	}
	root.SetDirty(true)
	s.refreshInternalEpochMask(root)
	return root, nil
}

func archiveItemsMatchPath(items []ArchivedKV, path []byte, pathBits int) bool {
	for _, item := range items {
		if item.SuffixBits < pathBits || !hasBitPrefix(item.Suffix, item.SuffixBits, path, pathBits) {
			return false
		}
	}
	return true
}

func (s *Shard) internalNodeArchivePath(n *InternalNode, prefix []byte, prefixBits int) ([]byte, int) {
	if n == nil || n.PathBits == 0 {
		return prefix, prefixBits
	}
	return s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
}

func (s *Shard) shouldRepackRootArchiveStubs(stubs []*ArchiveBucketNode, nodePath []byte, nodeBits int) bool {
	if len(stubs) == 0 || s.config == nil || !s.config.CompactArchiveStubs {
		return false
	}
	limit := s.config.ResolveArchiveBucketSize()
	if limit <= 0 {
		return false
	}
	nonnull := 0
	var only *ArchiveBucketNode
	for _, bucket := range stubs {
		if bucket == nil {
			return true
		}
		nonnull++
		only = bucket
		if bucket.Count > uint64(limit) {
			return true
		}
	}
	if nonnull == 0 {
		return len(stubs) > 0
	}
	return nonnull > 1 || only.Count > uint64(limit)
}

func (s *Shard) tryCompactRootArchiveStubsBlind(root *InternalNode, nodePath []byte, nodeBits int) (bool, bool, error) {
	if root == nil || len(root.StubList) == 0 {
		return false, true, nil
	}
	limit := s.config.ResolveArchiveBucketSize()
	if limit <= 0 {
		limit = int(^uint(0) >> 1)
	}

	merged, ok := s.mergeRootArchiveStubsBlind(root.StubList, nodePath, nodeBits)
	if !ok {
		return false, false, nil
	}
	root.StubList = merged
	if len(root.StubList) <= 1 {
		if len(root.StubList) == 1 && root.StubList[0] != nil && root.StubList[0].Count > uint64(limit) {
			return true, false, nil
		}
		return true, true, nil
	}

	remainderIdx := s.chooseRootStubRemainder(root.StubList, nodePath, nodeBits)
	if remainderIdx < 0 {
		return false, false, nil
	}
	overflow := make([]*ArchiveBucketNode, 0, len(root.StubList)-1)
	for i, bucket := range root.StubList {
		if i == remainderIdx {
			continue
		}
		if !s.canSinkStubAtPath(bucket, nodePath, nodeBits) {
			return false, false, nil
		}
		overflow = append(overflow, bucket)
	}

	remainder := root.StubList[remainderIdx]
	root.StubList = []*ArchiveBucketNode{remainder}
	var groups [2][]*ArchiveBucketNode
	for _, bucket := range overflow {
		bit := s.getBitFromBytes(bucket.Path, nodeBits)
		groups[bit] = append(groups[bit], bucket)
	}
	for bit := 0; bit < 2; bit++ {
		if len(groups[bit]) == 0 {
			continue
		}
		childPath, childBits := s.appendBit(nodePath, nodeBits, byte(bit))
		var child Node
		var childHash []byte
		if bit == 0 {
			child, childHash = root.Left, root.LeftHash
		} else {
			child, childHash = root.Right, root.RightHash
		}
		if child == nil && len(childHash) > 0 {
			loaded, err := s.loadChildNode(root, byte(bit), childHash)
			if err != nil {
				return true, true, err
			}
			child = loaded
		}
		newChild, childChanged, err := s.sinkArchiveBucketsIntoChild(child, groups[bit], childPath, childBits)
		if err != nil {
			return true, true, err
		}
		if !childChanged {
			continue
		}
		if bit == 0 {
			root.Left, root.LeftHash = newChild, nil
			if newChild != nil {
				root.LeftEpoch = newChild.Epoch()
			} else {
				root.LeftEpoch = 0
			}
		} else {
			root.Right, root.RightHash = newChild, nil
			if newChild != nil {
				root.RightEpoch = newChild.Epoch()
			} else {
				root.RightEpoch = 0
			}
		}
	}
	return true, true, nil
}

func (s *Shard) mergeRootArchiveStubsBlind(stubs []*ArchiveBucketNode, nodePath []byte, nodeBits int) ([]*ArchiveBucketNode, bool) {
	if len(stubs) == 0 {
		return nil, true
	}
	limit := s.config.ResolveArchiveBucketSize()
	if limit <= 0 {
		limit = int(^uint(0) >> 1)
	}
	var out []*ArchiveBucketNode
	for _, stub := range stubs {
		if stub == nil || stub.Count == 0 {
			continue
		}
		s.detachArchiveStub(stub)
		current := stub
		for {
			idx, path, bits := s.findMergeCandidate(out, current, limit)
			if idx < 0 {
				out = append(out, current)
				break
			}
			if bits < nodeBits && hasBitPrefix(current.Path, current.PathBits, nodePath, nodeBits) && hasBitPrefix(out[idx].Path, out[idx].PathBits, nodePath, nodeBits) {
				path = common.CopyBytes(nodePath)
				bits = nodeBits
			}
			merged, ok := s.mergeArchiveBuckets(out[idx], current, path, bits)
			if !ok {
				return nil, false
			}
			out = append(out[:idx], out[idx+1:]...)
			current = merged
		}
	}
	return out, true
}

func (s *Shard) chooseRootStubRemainder(stubs []*ArchiveBucketNode, nodePath []byte, nodeBits int) int {
	if len(stubs) == 0 {
		return -1
	}
	unsinkable := -1
	for i, bucket := range stubs {
		if bucket == nil {
			continue
		}
		if !s.canSinkStubAtPath(bucket, nodePath, nodeBits) {
			if unsinkable >= 0 {
				return -1
			}
			unsinkable = i
		}
	}
	if unsinkable >= 0 {
		return unsinkable
	}
	best := -1
	var bestCount uint64
	for i, bucket := range stubs {
		if bucket == nil {
			continue
		}
		if best < 0 || bucket.Count < bestCount {
			best = i
			bestCount = bucket.Count
		}
	}
	return best
}

type rootArchiveItemGroup struct {
	bit   byte
	items []ArchivedKV
}

func (s *Shard) placeRootArchiveItems(root *InternalNode, items []ArchivedKV, nodePath []byte, nodeBits int) error {
	if root == nil || len(items) == 0 {
		return nil
	}
	items = s.deduplicateArchiveItems(items, nil, 0)
	if len(items) == 0 {
		return nil
	}
	limit := s.config.ResolveArchiveBucketSize()
	if limit <= 0 {
		limit = len(items)
	}
	s.sortArchiveItemsByKey(items)

	remainder, err := s.sinkRootArchiveOverflow(root, items, nodePath, nodeBits, limit)
	if err != nil {
		return err
	}
	root.StubList = nil
	if len(remainder) > 0 {
		commonBits := s.commonArchiveItemPrefixBits(remainder, nodeBits)
		commonPath := s.prefixBits(remainder[0].Suffix, commonBits, nil)
		root.StubList = []*ArchiveBucketNode{s.buildArchiveBucket(remainder, commonPath, commonBits).(*ArchiveBucketNode)}
	}
	s.markPersistedNodeStale(root)
	root.SetDirty(true)
	s.refreshInternalEpochMask(root)
	return nil
}

func (s *Shard) sinkRootArchiveOverflow(root *InternalNode, items []ArchivedKV, nodePath []byte, nodeBits int, limit int) ([]ArchivedKV, error) {
	if len(items) <= limit || nodeBits >= MaxPathBits {
		return items, nil
	}

	threshold := s.archiveSideMountSinkThreshold()
	if threshold <= 0 || threshold > limit {
		threshold = limit
	}

	var groups [2][]ArchivedKV
	var fallback []ArchivedKV
	for _, item := range items {
		if item.SuffixBits <= nodeBits || !hasBitPrefix(item.Suffix, item.SuffixBits, nodePath, nodeBits) {
			fallback = append(fallback, item)
			continue
		}
		bit := s.getBitFromBytes(item.Suffix, nodeBits)
		groups[bit] = append(groups[bit], item)
	}

	var held []rootArchiveItemGroup
	remainder := fallback
	for bit := 0; bit < 2; bit++ {
		group := groups[bit]
		if len(group) == 0 {
			continue
		}
		if len(group) >= threshold || len(group) > limit {
			if err := s.sinkRootArchiveItemGroup(root, byte(bit), group, nodePath, nodeBits); err != nil {
				return nil, err
			}
			continue
		}
		held = append(held, rootArchiveItemGroup{bit: byte(bit), items: group})
	}

	heldTotal := 0
	for _, group := range held {
		heldTotal += len(group.items)
	}
	sort.SliceStable(held, func(i, j int) bool {
		return len(held[i].items) > len(held[j].items)
	})
	for i := range held {
		if len(remainder)+heldTotal <= limit {
			break
		}
		if err := s.sinkRootArchiveItemGroup(root, held[i].bit, held[i].items, nodePath, nodeBits); err != nil {
			return nil, err
		}
		heldTotal -= len(held[i].items)
		held[i].items = nil
	}
	for _, group := range held {
		if len(group.items) > 0 {
			remainder = append(remainder, group.items...)
		}
	}
	if len(remainder) > limit {
		return s.forceRootArchiveRemainderUnderLimit(root, remainder, nodePath, nodeBits, limit)
	}
	return remainder, nil
}

func (s *Shard) forceRootArchiveRemainderUnderLimit(root *InternalNode, items []ArchivedKV, nodePath []byte, nodeBits int, limit int) ([]ArchivedKV, error) {
	if len(items) <= limit || nodeBits >= MaxPathBits {
		return items, nil
	}
	var groups []rootArchiveItemGroup
	var fallback []ArchivedKV
	for _, item := range items {
		if item.SuffixBits <= nodeBits || !hasBitPrefix(item.Suffix, item.SuffixBits, nodePath, nodeBits) {
			fallback = append(fallback, item)
			continue
		}
		bit := s.getBitFromBytes(item.Suffix, nodeBits)
		found := false
		for i := range groups {
			if groups[i].bit == bit {
				groups[i].items = append(groups[i].items, item)
				found = true
				break
			}
		}
		if !found {
			groups = append(groups, rootArchiveItemGroup{bit: bit, items: []ArchivedKV{item}})
		}
	}
	sort.SliceStable(groups, func(i, j int) bool {
		return len(groups[i].items) < len(groups[j].items)
	})
	remainder := fallback
	for _, group := range groups {
		if len(group.items) == 0 {
			continue
		}
		if len(group.items) > limit || len(remainder)+len(group.items) > limit {
			if err := s.sinkRootArchiveItemGroup(root, group.bit, group.items, nodePath, nodeBits); err != nil {
				return nil, err
			}
			continue
		}
		remainder = append(remainder, group.items...)
	}
	return remainder, nil
}

func (s *Shard) sinkRootArchiveItemGroup(root *InternalNode, bit byte, items []ArchivedKV, nodePath []byte, nodeBits int) error {
	if root == nil || len(items) == 0 {
		return nil
	}
	childPath, childBits := s.appendBit(nodePath, nodeBits, bit)
	var child Node
	var childHash []byte
	if bit == 0 {
		child, childHash = root.Left, root.LeftHash
	} else {
		child, childHash = root.Right, root.RightHash
	}
	if child == nil && len(childHash) > 0 {
		loaded, err := s.loadChildNode(root, bit, childHash)
		if err != nil {
			return err
		}
		child = loaded
	}
	newChild, changed, err := s.sinkArchiveItemsIntoChild(child, items, childPath, childBits)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if bit == 0 {
		root.Left, root.LeftHash = newChild, nil
		if newChild != nil {
			root.LeftEpoch = newChild.Epoch()
		} else {
			root.LeftEpoch = 0
		}
	} else {
		root.Right, root.RightHash = newChild, nil
		if newChild != nil {
			root.RightEpoch = newChild.Epoch()
		} else {
			root.RightEpoch = 0
		}
	}
	s.markPersistedNodeStale(root)
	root.SetDirty(true)
	s.refreshInternalEpochMask(root)
	return nil
}

func (s *Shard) liftSparseArchiveBucketsToRoot(root Node) (Node, error) {
	newRoot, promotedStubs, _, err := s.liftSparseArchiveEdgeBuckets(root)
	if err != nil {
		return root, err
	}
	if len(promotedStubs) == 0 {
		return newRoot, nil
	}
	return s.attachPromotedStubsToRoot(newRoot, promotedStubs)
}

func (s *Shard) attachRootArchiveBucket(root Node, bucket *ArchiveBucketNode, prefix []byte, prefixBits int) (Node, error) {
	if bucket == nil {
		return root, nil
	}
	if root == nil {
		if threshold := s.archiveRootSideMountThreshold(); threshold <= 0 || bucket.Count < uint64(threshold) {
			return bucket, nil
		}
		container := s.newStubContainer(nil).(*InternalNode)
		if _, err := s.attachStubsAtPath(container, []*ArchiveBucketNode{bucket}, prefix, prefixBits); err != nil {
			return root, err
		}
		if len(container.StubList) == 0 && container.Left == nil && container.Right == nil {
			return bucket, nil
		}
		return container, nil
	}
	if in, ok := root.(*InternalNode); ok {
		nodePath, nodeBits := s.internalNodeArchivePath(in, prefix, prefixBits)
		if _, err := s.attachStubsAtPath(in, []*ArchiveBucketNode{bucket}, nodePath, nodeBits); err != nil {
			return root, err
		}
		return s.normalizeRootArchivePlacement(in, prefix, prefixBits)
	}
	if existing, ok := root.(*ArchiveBucketNode); ok {
		container := s.newStubContainer(nil).(*InternalNode)
		if _, err := s.attachStubsAtPath(container, []*ArchiveBucketNode{existing, bucket}, prefix, prefixBits); err != nil {
			return root, err
		}
		if len(container.StubList) == 0 && container.Left == nil && container.Right == nil {
			return root, nil
		}
		return s.normalizeRootArchivePlacement(container, prefix, prefixBits)
	}
	if threshold := s.archiveRootSideMountThreshold(); threshold > 0 && bucket.Count >= uint64(threshold) {
		container := s.newStubContainer([]*ArchiveBucketNode{bucket}).(*InternalNode)
		bit, ok := s.detachLeadingPathBit(root)
		if !ok {
			container.Left = root
		} else if bit == 0 {
			container.Left = root
		} else {
			container.Right = root
		}
		s.refreshInternalEpochMask(container)
		if s.shouldSinkSideMountedBucket(bucket) {
			if _, err := s.sinkSpecificMatureStub(container, prefix, prefixBits, bucket); err != nil {
				return root, err
			}
		}
		return container, nil
	}
	return s.attachPromotedStubsToRoot(root, []*ArchiveBucketNode{bucket})
}

func (s *Shard) estimatedArchiveBucketCount(items int) int {
	if items <= 0 {
		return 0
	}
	limit := s.config.ResolveArchiveBucketSize()
	if limit <= 0 {
		return 1
	}
	return (items + limit - 1) / limit
}

func (s *Shard) sortArchiveItemsByKey(items []ArchivedKV) {
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		limit := a.SuffixBits
		if b.SuffixBits < limit {
			limit = b.SuffixBits
		}
		for bit := 0; bit < limit; bit++ {
			abit := s.getBitFromBytes(a.Suffix, bit)
			bbit := s.getBitFromBytes(b.Suffix, bit)
			if abit != bbit {
				return abit < bbit
			}
		}
		if a.SuffixBits != b.SuffixBits {
			return a.SuffixBits < b.SuffixBits
		}
		return bytes.Compare(a.Value, b.Value) < 0
	})
}

// absorbArchiveItemsIntoExistingBuckets tries to append cold items into archive
// buckets already reachable from node. nodePath is the absolute prefix before
// node.Path; the helper adds node.Path itself when it sees an InternalNode.
func (s *Shard) absorbArchiveItemsIntoExistingBuckets(node Node, items []ArchivedKV, nodePath []byte, nodeBits int) ([]ArchivedKV, int, error) {
	if node == nil || len(items) == 0 {
		return items, 0, nil
	}

	switch n := node.(type) {
	case *ArchiveBucketNode:
		remaining, absorbed := s.absorbArchiveItemsIntoBucket(n, items, nodePath, nodeBits)
		return remaining, absorbed, nil

	case *InternalNode:
		currentP, currentB := nodePath, nodeBits
		if n.PathBits > 0 {
			currentP, currentB = s.prependPath(n.Path, n.PathBits, nodePath, nodeBits)
		}

		remaining := items
		totalAbsorbed := 0
		parentChanged := false

		// Prefer side-mounted buckets on the current path as cheap absorption points.
		for _, bucket := range n.StubList {
			var absorbed int
			remaining, absorbed = s.absorbArchiveItemsIntoBucket(bucket, remaining, currentP, currentB)
			if absorbed > 0 {
				totalAbsorbed += absorbed
				parentChanged = true
			}
		}

		var fallback []ArchivedKV
		var groups [2][]ArchivedKV
		for _, item := range remaining {
			if item.SuffixBits <= currentB || !hasBitPrefix(item.Suffix, item.SuffixBits, currentP, currentB) {
				fallback = append(fallback, item)
				continue
			}
			bit := s.getBitFromBytes(item.Suffix, currentB)
			groups[bit] = append(groups[bit], item)
		}

		for bit := 0; bit < 2; bit++ {
			if len(groups[bit]) == 0 {
				continue
			}
			var child Node
			var childHash []byte
			if bit == 0 {
				child, childHash = n.Left, n.LeftHash
			} else {
				child, childHash = n.Right, n.RightHash
			}
			if child == nil && len(childHash) > 0 {
				loaded, err := s.loadChildNode(n, byte(bit), childHash)
				if err != nil {
					return fallback, totalAbsorbed, err
				}
				child = loaded
			}
			if child == nil {
				fallback = append(fallback, groups[bit]...)
				continue
			}

			childPath, childBits := s.appendBit(currentP, currentB, byte(bit))
			childRemaining, absorbed, err := s.absorbArchiveItemsIntoExistingBuckets(child, groups[bit], childPath, childBits)
			if err != nil {
				return fallback, totalAbsorbed, err
			}
			if absorbed > 0 {
				totalAbsorbed += absorbed
				parentChanged = true
				if bit == 0 {
					n.Left, n.LeftHash = child, nil
					n.LeftEpoch = child.Epoch()
				} else {
					n.Right, n.RightHash = child, nil
					n.RightEpoch = child.Epoch()
				}
			}
			fallback = append(fallback, childRemaining...)
		}

		if parentChanged {
			s.markPersistedNodeStale(n)
			n.SetDirty(true)
			s.refreshInternalEpochMask(n)
		}
		return fallback, totalAbsorbed, nil

	default:
		return items, 0, nil
	}
}

func (s *Shard) absorbArchiveItemsIntoBucket(bucket *ArchiveBucketNode, items []ArchivedKV, minPath []byte, minBits int) ([]ArchivedKV, int) {
	if bucket == nil || len(items) == 0 {
		return items, 0
	}
	limit := s.config.ResolveArchiveBucketSize()
	if limit > 0 && bucket.Count >= uint64(limit) {
		return items, 0
	}

	capacity := len(items)
	if limit > 0 {
		left := limit - int(bucket.Count)
		if left <= 0 {
			return items, 0
		}
		if left < capacity {
			capacity = left
		}
	}

	if !hasBitPrefix(bucket.Path, bucket.PathBits, minPath, minBits) {
		return items, 0
	}

	selectedAbs := make([]ArchivedKV, 0, capacity)
	remaining := make([]ArchivedKV, 0, len(items))
	allInsideBucketPath := true
	for _, item := range items {
		if len(selectedAbs) >= capacity || !hasBitPrefix(item.Suffix, item.SuffixBits, minPath, minBits) {
			remaining = append(remaining, item)
			continue
		}
		if _, commonBits := s.commonArchivePath(item.Suffix, item.SuffixBits, bucket.Path, bucket.PathBits); commonBits < minBits {
			remaining = append(remaining, item)
			continue
		}
		if !hasBitPrefix(item.Suffix, item.SuffixBits, bucket.Path, bucket.PathBits) {
			allInsideBucketPath = false
		}
		selectedAbs = append(selectedAbs, item)
	}
	if len(selectedAbs) == 0 {
		return items, 0
	}

	if allInsideBucketPath {
		add := make([]ArchivedKV, len(selectedAbs))
		for i := range selectedAbs {
			suffix, suffixBits := s.stripPrefix(selectedAbs[i].Suffix, selectedAbs[i].SuffixBits, 0, bucket.Path, bucket.PathBits)
			add[i] = ArchivedKV{
				Suffix:     suffix,
				SuffixBits: suffixBits,
				Value:      selectedAbs[i].Value,
			}
		}
		if !s.blindAppendToBucket(bucket, add) {
			return items, 0
		}
		return remaining, len(add)
	}

	if err := s.mergeAdjacentArchiveItemsIntoBucket(bucket, selectedAbs, minBits); err != nil {
		return items, 0
	}
	return remaining, len(selectedAbs)
}

func (s *Shard) mergeAdjacentArchiveItemsIntoBucket(bucket *ArchiveBucketNode, newItems []ArchivedKV, minBits int) error {
	oldItems, err := s.collectLeavesRecursive(bucket, nil, 0)
	if err != nil {
		return err
	}
	allItems := append(oldItems, newItems...)
	commonBits := s.commonArchiveItemPrefixBits(allItems, minBits)
	commonPath := s.prefixBits(allItems[0].Suffix, commonBits, nil)

	s.markPersistedNodeStale(bucket)
	if oldHash := s.ensureBucketHash(bucket); len(oldHash) > 0 {
		if s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) {
			s.staleSet[string(oldHash)] = struct{}{}
		}
	}

	localItems := make([]ArchivedKV, len(allItems))
	for i := range allItems {
		suffix, suffixBits := s.stripPrefix(allItems[i].Suffix, allItems[i].SuffixBits, 0, commonPath, commonBits)
		localItems[i] = ArchivedKV{
			Suffix:     suffix,
			SuffixBits: suffixBits,
			Value:      allItems[i].Value,
		}
	}
	bucket.Path = common.CopyBytes(commonPath)
	bucket.PathBits = commonBits
	bucket.SetOriginalHash(nil)
	bucket.SetStoragePath(nil, 0)
	s.recomputeBucket(bucket, localItems)
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

func (s *Shard) attachPromotedStubsToRoot(root Node, stubs []*ArchiveBucketNode) (Node, error) {
	if len(stubs) == 0 {
		return root, nil
	}
	prefix, prefixBits := s.getShardPrefix()
	if root == nil {
		container := s.newStubContainer(nil).(*InternalNode)
		if _, err := s.attachStubsAtPath(container, stubs, prefix, prefixBits); err != nil {
			return root, err
		}
		return s.normalizeRootArchivePlacement(container, prefix, prefixBits)
	}
	if in, ok := root.(*InternalNode); ok {
		s.markPersistedNodeStale(in)
		nodePath, nodeBits := s.internalNodeArchivePath(in, prefix, prefixBits)
		if _, err := s.attachStubsAtPath(in, stubs, nodePath, nodeBits); err != nil {
			return root, err
		}
		in.SetDirty(true)
		return s.normalizeRootArchivePlacement(in, prefix, prefixBits)
	}

	container := s.newStubContainer(nil).(*InternalNode)
	if _, err := s.attachStubsAtPath(container, stubs, prefix, prefixBits); err != nil {
		return root, err
	}
	if bucket, ok := root.(*ArchiveBucketNode); ok {
		if _, err := s.attachStubsAtPath(container, []*ArchiveBucketNode{bucket}, prefix, prefixBits); err != nil {
			return root, err
		}
		return s.normalizeRootArchivePlacement(container, prefix, prefixBits)
	}
	bit, ok := s.detachLeadingPathBit(root)
	if !ok {
		// Root is the only place without a parent to receive promoted buckets.
		// Keep a minimal container rather than dropping either side.
		container.Left = root
		s.refreshInternalEpochMask(container)
		return s.normalizeRootArchivePlacement(container, prefix, prefixBits)
	}
	if bit == 0 {
		container.Left = root
	} else {
		container.Right = root
	}
	s.refreshInternalEpochMask(container)
	return s.normalizeRootArchivePlacement(container, prefix, prefixBits)
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

// pruneAndArchive rewrites one subtree and returns archive items as absolute paths.
func (s *Shard) pruneAndArchive(node Node, prefix []byte, prefixBits int, global byte) (Node, []ArchivedKV, []*ArchiveBucketNode, error) {
	if node == nil {
		return nil, nil, nil, nil
	}

	switch n := node.(type) {
	case *LeafNode:
		if (n.Epoch() & 1) != global {
			recordPruneCollectedLeafIfEnabled(s.config)
			s.markPersistedNodeStale(n)
			// 缁勮缁濆璺緞
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
		// The node epoch is only a summary; use the subtree mask for the fast path.
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

		// Compute the absolute path at the current internal node.
		currentP, currentB := prefix, prefixBits
		if n.PathBits > 0 {
			currentP, currentB = s.prependPath(n.Path, n.PathBits, prefix, prefixBits)
		}

		var allItems []ArchivedKV
		var allStubs []*ArchiveBucketNode
		parentChanged := false
		var newLeft, newRight Node
		lp, lb := s.appendBit(currentP, currentB, 0)
		rp, rb := s.appendBit(currentP, currentB, 1)
		leftNeedsPrune := s.childMayNeedPrune(n.Left, n.LeftEpoch, len(n.LeftHash) > 0, global)
		recordPruneChildDecisionIfEnabled(s.config, leftNeedsPrune)
		if leftNeedsPrune {

			// Process the left child.
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
				allStubs = append(allStubs, leftStubs...)
				parentChanged = true
			}

		}

		// Process the right child.
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
				allStubs = append(allStubs, rightStubs...)
				parentChanged = true
			}
		}

		hasRemainingChildren := n.Left != nil || n.Right != nil || len(n.LeftHash) > 0 || len(n.RightHash) > 0
		if len(allItems) > 0 && (hasRemainingChildren || len(n.StubList) > 0) {
			remaining, absorbed, err := s.absorbArchiveItemsIntoExistingBuckets(n, allItems, prefix, prefixBits)
			if err != nil {
				return nil, nil, nil, err
			}
			if absorbed > 0 {
				recordPrunePathAbsorbedIfEnabled(s.config, absorbed)
				parentChanged = true
			}
			allItems = remaining
			hasRemainingChildren = n.Left != nil || n.Right != nil || len(n.LeftHash) > 0 || len(n.RightHash) > 0
		}
		if !hasRemainingChildren && len(n.StubList) == 0 {
			if parentChanged {
				s.markPersistedNodeStale(n)
				n.SetDirty(true)
			}
			return nil, allItems, allStubs, nil
		}

		if parentChanged || len(n.StubList) != origStubCount ||
			(newLeft != nil && newLeft.IsDirty()) || (newRight != nil && newRight.IsDirty()) {
			s.markPersistedNodeStale(n)
			n.LeftHash, n.RightHash = nil, nil
			s.refreshInternalEpochMask(n)
			n.SetDirty(true)
		}

		resNode, promoted := s.shrinkPromote(n)
		if len(promoted) > 0 {
			allStubs = append(allStubs, promoted...)
		}
		return resNode, allItems, allStubs, nil

	case *ArchiveBucketNode:
		// 宸插綊妗ｆ《淇濇寔鍘熸牱
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

		// 缁熶竴瀵煎嚭涓虹粷瀵圭墿鐞嗚矾寰勶細n.Path (妗剁粷瀵硅矾寰? + item.Suffix (妗剁浉瀵瑰悗缂€)
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
	if s.config != nil && !s.config.PhysicalDelete {
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
			if s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage()) {
				s.staleSet[string(hash)] = struct{}{}
			}
		}
	}
	return nil
}

func (s *Shard) shouldSideMountArchiveItems(count int) bool {
	threshold := s.archiveRootSideMountThreshold()
	return threshold <= 0 || count < threshold
}

func (s *Shard) archiveBucketThreshold(percent int) int {
	if s.config == nil {
		return 0
	}
	limit := s.config.ResolveArchiveBucketSize()
	if limit <= 0 {
		return 0
	}
	threshold := (limit*percent + 99) / 100
	if threshold < 1 {
		return 1
	}
	if threshold > limit {
		return limit
	}
	return threshold
}

func (s *Shard) archiveRootSideMountThreshold() int {
	return s.archiveBucketThreshold(70)
}

func (s *Shard) archiveSideMountSinkThreshold() int {
	return s.archiveBucketThreshold(95)
}

func (s *Shard) archiveEdgeRiseThreshold() int {
	return s.archiveBucketThreshold(80)
}

func (s *Shard) archiveStubRootReturnThreshold() int {
	return s.archiveBucketThreshold(20)
}

func (s *Shard) shouldSinkSideMountedBucket(bucket *ArchiveBucketNode) bool {
	threshold := s.archiveSideMountSinkThreshold()
	return threshold > 0 && bucket != nil && bucket.Count >= uint64(threshold)
}

func (s *Shard) shouldLiftEdgeBucketToStub(bucket *ArchiveBucketNode) bool {
	threshold := s.archiveEdgeRiseThreshold()
	return threshold > 0 && bucket != nil && bucket.Count > 0 && bucket.Count < uint64(threshold)
}

func (s *Shard) shouldReturnStubBucketToRoot(bucket *ArchiveBucketNode) bool {
	threshold := s.archiveStubRootReturnThreshold()
	return threshold > 0 && bucket != nil && bucket.Count > 0 && bucket.Count < uint64(threshold)
}

func (s *Shard) archiveStubPathPressureLimit() int {
	if s.config == nil {
		return 0
	}
	limit := s.config.ArchiveStubMaxBucketsPath
	if limit < 0 {
		return 0
	}
	if limit == 0 {
		return 64
	}
	return limit
}

func (s *Shard) canSinkStubAtPath(bucket *ArchiveBucketNode, nodePath []byte, nodeBits int) bool {
	if bucket == nil {
		return false
	}
	return bucket.PathBits > nodeBits && hasBitPrefix(bucket.Path, bucket.PathBits, nodePath, nodeBits)
}

func (s *Shard) relieveStubPathPressure(parent *InternalNode, nodePath []byte, nodeBits int) (bool, error) {
	limit := s.archiveStubPathPressureLimit()
	if parent == nil || limit <= 0 || len(parent.StubList) <= limit {
		return false, nil
	}

	excess := len(parent.StubList) - limit
	selected := make([]bool, len(parent.StubList))
	var groups [2][]*ArchiveBucketNode
	selectedCount := 0
	for i := len(parent.StubList) - 1; i >= 0 && selectedCount < excess; i-- {
		bucket := parent.StubList[i]
		if !s.canSinkStubAtPath(bucket, nodePath, nodeBits) {
			continue
		}
		bit := s.getBitFromBytes(bucket.Path, nodeBits)
		groups[bit] = append(groups[bit], bucket)
		selected[i] = true
		selectedCount++
	}
	if selectedCount == 0 {
		return false, nil
	}

	newChildren := [2]Node{}
	childChanged := [2]bool{}
	for bit := 0; bit < 2; bit++ {
		if len(groups[bit]) == 0 {
			continue
		}
		childPath, childBits := s.appendBit(nodePath, nodeBits, byte(bit))
		var child Node
		var childHash []byte
		if bit == 0 {
			child, childHash = parent.Left, parent.LeftHash
		} else {
			child, childHash = parent.Right, parent.RightHash
		}
		if child == nil && len(childHash) > 0 {
			loaded, err := s.loadChildNode(parent, byte(bit), childHash)
			if err != nil {
				return false, err
			}
			child = loaded
		}
		newChild, changed, err := s.sinkArchiveBucketsIntoChild(child, groups[bit], childPath, childBits)
		if err != nil {
			return false, err
		}
		if changed {
			newChildren[bit] = newChild
			childChanged[bit] = true
		}
	}

	kept := parent.StubList[:0]
	for i, bucket := range parent.StubList {
		if !selected[i] {
			kept = append(kept, bucket)
		}
	}
	parent.StubList = kept

	s.markPersistedNodeStale(parent)
	parent.SetDirty(true)
	if childChanged[0] {
		parent.Left, parent.LeftHash = newChildren[0], nil
		if newChildren[0] != nil {
			parent.LeftEpoch = newChildren[0].Epoch()
		} else {
			parent.LeftEpoch = 0
		}
	}
	if childChanged[1] {
		parent.Right, parent.RightHash = newChildren[1], nil
		if newChildren[1] != nil {
			parent.RightEpoch = newChildren[1].Epoch()
		} else {
			parent.RightEpoch = 0
		}
	}
	s.refreshInternalEpochMask(parent)
	return true, nil
}

func (s *Shard) sinkArchiveBucketsIntoChild(child Node, buckets []*ArchiveBucketNode, childPath []byte, childBits int) (Node, bool, error) {
	if len(buckets) == 0 {
		return child, false, nil
	}

	bucketItems := make([]ArchivedKV, 0)
	for _, bucket := range buckets {
		items, err := s.collectLeavesRecursive(bucket, nil, 0)
		if err != nil {
			return child, false, err
		}
		bucketItems = append(bucketItems, items...)
	}
	if len(bucketItems) == 0 {
		return child, false, nil
	}

	rebuilt, changed, err := s.sinkArchiveItemsIntoChild(child, bucketItems, childPath, childBits)
	if err != nil || !changed {
		return child, changed, err
	}
	for _, bucket := range buckets {
		if oldHash := s.ensureBucketHash(bucket); len(oldHash) > 0 {
			if s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) {
				s.staleSet[string(oldHash)] = struct{}{}
			}
		}
	}
	return rebuilt, true, nil
}

func (s *Shard) sinkArchiveItemsIntoChild(child Node, items []ArchivedKV, childPath []byte, childBits int) (Node, bool, error) {
	if len(items) == 0 {
		return child, false, nil
	}

	var rebuilt Node
	if child == nil {
		rebuilt = s.buildArchiveSubtreeFast(items, childPath, childBits)
	} else {
		_, archiveOnly, err := s.archiveOnlyItemCount(child)
		if err != nil {
			return child, false, err
		}
		if archiveOnly {
			childItems, err := s.collectLeavesRecursive(child, childPath, childBits)
			if err != nil {
				return child, false, err
			}
			allItems := append(childItems, items...)
			if err := s.markSubtreeStaleRecursive(child); err != nil {
				return child, false, err
			}
			rebuilt = s.buildArchiveSubtreeFast(allItems, childPath, childBits)
		} else {
			archiveItems, hotLeaves, err := s.collectArchiveItemsAndHotLeaves(child, childPath, childBits)
			if err != nil {
				return child, false, err
			}
			archiveItems = append(archiveItems, items...)
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
		}
	}

	if rebuilt != nil {
		rebuilt.SetDirty(true)
	}
	return rebuilt, true, nil
}

func (s *Shard) attachArchiveItemsToChildEdges(parent *InternalNode, items []ArchivedKV, nodePath []byte, nodeBits int) (bool, error) {
	if parent == nil || len(items) == 0 {
		return false, nil
	}

	var groups [2][]ArchivedKV
	var fallback []ArchivedKV
	for _, item := range items {
		if item.SuffixBits <= nodeBits || !hasBitPrefix(item.Suffix, item.SuffixBits, nodePath, nodeBits) {
			fallback = append(fallback, item)
			continue
		}
		bit := s.getBitFromBytes(item.Suffix, nodeBits)
		groups[bit] = append(groups[bit], item)
	}

	changed := false
	for bit := 0; bit < 2; bit++ {
		if len(groups[bit]) == 0 {
			continue
		}
		childPath, childBits := s.appendBit(nodePath, nodeBits, byte(bit))
		limit := s.config.ResolveArchiveBucketSize()
		edgeThreshold := s.archiveSideMountSinkThreshold()
		if (limit <= 0 || len(groups[bit]) <= limit) && (edgeThreshold <= 0 || len(groups[bit]) < edgeThreshold) {
			bucket := s.buildArchiveBucket(groups[bit], childPath, childBits).(*ArchiveBucketNode)
			if !s.canSinkStubAtPath(bucket, nodePath, nodeBits) || !s.shouldSinkSideMountedBucket(bucket) {
				stubChanged, err := s.attachStubsAtPath(parent, []*ArchiveBucketNode{bucket}, nodePath, nodeBits)
				if err != nil {
					return changed, err
				}
				if stubChanged {
					changed = true
				}
				continue
			}
		}
		var child Node
		var childHash []byte
		if bit == 0 {
			child, childHash = parent.Left, parent.LeftHash
		} else {
			child, childHash = parent.Right, parent.RightHash
		}
		if child == nil && len(childHash) > 0 {
			loaded, err := s.loadChildNode(parent, byte(bit), childHash)
			if err != nil {
				return changed, err
			}
			child = loaded
		}

		newChild, childChanged, err := s.sinkArchiveItemsIntoChild(child, groups[bit], childPath, childBits)
		if err != nil {
			return changed, err
		}
		if !childChanged {
			continue
		}
		var promotedStubs []*ArchiveBucketNode
		newChild, promotedStubs, _, err = s.liftSparseArchiveEdgeBuckets(newChild)
		if err != nil {
			return changed, err
		}
		changed = true
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
		if len(promotedStubs) > 0 {
			// Freshly lifted sparse split buckets should aggregate first; sinking them in
			// the same pass can immediately rebuild the mixed child we just avoided.
			stubChanged, err := s.attachStubsInternal(parent, promotedStubs, nodePath, nodeBits, false)
			if err != nil {
				return changed, err
			}
			if stubChanged {
				changed = true
			}
		}
	}

	if len(fallback) > 0 {
		archNode := s.buildArchiveSubtreeFast(fallback, nodePath, nodeBits)
		switch n := archNode.(type) {
		case *ArchiveBucketNode:
			stubChanged, err := s.attachStubsAtPath(parent, []*ArchiveBucketNode{n}, nodePath, nodeBits)
			if err != nil {
				return changed, err
			}
			if stubChanged {
				changed = true
			}
		case *InternalNode:
			stubs := s.collectArchiveBucketStubs(n, nil)
			stubChanged, err := s.attachStubsAtPath(parent, stubs, nodePath, nodeBits)
			if err != nil {
				return changed, err
			}
			if stubChanged {
				changed = true
			}
		}
	}

	if changed {
		s.markPersistedNodeStale(parent)
		parent.SetDirty(true)
		s.refreshInternalEpochMask(parent)
	}
	return changed, nil
}

func (s *Shard) liftSparseArchiveEdgeBuckets(node Node) (Node, []*ArchiveBucketNode, bool, error) {
	if node == nil {
		return nil, nil, false, nil
	}

	switch n := node.(type) {
	case *ArchiveBucketNode:
		if n.Count == 0 {
			return nil, nil, true, nil
		}
		if s.shouldSinkSideMountedBucket(n) {
			return n, nil, false, nil
		}
		return nil, []*ArchiveBucketNode{n}, true, nil

	case *InternalNode:
		var promoted []*ArchiveBucketNode
		changed := false
		if n.Left != nil {
			newLeft, leftPromoted, leftChanged, err := s.liftSparseArchiveEdgeBuckets(n.Left)
			if err != nil {
				return node, promoted, changed, err
			}
			if leftChanged {
				n.Left, n.LeftHash = newLeft, nil
				if newLeft != nil {
					n.LeftEpoch = newLeft.Epoch()
				} else {
					n.LeftEpoch = 0
				}
				changed = true
			}
			promoted = append(promoted, leftPromoted...)
		}
		if n.Right != nil {
			newRight, rightPromoted, rightChanged, err := s.liftSparseArchiveEdgeBuckets(n.Right)
			if err != nil {
				return node, promoted, changed, err
			}
			if rightChanged {
				n.Right, n.RightHash = newRight, nil
				if newRight != nil {
					n.RightEpoch = newRight.Epoch()
				} else {
					n.RightEpoch = 0
				}
				changed = true
			}
			promoted = append(promoted, rightPromoted...)
		}
		if !changed {
			return n, promoted, false, nil
		}
		s.markPersistedNodeStale(n)
		s.refreshInternalEpochMask(n)
		n.SetDirty(true)
		if n.Left == nil && n.Right == nil && len(n.LeftHash) == 0 && len(n.RightHash) == 0 && len(n.StubList) == 0 {
			return nil, promoted, true, nil
		}
		return n, promoted, true, nil

	default:
		return node, nil, false, nil
	}
}

func (s *Shard) childMayNeedPrune(node Node, epoch byte, hasHash bool, global byte) bool {
	mask, ok := s.childEpochMask(node, epoch, hasHash)
	if !ok {
		return true
	}
	return mask&^leafEpochMask(global) != 0
}

func (s *Shard) collapseSmallArchiveChildToStub(node Node, entryPath []byte, entryBits int) (*ArchiveBucketNode, error) {
	threshold := s.archiveEdgeRiseThreshold()
	if threshold <= 0 || node == nil {
		return nil, nil
	}
	limit := s.config.ResolveArchiveBucketSize()
	if limit > 0 && threshold > limit {
		threshold = limit
	}

	count, archiveOnly, reachedThreshold, err := s.archiveOnlyItemCountUpTo(node, uint64(threshold))
	if err != nil || !archiveOnly {
		return nil, err
	}
	if count == 0 || reachedThreshold {
		return nil, nil
	}
	if limit > 0 && count > uint64(limit) {
		return nil, nil
	}

	items, err := s.collectLeavesRecursive(node, entryPath, entryBits)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 || len(items) >= threshold {
		return nil, nil
	}
	if limit > 0 && len(items) > limit {
		return nil, nil
	}

	if err := s.markSubtreeStaleRecursive(node); err != nil {
		return nil, err
	}
	bucket := s.buildArchiveBucket(items, entryPath, entryBits).(*ArchiveBucketNode)
	bucket.SetDirty(true)
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
	var promotedStubs []*ArchiveBucketNode
	newChild, promotedStubs, _, err = s.liftSparseArchiveEdgeBuckets(newChild)
	if err != nil {
		return false, err
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
	if len(promotedStubs) > 0 {
		if _, err := s.attachStubsInternal(parent, promotedStubs, nodePath, nodeBits, false); err != nil {
			return false, err
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
		if s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) {
			s.staleSet[string(oldHash)] = struct{}{}
		}
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
		if s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) {
			s.staleSet[string(oldHash)] = struct{}{}
		}
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
		if s.pruning && (s.config == nil || (s.config.PhysicalDelete && !s.config.UsePathStorage())) {
			s.staleSet[string(oldHash)] = struct{}{}
		}
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

func (s *Shard) archiveOnlyItemCountUpTo(node Node, limit uint64) (uint64, bool, bool, error) {
	if limit == 0 {
		return 0, true, true, nil
	}
	switch n := node.(type) {
	case nil:
		return 0, true, false, nil
	case *LeafNode:
		return 0, false, false, nil
	case *ArchiveBucketNode:
		if n.Count >= limit {
			return limit, true, true, nil
		}
		return n.Count, true, false, nil
	case *InternalNode:
		var err error
		var total uint64
		if n.Left == nil && len(n.LeftHash) > 0 {
			n.Left, err = s.loadChildNode(n, 0, n.LeftHash)
			if err != nil {
				return 0, false, false, err
			}
		}
		leftCount, leftArchiveOnly, reached, err := s.archiveOnlyItemCountUpTo(n.Left, limit-total)
		if err != nil || !leftArchiveOnly {
			return total, leftArchiveOnly, false, err
		}
		total += leftCount
		if reached || total >= limit {
			return limit, true, true, nil
		}

		if n.Right == nil && len(n.RightHash) > 0 {
			n.Right, err = s.loadChildNode(n, 1, n.RightHash)
			if err != nil {
				return 0, false, false, err
			}
		}
		rightCount, rightArchiveOnly, reached, err := s.archiveOnlyItemCountUpTo(n.Right, limit-total)
		if err != nil || !rightArchiveOnly {
			return total, rightArchiveOnly, false, err
		}
		total += rightCount
		if reached || total >= limit {
			return limit, true, true, nil
		}

		for _, bucket := range n.StubList {
			if bucket == nil {
				continue
			}
			remaining := limit - total
			if bucket.Count >= remaining {
				return limit, true, true, nil
			}
			total += bucket.Count
		}
		return total, true, false, nil
	default:
		return 0, false, false, nil
	}
}
