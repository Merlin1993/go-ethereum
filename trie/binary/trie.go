package binary

import (
	"errors"
)

var (
	ErrNodeNotFound = errors.New("node not found")
)

// Shard 表示一个二叉 Merkle Patricia Trie 的分片（子树）。
type Shard struct {
	root     Node
	db       KVStore
	hasher   Hasher
	pruning  bool
	staleSet map[string]struct{} // 以哈希字节为键的集合（用于修剪）

	// Shard specific state
	id       int
	isPruned bool // "本年度已剪枝 / 未剪枝"

	// Reference to global config (in a real implementation this might be cleaner)
	globalEpochBit func() byte

	// scratch buffer for temporary bit operations
	scratch []byte
}

// NewShard 创建一个新的 Shard（若提供 rootHash 则从 DB 加载根节点）。
func NewShard(id int, db KVStore, hasher Hasher, rootHash []byte, pruning bool, globalEpochBit func() byte) (*Shard, error) {
	s := &Shard{
		id:             id,
		db:             db,
		hasher:         hasher,
		pruning:        pruning,
		staleSet:       make(map[string]struct{}),
		globalEpochBit: globalEpochBit,
		isPruned:       false,
		scratch:        make([]byte, 128), // Initial capacity, will grow if needed
	}

	if len(rootHash) > 0 {
		// 为简化实现，若提供 rootHash，则立即从 DB 加载根节点（不做“仅哈希”占位）。
		node, err := s.loadNode(rootHash)
		if err != nil {
			return nil, err
		}
		s.root = node
	}
	return s, nil
}

// loadNode 根据哈希从 DB 读取并反序列化节点。
func (s *Shard) loadNode(hash []byte) (Node, error) {
	data, err := s.db.Get(hash)
	if err != nil {
		return nil, err
	}
	node, err := DeserializeNode(data)
	if err != nil {
		return nil, err
	}
	node.SetHash(hash)
	node.SetOriginalHash(hash) // 记录节点当前持久化版本，用于之后修剪
	return node, nil
}

// Get 返回指定 key 的值哈希（不存在则返回 ErrNodeNotFound）。
func (s *Shard) Get(key []byte) ([]byte, error) {
	if s.root == nil {
		return nil, ErrNodeNotFound
	}
	// Shards start at depth 16
	return s.get(s.root, key, 16)
}

func (s *Shard) get(node Node, key []byte, depth int) ([]byte, error) {
	if node == nil {
		return nil, ErrNodeNotFound
	}

	switch n := node.(type) {
	case *LeafNode:
		// 检查叶子后缀是否完全匹配 key 的剩余位
		matchLen := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
		if matchLen == n.PathBits && depth+matchLen == len(key)*8 {
			return n.ValueHash, nil
		}
		return nil, ErrNodeNotFound

	case *InternalNode:
		// 先匹配内部节点的路径前缀
		if n.PathBits > 0 {
			matched := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
			if matched != n.PathBits {
				return nil, ErrNodeNotFound
			}
			depth += n.PathBits
		}

		// 读取当前深度位，选择左右子树
		bit := s.getBit(key, depth)
		var next Node
		var nextHash []byte

		if bit == 0 {
			next = n.Left
			nextHash = n.LeftHash
		} else {
			next = n.Right
			nextHash = n.RightHash
		}

		if next == nil && len(nextHash) > 0 {
			// 延迟加载子节点
			loaded, err := s.loadNode(nextHash)
			if err != nil {
				return nil, err
			}
			// 更新父节点指针并缓存
			if bit == 0 {
				n.Left = loaded
			} else {
				n.Right = loaded
			}
			next = loaded
		}

		if next == nil {
			return nil, ErrNodeNotFound
		}

		return s.get(next, key, depth+1)

	default:
		return nil, errors.New("unknown node type")
	}
}

// Put 插入/更新指定 key 的值（以哈希存储）。
func (s *Shard) Put(key []byte, value []byte) error {
	valHash := s.hasher.Hash(value)

	// Shard start depth = 16
	newRoot, err := s.insert(s.root, key, 16, valHash)
	if err != nil {
		return err
	}
	s.root = newRoot
	return nil
}

func (s *Shard) insert(node Node, key []byte, depth int, valueHash []byte) (Node, error) {
	// 空位：创建叶子
	if node == nil {
		// 叶子后缀为 key[depth:] 的位序列
		pathBits := len(key)*8 - depth
		path := s.getSuffix(key, depth, nil)
		leaf := NewLeafNode(path, pathBits, valueHash)
		s.updateEpoch(leaf) // Update epoch for new leaf
		return leaf, nil
	}

	switch n := node.(type) {
	case *LeafNode:
		// 检查冲突（共同前缀位数）
		matchBits := s.commonPrefixLen(n.Path, n.PathBits, key, depth)

		// 完全匹配：更新值哈希
		if matchBits == n.PathBits && depth+matchBits == len(key)*8 {
			n.ValueHash = valueHash
			n.SetDirty(true)
			s.updateEpoch(n) // Update epoch
			// 若叶子已持久化过，记录旧哈希用于修剪
			if s.pruning && len(n.OriginalHash()) > 0 {
				s.staleSet[string(n.OriginalHash())] = struct{}{}
			}
			return n, nil
		}

		// 冲突/分裂（不使用扩展节点）：
		// - 在分裂位创建内部节点，左右挂载两个叶子；
		// - 将共同前缀向上展开为一串内部节点。

		// So:
		// Create a chain of Internal Nodes for the shared prefix.
		// Root (d=0) -> Internal(0) -> Internal(1) -> Split(2).

		// Let's implement this expansion.

		// Common prefix length: matchBits.
		// We need to create `matchBits` Internal nodes?
		// Wait, `node` (Leaf) was covering these bits.
		// Now we replace `node` with a structure.
		// 1. Mark `node` as stale (it's being replaced/modified).
		if s.pruning && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}

		oldLeafSuffix := s.shiftBits(n.Path, n.PathBits, matchBits+1, nil)

		// Optimize allocation using scratch buffer for temporary suffix
		tmpSuffix := s.getSuffix(key, depth, s.scratch)
		newLeafSuffix := s.shiftBits(tmpSuffix, len(key)*8-depth, matchBits+1, nil)

		oldLeaf := NewLeafNode(oldLeafSuffix, n.PathBits-(matchBits+1), n.ValueHash)
		oldLeaf.SetEpoch(n.Epoch())

		newLeaf := NewLeafNode(newLeafSuffix, (len(key)*8-depth)-(matchBits+1), valueHash)
		s.updateEpoch(newLeaf)

		prefixPath := s.prefixBits(n.Path, matchBits, nil)
		splitBit := s.getBitFromBytes(n.Path, matchBits)
		splitNode := NewInternalNode(nil, nil)
		splitNode.Path = prefixPath
		splitNode.PathBits = matchBits
		if splitBit == 0 {
			splitNode.Left = oldLeaf
			splitNode.Right = newLeaf
		} else {
			splitNode.Left = newLeaf
			splitNode.Right = oldLeaf
		}
		s.updateEpoch(splitNode)

		return splitNode, nil

	case *InternalNode:
		// 子树被修改：标记此内部节点为 dirty，并记录旧哈希以便修剪
		if s.pruning && !n.dirty && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}
		n.SetDirty(true)

		// 先匹配内部节点路径前缀
		if n.PathBits > 0 {
			matched := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
			if matched < n.PathBits {
				// 需要在当前内部节点上进行分裂
				if matched < 0 {
					matched = 0
				}

				// 新父节点路径为共同前缀 [0, matched)
				prefixPath := s.prefixBits(n.Path, matched, nil)
				parent := NewInternalNode(nil, nil)
				parent.Path = prefixPath
				parent.PathBits = matched

				// 原内部节点作为一侧子树，路径调整为剩余后缀（跳过分裂位）
				oldBit := s.getBitFromBytes(n.Path, matched)
				remainingBits := n.PathBits - (matched + 1)
				if remainingBits > 0 {
					newPath := s.shiftBits(n.Path, n.PathBits, matched+1, nil)
					n.Path = newPath
					n.PathBits = remainingBits
				} else {
					n.Path = nil
					n.PathBits = 0
				}

				if oldBit == 0 {
					parent.Left = n
				} else {
					parent.Right = n
				}

				// 新 key 一侧：从分裂位之后创建叶子
				newBit := s.getBit(key, depth+matched)
				newSuffixBits := len(key)*8 - (depth + matched + 1)
				newSuffix := s.getSuffix(key, depth+matched+1, nil)
				newLeaf := NewLeafNode(newSuffix, newSuffixBits, valueHash)
				s.updateEpoch(newLeaf)

				if newBit == 0 {
					parent.Left = newLeaf
				} else {
					parent.Right = newLeaf
				}

				s.updateEpoch(parent)
				return parent, nil
			}
			depth += n.PathBits
		}

		bit := s.getBit(key, depth)

		if bit == 0 {
			// 确保左子已加载（有哈希则延迟加载）
			if n.Left == nil && len(n.LeftHash) > 0 {
				loaded, err := s.loadNode(n.LeftHash)
				if err != nil {
					return nil, err
				}
				n.Left = loaded
			}
			newLeft, err := s.insert(n.Left, key, depth+1, valueHash)
			if err != nil {
				return nil, err
			}
			n.Left = newLeft
		} else {
			// 确保右子已加载
			if n.Right == nil && len(n.RightHash) > 0 {
				loaded, err := s.loadNode(n.RightHash)
				if err != nil {
					return nil, err
				}
				n.Right = loaded
			}
			newRight, err := s.insert(n.Right, key, depth+1, valueHash)
			if err != nil {
				return nil, err
			}
			n.Right = newRight
		}

		s.updateEpoch(n) // Update epoch after child update
		return n, nil

	default:
		return nil, errors.New("unknown node type")
	}
}

// Delete 删除指定 key，如果 key 不存在则为无操作。
func (s *Shard) Delete(key []byte) error {
	newRoot, _, err := s.delete(s.root, key, 16)
	if err != nil {
		if err == ErrNodeNotFound {
			return nil // Deleting non-existent key is no-op
		}
		return err
	}
	s.root = newRoot
	return nil
}

// 删除实现返回：新子树节点、是否发生删除、错误信息
func (s *Shard) delete(node Node, key []byte, depth int) (Node, bool, error) {
	if node == nil {
		return nil, false, ErrNodeNotFound
	}

	switch n := node.(type) {
	case *LeafNode:
		// 检查叶子是否与 key 完全匹配
		matchLen := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
		if matchLen == n.PathBits && depth+matchLen == len(key)*8 {
			// 命中：删除叶子并记录旧哈希以便修剪
			if s.pruning && len(n.OriginalHash()) > 0 {
				s.staleSet[string(n.OriginalHash())] = struct{}{}
			}
			return nil, true, nil
		}
		return node, false, ErrNodeNotFound

	case *InternalNode:
		// 先匹配内部节点的路径前缀
		if n.PathBits > 0 {
			matched := s.commonPrefixLen(n.Path, n.PathBits, key, depth)
			if matched != n.PathBits {
				return node, false, ErrNodeNotFound
			}
			depth += n.PathBits
		}

		bit := s.getBit(key, depth)
		var child Node
		var childHash []byte

		if bit == 0 {
			child = n.Left
			childHash = n.LeftHash
		} else {
			child = n.Right
			childHash = n.RightHash
		}

		// 需要时加载子节点
		if child == nil && len(childHash) > 0 {
			loaded, err := s.loadNode(childHash)
			if err != nil {
				return node, false, err
			}
			child = loaded
			if bit == 0 {
				n.Left = child
			} else {
				n.Right = child
			}
		}

		newChild, deleted, err := s.delete(child, key, depth+1)
		if err != nil {
			return node, false, err
		}

		if !deleted {
			return node, false, nil
		}

		// 标记当前内部节点为 dirty，并记录旧哈希
		if s.pruning && !n.dirty && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}
		n.SetDirty(true)

		if bit == 0 {
			n.Left = newChild
			n.LeftHash = nil // Clear hash as child might be deleted/modified
		} else {
			n.Right = newChild
			n.RightHash = nil // Clear hash
		}

		// 结构收缩：统计仍存在的子节点（指针或哈希）
		var remaining ChildInfo
		count := 0
		if n.Left != nil || len(n.LeftHash) > 0 {
			count++
			remaining = ChildInfo{node: n.Left, hash: n.LeftHash, bit: 0}
		}
		if n.Right != nil || len(n.RightHash) > 0 {
			count++
			remaining = ChildInfo{node: n.Right, hash: n.RightHash, bit: 1}
		}

		if count == 0 {
			// 当前节点无子：删除
			return nil, true, nil
		}

		if count == 1 {
			// 若内部节点自身带有路径，则不再向下合并，避免丢失前缀信息
			if n.PathBits > 0 {
				s.updateEpoch(n)
				return n, true, nil
			}

			target := remaining.node
			if target == nil {
				target, err = s.loadNode(remaining.hash)
				if err != nil {
					return nil, false, err
				}
			}

			if leaf, ok := target.(*LeafNode); ok {
				newPath := s.prependBit(leaf.Path, leaf.PathBits, remaining.bit, nil)
				leaf.Path = newPath
				leaf.PathBits++
				leaf.SetDirty(true)
				if s.pruning && len(leaf.OriginalHash()) > 0 {
					s.staleSet[string(leaf.OriginalHash())] = struct{}{}
				}

				return leaf, true, nil
			}

			s.updateEpoch(n)
			return n, true, nil
		}

		// Update epoch
		s.updateEpoch(n)
		return n, true, nil

	default:
		return node, false, errors.New("unknown node type")
	}
}

type ChildInfo struct {
	node Node
	hash []byte
	bit  byte
}

// Commit 递归提交改动并执行修剪，返回最新根哈希（空树返回 nil）。
func (s *Shard) Commit() ([]byte, error) {
	var rootHash []byte
	var err error

	batch := s.db.NewBatch()
	defer batch.Reset()

	if s.root != nil {
		// Recursively hash and save
		rootHash, err = s.commit(s.root, batch)
		if err != nil {
			return nil, err
		}
	}

	// Pruning
	if s.pruning {
		for h := range s.staleSet {
			err := batch.Delete([]byte(h))
			if err != nil {
				return nil, err
			}
		}
		s.staleSet = make(map[string]struct{})
	}

	if err := batch.Write(); err != nil {
		return nil, err
	}

	return rootHash, nil
}

func (s *Shard) commit(node Node, batch Batcher) ([]byte, error) {
	if !node.IsDirty() {
		return node.Hash(), nil
	}

	switch n := node.(type) {
	case *InternalNode:
		// 在持久化前，根据子节点刷新 epoch（避免在每次插入/删除时重复计算）
		s.updateEpoch(n)

		// Commit children
		if n.Left != nil {
			h, err := s.commit(n.Left, batch)
			if err != nil {
				return nil, err
			}
			n.LeftHash = h
		}
		if n.Right != nil {
			h, err := s.commit(n.Right, batch)
			if err != nil {
				return nil, err
			}
			n.RightHash = h
		}

		// 序列化内部节点（包含 epoch 与子节点哈希）
		data, err := n.Serialize()
		if err != nil {
			return nil, err
		}

		// Hash
		h := s.hasher.Hash(data)
		n.SetHash(h)
		n.SetDirty(false)
		n.SetOriginalHash(h) // It's now persisted (logically)

		// Write to DB
		err = batch.Put(h, data)
		if err != nil {
			return nil, err
		}
		return h, nil

	case *LeafNode:
		// 在持久化前根据当前分片剪枝状态与全局年度位刷新叶子 epoch
		s.updateEpoch(n)

		// 序列化叶子节点
		data, err := n.Serialize()
		if err != nil {
			return nil, err
		}

		// Hash
		h := s.hasher.Hash(data)
		n.SetHash(h)
		n.SetDirty(false)
		n.SetOriginalHash(h)

		// Write to DB
		err = batch.Put(h, data)
		if err != nil {
			return nil, err
		}
		return h, nil

	default:
		return nil, errors.New("unknown node type")
	}
}

// 工具方法

func (s *Shard) getBit(key []byte, depth int) byte {
	byteIdx := depth / 8
	bitIdx := 7 - (depth % 8)
	if byteIdx >= len(key) {
		return 0
	}
	if (key[byteIdx] & (1 << bitIdx)) != 0 {
		return 1
	}
	return 0
}

func (s *Shard) getBitFromBytes(data []byte, bitIndex int) byte {
	byteIdx := bitIndex / 8
	bitIdx := 7 - (bitIndex % 8)
	if byteIdx >= len(data) {
		return 0
	}
	if (data[byteIdx] & (1 << bitIdx)) != 0 {
		return 1
	}
	return 0
}

func (s *Shard) commonPrefixLen(a []byte, aBits int, b []byte, bStartBit int) int {
	// a 为路径后缀（按位），b 为完整 key（从 bStartBit 位开始对齐比较）
	matched := 0
	for i := 0; i < aBits; i++ {
		bitA := s.getBitFromBytes(a, i)
		bitB := s.getBit(b, bStartBit+i)
		if bitA != bitB {
			break
		}
		matched++
	}
	return matched
}

func (s *Shard) getSuffix(key []byte, depth int, buf []byte) []byte {
	// 提取从 depth 位开始的 key 剩余位，并按高位在前压缩为字节数组返回
	totalBits := len(key)*8 - depth
	if totalBits <= 0 {
		return nil
	}

	// 位移压缩（逐位处理，原型实现即可）
	size := (totalBits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	for i := 0; i < totalBits; i++ {
		if s.getBit(key, depth+i) == 1 {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

func (s *Shard) shiftBits(data []byte, bits int, shift int, buf []byte) []byte {
	// 跳过前 shift 位，返回剩余位的字节压缩表示
	newBits := bits - shift
	if newBits <= 0 {
		return nil
	}
	size := (newBits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	for i := 0; i < newBits; i++ {
		// 原位下标 = shift + i
		if s.getBitFromBytes(data, shift+i) == 1 {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

func (s *Shard) prefixBits(data []byte, bits int, buf []byte) []byte {
	if bits <= 0 {
		return nil
	}
	size := (bits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	for i := 0; i < bits; i++ {
		if s.getBitFromBytes(data, i) == 1 {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

func (s *Shard) prependBit(data []byte, bits int, bit byte, buf []byte) []byte {
	newBits := bits + 1
	size := (newBits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	// 写入首位
	if bit == 1 {
		res[0] |= 0x80
	}

	// 拷贝后续位（整体右移）
	for i := 0; i < bits; i++ {
		if s.getBitFromBytes(data, i) == 1 {
			// 新位下标 = i + 1
			byteIdx := (i + 1) / 8
			bitIdx := 7 - ((i + 1) % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

func (s *Shard) updateEpoch(node Node) {
	if node == nil {
		return
	}

	global := s.globalEpochBit()

	switch n := node.(type) {
	case *LeafNode:
		// 叶子 bit0：按分片剪枝状态与全局年度位设定
		var bit0 byte
		if s.isPruned {
			bit0 = global
		} else {
			bit0 = 1 - global // !global
		}

		// 叶子 bit1 固定为 0；bit2~bit7 预留为 0
		n.epoch = bit0

	case *InternalNode:
		bits := []byte{}
		if n.Left != nil {
			bits = append(bits, n.Left.Epoch()&1)
		}
		if n.Right != nil {
			bits = append(bits, n.Right.Epoch()&1)
		}

		var parentBit0 byte
		if len(bits) > 0 {
			if global == 1 {
				// 全局=1：父 bit0 使用 AND（所有子 bit0 为 1 才为 1）
				allOne := true
				for _, b := range bits {
					if b == 0 {
						allOne = false
						break
					}
				}
				if allOne {
					parentBit0 = 1
				}
			} else {
				// 全局=0：父 bit0 使用 OR（任一子 bit0 为 1 则为 1）
				anyOne := false
				for _, b := range bits {
					if b == 1 {
						anyOne = true
						break
					}
				}
				if anyOne {
					parentBit0 = 1
				}
			}
		}

		// 父 bit1：子 bit0 一致为 1，否则为 0
		var bit1 byte
		if len(bits) <= 1 {
			bit1 = 1
		} else {
			if bits[0] == bits[1] {
				bit1 = 1
			} else {
				bit1 = 0
			}
		}

		n.epoch = (bit1 << 1) | parentBit0
	}
}

// Prune：遍历分片并删除过期节点；返回被删除的数据（包含分片 ID 与值内容）。
// 说明：叶子仅存储 ValueHash，因此插入时将 valHash->value 写入 DB，剪枝时据此取回“数据内容”。

type DeletedItem struct {
	ShardID   int
	KeySuffix []byte
	Value     []byte
}

func (s *Shard) Prune(global byte) ([]DeletedItem, error) {
	// Atomic: Pause inserts (handled by caller/lock).

	deletedItems := []DeletedItem{}

	// Helper to traverse and prune
	var pruneNode func(node Node, path []byte, depth int) (Node, bool, error)
	pruneNode = func(node Node, path []byte, depth int) (Node, bool, error) {
		if node == nil {
			return nil, false, nil
		}

		switch n := node.(type) {
		case *LeafNode:
			// Check Epoch Bit 0
			bit0 := n.Epoch() & 1
			if bit0 == global {
				// Expired
				// Fetch value
				val, _ := s.db.Get(n.ValueHash) // Ignore error

				// Reconstruct partial key?
				// path argument is the path accumulated so far?
				// `n.Path` is the suffix.
				// We need the full key for the record?
				// "节点路径".
				// We can construct the suffix inside the shard.
				// Shard starts at depth 16.

				// Reconstruct path from current traversal + n.Path
				// Note: `path` passed to recursive func is the prefix bits.

				// For now, let's just return what we can.

				deletedItems = append(deletedItems, DeletedItem{
					ShardID: s.id,
					Value:   val,
					// Key? We need to track the key bits during traversal.
				})

				// Mark for deletion from DB
				if s.pruning && len(n.OriginalHash()) > 0 {
					s.staleSet[string(n.OriginalHash())] = struct{}{}
				}

				return nil, true, nil
			}
			return n, false, nil

		case *InternalNode:
			// Optimization: If Parent Epoch Bit 1 (Validation) is 1, and Bit 0 == global
			// Then ALL children are expired. We can prune the whole subtree?
			// Yes, "Result=1 means all children bit 0 are same".
			// If Bit0 == global, then all children are global (Expired).
			// So we can drop this entire subtree.
			if (n.Epoch()>>1)&1 == 1 {
				if (n.Epoch() & 1) == global {
					// Prune whole subtree
					// We need to collect all leaves in this subtree to return them.
					s.collectLeaves(n, &deletedItems)

					// Mark n as stale
					if s.pruning && len(n.OriginalHash()) > 0 {
						s.staleSet[string(n.OriginalHash())] = struct{}{}
					}
					// Also need to mark all children as stale recursively?
					// Ideally yes, but if we delete the root of subtree, the children become garbage.
					// If we rely on ref-counting or GC, it's fine.
					// But `staleSet` is for explicit DB deletion.
					// If we don't add children to staleSet, they remain in DB (leak).
					// So we must traverse and mark all persisted nodes as stale.
					s.markSubtreeStale(n)

					return nil, true, nil
				}
			}

			// Otherwise traverse
			changed := false

			// Left
			if n.Left == nil && len(n.LeftHash) > 0 {
				loaded, err := s.loadNode(n.LeftHash)
				if err != nil {
					return nil, false, err
				}
				n.Left = loaded
			}
			newLeft, leftDeleted, err := pruneNode(n.Left, nil, depth+1) // path tracking omitted for brevity
			if err != nil {
				return nil, false, err
			}
			if leftDeleted || newLeft != n.Left {
				n.Left = newLeft
				n.LeftHash = nil // 清理已变更子节点的持久化哈希，避免后续懒加载读取不存在的条目
				changed = true
			}

			// Right
			if n.Right == nil && len(n.RightHash) > 0 {
				loaded, err := s.loadNode(n.RightHash)
				if err != nil {
					return nil, false, err
				}
				n.Right = loaded
			}
			newRight, rightDeleted, err := pruneNode(n.Right, nil, depth+1)
			if err != nil {
				return nil, false, err
			}
			if rightDeleted || newRight != n.Right {
				n.Right = newRight
				n.RightHash = nil // 清理已变更子节点的持久化哈希
				changed = true
			}

			if changed {
				// Handle merging if needed
				// Count children
				count := 0
				var remaining Node
				var remainingBit byte

				if n.Left != nil {
					count++
					remaining = n.Left
					remainingBit = 0
				}
				if n.Right != nil {
					count++
					remaining = n.Right
					remainingBit = 1
				}

				if count == 0 {
					if s.pruning && len(n.OriginalHash()) > 0 {
						s.staleSet[string(n.OriginalHash())] = struct{}{}
					}
					return nil, true, nil
				}

				if count == 1 {
					// 若内部节点自身带有路径，则不再向下合并，避免丢失前缀信息
					if n.PathBits > 0 {
						n.SetDirty(true)
						s.updateEpoch(n)
						return n, false, nil
					}

					if leaf, ok := remaining.(*LeafNode); ok {
						newPath := s.prependBit(leaf.Path, leaf.PathBits, remainingBit, nil)
						leaf.Path = newPath
						leaf.PathBits++
						leaf.SetDirty(true)
						if s.pruning && len(leaf.OriginalHash()) > 0 {
							s.staleSet[string(leaf.OriginalHash())] = struct{}{}
						}

						if s.pruning && len(n.OriginalHash()) > 0 {
							s.staleSet[string(n.OriginalHash())] = struct{}{}
						}
						return leaf, true, nil
					}
				}

				n.SetDirty(true)
				s.updateEpoch(n)
			}

			return n, false, nil

		default:
			return nil, false, nil
		}
	}

	newRoot, _, err := pruneNode(s.root, nil, 16)
	if err != nil {
		return nil, err
	}
	s.root = newRoot
	s.isPruned = true

	return deletedItems, nil
}

func (s *Shard) collectLeaves(node Node, items *[]DeletedItem) {
	if node == nil {
		return
	}

	// Load if needed
	if internal, ok := node.(*InternalNode); ok {
		if internal.Left == nil && len(internal.LeftHash) > 0 {
			internal.Left, _ = s.loadNode(internal.LeftHash)
		}
		if internal.Right == nil && len(internal.RightHash) > 0 {
			internal.Right, _ = s.loadNode(internal.RightHash)
		}
		s.collectLeaves(internal.Left, items)
		s.collectLeaves(internal.Right, items)
	} else if leaf, ok := node.(*LeafNode); ok {
		val, _ := s.db.Get(leaf.ValueHash)
		*items = append(*items, DeletedItem{
			ShardID: s.id,
			Value:   val,
		})
	}
}

func (s *Shard) markSubtreeStale(node Node) {
	if node == nil {
		return
	}

	if n, ok := node.(*InternalNode); ok {
		if s.pruning && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}
		// Need to load children to find their hashes?
		// If child is not loaded, we have the hash in LeftHash/RightHash.
		// We can just add those hashes to staleSet!
		// No need to load.
		if len(n.LeftHash) > 0 {
			s.staleSet[string(n.LeftHash)] = struct{}{}
			// If it was loaded, recurse?
			// If it's not loaded, we assume the whole subtree under it is on disk.
			// We need to traverse the disk nodes to find all hashes?
			// Yes, deleting a subtree requires deleting all its descendants from DB.
			// This is expensive. We need to walk the tree.
			if n.Left == nil {
				loaded, _ := s.loadNode(n.LeftHash)
				s.markSubtreeStale(loaded)
			} else {
				s.markSubtreeStale(n.Left)
			}
		} else if n.Left != nil {
			s.markSubtreeStale(n.Left)
		}

		if len(n.RightHash) > 0 {
			s.staleSet[string(n.RightHash)] = struct{}{}
			if n.Right == nil {
				loaded, _ := s.loadNode(n.RightHash)
				s.markSubtreeStale(loaded)
			} else {
				s.markSubtreeStale(n.Right)
			}
		} else if n.Right != nil {
			s.markSubtreeStale(n.Right)
		}

	} else if n, ok := node.(*LeafNode); ok {
		if s.pruning && len(n.OriginalHash()) > 0 {
			s.staleSet[string(n.OriginalHash())] = struct{}{}
		}
	}
}

// Trie Manager

type Trie struct {
	shards           [65536]*Shard
	db               KVStore
	hasher           Hasher
	pruning          bool
	globalEpochBit   byte
	insertCount      int
	pruneShardIdx    int
	prunedShardCount int
}

func NewTrie(db KVStore, hasher Hasher, pruning bool) *Trie {
	t := &Trie{
		db:      db,
		hasher:  hasher,
		pruning: pruning,
	}

	// Init shards
	// Note: In real app, we would load root hashes.
	// Here we assume new or empty.
	for i := 0; i < 65536; i++ {
		// Use a closure to capture 't'
		s, _ := NewShard(i, db, hasher, nil, pruning, func() byte { return t.globalEpochBit })
		t.shards[i] = s
	}
	return t
}

func (t *Trie) SetGlobalEpoch(bit byte) {
	if bit != t.globalEpochBit {
		t.globalEpochBit = bit
		// Reset prune status of all shards
		for _, s := range t.shards {
			s.isPruned = false
		}
	}
}

func (t *Trie) Put(key []byte, value []byte) error {
	// Store value in DB for retrieval during prune (Requirement)
	valHash := t.hasher.Hash(value)
	t.db.Put(valHash, value)

	shardID := t.getShardID(key)
	err := t.shards[shardID].Put(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (t *Trie) Get(key []byte) ([]byte, error) {
	shardID := t.getShardID(key)
	return t.shards[shardID].Get(key)
}

func (t *Trie) BatchDelete(key []byte) error {
	shardID := t.getShardID(key)
	return t.shards[shardID].Delete(key)
}

func (t *Trie) Commit() ([]byte, error) {
	// Commit all shards
	// Return a hash of all shard roots?
	// Or just commit to DB.
	// Requirement: "每次提交完成后...".
	// We need to commit all modified shards.

	// We can hash the list of shard roots to get a "Global Root".
	// For now, just commit each shard.

	rootHashes := make([]byte, 0, 65536*32)
	for _, s := range t.shards {
		h, err := s.Commit()
		if err != nil {
			return nil, err
		}
		// If empty, append nil hash?
		if len(h) == 0 {
			// Append 32 bytes of zero?
			rootHashes = append(rootHashes, make([]byte, 32)...)
		} else {
			rootHashes = append(rootHashes, h...)
		}
	}

	// Hash the root hashes
	globalRoot := t.hasher.Hash(rootHashes)
	return globalRoot, nil
}

func (t *Trie) PruneNextShard() ([]DeletedItem, error) {
	shard := t.shards[t.pruneShardIdx]
	t.pruneShardIdx = (t.pruneShardIdx + 1) % 65536

	wasPruned := shard.isPruned
	deleted, err := shard.Prune(t.globalEpochBit)
	if err != nil {
		return nil, err
	}
	// 若本分片在当前年度首次完成剪枝，计数 +1
	if !wasPruned && shard.isPruned {
		t.prunedShardCount++
		// 当 65536 个分片均已剪枝，自动切换年度，并重置所有分片的剪枝标记与计数
		if t.prunedShardCount >= 65536 {
			if t.globalEpochBit == 0 {
				t.globalEpochBit = 1
			} else {
				t.globalEpochBit = 0
			}
			t.prunedShardCount = 0
			for _, s := range t.shards {
				s.isPruned = false
			}
		}
	}
	return deleted, nil
}

func (t *Trie) getShardID(key []byte) int {
	if len(key) < 2 {
		return 0 // Should not happen if keys are 32 bytes
	}
	return int(key[0])<<8 | int(key[1])
}
