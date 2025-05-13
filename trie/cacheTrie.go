// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// CacheTrieDatabase is an interface that combines necessary functionality from triedb.Database.
type CacheTrieDatabase interface {
	database.NodeDatabase
}

// CacheTrie is a Merkle Patricia Trie variant with enhanced caching capabilities.
// It extends the regular Trie functionality with additional caching features.
//
// CacheTrie is not safe for concurrent use.
type CacheTrie struct {
	root  cacheNode
	owner common.Hash

	// Flag whether the commit operation is already performed. If so the
	// trie is not usable(latest states is invisible).
	committed bool

	// Keep track of the number leaves which have been inserted since the last
	// hashing operation. This number will not directly map to the number of
	// actually unhashed nodes.
	unhashed int

	// uncommitted is the number of updates since last commit.
	uncommitted int

	// reader is the handler trie can retrieve nodes from.
	reader *trieReader

	// tracer is the tool to track the trie changes.
	tracer *tracer

	// blockNum is the current block height for read/write operations
	blockNum uint64

	// startNum is the starting block number for cached data
	startNum uint64

	// multiple represents how many blocks each bit in the window caches
	multiple uint64

	// maxSize is the maximum number of key-value pairs that can be cached in the tree
	maxSize int
}

// newCacheFlag returns the cache flag value for a newly created node.
func (t *CacheTrie) newCacheFlag() cacheNodeFlag {
	return cacheNodeFlag{dirty: true}
}

// Copy returns a copy of CacheTrie.
func (t *CacheTrie) Copy() *CacheTrie {
	return &CacheTrie{
		root:        t.root,
		owner:       t.owner,
		committed:   t.committed,
		reader:      t.reader,
		tracer:      t.tracer.copy(),
		uncommitted: t.uncommitted,
		unhashed:    t.unhashed,
		blockNum:    t.blockNum,
		startNum:    t.startNum,
		multiple:    t.multiple,
		maxSize:     t.maxSize,
	}
}

// NewCacheTrie creates the cache trie instance with provided trie id and the read-only
// database. The state specified by trie id must be available, otherwise
// an error will be returned. The trie root specified by trie id can be
// zero hash or the sha3 hash of an empty string, then trie is initially
// empty, otherwise, the root node must be present in database or returns
// a MissingNodeError if not.
func NewCacheTrie(id *ID, db database.NodeDatabase) (*CacheTrie, error) {
	reader, err := newTrieReader(id.StateRoot, id.Owner, db)
	if err != nil {
		return nil, err
	}
	trie := &CacheTrie{
		owner:    id.Owner,
		reader:   reader,
		tracer:   newTracer(),
		blockNum: 0,
		startNum: 0,
		multiple: 1, // 默认每位存储1个区块的缓存
		maxSize:  0, // 默认无限制
	}
	if id.Root != (common.Hash{}) && id.Root != types.EmptyRootHash {
		rootnode, err := trie.resolveAndTrack(id.Root[:], nil)
		if err != nil {
			return nil, err
		}
		trie.root = rootnode
	}
	return trie, nil
}

// SetBlockNum updates the current block height for the CacheTrie.
func (t *CacheTrie) SetBlockNum(blockNum uint64) {
	t.blockNum = blockNum
}

// SetCacheParams sets the cache parameters for the CacheTrie.
func (t *CacheTrie) SetCacheParams(startNum, multiple uint64) {
	t.startNum = startNum
	t.multiple = multiple
}

// NewEmptyCacheTrie is a shortcut to create empty cache trie. It's mostly used in tests.
func NewEmptyCacheTrie(db database.NodeDatabase) *CacheTrie {
	tr, _ := NewCacheTrie(TrieID(types.EmptyRootHash), db)
	return tr
}

// resolveAndTrack resolves the node from the provided hash and also tracks it.
func (t *CacheTrie) resolveAndTrack(hash []byte, path []byte) (cacheNode, error) {
	retrieved, err := t.reader.node(path, common.BytesToHash(hash))
	if err != nil {
		return nil, err
	}
	// Track the retrieve request no matter if it's successful or not.
	// It's used for evaluating the database accesss cost.
	if t.tracer != nil {
		t.tracer.onRead(path, retrieved)
	}
	return cacheDecodeNode(hash, retrieved)
}

// Get returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
//
// If the requested node is not present in trie, no error will be returned.
// If the trie is corrupted, a MissingNodeError is returned.
func (t *CacheTrie) Get(key []byte) ([]byte, error) {
	// Short circuit if the trie is already committed and not usable.
	if t.committed {
		return nil, ErrCommitted
	}

	// 获取当前区块对应的位置
	bitPos := t.getCacheBitPosition()

	// 使用十六进制格式的key
	hexKey := keybytesToHex(key)

	// 首先尝试直接访问节点以更新window
	node, err := t.getNodeForPath(key)
	if err == nil && node != nil {
		// 如果找到了节点，首先更新它的window
		switch n := node.(type) {
		case *cacheShortNode:
			n.window |= (1 << bitPos)
		case *cacheFullNode:
			n.window |= (1 << bitPos)
		}
	}

	// 然后正常执行Get操作
	value, newRoot, _, err := t.get(t.root, hexKey, 0, bitPos)
	if err == nil {
		// 不管是否解析了新节点，都更新根节点
		// 这样确保window更改被保存
		t.root = newRoot
	}

	return value, err
}

func (t *CacheTrie) get(origNode cacheNode, key []byte, pos int, bitPos int) (value []byte, newnode cacheNode, didResolve bool, err error) {
	switch n := (origNode).(type) {
	case nil:
		return nil, nil, false, nil
	case cacheValueNode:
		// 检查是否是墓碑标记（空值）
		if len(n) == 0 {
			return nil, n, false, nil // 墓碑标记返回nil
		}
		return n, n, false, nil
	case *cacheShortNode:
		// 对短节点总是创建一个副本，以便更新window
		n = n.copy()
		// 设置当前区块对应的位
		n.window |= (1 << bitPos)

		// Check if we're at the end of the key
		if pos >= len(key) {
			if valueNode, isValue := n.Val.(cacheValueNode); isValue {
				// 检查是否是墓碑标记（空值）
				if len(valueNode) == 0 {
					return nil, n, true, nil // 墓碑标记返回nil
				}
				return valueNode, n, true, nil
			}
			return nil, n, true, nil
		}

		if !bytes.HasPrefix(key[pos:], n.Key) {
			// key not found in trie
			return nil, n, true, nil
		}

		value, newChild, _, err := t.get(n.Val, key, pos+len(n.Key), bitPos)

		// 不管子节点是否改变，都返回更新了window的父节点
		n.Val = newChild
		return value, n, true, err

	case *cacheFullNode:
		// 对全节点总是创建一个副本，以便更新window
		n = n.copy()
		// 设置当前区块对应的位
		n.window |= (1 << bitPos)

		// Check if we're at the end of the key
		if pos >= len(key) {
			if n.Children[16] != nil {
				if valueNode, isValue := n.Children[16].(cacheValueNode); isValue {
					// 检查是否是墓碑标记（空值）
					if len(valueNode) == 0 {
						return nil, n, true, nil // 墓碑标记返回nil
					}
					return valueNode, n, true, nil
				}
			}
			return nil, n, true, nil
		}

		// 确保我们不会越界
		if pos >= len(key) || int(key[pos]) >= len(n.Children) {
			return nil, n, true, nil
		}

		value, newChild, _, err := t.get(n.Children[key[pos]], key, pos+1, bitPos)

		// 不管子节点是否改变，都返回更新了window的父节点
		n.Children[key[pos]] = newChild
		return value, n, true, err

	case cacheHashNode:
		child, err := t.resolveAndTrack(n, key[:pos])
		if err != nil {
			return nil, n, true, err
		}
		value, newnode, _, err := t.get(child, key, pos, bitPos)
		return value, newnode, true, err
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

// Update associates key with value in the trie.
func (t *CacheTrie) Update(key, value []byte) error {
	// Short circuit if the trie is already committed and not usable.
	if t.committed {
		return ErrCommitted
	}
	t.uncommitted++

	if len(value) == 0 {
		return t.Delete(key)
	}

	hexKey := keybytesToHex(key)

	// 设置当前位置
	bitPos := t.getCacheBitPosition()

	// 将value转换为cacheValueNode类型
	valueNode := cacheValueNode(value)

	newroot, err := t.insert(t.root, hexKey, valueNode, bitPos)
	if err != nil {
		return err
	}
	t.root = newroot
	t.unhashed++
	return nil
}

func (t *CacheTrie) insert(n cacheNode, key []byte, value cacheNode, bitPos int) (cacheNode, error) {
	if len(key) == 0 {
		// Value nodes can only be inserted at the end of a path
		if _, ok := value.(cacheValueNode); ok {
			// 当插入叶子节点时，设置window字段
			// 如果是cacheValueNode类型，我们需要在这里实现window的设置
			// 但由于cacheValueNode是[]byte类型，无法直接添加window字段
			// 在这里我们需要在上层处理
			return value, nil
		}
		panic("Invalid attempt to insert non-value node at the end of a path")
	}

	switch n := n.(type) {
	case *cacheShortNode:
		// Determine the common prefix length with the new key
		var prefixLength int
		for ; prefixLength < len(n.Key) && prefixLength < len(key); prefixLength++ {
			if n.Key[prefixLength] != key[prefixLength] {
				break
			}
		}

		// If the keys fully match, just update the value
		if prefixLength == len(n.Key) && prefixLength == len(key) {
			shortNode := &cacheShortNode{
				Key:   n.Key,
				Val:   value,
				flags: t.newCacheFlag(),
			}

			// 更新window属性，将其重置为0并在当前位置设置为1
			shortNode.window = 0
			if _, ok := value.(cacheValueNode); ok {
				// 这是叶子节点，设置当前区块对应的位
				shortNode.window = 1 << bitPos
			}

			return shortNode, nil
		}

		// If they share a prefix, split at the fork point
		if prefixLength > 0 {
			// Branch node for the fork point
			branch := &cacheFullNode{flags: t.newCacheFlag()}

			// Re-use existing child, if any
			if prefixLength < len(n.Key) {
				// Create a short node with the remaining part of the key
				child := &cacheShortNode{
					Key:   n.Key[prefixLength+1:],
					Val:   n.Val,
					flags: t.newCacheFlag(),
					// 保留原始窗口信息
					window: n.window,
				}
				branch.Children[n.Key[prefixLength]] = child
			} else {
				// This is the case where the full key of the shortNode matches
				// the prefix of the new key. In this case, we use the shortNode's
				// value as is in the branch node.
				branch.Children[key[prefixLength]] = n.Val
			}

			// Insert the new value into the branch node
			var err error
			if prefixLength < len(key) {
				branch.Children[key[prefixLength]], err = t.insert(nil, key[prefixLength+1:], value, bitPos)
				if err != nil {
					return nil, err
				}
			} else {
				// The key from the update is shorter, so we just add a value node
				branch.Children[16] = value

				// 如果这是叶子节点，更新窗口位
				if _, ok := value.(cacheValueNode); ok {
					branch.window = 1 << bitPos
				}
			}

			if prefixLength == 1 { // Only a single character prefix
				return branch, nil
			}

			// Return the branch node wrapped in a short node
			shortNode := &cacheShortNode{
				Key:   key[:prefixLength],
				Val:   branch,
				flags: t.newCacheFlag(),
			}

			// 短节点的窗口应继承其子节点的窗口
			if branch.window != 0 {
				shortNode.window = branch.window
			}

			return shortNode, nil
		}

		// If no common prefix, convert to a branch node
		branch := &cacheFullNode{flags: t.newCacheFlag()}

		// Insert the old node
		if len(n.Key) == 0 {
			return nil, fmt.Errorf("shortNode has empty key")
		}

		branch.Children[n.Key[0]] = &cacheShortNode{
			Key:    n.Key[1:],
			Val:    n.Val,
			flags:  t.newCacheFlag(),
			window: n.window, // 保留原始窗口信息
		}

		// Insert the new node
		var err error
		if len(key) == 0 {
			return nil, fmt.Errorf("key has empty content")
		}

		branch.Children[key[0]], err = t.insert(nil, key[1:], value, bitPos)
		if err != nil {
			return nil, err
		}

		// 更新branch的window
		if _, ok := value.(cacheValueNode); ok && len(key) == 1 {
			// 如果新插入的是叶子节点，并且在当前branch层结束
			branch.window = 1 << bitPos
		}

		return branch, nil

	case *cacheFullNode:
		// Update the specific branch
		var err error
		if len(key) == 0 {
			// When key is empty, this is a value node for the full node
			n = n.copy()
			n.Children[16] = value

			// 如果这是叶子节点，更新窗口位
			if _, ok := value.(cacheValueNode); ok {
				n.window = 1 << bitPos
			}

			return n, nil
		}

		n = n.copy()
		n.Children[key[0]], err = t.insert(n.Children[key[0]], key[1:], value, bitPos)
		if err != nil {
			return nil, err
		}

		// 如果子节点被更新，可能需要更新当前节点的window
		// 这个逻辑会在commit阶段完全处理，这里只是初始化设置
		if childNode, ok := n.Children[key[0]].(*cacheShortNode); ok && childNode.window != 0 {
			if n.window == 0 {
				n.window = childNode.window
			}
		} else if childNode, ok := n.Children[key[0]].(*cacheFullNode); ok && childNode.window != 0 {
			if n.window == 0 {
				n.window = childNode.window
			}
		}

		return n, nil

	case cacheHashNode:
		// We've hit a part of the trie that isn't loaded yet
		// This should never happen if prefetching is working correctly
		child, err := t.resolveAndTrack(n, key)
		if err != nil {
			return nil, err
		}
		return t.insert(child, key, value, bitPos)

	case nil:
		// When inserting into an empty trie, just use a short node
		shortNode := &cacheShortNode{
			Key:   key,
			Val:   value,
			flags: t.newCacheFlag(),
		}

		// 如果这是叶子节点，设置当前区块对应的位
		if _, ok := value.(cacheValueNode); ok {
			shortNode.window = 1 << bitPos
		}

		return shortNode, nil

	default:
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// Delete removes any existing value for key from the trie.
func (t *CacheTrie) Delete(key []byte) error {
	// Short circuit if the trie is already committed and not usable.
	if t.committed {
		return ErrCommitted
	}
	t.uncommitted++

	hexKey := keybytesToHex(key)

	// 创建一个特殊的标记值作为"墓碑"
	// 这里使用一个空的cacheValueNode作为墓碑标记
	// 并在后续处理中将其识别为删除标记
	tombstone := cacheValueNode([]byte{})

	// 获取当前block位置
	bitPos := t.getCacheBitPosition()

	// 使用Update方法插入墓碑标记，而不是真正删除
	newroot, err := t.insert(t.root, hexKey, tombstone, bitPos)
	if err != nil {
		return err
	}

	t.root = newroot
	t.unhashed++
	return nil
}

// Hash returns the root hash of the trie.
func (t *CacheTrie) Hash() common.Hash {
	if t.root == nil {
		return types.EmptyRootHash
	}
	hash, cached := t.root.cache()
	if hash != nil && !cached {
		return common.BytesToHash(hash)
	}

	// Trie not yet written to disk, generate hash on the fly
	h := newCacheHasher(false)
	defer returnCacheHasherToPool(h)

	hashed, _ := h.hash(t.root, true)
	if hash, ok := hashed.(cacheHashNode); ok {
		return common.BytesToHash(hash)
	}
	// Small node that wasn't hashed
	return common.Hash{}
}

// Commit collects all dirty nodes in the trie and updates window information.
func (t *CacheTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet) {
	if t.committed {
		return common.Hash{}, nil
	}

	// 首先更新树中所有节点的window
	if t.root != nil {
		t.updateNodeWindow(t.root)
	}

	// 更新size信息，计算缓存的键值对数量
	totalSize := 0
	if t.root != nil {
		totalSize = t.updateNodeSize(t.root)
	}

	// 检查是否超过了最大缓存数量限制
	if t.maxSize > 0 && totalSize > t.maxSize {
		// 超过限制，需要进行清理
		// 清理操作将在下一个需求中实现
		// 这里先记录日志
		fmt.Printf("CacheTrie size (%d) exceeds maxSize (%d), cleanup needed\n", totalSize, t.maxSize)
	}

	// Ensure historical version of the tree is not modified later.
	t.committed = true

	h := newCacheHasher(false)
	defer returnCacheHasherToPool(h)

	var (
		nodes = trienode.NewNodeSet(t.owner)
		hash  common.Hash
	)
	hash, t.root = h.hashCacheRoot(t.root, collectLeaf, nodes)
	return hash, nodes
}

// updateNodeWindow recursively updates the window field of all nodes
// so that for internal nodes, window is the XOR of all child windows
func (t *CacheTrie) updateNodeWindow(n cacheNode) int {
	switch n := n.(type) {
	case *cacheShortNode:
		if n.Val == nil {
			return n.window
		}

		// 递归更新子节点window
		childWindow := t.updateNodeWindow(n.Val)

		// 如果当前节点没有window设置，但子节点有，则更新
		if n.window == 0 && childWindow != 0 {
			n.window = childWindow
		} else if childWindow != 0 {
			// 否则如果子节点有window，则与当前window进行XOR
			n.window ^= childWindow
		}

		return n.window

	case *cacheFullNode:
		var combinedWindow int

		// 处理所有常规子节点
		for i := 0; i < 16; i++ {
			if n.Children[i] != nil {
				childWindow := t.updateNodeWindow(n.Children[i])
				if childWindow != 0 {
					combinedWindow ^= childWindow
				}
			}
		}

		// 处理值节点（如果存在）
		if n.Children[16] != nil {
			childWindow := t.updateNodeWindow(n.Children[16])
			if childWindow != 0 {
				combinedWindow ^= childWindow
			}
		}

		// 更新当前节点的window
		if combinedWindow != 0 {
			n.window = combinedWindow
		}

		return n.window

	case cacheValueNode:
		// 值节点不存储window，返回0
		return 0

	case cacheHashNode:
		// 哈希节点不存储window，返回0
		return 0

	default:
		return 0
	}
}

// NodeIterator returns an iterator that returns nodes of the trie.
func (t *CacheTrie) NodeIterator(startKey []byte) (NodeIterator, error) {
	// Short circuit if the trie is already committed and not usable.
	if t.committed {
		return nil, ErrCommitted
	}
	// TODO: Implement proper NodeIterator for CacheTrie
	// This would require adapting the existing iterator to work with cacheNode
	return nil, nil
}

// Prove constructs a Merkle proof for key.
func (t *CacheTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	// TODO: Implement Prove for CacheTrie
	return nil
}

// Witness returns all accessed nodes in the trie.
func (t *CacheTrie) Witness() map[string]struct{} {
	// Convert map[string][]byte to map[string]struct{}
	result := make(map[string]struct{})
	for path := range t.tracer.accessList {
		result[path] = struct{}{}
	}
	return result
}

// getCacheBitPosition calculates the bit position in the window for the current block.
// Returns the bit position (0-31) based on current blockNum, startNum and multiple.
func (t *CacheTrie) getCacheBitPosition() int {
	if t.blockNum < t.startNum {
		return 0 // 如果当前区块小于起始区块，默认使用第0位
	}

	position := (t.blockNum - t.startNum) / t.multiple
	return int(position % 32) // 限制在32位之内，因为window是一个int
}

// IsCachedAtBlock checks if the node at the given path has cached data for a specific block
func (t *CacheTrie) IsCachedAtBlock(key []byte, blockNum uint64) (bool, error) {
	// 短路检查
	if t.committed {
		return false, ErrCommitted
	}

	// 如果目标区块小于起始区块或者超出了窗口范围（32位*multiple），返回false
	if blockNum < t.startNum || (blockNum-t.startNum)/t.multiple >= 32 {
		return false, nil
	}

	// 计算目标区块在window中的位置
	position := (blockNum - t.startNum) / t.multiple
	bitPos := int(position % 32)
	blockPosition := 1 << bitPos

	// 直接调用Get方法获取对应路径的节点
	// 这会更新节点的window，但我们需要在检查前先保存原始window
	node, err := t.getNodeForPath(key)
	if err != nil {
		return false, err
	}
	if node == nil {
		return false, nil
	}

	// 获取节点的window并检查指定位置是否设置
	window := t.getNodeWindow(node)
	return (window & blockPosition) != 0, nil
}

// 获取节点的window
func (t *CacheTrie) getNodeWindow(n cacheNode) int {
	switch n := n.(type) {
	case *cacheShortNode:
		return n.window
	case *cacheFullNode:
		return n.window
	default:
		return 0
	}
}

// 通过路径获取节点
func (t *CacheTrie) getNodeForPath(key []byte) (cacheNode, error) {
	if t.committed {
		return nil, ErrCommitted
	}

	hexKey := keybytesToHex(key)

	// 从根节点开始遍历
	node := t.root
	pos := 0

	// 逐步匹配路径
	for node != nil && pos < len(hexKey) {
		switch n := node.(type) {
		case *cacheShortNode:
			// 如果路径不匹配短节点的键，则找不到节点
			if len(hexKey)-pos < len(n.Key) || !bytes.HasPrefix(hexKey[pos:], n.Key) {
				return nil, nil
			}
			// 如果已经到达路径末尾，返回当前节点
			if pos+len(n.Key) == len(hexKey) {
				return n, nil
			}
			// 否则继续查找
			pos += len(n.Key)
			node = n.Val
		case *cacheFullNode:
			// 如果已经到达路径末尾，返回值节点（如果存在）
			if pos == len(hexKey) {
				return n.Children[16], nil
			}
			// 否则跟随路径继续查找
			childIndex := hexKey[pos]
			node = n.Children[childIndex]
			pos++
		case cacheHashNode:
			// 解析哈希节点
			child, err := t.resolveAndTrack(n, hexKey[:pos])
			if err != nil {
				return nil, err
			}
			node = child
		case cacheValueNode:
			// 如果找到了值节点，检查我们是否已经完成路径
			if pos == len(hexKey) {
				return n, nil
			}
			// 否则无法继续（值节点是叶子节点）
			return nil, nil
		default:
			return nil, nil
		}
	}

	// 如果找到了完整路径匹配
	if pos == len(hexKey) {
		return node, nil
	}

	// 没有完整路径匹配
	return nil, nil
}

// 辅助方法，根据路径找到对应的节点
func (t *CacheTrie) getNode(n cacheNode, path []byte, pos int) (cacheNode, error) {
	switch n := n.(type) {
	case nil:
		return nil, nil
	case *cacheShortNode:
		if len(path)-pos < len(n.Key) || !bytes.Equal(path[pos:pos+len(n.Key)], n.Key) {
			return nil, nil
		}
		if pos+len(n.Key) == len(path) {
			return n, nil
		}
		return t.getNode(n.Val, path, pos+len(n.Key))
	case *cacheFullNode:
		if pos >= len(path) {
			return n, nil
		}
		return t.getNode(n.Children[path[pos]], path, pos+1)
	case cacheHashNode:
		child, err := t.resolveAndTrack(n, path[:pos])
		if err != nil {
			return nil, err
		}
		return t.getNode(child, path, pos)
	default:
		return n, nil
	}
}

// GetRootHash 返回当前trie的root hash，方便测试使用
func (t *CacheTrie) GetRootHash() common.Hash {
	return t.Hash()
}

// SetMaxSize sets the maximum number of key-value pairs that can be cached in the tree.
// A value of 0 means no limit.
func (t *CacheTrie) SetMaxSize(maxSize int) {
	t.maxSize = maxSize
}

// updateNodeSize recursively calculates the number of key-value pairs in the tree
// and updates the size field of nodes.
func (t *CacheTrie) updateNodeSize(n cacheNode) int {
	switch n := n.(type) {
	case *cacheShortNode:
		if n.Val == nil {
			n.size = 0
			return 0
		}

		// 检查是否是叶子节点
		if valueNode, isValue := n.Val.(cacheValueNode); isValue {
			if len(valueNode) == 0 {
				// 墓碑标记不计入大小
				n.size = 0
				return 0
			}
			// 这是一个有效的叶子节点
			n.size = 1
			return 1
		}

		// 递归计算子节点的size
		childSize := t.updateNodeSize(n.Val)
		n.size = childSize
		return childSize

	case *cacheFullNode:
		var totalSize int

		// 处理所有子节点 (0-15)
		for i := 0; i < 16; i++ {
			if n.Children[i] != nil {
				totalSize += t.updateNodeSize(n.Children[i])
			}
		}

		// 处理值节点 (子节点16)
		if n.Children[16] != nil {
			if valueNode, isValue := n.Children[16].(cacheValueNode); isValue {
				if len(valueNode) == 0 {
					// 墓碑标记不计入大小
				} else {
					// 有效值节点计为1
					totalSize++
				}
			} else {
				// 非值节点，递归计算
				totalSize += t.updateNodeSize(n.Children[16])
			}
		}

		n.size = totalSize
		return totalSize

	case cacheValueNode:
		// 值节点本身代表1个键值对，但墓碑标记除外
		if len(n) == 0 {
			return 0 // 墓碑标记不计数
		}
		return 1

	case cacheHashNode:
		// 哈希节点无法直接计算其内部键值对数量，需要先解析
		resolved, err := t.resolveAndTrack(n, nil)
		if err != nil {
			return 0 // 解析失败时返回0
		}
		return t.updateNodeSize(resolved)

	default:
		return 0
	}
}

// GetSize returns the total number of key-value pairs in the trie.
func (t *CacheTrie) GetSize() int {
	if t.root == nil {
		return 0
	}

	// 创建一个简单的结构来存储结果
	result := struct{ count int }{0}

	// 从根节点开始递归计算非墓碑的值节点数量
	t.countNodesWithoutTombstones(t.root, &result)

	return result.count
}

// countNodesWithoutTombstones 计算trie中的实际键值对数量，不包括墓碑标记
func (t *CacheTrie) countNodesWithoutTombstones(n cacheNode, result *struct{ count int }) {
	if n == nil {
		return
	}

	switch n := n.(type) {
	case *cacheShortNode:
		if valueNode, isValue := n.Val.(cacheValueNode); isValue {
			// 是否是墓碑标记
			if len(valueNode) > 0 {
				result.count++
			}
		} else if n.Val != nil {
			// 递归处理非值类型的子节点
			t.countNodesWithoutTombstones(n.Val, result)
		}

	case *cacheFullNode:
		// 检查节点16（值节点）
		if n.Children[16] != nil {
			if valueNode, isValue := n.Children[16].(cacheValueNode); isValue {
				if len(valueNode) > 0 {
					result.count++
				}
			} else {
				t.countNodesWithoutTombstones(n.Children[16], result)
			}
		}

		// 递归处理所有子节点
		for i := 0; i < 16; i++ {
			if n.Children[i] != nil {
				t.countNodesWithoutTombstones(n.Children[i], result)
			}
		}

	case cacheValueNode:
		// 独立的值节点，检查是否是墓碑标记
		if len(n) > 0 {
			result.count++
		}

	case cacheHashNode:
		// 解析哈希节点
		resolved, err := t.resolveAndTrack(n, nil)
		if err != nil {
			return
		}
		t.countNodesWithoutTombstones(resolved, result)
	}
}
