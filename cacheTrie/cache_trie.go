// Copyright 2023 The go-ethereum Authors
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

package cacheTrie

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// EmptyRoot是一个特殊的根哈希，表示空树
var EmptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

// CacheTrie是一个Merkle Patricia树变种，具有增强的缓存功能。
// 它扩展了常规Trie功能，增加了额外的缓存特性。
//
// CacheTrie不是线程安全的。
type CacheTrie struct {
	root   cacheNode
	nodeDB map[string][]byte // 简化版数据库，存储hash和节点的对应关系

	// 标记commit操作是否已执行
	committed bool

	// 跟踪自上次哈希操作以来插入的叶子数量
	unhashed int

	// uncommitted是自上次commit以来的更新数
	uncommitted int

	// 当前区块高度
	blockNum uint64

	// 起始区块号
	startNum uint64

	// multiple表示window中每个位存储多少个区块
	multiple uint64

	// maxSize是树中可以缓存的键值对的最大数量
	maxSize int
}

// newNodeFlag返回新创建节点的缓存标志值
func (t *CacheTrie) newNodeFlag() nodeFlag {
	return nodeFlag{dirty: true}
}

// Copy返回CacheTrie的副本
func (t *CacheTrie) Copy() *CacheTrie {
	return &CacheTrie{
		root:        t.root,
		nodeDB:      t.nodeDB,
		committed:   t.committed,
		uncommitted: t.uncommitted,
		unhashed:    t.unhashed,
		blockNum:    t.blockNum,
		startNum:    t.startNum,
		multiple:    t.multiple,
		maxSize:     t.maxSize,
	}
}

// NewCacheTrie创建一个新的缓存树实例
func NewCacheTrie() *CacheTrie {
	trie := &CacheTrie{
		nodeDB:   make(map[string][]byte),
		blockNum: 0,
		startNum: 0,
		multiple: 1, // 默认每位存储1个区块的缓存
		maxSize:  0, // 默认无限制
	}
	return trie
}

// NewCacheTrieWithRoot创建一个具有指定根的缓存树实例
func NewCacheTrieWithRoot(root common.Hash) (*CacheTrie, error) {
	trie := &CacheTrie{
		nodeDB:   make(map[string][]byte),
		blockNum: 0,
		startNum: 0,
		multiple: 1, // 默认每位存储1个区块的缓存
		maxSize:  0, // 默认无限制
	}

	if root != (common.Hash{}) && root != EmptyRoot {
		// 在实际情况下，我们会尝试从nodeDB中加载根节点
		// 但在简化实现中，我们直接返回一个空树
	}

	return trie, nil
}

// SetBlockNum更新CacheTrie的当前区块高度
func (t *CacheTrie) SetBlockNum(blockNum uint64) {
	t.blockNum = blockNum
}

// SetCacheParams设置CacheTrie的缓存参数
func (t *CacheTrie) SetCacheParams(startNum, multiple uint64) {
	t.startNum = startNum
	t.multiple = multiple
}

// NewEmptyCacheTrie创建一个空的缓存树，主要用于测试
func NewEmptyCacheTrie() *CacheTrie {
	return NewCacheTrie()
}

// SetMaxSize设置树中可缓存的键值对的最大数量，0表示无限制
func (t *CacheTrie) SetMaxSize(maxSize int) {
	t.maxSize = maxSize
}

// Get返回trie中存储的key对应的值
func (t *CacheTrie) Get(key []byte) ([]byte, error) {
	if t.committed {
		return nil, ErrCommitted
	}

	// 获取当前区块对应的位置
	bitPos := t.getCacheBitPosition()

	// 将key转换为十六进制格式
	hexKey := keybytesToHex(key)

	// 首先尝试直接访问节点以更新window
	node, err := t.getNodeForPath(key)
	if err == nil && node != nil {
		// 如果找到了节点，更新它的window
		switch n := node.(type) {
		case *ShortNode:
			n.window |= (1 << bitPos)
		case *FullNode:
			n.window |= (1 << bitPos)
		}
	}

	// 执行Get操作
	value, newRoot, _, err := t.get(t.root, hexKey, 0, bitPos)
	if err == nil {
		// 不管是否解析了新节点，都更新根节点
		// 这样确保window更改被保存
		t.root = newRoot
	}

	return value, err
}

// get是Get的内部实现，递归查找键值
func (t *CacheTrie) get(origNode cacheNode, key []byte, pos int, bitPos int) (value []byte, newnode cacheNode, didResolve bool, err error) {
	switch n := (origNode).(type) {
	case nil:
		return nil, nil, false, nil
	case ValueNode:
		// 检查是否是墓碑标记（空值）
		if len(n) == 0 {
			return nil, n, false, nil // 墓碑标记返回nil
		}
		return n, n, false, nil
	case *ShortNode:
		// 对短节点总是创建一个副本，以便更新window
		n = n.copy()
		// 设置当前区块对应的位
		n.window |= (1 << bitPos)

		// 检查是否到达key的末尾
		if pos >= len(key) {
			if valueNode, isValue := n.Val.(ValueNode); isValue {
				// 检查是否是墓碑标记（空值）
				if len(valueNode) == 0 {
					return nil, n, true, nil // 墓碑标记返回nil
				}
				return valueNode, n, true, nil
			}
			return nil, n, true, nil
		}

		if !bytes.HasPrefix(key[pos:], n.Key) {
			// key不在trie中
			return nil, n, true, nil
		}

		value, newChild, _, err := t.get(n.Val, key, pos+len(n.Key), bitPos)

		// 不管子节点是否改变，都返回更新了window的父节点
		n.Val = newChild
		return value, n, true, err

	case *FullNode:
		// 对全节点总是创建一个副本，以便更新window
		n = n.copy()
		// 设置当前区块对应的位
		n.window |= (1 << bitPos)

		// 检查是否到达key的末尾
		if pos >= len(key) {
			if n.Children[16] != nil {
				if valueNode, isValue := n.Children[16].(ValueNode); isValue {
					// 检查是否是墓碑标记（空值）
					if len(valueNode) == 0 {
						return nil, n, true, nil // 墓碑标记返回nil
					}
					return valueNode, n, true, nil
				}
			}
			return nil, n, true, nil
		}

		// 确保不会越界
		if pos >= len(key) || int(key[pos]) >= len(n.Children) {
			return nil, n, true, nil
		}

		value, newChild, _, err := t.get(n.Children[key[pos]], key, pos+1, bitPos)

		// 不管子节点是否改变，都返回更新了window的父节点
		n.Children[key[pos]] = newChild
		return value, n, true, err

	case HashNode:
		// 在简化版实现中，我们直接从nodeDB获取节点
		nodehash := common.BytesToHash(n)
		if encoded, ok := t.nodeDB[nodehash.Hex()]; ok {
			child, err := decodeNode(n, encoded)
			if err != nil {
				return nil, n, true, err
			}
			value, newnode, _, err := t.get(child, key, pos, bitPos)
			return value, newnode, true, err
		}
		return nil, n, true, &MissingNodeError{NodeHash: common.BytesToHash(n), Path: key[:pos]}
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

// Update将键值对添加到trie中
func (t *CacheTrie) Update(key, value []byte) error {
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

	// 将value转换为ValueNode类型
	valueNode := ValueNode(value)

	newroot, err := t.insert(t.root, hexKey, valueNode, bitPos)
	if err != nil {
		return err
	}
	t.root = newroot
	t.unhashed++
	return nil
}

// insert是Update的内部实现，递归插入键值
func (t *CacheTrie) insert(n cacheNode, key []byte, value cacheNode, bitPos int) (cacheNode, error) {
	if len(key) == 0 {
		// 值节点只能在路径末尾插入
		if _, ok := value.(ValueNode); ok {
			return value, nil
		}
		panic("Invalid attempt to insert non-value node at the end of a path")
	}

	switch n := n.(type) {
	case *ShortNode:
		// 确定与新key的公共前缀长度
		var prefixLength int
		for ; prefixLength < len(n.Key) && prefixLength < len(key); prefixLength++ {
			if n.Key[prefixLength] != key[prefixLength] {
				break
			}
		}

		// 如果键完全匹配，直接更新值
		if prefixLength == len(n.Key) && prefixLength == len(key) {
			shortNode := &ShortNode{
				Key:   n.Key,
				Val:   value,
				flags: t.newNodeFlag(),
			}

			// 更新window属性，将其重置为0并在当前位置设置为1
			shortNode.window = 0
			if _, ok := value.(ValueNode); ok {
				// 这是叶子节点，设置当前区块对应的位
				shortNode.window = 1 << bitPos
			}

			return shortNode, nil
		}

		// 如果共享前缀，在分叉点分裂
		if prefixLength > 0 {
			// 分叉点的分支节点
			branch := &FullNode{flags: t.newNodeFlag()}

			// 复用现有子节点（如果有）
			if prefixLength < len(n.Key) {
				// 创建一个带有键剩余部分的短节点
				child := &ShortNode{
					Key:   n.Key[prefixLength+1:],
					Val:   n.Val,
					flags: t.newNodeFlag(),
					// 保留原始窗口信息
					window: n.window,
				}
				branch.Children[n.Key[prefixLength]] = child
			} else {
				// 这种情况下，短节点的完整键与新键的前缀匹配
				// 在这种情况下，我们在分支节点中按原样使用短节点的值
				branch.Children[key[prefixLength]] = n.Val
			}

			// 在分支节点中插入新值
			var err error
			if prefixLength < len(key) {
				branch.Children[key[prefixLength]], err = t.insert(nil, key[prefixLength+1:], value, bitPos)
				if err != nil {
					return nil, err
				}
			} else {
				// 新键更短，所以我们只添加一个值节点
				branch.Children[16] = value

				// 如果这是叶子节点，更新窗口位
				if _, ok := value.(ValueNode); ok {
					branch.window = 1 << bitPos
				}
			}

			if prefixLength == 1 { // 只有一个字符前缀
				return branch, nil
			}

			// 返回包装在短节点中的分支节点
			shortNode := &ShortNode{
				Key:   key[:prefixLength],
				Val:   branch,
				flags: t.newNodeFlag(),
			}

			// 短节点的窗口应继承其子节点的窗口
			if branch.window != 0 {
				shortNode.window = branch.window
			}

			return shortNode, nil
		}

		// 如果没有公共前缀，转换为分支节点
		branch := &FullNode{flags: t.newNodeFlag()}

		// 插入旧节点
		if len(n.Key) == 0 {
			return nil, fmt.Errorf("shortNode has empty key")
		}

		branch.Children[n.Key[0]] = &ShortNode{
			Key:    n.Key[1:],
			Val:    n.Val,
			flags:  t.newNodeFlag(),
			window: n.window, // 保留原始窗口信息
		}

		// 插入新节点
		var err error
		if len(key) == 0 {
			return nil, fmt.Errorf("key has empty content")
		}

		branch.Children[key[0]], err = t.insert(nil, key[1:], value, bitPos)
		if err != nil {
			return nil, err
		}

		// 更新branch的window
		if _, ok := value.(ValueNode); ok && len(key) == 1 {
			// 如果新插入的是叶子节点，并且在当前branch层结束
			branch.window = 1 << bitPos
		}

		return branch, nil

	case *FullNode:
		// 更新特定分支
		var err error
		if len(key) == 0 {
			// 当key为空时，这是全节点的值节点
			n = n.copy()
			n.Children[16] = value

			// 如果这是叶子节点，更新窗口位
			if _, ok := value.(ValueNode); ok {
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
		// 这个逻辑会在Hash阶段完全处理，这里只是初始化设置
		if childNode, ok := n.Children[key[0]].(*ShortNode); ok && childNode.window != 0 {
			if n.window == 0 {
				n.window = childNode.window
			}
		} else if childNode, ok := n.Children[key[0]].(*FullNode); ok && childNode.window != 0 {
			if n.window == 0 {
				n.window = childNode.window
			}
		}

		return n, nil

	case HashNode:
		// 在简化版实现中，从nodeDB获取节点
		nodehash := common.BytesToHash(n)
		if encoded, ok := t.nodeDB[nodehash.Hex()]; ok {
			child, err := decodeNode(n, encoded)
			if err != nil {
				return nil, err
			}
			return t.insert(child, key, value, bitPos)
		}
		return nil, &MissingNodeError{NodeHash: common.BytesToHash(n), Path: key}

	case nil:
		// 当插入到空树时，只使用一个短节点
		shortNode := &ShortNode{
			Key:   key,
			Val:   value,
			flags: t.newNodeFlag(),
		}

		// 如果这是叶子节点，设置当前区块对应的位
		if _, ok := value.(ValueNode); ok {
			shortNode.window = 1 << bitPos
		}

		return shortNode, nil

	default:
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// Delete从trie中删除key
func (t *CacheTrie) Delete(key []byte) error {
	if t.committed {
		return ErrCommitted
	}
	t.uncommitted++

	hexKey := keybytesToHex(key)

	// 创建一个特殊的标记值作为"墓碑"
	// 这里使用一个空的ValueNode作为墓碑标记
	tombstone := ValueNode([]byte{})

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

// Hash返回trie的根哈希
func (t *CacheTrie) Hash() common.Hash {
	if t.root == nil {
		return EmptyRoot
	}
	hash, cached := t.root.cache()
	if hash != nil && !cached {
		return common.BytesToHash(hash)
	}

	// Trie还没有写入磁盘，即时生成哈希
	h := newHasher(false)
	defer returnHasherToPool(h)

	rootHash, newRoot := h.hashRoot(t.root, true)
	t.root = newRoot
	return rootHash
}

// IsCachedAtBlock检查节点在给定路径是否有指定区块的缓存数据
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

	// 获取对应路径的节点
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
	case *ShortNode:
		return n.window
	case *FullNode:
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
		case *ShortNode:
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
		case *FullNode:
			// 如果已经到达路径末尾，返回值节点（如果存在）
			if pos == len(hexKey) {
				return n.Children[16], nil
			}
			// 否则跟随路径继续查找
			childIndex := hexKey[pos]
			node = n.Children[childIndex]
			pos++
		case HashNode:
			// 解析哈希节点
			nodehash := common.BytesToHash(n)
			if encoded, ok := t.nodeDB[nodehash.Hex()]; ok {
				child, err := decodeNode(n, encoded)
				if err != nil {
					return nil, err
				}
				node = child
			} else {
				return nil, &MissingNodeError{NodeHash: nodehash, Path: hexKey[:pos]}
			}
		case ValueNode:
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

// getCacheBitPosition计算window中当前区块的位位置
func (t *CacheTrie) getCacheBitPosition() int {
	if t.blockNum < t.startNum {
		return 0 // 如果当前区块小于起始区块，默认使用第0位
	}

	position := (t.blockNum - t.startNum) / t.multiple
	return int(position % 32) // 限制在32位之内，因为window是一个int
}

// GetSize返回trie中键值对的总数
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

// countNodesWithoutTombstones计算trie中的实际键值对数量，不包括墓碑标记
func (t *CacheTrie) countNodesWithoutTombstones(n cacheNode, result *struct{ count int }) {
	if n == nil {
		return
	}

	switch n := n.(type) {
	case *ShortNode:
		if valueNode, isValue := n.Val.(ValueNode); isValue {
			// 是否是墓碑标记
			if len(valueNode) > 0 {
				result.count++
			}
		} else if n.Val != nil {
			// 递归处理非值类型的子节点
			t.countNodesWithoutTombstones(n.Val, result)
		}

	case *FullNode:
		// 检查节点16（值节点）
		if n.Children[16] != nil {
			if valueNode, isValue := n.Children[16].(ValueNode); isValue {
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

	case ValueNode:
		// 独立的值节点，检查是否是墓碑标记
		if len(n) > 0 {
			result.count++
		}

	case HashNode:
		// 解析哈希节点
		nodehash := common.BytesToHash(n)
		if encoded, ok := t.nodeDB[nodehash.Hex()]; ok {
			child, err := decodeNode(n, encoded)
			if err != nil {
				return
			}
			t.countNodesWithoutTombstones(child, result)
		}
	}
}
