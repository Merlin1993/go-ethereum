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

// Package cacheTrie 提供了一个带有增强缓存功能的Merkle Patricia树实现。
// 该实现使用位图窗口(window bitmap)跟踪不同区块高度的缓存状态。
package cacheTrie

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// -----------------------------------------------------------------------------
// 常量和变量定义

// EmptyRoot是一个特殊的根哈希，表示空树
var EmptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

const WindowLeft = 0

// -----------------------------------------------------------------------------
// 数据结构定义

// CacheTrie是一个Merkle Patricia树变种，具有增强的缓存功能。
// 它通过window位图机制跟踪键值对在不同区块高度的状态，
// 使得可以在一定范围内实现区块特定的缓存查询。
//
// CacheTrie不是线程安全的。
type CacheTrie struct {
	// 树的根节点
	root cacheNode

	// 当前区块高度
	blockNum uint64

	// 起始区块号，表示窗口的起始位置
	startNum uint64

	// multiple表示window中每个位存储多少个区块
	// 例如：multiple=10表示每个位记录10个区块的状态
	multiple uint64

	// 缓存大小限制和计数
	maxSize int // 可缓存的最大键值对数量，0表示无限制
}

// -----------------------------------------------------------------------------
// 内部辅助方法

// newNodeFlag 返回新创建节点的缓存标志值
func (t *CacheTrie) newNodeFlag() nodeFlag {
	return nodeFlag{dirty: true}
}

// getCacheBitPosition 计算window中当前区块的位位置
// 根据当前区块高度、起始区块和倍数计算当前区块在32位window中的位置
func (t *CacheTrie) getCacheBitPosition() int {
	if t.blockNum < t.startNum {
		return 0 // 如果当前区块小于起始区块，默认使用第0位
	}

	//position表示当前区块在window中的位置,因为是一个滑动窗口，所以会循环使用window得位，只要不超过最大缓存两，就和startNum无关
	position := t.blockNum / t.multiple
	return int(position % 32) // 限制在32位之内，因为window是一个int
}

// 获取剩余可用的position
func (t *CacheTrie) getWindowPosition() int {
	return 32 - int((t.blockNum-t.startNum)/t.multiple) - 1
}

// getNodeWindow 获取节点的window位图
func (t *CacheTrie) getNodeWindow(n cacheNode) int {
	return n.window()
}

// getNodeForPathRecursive 是getNodeForPath的递归实现
// 参数:
//   - node: 当前检查的节点
//   - key: 十六进制格式的键
//   - pos: 当前在key中的位置
//
// 返回:
//   - 找到的节点
//   - 错误信息
func (t *CacheTrie) get(node cacheNode, key []byte, pos int, bitPosition int) (cacheNode, error) {
	// 基本情况: 节点为nil
	if node == nil {
		return nil, nil
	}

	// 基本情况: 已经处理完整个key
	if pos >= len(key) {
		return node, nil
	}

	switch n := node.(type) {
	case *ShortNode:
		// 检查key是否匹配短节点的前缀
		if len(key)-pos < len(n.Key) || !bytes.HasPrefix(key[pos:], n.Key) {
			// 不匹配，返回nil
			return nil, nil
		}

		// 移动位置并递归到子节点
		newPos := pos + len(n.Key)

		// 如果到达key末尾，返回当前节点的Val
		if newPos == len(key) {
			return n.Val, nil
		}

		// 否则继续递归查找
		childNode, err := t.get(n.Val, key, newPos, bitPosition)

		//如果找到了，则更新其window字段
		if childNode != nil {
			n.updateFlag(bitPosition)
		}

		return childNode, err

	case *FullNode:
		// 获取下一个字符作为分支索引
		childIndex := key[pos]

		// 检查索引是否有效
		if int(childIndex) >= len(n.Children) {
			return nil, nil
		}

		// 继续递归查找
		childNode, err := t.get(n.Children[childIndex], key, pos+1, bitPosition)

		//如果找到了，则更新其window字段
		if childNode != nil {
			n.updateFlag(bitPosition)
		}

		return childNode, err

	case ValueNode:
		// 如果已经到达key末尾，返回值节点
		if pos == len(key) {
			return n, nil
		}
		// 否则值节点不能有子节点，返回nil
		return nil, nil

	default:
		// 未知节点类型
		return nil, fmt.Errorf("unknown node type: %T", node)
	}
}

// countBits 计算整数中设置的位数
// 用于统计window位图中有多少位被设置
func countBits(n int) int {
	count := 0
	for n != 0 {
		count += n & 1
		n >>= 1
	}
	return count
}

// -----------------------------------------------------------------------------
// 构造和配置方法 - 公共API

// NewCacheTrie 创建一个具有指定参数的缓存树实例
// 参数：
//   - startNum: 起始区块号，表示窗口的起始位置
//   - multiple: 每个位表示的区块数量
//   - maxSize: 最大缓存键值对数量，0表示无限制
func NewCacheTrie(startNum, multiple uint64, maxSize int) *CacheTrie {
	trie := &CacheTrie{
		blockNum: 0,
		startNum: startNum,
		multiple: multiple,
		maxSize:  maxSize,
	}
	return trie
}

// SetBlockNum 更新CacheTrie的当前区块高度
// 这会影响后续操作的缓存位置计算
func (t *CacheTrie) SetBlockNum(blockNum uint64) {
	t.blockNum = blockNum
}

// GetSize 返回trie中键值对的总数
func (t *CacheTrie) GetSize() int {
	if t.root == nil {
		return 0
	}
	return t.root.size()
}

// -----------------------------------------------------------------------------
// 主要操作方法 - 公共API

// Get 通过路径获取节点
// 从根节点开始，沿着key指定的路径查找，返回路径末端的节点
//
// 参数:
//   - key: 要查找的键
//
// 返回:
//   - 找到的节点
//   - 错误信息
func (t *CacheTrie) Get(key []byte) (cacheNode, error) {
	// 确保key是哈希值（固定长度）
	hashedKey := hashKey(key)
	hexKey := keybytesToHex(hashedKey)
	bitPosition := t.getCacheBitPosition()
	return t.get(t.root, hexKey, 0, bitPosition)
}

// Update 将键值对添加到trie中
// 如果value为空，则调用Delete方法删除该键
func (t *CacheTrie) Update(key, value []byte, isNew bool) error {
	if len(value) == 0 {
		return t.Delete(key)
	}

	// 确保key是哈希值（固定长度）
	hashedKey := hashKey(key)
	hexKey := keybytesToHex(hashedKey)

	// 设置当前位置
	bitPos := t.getCacheBitPosition()

	// 将value转换为ValueNode类型
	valueNode := ValueNode{Data: value, New: isNew}

	root, err := t.insert(t.root, hexKey, valueNode, bitPos)
	if err != nil {
		return err
	}

	// 更新根节点
	t.root = root
	return nil
}

// Delete 从trie中删除key
// 内部实现使用"墓碑"标记(空值节点)替代真正的删除
func (t *CacheTrie) Delete(key []byte) error {
	// 确保key是哈希值（固定长度）
	hashedKey := hashKey(key)
	hexKey := keybytesToHex(hashedKey)

	// 创建一个特殊的标记值作为"墓碑"
	// 这里使用一个空的ValueNode作为墓碑标记，并设置New为true
	tombstone := ValueNode{Data: []byte{}, New: true}

	// 获取当前block位置
	bitPos := t.getCacheBitPosition()

	// 使用insert方法插入墓碑标记，而不是真正删除
	newroot, err := t.insert(t.root, hexKey, tombstone, bitPos)
	if err != nil {
		return err
	}

	t.root = newroot
	return nil
}

// Hash 返回trie的根哈希
// 同时执行必要的缓存清理
// 返回trie的根哈希和在pruneCache过程中删除的键值对
func (t *CacheTrie) Hash() (common.Hash, *DeleteKVList) {
	if t.root == nil {
		return EmptyRoot, nil
	}

	// 如果设置了最大大小且超出限制，或window的位数不足，清理不常用的缓存
	deleteKeyValues := t.pruneCache()

	// 即时生成哈希
	h := newHasher(false)
	defer returnHasherToPool(h)

	if t.root == nil {
		return EmptyRoot, deleteKeyValues
	}
	rootHash := h.hash(t.root)

	return common.BytesToHash(rootHash), deleteKeyValues
}

type DeleteKV struct {
	Key   []byte
	Value []byte
}

type DeleteKVList struct {
	Data []*DeleteKV
}

// pruneCache清理不常用的缓存节点
// 返回在清理过程中删除的键值对列表
func (t *CacheTrie) pruneCache() *DeleteKVList {
	// 计算当前window的可用位数
	// 当前位置表示已经使用了多少位
	windowBits := t.getWindowPosition()

	// 判断是否需要清理
	needPrune := false

	// 条件1：当window的位数只剩下8bit（也就是用了四分之三的窗口），触发清理
	if windowBits <= WindowLeft {
		needPrune = true
	}

	// 条件2：size大于MaxSize时，触发清理
	if t.maxSize > 0 && t.root.size() > t.maxSize {
		needPrune = true
	}

	// 如果不需要清理，直接返回
	if !needPrune {
		return nil
	}

	// 执行循环操作，直到根节点size数量小于2/3的maxSize且window位数等于16bit
	// 使用2/3作为阈值
	targetSize := t.maxSize * 2 / 3
	if targetSize <= 0 {
		targetSize = 1 // 确保至少有一个目标大小
	}
	deleteKVList := &DeleteKVList{make([]*DeleteKV, 0)}
	// 循环直到满足条件
	for {
		// 检查是否已经满足条件：size小于目标值，且window位数大于等于16bit
		if t.root == nil || (t.root.size() <= targetSize && windowBits >= WindowLeft+8) {
			break
		}

		// 找到最低一位的缓存窗口（即最早的缓存）
		lowestBit := int(t.startNum / t.multiple % 32)

		// 从根节点递归查找所有节点，对于所有最低一位的叶子节点进行实际的删除
		if t.root != nil {
			t.root = t.pruneNodeAtBit(t.root, make([]byte, 0), lowestBit, deleteKVList)
		}

		// 更新startNum（向前移动window）
		t.startNum += t.multiple

		// 重新计算windowBits
		windowBits = t.getWindowPosition()
	}
	return deleteKVList
}

// pruneNodeAtBit递归查找指定位设置的节点并清理
func (t *CacheTrie) pruneNodeAtBit(n cacheNode, prefixKey []byte, bit int, deleteKVList *DeleteKVList) cacheNode {
	if n == nil {
		return nil
	}

	//首先需要向下查找，然后删除对应的值。
	//然后回到父节点，重新计算window和size
	switch node := n.(type) {
	case *ShortNode:
		// 检查这个节点是否有指定的位设置
		if (node.window() & (1 << bit)) != 0 {
			// 如果是叶子节点（Val是ValueNode）且window变为0，则清除此节点
			if valueNode, isValueNode := node.Val.(ValueNode); isValueNode {
				// 使用append合并字节切片，而不是加法操作符
				fullKey := append(append([]byte{}, prefixKey...), node.Key...)
				// 将十六进制格式的键转换回二进制格式
				binaryKey := hexToKeybytes(fullKey)

				// 只有当ValueNode.New为true时才添加到deleteKeyValue
				if valueNode.New {
					deleteKVList.Data = append(deleteKVList.Data, &DeleteKV{
						Key:   binaryKey,
						Value: valueNode.Data,
					})
				}
				return nil
			} else {
				// 对子节点递归处理，使用append合并路径
				newPrefixKey := append(append([]byte{}, prefixKey...), node.Key...)
				newVal := t.pruneNodeAtBit(node.Val, newPrefixKey, bit, deleteKVList)

				// 如果子节点被删除并且这个节点的window为0，则删除此节点
				if newVal == nil {
					return nil
				} else {
					node.Val = newVal
					//如果是valueNode，肯定已经被删除了，如果是branchNode,那么没问题，所以这样写可以
					node.updateFlag(bit)
				}
			}
		}
		return node

	case *FullNode:
		// 检查这个节点是否有指定的位设置
		if (node.window() & (1 << bit)) != 0 {
			// 对所有子节点递归处理
			allChildrenNil := true
			for i := 0; i < 16; i++ {
				if node.Children[i] != nil {
					// 记录子节点路径，添加当前索引作为一个字节
					newPrefixKey := append(append([]byte{}, prefixKey...), byte(i))

					// 递归处理子节点
					node.Children[i] = t.pruneNodeAtBit(node.Children[i], newPrefixKey, bit, deleteKVList)

					// 检查子节点是否被删除
					if node.Children[i] != nil {
						allChildrenNil = false
					}
				}
			}

			// 如果所有子节点都为空且window为0，则删除此节点
			if allChildrenNil {
				return nil
			}
			node.updateFlag(bit)
		}
		return node
	}
	return n
}

// hexToKeybytes 将十六进制格式的键转换回二进制格式
// 此函数处理终端标志并支持奇数长度的hex键
func hexToKeybytes(hex []byte) []byte {
	// 如果有终端标志，移除它
	if hasTerm(hex) {
		hex = hex[:len(hex)-1]
	}

	// 如果长度为奇数，则无法正确转换
	if len(hex)%2 != 0 {
		panic("无法转换奇数长度的十六进制键")
	}

	// 长度为偶数的正常处理
	result := make([]byte, len(hex)/2)
	for i := 0; i < len(hex); i += 2 {
		result[i/2] = (hex[i] << 4) | hex[i+1]
	}

	return result
}

// insert 是Update的内部实现，递归插入键值
// 参数:
//   - n: 当前操作的节点
//   - key: 十六进制格式的键
//   - value: 要插入的值节点
//   - bitPos: 当前区块在window中的位置
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

			// 更新window属性
			shortNode.updateFlag(bitPos)
			return shortNode, nil
		}

		//1.如果有相同的前缀，那么现构造一个相同前缀的short，然后生成一个fullNode，再fullNode生成两个short。
		//2.如果没有相同的前缀，那么构造一个fullNode，然后直接生成两个shortNode放进去。

		// 如果共享前缀，在分叉点分裂
		// 分叉点的分支节点
		branch := &FullNode{flags: t.newNodeFlag()}
		//首先把旧的数据取出来，放到fullNode的位置
		// 处理旧的shortNode，允许shortNode的key是空数组
		// 创建一个带有键剩余部分的短节点
		var child *ShortNode
		if prefixLength < len(n.Key) {
			child = &ShortNode{
				Key:   n.Key[prefixLength+1:],
				Val:   n.Val,
				flags: n.flags,
			}
		} else {
			child = &ShortNode{
				Key:   make([]byte, 0),
				Val:   n.Val,
				flags: n.flags,
			}
		}
		branch.Children[n.Key[prefixLength]] = child

		// 创建一个新的节点
		var child2 *ShortNode
		if prefixLength < len(key) {
			child2 = &ShortNode{
				Key:   key[prefixLength+1:],
				Val:   value,
				flags: t.newNodeFlag(),
			}
		} else {
			child2 = &ShortNode{
				Key:   make([]byte, 0),
				Val:   value,
				flags: t.newNodeFlag(),
			}
		}
		child2.updateFlag(bitPos)
		branch.Children[key[prefixLength]] = child2

		// 然后在分支节点中插入新的shortNode
		if prefixLength != 0 {
			parentNode := &ShortNode{
				Key:   n.Key[:prefixLength],
				Val:   branch,
				flags: t.newNodeFlag(),
			}
			parentNode.updateFlag(bitPos)
			return parentNode, nil
		} else {
			branch.updateFlag(bitPos)
			return branch, nil
		}
	case *FullNode:
		// 更新特定分支
		var err error

		n.Children[key[0]], err = t.insert(n.Children[key[0]], key[1:], value, bitPos)
		if err != nil {
			return nil, err
		}
		n.updateFlag(bitPos)

		return n, nil
	case nil:
		// 如果当前节点为空，创建一个新的短节点
		shortNode := &ShortNode{
			Key:   key,
			Val:   value,
			flags: t.newNodeFlag(),
		}
		shortNode.updateFlag(bitPos)
		return shortNode, nil
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// hashKey 对输入的key进行哈希处理，确保返回固定长度的键
func hashKey(key []byte) []byte {
	// 使用crypto包中的Keccak256哈希函数
	return crypto.Keccak256(key)
}
