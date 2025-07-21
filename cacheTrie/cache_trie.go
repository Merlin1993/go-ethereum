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
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// -----------------------------------------------------------------------------
// 常量和变量定义

// EmptyRoot是一个特殊的根哈希，表示空树
var EmptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

const WindowLeft = 4

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

	// 当前记录位
	currentBit int

	// 清理操作相关
	cleanupMu        sync.Mutex    // 清理操作的互斥锁
	cleanupResults   common.Hash   // 清理结果缓存
	isCleaningUp     bool          // 是否正在清理
	cleanupCount     int           // 清理次数统计
	totalCleanupTime time.Duration // 清理总时间
	maxCleanupTime   time.Duration // 最大清理时间

	// 新增统计字段
	cleanupDuration           time.Duration // startCleanup到FinishCleanup的总耗时
	cleanupMaxDuration        time.Duration // startCleanup到FinishCleanup的最大耗时
	cleanupStartTime          time.Time     // 当前一次cleanup的开始时间
	pruneNodeAtBitDuration    time.Duration // pruneNodeAtBit累计耗时
	pruneNodeAtBitMaxDuration time.Duration // pruneNodeAtBit最大耗时

	hrw           *HeightRangeWindow //拥塞控制
	pruneBitCount int

	codes map[common.Hash][]byte

	// 命中/未命中统计
	hitCount        uint64     // Get/GetWithAddress 操作命中次数
	missCount       uint64     // Get/GetWithAddress 操作未命中次数
	updateHitCount  uint64     // Update/UpdateWithAddress 操作命中次数（更新已存在的键）
	updateMissCount uint64     // Update/UpdateWithAddress 操作未命中次数（插入新键）
	statsMu         sync.Mutex // 统计操作的互斥锁

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
	return t.currentBit
}

// getNodeWindow 获取节点的window位图
func (t *CacheTrie) getNodeWindow(n cacheNode) int {
	return n.window()
}

// prepareKey 处理键并返回十六进制格式的哈希键
// 可选参数address允许将地址与键组合
func (t *CacheTrie) prepareKey(key []byte, address ...common.Address) []byte {
	var dataToHash []byte

	// 如果提供了地址，将地址与键组合
	if len(address) > 0 {
		dataToHash = append(address[0].Bytes(), key...)
		dataToHash = hashKey(dataToHash)
	} else {
		dataToHash = hashKey(key)
	}
	return keybytesToHex(dataToHash)
}

// getInternal 是Get和GetWithAddress的内部实现
func (t *CacheTrie) getInternal(hexKey []byte) (cacheNode, error) {
	bitPosition := t.getCacheBitPosition()
	return t.get(t.root, hexKey, 0, bitPosition)
}

// updateInternal 是Update和UpdateWithAddress的内部实现
// 返回:
//   - error: 操作错误
//   - bool: 如果为true表示更新已存在键，false表示插入新键
func (t *CacheTrie) updateInternal(hexKey []byte, value []byte, isNew bool, originalKey []byte, address ...common.Address) (error, bool) {
	// 设置当前位置
	bitPos := t.getCacheBitPosition()

	// 将value转换为ValueNode类型
	valueNode := ValueNode{
		Data:   value,
		New:    isNew,
		RawKey: originalKey,
	}

	// 如果提供了地址，保存它
	if len(address) > 0 {
		valueNode.Address = address[0]
	}

	root, nodeExists, err := t.insert(t.root, hexKey, valueNode, bitPos)
	t.hrw.addLogicalSize(t.currentBit)
	if err != nil {
		return err, nodeExists
	}

	// 更新根节点
	t.root = root
	return nil, nodeExists
}

// deleteInternal 是Delete和DeleteWithAddress的内部实现
// 返回:
//   - error: 操作错误
//   - bool: 如果为true表示删除已存在键，false表示删除不存在键
func (t *CacheTrie) deleteInternal(hexKey []byte, originalKey []byte, address ...common.Address) (error, bool) {
	// 创建一个特殊的标记值作为"墓碑"
	// 使用一个空的ValueNode作为墓碑标记，并设置New为true
	tombstone := ValueNode{
		Data:   []byte{},
		New:    true,
		RawKey: originalKey,
	}

	// 如果提供了地址，保存它
	if len(address) > 0 {
		tombstone.Address = address[0]
	}

	// 获取当前block位置
	bitPos := t.getCacheBitPosition()

	// 使用insert方法插入墓碑标记，而不是真正删除
	newroot, nodeExists, err := t.insert(t.root, hexKey, tombstone, bitPos)
	t.hrw.addLogicalSize(t.currentBit)
	if err != nil {
		return err, nodeExists
	}

	t.root = newroot
	return nil, nodeExists
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
	}
	trie.codes = make(map[common.Hash][]byte)
	trie.hrw = NewHeightRangeWindow(startNum, multiple, maxSize)
	return trie
}

// SetBlockNum 更新CacheTrie的当前区块高度
// 这会影响后续操作的缓存位置计算
func (t *CacheTrie) SetBlockNum(blockNum uint64) {
	t.blockNum = blockNum
	t.currentBit = t.hrw.GetBitPosition(blockNum)
	if t.currentBit < 0 {
		t.hrw.getWindowPosition()
		panic(fmt.Sprintf("CacheTrie位置计算错误: 当前区块高度=%v, 窗口起始区块=%v, 窗口结束区块=%v, 错误码=%v (若-1则表示区块高度小于窗口起始位置; 若-2则表示区块高度超出窗口最大容量), 当前窗口位数=%v, 首段索引=%v, 慢启动阈值=%v",
			blockNum, t.hrw.windowStartNumber, t.hrw.windowEndNumber, t.currentBit, t.hrw.getWindowPosition(), t.hrw.firstSegmentIndex, t.hrw.currentSsthresh))
	}
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
	// 首先从树中查找
	hexKey := t.prepareKey(key)
	node, err := t.getInternal(hexKey)

	// 更新统计信息 - 命中或未命中
	t.statsMu.Lock()
	if node != nil {
		t.hitCount++
	} else {
		t.missCount++
	}
	t.statsMu.Unlock()

	return node, err
}

// GetWithAddress 通过地址和路径获取节点
// 从根节点开始，沿着由address+key组合的路径查找，返回路径末端的节点
//
// 参数:
//   - address: 账户地址
//   - key: 要查找的键
//
// 返回:
//   - 找到的节点
//   - 错误信息
func (t *CacheTrie) GetWithAddress(address common.Address, key []byte) (cacheNode, error) {
	//if t.blockNum >= 757462 && address.String() == "0x7De5abA7DE728950c92C57d08e20D4077161F12F" && common.Bytes2Hex(key) == "23b9648796b3c6214e5a66544bfc6d76a8c3a8ebe1a6720267b443cee4302a5f" {
	//	address.String()
	//}
	// 首先从树中查找
	hexKey := t.prepareKey(key, address)
	node, err := t.getInternal(hexKey)

	// 更新统计信息 - 命中或未命中
	t.statsMu.Lock()
	if node != nil {
		t.hitCount++
	} else {
		t.missCount++
	}
	t.statsMu.Unlock()

	return node, err
}

// Update 将键值对添加到trie中
// 如果value为空，则调用Delete方法删除该键
func (t *CacheTrie) Update(key, value []byte, isNew bool) error {
	if len(value) == 0 {
		return t.Delete(key)
	}

	hexKey := t.prepareKey(key)
	err, nodeExists := t.updateInternal(hexKey, value, isNew, key)

	// 更新统计信息
	t.statsMu.Lock()
	if nodeExists { // 键已存在，是更新操作
		t.updateHitCount++
	} else { // 键不存在，是插入操作
		t.updateMissCount++
	}
	t.statsMu.Unlock()

	return err
}

// UpdateWithAddress 将带有地址前缀的键值对添加到trie中
// 如果value为空，则调用DeleteWithAddress方法删除该键
func (t *CacheTrie) UpdateWithAddress(address common.Address, key, value []byte, isNew bool) error {
	if len(value) == 0 {
		return t.DeleteWithAddress(address, key)
	}

	//if t.blockNum >= 757462 && address.String() == "0x7De5abA7DE728950c92C57d08e20D4077161F12F" && common.Bytes2Hex(key) == "23b9648796b3c6214e5a66544bfc6d76a8c3a8ebe1a6720267b443cee4302a5f" {
	//	address.String()
	//}

	hexKey := t.prepareKey(key, address)
	err, nodeExists := t.updateInternal(hexKey, value, isNew, key, address)

	// 更新统计信息
	t.statsMu.Lock()
	if nodeExists { // 键已存在，是更新操作
		t.updateHitCount++
	} else { // 键不存在，是插入操作
		t.updateMissCount++
	}
	t.statsMu.Unlock()

	return err
}

// Delete 从trie中删除key
// 内部实现使用"墓碑"标记(空值节点)替代真正的删除
func (t *CacheTrie) Delete(key []byte) error {
	hexKey := t.prepareKey(key)
	err, nodeExists := t.deleteInternal(hexKey, key)

	// 更新统计信息 - 删除也是一种更新操作
	t.statsMu.Lock()
	if nodeExists { // 键已存在，是删除操作
		t.updateHitCount++
	} else { // 键不存在，是无效删除
		t.updateMissCount++
	}
	t.statsMu.Unlock()

	return err
}

// DeleteWithAddress 从trie中删除带有地址前缀的key
// 内部实现使用"墓碑"标记(空值节点)替代真正的删除
func (t *CacheTrie) DeleteWithAddress(address common.Address, key []byte) error {
	//if t.blockNum >= 757462 && address.String() == "0x7De5abA7DE728950c92C57d08e20D4077161F12F" && common.Bytes2Hex(key) == "23b9648796b3c6214e5a66544bfc6d76a8c3a8ebe1a6720267b443cee4302a5f" {
	//	address.String()
	//}
	hexKey := t.prepareKey(key, address)
	err, nodeExists := t.deleteInternal(hexKey, key, address)

	// 更新统计信息 - 删除也是一种更新操作
	t.statsMu.Lock()
	if nodeExists { // 键已存在，是删除操作
		t.updateHitCount++
	} else { // 键不存在，是无效删除
		t.updateMissCount++
	}
	t.statsMu.Unlock()

	return err
}

// Hash 返回trie的根哈希
// 同时执行必要的缓存清理
// 返回trie的根哈希和在pruneCache过程中删除的键值对
func (t *CacheTrie) Hash() (common.Hash, common.Hash, *DeleteKVList) {
	if t.root == nil {
		return EmptyRoot, common.Hash{}, nil
	}

	// 如果设置了最大大小且超出限制，或window的位数不足，清理不常用的缓存
	deleteKeyValues, resultHash := t.pruneCache()
	t.hrw.triggerSize(t.currentBit)

	// 缓存删除的键值对
	if deleteKeyValues != nil && len(deleteKeyValues.Data) > 0 {
		// 确保BlockNum字段已设置
		deleteKeyValues.BlockNum = t.blockNum
	}

	//if t.isCleaningUp && t.blockNum-t.currentCleanupBlock >= 5 {
	//	panic(fmt.Sprintf("cache trie expire, blockNum: %v , currentCleanupBlock: %v", t.blockNum, t.currentCleanupBlock))
	//}

	// 即时生成哈希
	h := newHasher(false)
	defer returnHasherToPool(h)

	if t.root == nil {
		return EmptyRoot, resultHash, deleteKeyValues
	}
	rootHash := h.hash(t.root)

	return common.BytesToHash(rootHash), resultHash, deleteKeyValues
}

type DeleteKV struct {
	Key     []byte
	Value   []byte
	Address common.Address
}

type DeleteKVList struct {
	Data     []*DeleteKV
	BlockNum uint64 // 添加区块号字段
}

var CleanupTime time.Duration

// pruneCache清理不常用的缓存节点
// 返回在清理过程中删除的键值对列表
func (t *CacheTrie) pruneCache() (kvl *DeleteKVList, resultHash common.Hash) {
	// 计算当前window的可用位数
	// 当前位置表示已经使用了多少位
	windowBits := t.hrw.getWindowPosition()

	// 判断是否需要清理
	needStartPrune := false
	doPrune := false

	//当存在未清除的数据时，如果不超过最大限制，那么就先往前继续推进
	if t.pruneBitCount > 0 {
		//如果已经在清理了，那除非没有空间了，不然继续使用
		if windowBits <= 1 {
			doPrune = true
		}

		if t.hrw.CheckAndTriggerCongestionControl(t.root.size()) {
			doPrune = true
		}

	} else {
		// 条件1：当window的位数只剩下WindowLeft bit，触发清理
		if windowBits <= WindowLeft-2 {
			needStartPrune = true
		}

		// 条件2：size大于MaxSize时，触发清理
		if t.hrw.CheckAndTriggerCongestionControl(t.root.size() * 105 / 100) {
			needStartPrune = true
		}
	}

	// 如果不需要清理，直接返回
	if !needStartPrune && !doPrune {
		return nil, common.Hash{}
	}
	//当需要进行裁剪时，务必先获取锁, 能获取到，说明当前已无缓存，可以进行。如果不能获取到，说明还存在数据，此时不可以直接处理。
	t.startCleanup()

	//如果需要进行删除操作，先删除，再裁剪
	if doPrune {
		resultHash = t.GetCleanupResult()
		//fmt.Println(fmt.Sprintf("start prune node : %v , window : %v ,sshresh : %v， result： %v ", t.root.size(), windowBits, t.hrw.currentSsthresh, resultHash))
		t.pruneNode()
	}
	//fmt.Println(fmt.Sprintf("start prune, size : %v , window end : %v ,sshresh : %v ", t.root.size(), t.hrw.getWindowPosition(), t.hrw.currentSsthresh))

	// 记录清理开始时间
	startTime := time.Now()

	// 执行循环操作，直到根节点size数量小于80%的maxSize且window位数等于8bit
	// 使用2/3作为阈值
	targetSize := t.hrw.maxTotalAllowedSize * 90 / 100
	if targetSize <= 0 {
		targetSize = 1 // 确保至少有一个目标大小
	}
	deleteKVList := &DeleteKVList{
		Data:     make([]*DeleteKV, 0),
		BlockNum: t.blockNum, // 设置当前区块号
	}
	bitCount := 0
	// 找到最低一位的缓存窗口（即最早的缓存）
	lowestBit := t.hrw.firstSegmentIndex
	// 循环直到满足条件
	for {
		// 检查是否已经满足条件：size小于目标值，且window位数大于等于12bit
		if t.root == nil || ((t.root.size()-len(deleteKVList.Data) <= targetSize) && windowBits >= WindowLeft) {
			break
		}

		//if (lowestBit+bitCount)%32 == 30 {
		//	t.root.size()
		//}
		// 从根节点递归查找所有节点，对于所有最低一位的叶子节点进行查找
		t.findNodeAtBit(t.root, (lowestBit+bitCount)%32, deleteKVList)

		bitCount++

		// 重新计算windowBits
		windowBits++
	}
	t.pruneBitCount = bitCount

	// 计算清理时间并更新统计
	CleanupTime = time.Since(startTime)
	t.totalCleanupTime += CleanupTime
	if CleanupTime > t.maxCleanupTime {
		t.maxCleanupTime = CleanupTime
	}

	return deleteKVList, resultHash
}

func (t *CacheTrie) pruneNode() {
	start := time.Now() // 新增：记录开始时间
	defer func() {
		dur := time.Since(start)
		t.pruneNodeAtBitDuration += dur
		if dur > t.pruneNodeAtBitMaxDuration {
			t.pruneNodeAtBitMaxDuration = dur
		}
	}()
	bitCount := t.pruneBitCount
	if bitCount > 0 {
		//fmt.Println("prune node")
	}
	for bitCount > 0 {
		if t.root == nil {
			break
		}

		// 找到最低一位的缓存窗口（即最早的缓存）
		lowestBit := t.hrw.firstSegmentIndex
		// 从根节点递归查找所有节点，对于所有最低一位的叶子节点进行实际的删除
		if t.root != nil {
			t.hrw.resetLogicalSize(lowestBit)
			t.root = t.pruneNodeAtBit(t.root, lowestBit)
		}

		t.hrw.PruneWindow(1)
		bitCount--
	}
	t.pruneBitCount = 0
}

// pruneNodeAtBit递归查找指定位设置的节点并清理
func (t *CacheTrie) findNodeAtBit(n cacheNode, bit int, deleteKVList *DeleteKVList) {
	if n == nil {
		return
	}

	//首先需要向下查找。
	switch node := n.(type) {
	case *ShortNode:
		// 检查这个节点是否有指定的位设置
		if (node.window() & (1 << bit)) != 0 {
			// 如果是叶子节点（Val是ValueNode）
			if valueNode, isValueNode := node.Val.(ValueNode); isValueNode {
				//if valueNode.Address.Hex() == "0xcd134CE565e6b7f7CEfE2122A07A2e56D6ECbB26" && common.Bytes2Hex(valueNode.RawKey) == "0000000000000000000000000000000000000000000000000000000000000105" {
				//	valueNode.Address.Hex()
				//}
				// 只有当ValueNode.New为true时才添加到deleteKeyValue
				if valueNode.New {
					deleteKVList.Data = append(deleteKVList.Data, &DeleteKV{
						Key:     valueNode.RawKey,
						Value:   valueNode.Data,
						Address: valueNode.Address,
					})
				}
				return
			} else {
				t.findNodeAtBit(node.Val, bit, deleteKVList)
			}
		}
		return

	case *FullNode:
		// 检查这个节点是否有指定的位设置
		if (node.window() & (1 << bit)) != 0 {
			// 对所有子节点递归处理
			for i := 0; i < 16; i++ {
				if node.Children[i] != nil {
					// 递归处理子节点
					t.findNodeAtBit(node.Children[i], bit, deleteKVList)
				}
			}
		}
		return
	}
	return
}

// pruneNodeAtBit递归查找指定位设置的节点并清理
func (t *CacheTrie) pruneNodeAtBit(n cacheNode, bit int) cacheNode {
	if n == nil {
		return nil
	}

	//如果只有这个，那么就快速截断就好了
	if n.window() == 1<<bit {
		return nil
	}
	//首先需要向下查找，然后删除对应的值。
	//然后回到父节点，重新计算window和size
	switch node := n.(type) {
	case *ShortNode:
		// 检查这个节点是否有指定的位设置
		if (node.window() & (1 << bit)) != 0 {
			// 如果是叶子节点（Val是ValueNode）且window变为0，则清除此节点
			if _, isValueNode := node.Val.(ValueNode); isValueNode {
				//if valueNode.Address.Hex() == "0xcd134CE565e6b7f7CEfE2122A07A2e56D6ECbB26" && common.Bytes2Hex(valueNode.RawKey) == "0000000000000000000000000000000000000000000000000000000000000105" {
				//	valueNode.Address.Hex()
				//}
				return nil
			} else {
				newVal := t.pruneNodeAtBit(node.Val, bit)

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

					// 递归处理子节点
					node.Children[i] = t.pruneNodeAtBit(node.Children[i], bit)

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

// insert 是Update的内部实现，递归插入键值
// 参数:
//   - n: 当前操作的节点
//   - key: 十六进制格式的键
//   - value: 要插入的值节点
//   - bitPos: 当前区块在window中的位置
func (t *CacheTrie) insert(n cacheNode, key []byte, value cacheNode, bitPos int) (cacheNode, bool, error) {
	if len(key) == 0 {
		// 值节点只能在路径末尾插入
		if _, ok := value.(ValueNode); ok {
			return value, false, nil
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
		if prefixLength == len(n.Key) {
			if prefixLength == len(key) {
				shortNode := &ShortNode{
					Key:   n.Key,
					Val:   value,
					flags: t.newNodeFlag(),
				}

				// 更新window属性
				shortNode.updateFlag(bitPos)
				return shortNode, true, nil
			} else {
				//更新子节点
				childNode, exist, err := t.insert(n.Val, key[prefixLength:], value, bitPos)
				if err != nil {
					return nil, exist, err
				}
				n.Val = childNode
				n.updateFlag(bitPos)
				return n, exist, nil
			}
		}

		//1.如果有相同的前缀，那么现构造一个相同前缀的short，然后生成一个fullNode，再fullNode生成两个short。
		//2.如果没有相同的前缀，那么构造一个fullNode，然后直接生成两个shortNode放进去。

		// 如果共享前缀，在分叉点分裂
		// 分叉点的分支节点
		branch := &FullNode{flags: t.newNodeFlag()}
		//首先把旧的数据取出来，放到fullNode的位置
		// 处理旧的shortNode，允许shortNode的key是空数组
		// 创建一个带有键剩余部分的短节点
		var child = &ShortNode{
			Key:   n.Key[prefixLength+1:],
			Val:   n.Val,
			flags: n.flags,
		}
		branch.Children[n.Key[prefixLength]] = child

		// 创建一个新的节点
		var child2 = &ShortNode{
			Key:   key[prefixLength+1:],
			Val:   value,
			flags: t.newNodeFlag(),
		}
		child2.updateFlag(bitPos)
		branch.Children[key[prefixLength]] = child2
		branch.updateFlag(bitPos)

		// 然后在分支节点中插入新的shortNode
		if prefixLength != 0 {
			parentNode := &ShortNode{
				Key:   n.Key[:prefixLength],
				Val:   branch,
				flags: t.newNodeFlag(),
			}
			parentNode.updateFlag(bitPos)
			return parentNode, false, nil
		} else {
			return branch, false, nil
		}
	case *FullNode:
		// 更新特定分支
		var err error
		var exist bool
		n.Children[key[0]], exist, err = t.insert(n.Children[key[0]], key[1:], value, bitPos)
		if err != nil {
			return nil, exist, err
		}
		n.updateFlag(bitPos)

		return n, exist, nil
	case nil:
		// 如果当前节点为空，创建一个新的短节点
		shortNode := &ShortNode{
			Key:   key,
			Val:   value,
			flags: t.newNodeFlag(),
		}
		shortNode.updateFlag(bitPos)
		return shortNode, false, nil
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// hashKey 对输入的key进行哈希处理，确保返回固定长度的键
func hashKey(key []byte) []byte {
	// 使用crypto包中的Keccak256哈希函数
	return crypto.Keccak256(key)
}

var SStart time.Duration

// startCleanup 启动某区块号的清理操作
// 此接口支持异步调用，但会进行锁，保证不同区块号的清理操作会排队执行
// 返回是否成功获取到锁
func (t *CacheTrie) startCleanup() bool {
	sStart := time.Now()
	// 尝试获取锁
	t.cleanupMu.Lock()
	SStart = time.Since(sStart)

	t.isCleaningUp = true
	t.cleanupCount++ // 增加清理次数计数
	// 新增：记录cleanup开始时间
	t.cleanupStartTime = time.Now()

	return true
}

// FinishCleanup 完成某区块的清理操作
// 传入清理结果的哈希值，并清理缓存的deleteKVList
func (t *CacheTrie) FinishCleanup(blockNum uint64, resultHash common.Hash) {

	// 检查是否是当前正在清理的区块
	if !t.isCleaningUp {
		return
	}
	defer t.cleanupMu.Unlock()

	//fmt.Println(fmt.Sprintf("finsh clean : %v at %v", resultHash, blockNum))
	// 缓存清理结果
	t.cleanupResults = resultHash
	// 重置清理状态
	t.isCleaningUp = false
	// 新增：统计cleanup耗时
	if !t.cleanupStartTime.IsZero() {
		dur := time.Since(t.cleanupStartTime)
		t.cleanupDuration += dur
		if dur > t.cleanupMaxDuration {
			t.cleanupMaxDuration = dur
		}
		t.cleanupStartTime = time.Time{} // 重置
	}
}

// GetCleanupResult 获取某区块的清理结果
// 如果未进行清理或已完成清理，直接返回结果
// 如果正在清理中，则等待清理完成
func (t *CacheTrie) GetCleanupResult() common.Hash {
	// 使用读锁检查是否已有结果
	if t.cleanupResults != (common.Hash{}) {
		//用后即抛
		result := t.cleanupResults
		t.cleanupResults = common.Hash{}
		return result
	}

	// 既不在清理中，也没有结果，返回空哈希
	return common.Hash{}
}

func (t *CacheTrie) AddCode(codeHash common.Hash, code []byte) {
	t.codes[codeHash] = code
}

func (t *CacheTrie) PopCodes() map[common.Hash][]byte {
	tc := t.codes
	t.codes = make(map[common.Hash][]byte)
	return tc
}

// -----------------------------------------------------------------------------
// 命中率统计方法 - 公共API

// GetHitRate 返回当前的命中率统计信息
// 返回:
//   - 总Get请求数
//   - Get命中次数
//   - Get未命中次数
//   - Get命中率 (命中次数占总Get请求的百分比，范围0.0-1.0)
//   - 总Update请求数
//   - Update命中次数（更新已存在的键）
//   - Update未命中次数（插入新键）
//   - Update命中率 (已存在键的更新次数占总Update请求的百分比，范围0.0-1.0)
func (t *CacheTrie) GetHitRate() (uint64, uint64, uint64, float64, uint64, uint64, uint64, float64) {
	t.statsMu.Lock()
	defer t.statsMu.Unlock()

	totalGetRequests := t.hitCount + t.missCount
	var getHitRate float64 = 0
	if totalGetRequests > 0 {
		getHitRate = float64(t.hitCount) / float64(totalGetRequests)
	}

	var updateHitRate float64 = 0
	totalUpdateRequests := t.updateHitCount + t.updateMissCount
	if totalUpdateRequests > 0 {
		updateHitRate = float64(t.updateHitCount) / float64(totalUpdateRequests)
	}

	return totalGetRequests, t.hitCount, t.missCount, getHitRate,
		totalUpdateRequests, t.updateHitCount, t.updateMissCount, updateHitRate
}

// ResetStats 重置所有命中/未命中统计数据以及清理统计数据
func (t *CacheTrie) ResetStats() {
	t.statsMu.Lock()
	defer t.statsMu.Unlock()

	t.hitCount = 0
	t.missCount = 0
	t.updateHitCount = 0
	t.updateMissCount = 0

	// 同时重置清理统计
	t.cleanupCount = 0
	t.totalCleanupTime = 0
	t.maxCleanupTime = 0
	// 新增：重置自定义统计
	t.cleanupDuration = 0
	t.cleanupMaxDuration = 0
	t.pruneNodeAtBitDuration = 0
	t.pruneNodeAtBitMaxDuration = 0
}

// -----------------------------------------------------------------------------
// 内存大小计算方法

// 计算不同节点类型的内存占用基础值（字节）
const (
	fullNodeBaseSize  = 24 // FullNode结构体基本大小（不含子节点指针）
	shortNodeBaseSize = 40 // ShortNode结构体基本大小（不含Key、Val）
	valueNodeBaseSize = 32 // ValueNode结构体基本大小（不含Data、RawKey）
	nodeFlagSize      = 40 // nodeFlag结构体大小（包含memorySize字段）
	pointerSize       = 8  // 指针大小
)

// GetMemorySize 计算当前CacheTrie占用的内存大小（字节）
func (t *CacheTrie) GetMemorySize() int64 {
	if t.root == nil {
		return 0
	}

	// 直接使用根节点的memorySize()方法
	rootMemory := t.root.memorySize()

	// 添加额外的内存占用 (非节点树部分)
	size := rootMemory

	// 计算代码缓存的大小
	if t.codes != nil {
		// map结构本身的大小
		size += int64(48)
		// 所有代码条目的大小
		for _, code := range t.codes {
			size += int64(common.HashLength + len(code) + pointerSize)
		}
	}

	// 拥塞控制窗口大小
	if t.hrw != nil {
		size += int64(200) // HeightRangeWindow的大致大小
	}

	return size
}

// calculateNodeSize 递归计算节点及其子节点的内存占用大小
func (t *CacheTrie) calculateNodeSize(n cacheNode) int64 {
	if n == nil {
		return 0
	}

	var size int64

	switch node := n.(type) {
	case *FullNode:
		// 基本结构大小
		size = fullNodeBaseSize + nodeFlagSize

		// 子节点大小
		for _, child := range node.Children {
			if child != nil {
				size += pointerSize // 子节点指针
				size += t.calculateNodeSize(child)
			}
		}

		// hash缓存大小
		if hash, _ := node.cache(); hash != nil {
			size += int64(len(hash))
		}

	case *ShortNode:
		// 基本结构大小
		size = shortNodeBaseSize + nodeFlagSize

		// Key的大小
		size += int64(len(node.Key))

		// Val的大小
		size += pointerSize // Val指针
		size += t.calculateNodeSize(node.Val)

		// hash缓存大小
		if hash, _ := node.cache(); hash != nil {
			size += int64(len(hash))
		}

	case ValueNode:
		// 基本结构大小
		size = valueNodeBaseSize

		// Data的大小
		size += int64(len(node.Data))

		// RawKey的大小
		size += int64(len(node.RawKey))
	}

	return size
}

// 添加 GetCleanupCount 方法获取清理次数
func (t *CacheTrie) GetCleanupCount() int {
	return t.cleanupCount
}

// 获取清理时间统计
func (t *CacheTrie) GetCleanupTimes() (time.Duration, time.Duration) {
	return t.totalCleanupTime, t.maxCleanupTime
}

// 添加 ResetCleanupCount 方法重置清理次数
func (t *CacheTrie) ResetCleanupCount() {
	t.cleanupCount = 0
}

// 重置清理时间统计
func (t *CacheTrie) ResetCleanupTimes() {
	t.totalCleanupTime = 0
	t.maxCleanupTime = 0
}

// 添加 GetHRW 方法获取 HeightRangeWindow
func (t *CacheTrie) GetHRW() *HeightRangeWindow {
	return t.hrw
}

func (h *HeightRangeWindow) GetAllSize() int {
	return int(h.allSize)
}

// 为 HeightRangeWindow 添加 GetThreshold 方法
func (h *HeightRangeWindow) GetThreshold() int {
	return int(h.currentSsthresh)
}

// 获取自定义统计
func (t *CacheTrie) GetCustomCleanupDuration() time.Duration {
	return t.cleanupDuration
}

func (t *CacheTrie) GetCustomCleanupMaxDuration() time.Duration {
	return t.cleanupMaxDuration
}

func (t *CacheTrie) GetPruneNodeAtBitDuration() time.Duration {
	return t.pruneNodeAtBitDuration
}

func (t *CacheTrie) GetPruneNodeAtBitMaxDuration() time.Duration {
	return t.pruneNodeAtBitMaxDuration
}
