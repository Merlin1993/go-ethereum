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
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

var nodeIndices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "[17]"}

// cacheNode接口定义了缓存节点的基本操作
type cacheNode interface {
	cache() ([]byte, bool)
	fstring(string) string
	window() int
	size() int
	memorySize() int64
	updateFlag(int)
	updateCache([]byte)
	isNil() bool
}

// 各种节点类型
type (
	// 全节点，包含17个子节点
	FullNode struct {
		Children [16]cacheNode // 16个子节点加一个值节点
		flags    nodeFlag
	}

	// 短节点，用于压缩路径
	ShortNode struct {
		Key   []byte
		Val   cacheNode
		flags nodeFlag
	}

	// 值节点，存储实际的值
	ValueNode struct {
		Data    []byte
		New     bool           // 标记该节点是否为新节点
		RawKey  []byte         // 原始未哈希的键
		Address common.Address // 关联的地址
	}
)

// 节点标志，用于存储节点的哈希和状态
type nodeFlag struct {
	hash       []byte // 缓存的哈希值，只在需要时计算
	window     int    // 窗口标记，用于标识节点在哪些区块被访问
	size       int    // 节点大小
	memorySize int64  // 节点及其子节点的内存占用大小(字节)
	dirty      bool   // 标记节点是否被修改过
}

// 返回节点的哈希值和dirty标志
func (n *FullNode) cache() ([]byte, bool)  { return n.flags.hash, n.flags.dirty }
func (n *ShortNode) cache() ([]byte, bool) { return n.flags.hash, n.flags.dirty }
func (n ValueNode) cache() ([]byte, bool)  { return n.Data, false }

// 窗口访问方法
func (n *FullNode) window() int  { return n.flags.window }
func (n *ShortNode) window() int { return n.flags.window }
func (n ValueNode) window() int  { return 0 } // 值节点没有窗口

// 大小访问方法
func (n *FullNode) size() int  { return n.flags.size }
func (n *ShortNode) size() int { return n.flags.size }

func (n ValueNode) size() int {
	if len(n.Data) > 0 { // 非空值节点，算作1
		return 1
	}
	return 0 // 空值节点（墓碑），不计入size
}

// 内存大小访问方法
func (n *FullNode) memorySize() int64  { return n.flags.memorySize }
func (n *ShortNode) memorySize() int64 { return n.flags.memorySize }
func (n ValueNode) memorySize() int64 {
	// 计算值节点内存大小
	// ValueNode结构体基本大小 + Data大小 + RawKey大小
	return valueNodeBaseSize + int64(len(n.Data)) + int64(len(n.RawKey))
}

// 节点的字符串表示
func (n *FullNode) String() string  { return n.fstring("") }
func (n *ShortNode) String() string { return n.fstring("") }
func (n ValueNode) String() string  { return n.fstring("") }

// 格式化输出节点信息，用于调试
func (n *FullNode) fstring(ind string) string {
	resp := fmt.Sprintf("[\n%s  ", ind)
	for i, node := range &n.Children {
		if node == nil || node.isNil() {
			resp += fmt.Sprintf("%s: <nil> ", nodeIndices[i])
		} else {
			resp += fmt.Sprintf("%s: %v", nodeIndices[i], node.fstring(ind+"  "))
		}
	}
	return resp + fmt.Sprintf("\n%s] ", ind)
}

func (n *ShortNode) fstring(ind string) string {
	return fmt.Sprintf("{%x: %v} ", n.Key, n.Val.fstring(ind+"  "))
}

func (n ValueNode) fstring(ind string) string {
	return fmt.Sprintf("%x (New: %v) ", []byte(n.Data), n.New)
}

// NilValueNode 用于表示空值节点
var NilValueNode = ValueNode{Data: nil, New: false, RawKey: nil, Address: common.Address{}}

func (n *FullNode) updateFlag(bitPos int) {
	n.flags.window = 0
	n.flags.size = 0
	n.flags.memorySize = fullNodeBaseSize + nodeFlagSize // 基本大小

	// 使用range遍历，避免并发修改导致的竞态条件
	for _, child := range n.Children {
		if child != nil && !child.isNil() {
			n.flags.window |= child.window()
			n.flags.size += child.size()
			n.flags.memorySize += pointerSize + child.memorySize() // 指针大小 + 子节点大小
		}
	}

	// 如果有哈希缓存，加上它的大小
	if n.flags.hash != nil {
		n.flags.memorySize += int64(len(n.flags.hash))
	}
}

func (n *ShortNode) updateFlag(bitPos int) {
	// 基本大小：ShortNode结构 + Key + nodeFlag
	n.flags.memorySize = shortNodeBaseSize + int64(len(n.Key)) + pointerSize

	if _, ok := n.Val.(ValueNode); ok {
		// 这是叶子节点，设置当前区块对应的位
		n.flags.window = 1 << bitPos
		n.flags.size = 1
		// 加上值节点的大小
		n.flags.memorySize += n.Val.memorySize()
	} else {
		n.flags.window = n.Val.window()
		n.flags.size = n.Val.size()
		// 加上子节点的大小
		n.flags.memorySize += n.Val.memorySize()
	}

	// 如果有哈希缓存，加上它的大小
	if n.flags.hash != nil {
		n.flags.memorySize += int64(len(n.flags.hash))
	}
}

func (n ValueNode) updateFlag(w int) {} // 值节点不设置窗口

func (n *FullNode) updateCache(hash []byte)  { n.flags.hash = hash; n.flags.dirty = false }
func (n *ShortNode) updateCache(hash []byte) { n.flags.hash = hash; n.flags.dirty = false }
func (n ValueNode) updateCache(hash []byte)  {} // 值节点不设置窗口

// 实现isNil方法
func (n *FullNode) isNil() bool {
	if n == nil {
		return true
	}
	return false
}

func (n *ShortNode) isNil() bool {
	return n == nil
}

func (n ValueNode) isNil() bool {
	return false
}
