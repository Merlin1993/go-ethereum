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
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

// hasher用于计算trie节点的哈希值
type hasher struct {
	sha      crypto.KeccakState
	tmp      []byte
	encbuf   rlp.EncoderBuffer
	parallel bool // 是否使用并行线程进行哈希计算
}

// hasherPool保存hasher实例，减少内存分配
var hasherPool = sync.Pool{
	New: func() interface{} {
		return &hasher{
			tmp:    make([]byte, 0, 550), // 足够大以容纳完整节点的编码
			sha:    crypto.NewKeccakState(),
			encbuf: rlp.NewEncoderBuffer(nil),
		}
	},
}

// newHasher创建一个新的hasher实例
func newHasher(parallel bool) *hasher {
	h := hasherPool.Get().(*hasher)
	h.parallel = parallel
	return h
}

// returnHasherToPool将hasher归还到对象池中
func returnHasherToPool(h *hasher) {
	hasherPool.Put(h)
}

// hash将节点折叠为哈希节点，并返回带有计算哈希的原始节点副本
func (h *hasher) hash(n cacheNode, force bool) (hashed cacheNode, cached cacheNode) {
	// 如果节点已经有缓存的哈希值，直接返回
	if hash, _ := n.cache(); hash != nil {
		return hash, n
	}
	// 尚未处理的Trie，递归遍历子节点
	switch n := n.(type) {
	case *ShortNode:
		collapsed, cached := h.hashShortNodeChildren(n)
		hashed := h.shortnodeToHash(collapsed, force)
		if hn, ok := hashed.(HashNode); ok {
			cached.flags.hash = hn
		} else {
			cached.flags.hash = nil
		}
		return hashed, cached
	case *FullNode:
		collapsed, cached := h.hashFullNodeChildren(n)
		hashed = h.fullnodeToHash(collapsed, force)
		if hn, ok := hashed.(HashNode); ok {
			cached.flags.hash = hn
		} else {
			cached.flags.hash = nil
		}
		return hashed, cached
	default:
		// 值节点和哈希节点没有子节点，保持原样
		return n, n
	}
}

// hashShortNodeChildren折叠短节点，返回的折叠节点持有Key的引用
func (h *hasher) hashShortNodeChildren(n *ShortNode) (collapsed, cached *ShortNode) {
	collapsed, cached = n.copy(), n.copy()
	collapsed.Key = hexToCompact(n.Key)

	// 保留size字段
	cached.size = n.size
	collapsed.size = n.size

	// 保留window字段
	cached.window = n.window
	collapsed.window = n.window

	// 除非子节点是值节点或哈希节点，否则对其进行哈希处理
	switch n.Val.(type) {
	case *FullNode, *ShortNode:
		collapsed.Val, cached.Val = h.hash(n.Val, false)
	}
	return collapsed, cached
}

// hashFullNodeChildren折叠全节点的所有子节点
func (h *hasher) hashFullNodeChildren(n *FullNode) (collapsed, cached *FullNode) {
	cached = n.copy()
	collapsed = n.copy()

	// 保留size字段
	cached.size = n.size
	collapsed.size = n.size

	// 保留window字段
	cached.window = n.window
	collapsed.window = n.window

	// 如果开启并行处理，则使用goroutine处理子节点
	if h.parallel {
		var wg sync.WaitGroup
		wg.Add(16)
		for i := 0; i < 16; i++ {
			go func(i int) {
				hasher := newHasher(false)
				if child := n.Children[i]; child != nil {
					collapsed.Children[i], cached.Children[i] = hasher.hash(child, false)
				} else {
					collapsed.Children[i] = NilValueNode
				}
				returnHasherToPool(hasher)
				wg.Done()
			}(i)
		}
		wg.Wait()
	} else {
		for i := 0; i < 16; i++ {
			if child := n.Children[i]; child != nil {
				collapsed.Children[i], cached.Children[i] = h.hash(child, false)
			} else {
				collapsed.Children[i] = NilValueNode
			}
		}
	}
	return collapsed, cached
}

// shortnodeToHash从短节点创建哈希节点
func (h *hasher) shortnodeToHash(n *ShortNode, force bool) cacheNode {
	n.encode(h.encbuf)
	enc := h.encodedBytes()

	if len(enc) < 32 && !force {
		return n // 小于32字节的节点存储在父节点内
	}
	return h.hashData(enc)
}

// fullnodeToHash从全节点创建哈希节点
func (h *hasher) fullnodeToHash(n *FullNode, force bool) cacheNode {
	n.encode(h.encbuf)
	enc := h.encodedBytes()

	if len(enc) < 32 && !force {
		return n // 小于32字节的节点存储在父节点内
	}
	return h.hashData(enc)
}

// encodedBytes返回h.encbuf的最后一次编码操作的结果，同时重置编码缓冲区
func (h *hasher) encodedBytes() []byte {
	h.tmp = h.encbuf.AppendToBytes(h.tmp[:0])
	h.encbuf.Reset(nil)
	return h.tmp
}

// hashData对提供的数据进行哈希处理
func (h *hasher) hashData(data []byte) HashNode {
	n := make(HashNode, 32)
	h.sha.Reset()
	h.sha.Write(data)
	h.sha.Read(n)
	return n
}

// hashRoot计算根哈希。它会处理传入的节点并返回其哈希和更新后的节点。
func (h *hasher) hashRoot(n cacheNode, force bool) (common.Hash, cacheNode) {
	// 通过哈希处理解析根节点
	hashed, cached := h.hash(n, force)

	// 根据哈希节点类型返回相应的哈希值
	switch hashed := hashed.(type) {
	case HashNode:
		return common.BytesToHash(hashed), cached
	default:
		// 如果根节点小于32字节，则不会被哈希，被视为叶子节点
		return common.Hash{}, n
	}
}
