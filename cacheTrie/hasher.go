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

	"github.com/ethereum/go-ethereum/crypto"
)

// hasher用于计算trie节点的哈希值
type hasher struct {
	sha      crypto.KeccakState
	parallel bool // 是否使用并行线程进行哈希计算
}

// hasherPool保存hasher实例，减少内存分配
var hasherPool = sync.Pool{
	New: func() interface{} {
		return &hasher{
			sha: crypto.NewKeccakState(),
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

// hash计算节点的哈希值，保留原始节点的内存引用，并组合所有子节点的信息
func (h *hasher) hash(n cacheNode) []byte {
	// 如果节点为空，返回nil
	if n == nil || n.isNil() {
		return nil
	}

	// 如果节点已经有缓存的哈希值且不强制重新计算，直接返回
	if hash, dirty := n.cache(); hash != nil && !dirty {
		return hash
	}

	switch n := n.(type) {
	case *ShortNode:
		// 对子节点递归处理
		childHash := h.hash(n.Val)
		hash := h.hashData(childHash)
		n.updateCache(hash)
		return hash

	case *FullNode:
		// 处理子节点
		tmp := make([]byte, 512)
		if h.parallel {
			var wg sync.WaitGroup
			wg.Add(16)
			for i := 0; i < 16; i++ {
				if child := n.Children[i]; !child.isNil() {
					go func(i int) {
						defer wg.Done()
						hasher := hasherPool.Get().(*hasher)
						hash := hasher.hash(child)
						copy(tmp[i*32:], hash[:])
						returnHasherToPool(hasher)
					}(i)
				} else {
					wg.Done()
				}
			}
			wg.Wait()
		} else {
			// 串行处理子节点
			for i := 0; i < 16; i++ {
				if child := n.Children[i]; !child.isNil() {
					childHash := h.hash(child)
					copy(tmp[i*32:], childHash[:])
				}
			}
		}
		hash := h.hashData(tmp)
		n.updateCache(hash)
		return hash
	}
	return nil
}

// hashData对数据进行哈希处理并返回哈希值
func (h *hasher) hashData(data []byte) []byte {
	h.sha.Reset()
	h.sha.Write(data)
	hash := make([]byte, 32)
	h.sha.Read(hash)
	return hash
}
