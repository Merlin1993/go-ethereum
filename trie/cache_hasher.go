// Copyright 2016 The go-ethereum Authors
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
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// cacheHasher is a type used for the CacheTrie Hash operation.
// It's similar to hasher but works with cacheNode instead of node.
type cacheHasher struct {
	sha      crypto.KeccakState
	tmp      []byte
	encbuf   rlp.EncoderBuffer
	parallel bool // Whether to use parallel threads when hashing
}

// cacheHasherPool holds cacheHashers
var cacheHasherPool = sync.Pool{
	New: func() interface{} {
		return &cacheHasher{
			tmp:    make([]byte, 0, 550), // cap is as large as a full fullNode.
			sha:    crypto.NewKeccakState(),
			encbuf: rlp.NewEncoderBuffer(nil),
		}
	},
}

func newCacheHasher(parallel bool) *cacheHasher {
	h := cacheHasherPool.Get().(*cacheHasher)
	h.parallel = parallel
	return h
}

func returnCacheHasherToPool(h *cacheHasher) {
	cacheHasherPool.Put(h)
}

// hash collapses a node down into a hash node, also returning a copy of the
// original node initialized with the computed hash to replace the original one.
func (h *cacheHasher) hash(n cacheNode, force bool) (hashed cacheNode, cached cacheNode) {
	// Return the cached hash if it's available
	if hash, _ := n.cache(); hash != nil {
		return hash, n
	}
	// Trie not processed yet, walk the children
	switch n := n.(type) {
	case *cacheShortNode:
		collapsed, cached := h.hashShortNodeChildren(n)
		hashed := h.shortnodeToHash(collapsed, force)
		// We need to retain the possibly _not_ hashed node, in case it was too
		// small to be hashed
		if hn, ok := hashed.(cacheHashNode); ok {
			cached.flags.hash = hn
		} else {
			cached.flags.hash = nil
		}
		return hashed, cached
	case *cacheFullNode:
		collapsed, cached := h.hashFullNodeChildren(n)
		hashed = h.fullnodeToHash(collapsed, force)
		if hn, ok := hashed.(cacheHashNode); ok {
			cached.flags.hash = hn
		} else {
			cached.flags.hash = nil
		}
		return hashed, cached
	default:
		// Value and hash nodes don't have children, so they're left as were
		return n, n
	}
}

// hashShortNodeChildren collapses the short node. The returned collapsed node
// holds a live reference to the Key, and must not be modified.
func (h *cacheHasher) hashShortNodeChildren(n *cacheShortNode) (collapsed, cached *cacheShortNode) {
	// Hash the short node's child, caching the newly hashed subtree
	collapsed, cached = n.copy(), n.copy()
	// Previously, we did copy this one. We don't seem to need to actually
	// do that, since we don't overwrite/reuse keys
	// cached.Key = common.CopyBytes(n.Key)
	collapsed.Key = hexToCompact(n.Key)

	// 保留size字段
	cached.size = n.size
	collapsed.size = n.size

	// Unless the child is a valuenode or hashnode, hash it
	switch n.Val.(type) {
	case *cacheFullNode, *cacheShortNode:
		collapsed.Val, cached.Val = h.hash(n.Val, false)
	}
	return collapsed, cached
}

func (h *cacheHasher) hashFullNodeChildren(n *cacheFullNode) (collapsed *cacheFullNode, cached *cacheFullNode) {
	// Hash the full node's children, caching the newly hashed subtrees
	cached = n.copy()
	collapsed = n.copy()

	// 保留size字段
	cached.size = n.size
	collapsed.size = n.size

	if h.parallel {
		var wg sync.WaitGroup
		wg.Add(16)
		for i := 0; i < 16; i++ {
			go func(i int) {
				hasher := newCacheHasher(false)
				if child := n.Children[i]; child != nil {
					collapsed.Children[i], cached.Children[i] = hasher.hash(child, false)
				} else {
					collapsed.Children[i] = cacheNilValueNode
				}
				returnCacheHasherToPool(hasher)
				wg.Done()
			}(i)
		}
		wg.Wait()
	} else {
		for i := 0; i < 16; i++ {
			if child := n.Children[i]; child != nil {
				collapsed.Children[i], cached.Children[i] = h.hash(child, false)
			} else {
				collapsed.Children[i] = cacheNilValueNode
			}
		}
	}
	return collapsed, cached
}

// shortnodeToHash creates a hashNode from a shortNode. The supplied shortnode
// should have hex-type Key, which will be converted (without modification)
// into compact form for RLP encoding.
// If the rlp data is smaller than 32 bytes, `nil` is returned.
func (h *cacheHasher) shortnodeToHash(n *cacheShortNode, force bool) cacheNode {
	n.encode(h.encbuf)
	enc := h.encodedBytes()

	if len(enc) < 32 && !force {
		return n // Nodes smaller than 32 bytes are stored inside their parent
	}
	return h.hashData(enc)
}

// fullnodeToHash is used to create a hashNode from a fullNode,
// (which may contain nil values)
func (h *cacheHasher) fullnodeToHash(n *cacheFullNode, force bool) cacheNode {
	n.encode(h.encbuf)
	enc := h.encodedBytes()

	if len(enc) < 32 && !force {
		return n // Nodes smaller than 32 bytes are stored inside their parent
	}
	return h.hashData(enc)
}

// encodedBytes returns the result of the last encoding operation on h.encbuf.
// This also resets the encoder buffer.
//
// All node encoding must be done like this:
//
//	node.encode(h.encbuf)
//	enc := h.encodedBytes()
//
// This convention exists because node.encode can only be inlined/escape-analyzed when
// called on a concrete receiver type.
func (h *cacheHasher) encodedBytes() []byte {
	h.tmp = h.encbuf.AppendToBytes(h.tmp[:0])
	h.encbuf.Reset(nil)
	return h.tmp
}

// hashData hashes the provided data
func (h *cacheHasher) hashData(data []byte) cacheHashNode {
	n := make(cacheHashNode, 32)
	h.sha.Reset()
	h.sha.Write(data)
	h.sha.Read(n)
	return n
}

// hashDataTo hashes the provided data to the given destination buffer. The caller
// must ensure that the dst buffer is of appropriate size.
func (h *cacheHasher) hashDataTo(dst, data []byte) {
	h.sha.Reset()
	h.sha.Write(data)
	h.sha.Read(dst)
}

// hashCacheRoot is used to create the root hash for commit operation.
// It collects dirty nodes and puts them into the given nodeset.
func (h *cacheHasher) hashCacheRoot(n cacheNode, collectLeaf bool, set *trienode.NodeSet) (common.Hash, cacheNode) {
	// Resolve the root node by hashing it
	hashed, cached := h.hash(n, true)

	// Collect all dirty nodes for committing
	switch hashed := hashed.(type) {
	case cacheHashNode:
		// Collect all dirty nodes in the trie
		if set != nil {
			h.collectNodeSet(n, nil, collectLeaf, set)
		}

		// 将原始节点的size传给新节点
		if shortNode, ok := cached.(*cacheShortNode); ok && n != nil {
			if origNode, ok := n.(*cacheShortNode); ok {
				shortNode.size = origNode.size
			}
		} else if fullNode, ok := cached.(*cacheFullNode); ok && n != nil {
			if origNode, ok := n.(*cacheFullNode); ok {
				fullNode.size = origNode.size
			}
		}

		// Return the computed hash
		return common.BytesToHash(hashed), cached
	default:
		// If the root node is smaller than 32 bytes, it won't be hashed and
		// it's considered as a leaf node.
		return common.Hash{}, n
	}
}

// collectNodeSet traverses all dirty nodes and put them in the given nodeset.
func (h *cacheHasher) collectNodeSet(n cacheNode, path []byte, collectLeaf bool, set *trienode.NodeSet) {
	if _, dirty := n.cache(); !dirty {
		return // Skip if the node is clean
	}
	switch n := n.(type) {
	case *cacheShortNode:
		// Collect the short node itself.
		h.encbuf.Reset(nil)
		n.encode(h.encbuf)
		enc := h.encodedBytes()

		// Create a node with encoded blob and its hash
		hash := common.BytesToHash(h.hashData(enc))
		node := trienode.New(hash, enc)
		set.AddNode(path, node)

		// Collect the child of the short node.
		if collectLeaf {
			h.collectNodeSet(n.Val, append(path, n.Key...), collectLeaf, set)
		} else {
			switch n.Val.(type) {
			case *cacheShortNode, *cacheFullNode:
				h.collectNodeSet(n.Val, append(path, n.Key...), collectLeaf, set)
			}
		}
	case *cacheFullNode:
		// Collect the full node itself.
		h.encbuf.Reset(nil)
		n.encode(h.encbuf)
		enc := h.encodedBytes()

		// Create a node with encoded blob and its hash
		hash := common.BytesToHash(h.hashData(enc))
		node := trienode.New(hash, enc)
		set.AddNode(path, node)

		// Collect all the children of the full node.
		for i := 0; i < 16; i++ {
			child := n.Children[i]
			if child == nil {
				continue
			}
			// If leaf collection is enabled, collect all dirty leaf nodes.
			// Otherwise, only collect intermediate nodes.
			if collectLeaf {
				h.collectNodeSet(child, append(path, byte(i)), collectLeaf, set)
			} else {
				switch child.(type) {
				case *cacheShortNode, *cacheFullNode:
					h.collectNodeSet(child, append(path, byte(i)), collectLeaf, set)
				}
			}
		}
		// Value node of the full node is just like another child,
		// but with key 16 (which is not a valid nibble value, no conflict).
		if n.Children[16] != nil {
			h.collectNodeSet(n.Children[16], append(path, 16), collectLeaf, set)
		}
	}
}
